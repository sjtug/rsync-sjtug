use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use eyre::Result;
use futures::FutureExt;
use sqlx::PgPool;
use tokio::time::interval;
use tracing::{info, info_span, warn, Instrument};

use rsync_core::utils::AbortJoinHandle;

use crate::cache::NSCache;
use crate::listener::RevisionsChangeListener;
use crate::pg::{latest_live_revision, Revision};

/// Per-thread state.
pub struct State {
    // -1 stands for not found.
    revision: Arc<ArcSwap<Option<Revision>>>,
    _update_guard: Arc<AbortJoinHandle<()>>,
}

impl State {
    pub fn new(
        revision: Arc<ArcSwap<Option<Revision>>>,
        update_guard: Arc<AbortJoinHandle<()>>,
    ) -> Self {
        Self {
            revision,
            _update_guard: update_guard,
        }
    }
    /// Get the active revision.
    pub fn revision(&self) -> Option<Revision> {
        **self.revision.load()
    }
}

pub trait Invalidatable {
    fn invalidate(&self);
}

impl Invalidatable for Arc<NSCache> {
    fn invalidate(&self) {
        (**self).invalidate();
    }
}

/// Listen for pg revisions change events and update the active revision.
///
/// This differs from using a `RevisionsChangeListener` that it periodically polls the latest
/// revision from pg in case the listener misses some events.
///
/// Also it handles cache invalidation and manages the active revision cell.
pub async fn listen_for_updates(
    namespace: &str,
    update_interval: u64,
    cache: impl Invalidatable + Send + Sync + 'static,
    listener: &RevisionsChangeListener,
    pool: &PgPool,
) -> Result<(AbortJoinHandle<()>, Arc<ArcSwap<Option<Revision>>>)> {
    let namespace = namespace.to_string();

    let revision = latest_live_revision(&namespace, pool).await?;
    let cell = Arc::new(ArcSwap::new(Arc::new(revision)));

    let update_fn = {
        let cell = cell.clone();
        let namespace = namespace.clone();
        Arc::new(move |rev: Result<Option<Revision>>| match rev {
            Ok(Some(rev)) => {
                let old_idx = cell.load().map(|r| r.revision);
                cell.store(Arc::new(Some(rev)));
                if old_idx != Some(rev.revision) {
                    info!(idx=?rev.revision, namespace, "update active revision");
                    cache.invalidate();
                }
            }
            Ok(None) => {
                warn!(namespace, "no active revision found");
                let old_idx = cell.load().map(|r| r.revision);
                cell.store(Arc::new(None));
                if old_idx.is_some() {
                    cache.invalidate();
                }
            }
            Err(e) => warn!(?e, "failed to get latest live revision"),
        })
    };

    listener
        .register(&namespace, {
            let namespace = namespace.clone();
            let pool = pool.clone();
            let update_fn = update_fn.clone();
            Box::new(move |ev| {
                {
                    let pool = pool.clone();
                    let update_fn = update_fn.clone();
                    let namespace = namespace.clone();
                    async move {
                        info!(?ev, "received event, update active revision");
                        (update_fn.clone())(latest_live_revision(&namespace, &pool).await);
                        Ok(())
                    }
                }
                .instrument(info_span!("triggered revision update", namespace))
                .boxed()
            })
        })
        .await?;

    let timed_update_handle = tokio::spawn(
        {
            let mut interval = interval(Duration::from_secs(update_interval));
            interval.reset(); // sleep for one period before first update
            let update_fn = update_fn.clone();
            let namespace = namespace.clone();
            let pool = pool.clone();
            async move {
                loop {
                    interval.tick().await;
                    info!("timed update triggered");
                    (update_fn.clone())(latest_live_revision(&namespace, &pool).await);
                }
            }
        }
        .instrument(info_span!("timed revision update", namespace)),
    );

    Ok((AbortJoinHandle::new(timed_update_handle), cell))
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, UNIX_EPOCH};

    use arc_swap::ArcSwap;
    use chrono::DateTime;
    use sqlx::PgPool;
    use tokio::time::sleep;

    use rsync_core::pg::{
        change_revision_status, create_revision_at, ensure_repository, RevisionStatus,
    };
    use rsync_core::tests::generate_random_namespace;

    use crate::listener::RevisionsChangeListener;
    use crate::pg::Revision;
    use crate::state::{listen_for_updates, Invalidatable};

    #[derive(Debug, Clone, Default)]
    struct MockInvalidatable {
        invalidated: Arc<AtomicBool>,
    }

    impl MockInvalidatable {
        fn reset(&self) {
            self.invalidated.store(false, Ordering::SeqCst);
        }
        fn invalidated(&self) -> bool {
            self.invalidated.load(Ordering::SeqCst)
        }
    }

    impl Invalidatable for MockInvalidatable {
        fn invalidate(&self) {
            self.invalidated.store(true, Ordering::SeqCst);
        }
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_get_none(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();

        let _handle = listener.spawn(&pool);

        let (_guard, latest_index) = listen_for_updates(
            &namespace,
            999,
            MockInvalidatable::default(),
            &listener,
            &pool,
        )
        .await
        .expect("listen");
        assert!(latest_index.load().is_none());
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_get_latest_index(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();

        let _handle = listener.spawn(&pool);

        ensure_repository(&namespace, &pool)
            .await
            .expect("create repo");
        let rev = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        change_revision_status(
            rev,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");

        let (_guard, latest_index) = listen_for_updates(
            &namespace,
            999,
            MockInvalidatable::default(),
            &listener,
            &pool,
        )
        .await
        .expect("listen");
        assert_eq!(
            **latest_index.load(),
            Some(Revision {
                revision: rev,
                generated_at: DateTime::from(UNIX_EPOCH),
            })
        );
    }

    async fn assert_when_changed(
        latest_index: Arc<ArcSwap<Option<Revision>>>,
        f: impl Fn(Option<Revision>) -> bool, // -> break?
        init: Option<Revision>,
    ) {
        let index_seq = iter::repeat_with(|| **latest_index.load()).take(20);
        let mut ticker = tokio::time::interval(Duration::from_millis(500));
        for maybe_rev in index_seq {
            ticker.tick().await;
            if maybe_rev != init && f(maybe_rev) {
                return;
            }
        }
        panic!("no update received");
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_ignore_create_stale(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();
        let cache = MockInvalidatable::default();

        let _handle = listener.spawn(&pool);

        let (_guard, latest_index) =
            listen_for_updates(&namespace, 999, cache.clone(), &listener, &pool)
                .await
                .expect("listener");

        ensure_repository(&namespace, &pool)
            .await
            .expect("create repo");
        let rev = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");

        sleep(Duration::from_secs(3)).await;
        assert!(
            latest_index.load().is_none(),
            "partial index must not be loaded"
        );
        assert!(!cache.invalidated(), "cache must not be invalidated");

        change_revision_status(
            rev,
            RevisionStatus::Stale,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        sleep(Duration::from_secs(3)).await;
        assert!(
            latest_index.load().is_none(),
            "stale index must not be loaded"
        );
        assert!(!cache.invalidated(), "cache must not be invalidated");
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_update_go_down(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();
        let cache = MockInvalidatable::default();

        let _handle = listener.spawn(&pool);

        ensure_repository(&namespace, &pool)
            .await
            .expect("create repo");
        let rev = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let rev2 = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let expected_rev = Revision {
            revision: rev,
            generated_at: DateTime::from(UNIX_EPOCH),
        };
        let expected_rev2 = Revision {
            revision: rev2,
            generated_at: DateTime::from(UNIX_EPOCH),
        };

        change_revision_status(
            rev,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        change_revision_status(
            rev2,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");

        let (_guard, latest_index) =
            listen_for_updates(&namespace, 999, cache.clone(), &listener, &pool)
                .await
                .expect("listener");
        assert_eq!(
            **latest_index.load(),
            Some(expected_rev2),
            "must load latest index"
        );

        change_revision_status(
            rev2,
            RevisionStatus::Stale,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        assert_when_changed(
            latest_index.clone(),
            |maybe_rev| {
                assert_eq!(maybe_rev, Some(expected_rev), "must load old index");
                assert!(cache.invalidated(), "cache must be invalidated");
                true
            },
            Some(expected_rev2),
        )
        .await;
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_ignore_old_up(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();
        let cache = MockInvalidatable::default();

        let _handle = listener.spawn(&pool);

        ensure_repository(&namespace, &pool)
            .await
            .expect("create repo");
        let rev = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let rev2 = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let expected_rev2 = Revision {
            revision: rev2,
            generated_at: DateTime::from(UNIX_EPOCH),
        };

        change_revision_status(
            rev2,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");

        let (_guard, latest_index) =
            listen_for_updates(&namespace, 999, cache.clone(), &listener, &pool)
                .await
                .expect("listener");
        assert_eq!(
            **latest_index.load(),
            Some(expected_rev2),
            "must load latest index"
        );

        change_revision_status(
            rev,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        sleep(Duration::from_secs(3)).await;
        assert_eq!(
            **latest_index.load(),
            Some(expected_rev2),
            "must not load old index"
        );
        assert!(!cache.invalidated(), "cache must not be invalidated");
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_update_go_up(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let namespace = generate_random_namespace();
        let cache = MockInvalidatable::default();

        let _handle = listener.spawn(&pool);

        ensure_repository(&namespace, &pool)
            .await
            .expect("create repo");
        let rev = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let rev2 = create_revision_at(
            &namespace,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let expected_rev = Revision {
            revision: rev,
            generated_at: DateTime::from(UNIX_EPOCH),
        };
        let expected_rev2 = Revision {
            revision: rev2,
            generated_at: DateTime::from(UNIX_EPOCH),
        };

        let (_guard, latest_index) =
            listen_for_updates(&namespace, 999, cache.clone(), &listener, &pool)
                .await
                .expect("listener");
        assert!(latest_index.load().is_none());

        change_revision_status(
            rev,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");

        assert_when_changed(
            latest_index.clone(),
            |revision| {
                assert_eq!(
                    revision,
                    Some(expected_rev),
                    "revision must be updated when go live"
                );
                assert!(cache.invalidated(), "cache must be invalidated");
                true
            },
            None,
        )
        .await;

        cache.reset();
        change_revision_status(
            rev2,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");

        assert_when_changed(
            latest_index.clone(),
            |revision| {
                assert_eq!(
                    revision,
                    Some(expected_rev2),
                    "revision must be updated when go live"
                );
                assert!(cache.invalidated(), "cache must be invalidated");
                true
            },
            Some(expected_rev),
        )
        .await;
    }

    #[sqlx::test(migrations = "../tests/migrations")]
    async fn must_differentiate_namespace(pool: PgPool) {
        let listener = RevisionsChangeListener::default();
        let ns_a = generate_random_namespace();
        let ns_b = generate_random_namespace();
        let cache_a = MockInvalidatable::default();
        let cache_b = MockInvalidatable::default();

        let _handle = listener.spawn(&pool);

        ensure_repository(&ns_a, &pool).await.expect("create repo");
        ensure_repository(&ns_b, &pool).await.expect("create repo");
        let rev_a = create_revision_at(
            &ns_a,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let rev_b = create_revision_at(
            &ns_b,
            RevisionStatus::Partial,
            DateTime::from(UNIX_EPOCH),
            &pool,
        )
        .await
        .expect("create rev");
        let expected_rev_a = Revision {
            revision: rev_a,
            generated_at: DateTime::from(UNIX_EPOCH),
        };
        let expected_rev_b = Revision {
            revision: rev_b,
            generated_at: DateTime::from(UNIX_EPOCH),
        };

        let (_guard_a, latest_index_a) =
            listen_for_updates(&ns_a, 999, cache_a.clone(), &listener, &pool)
                .await
                .expect("listen");
        assert!(latest_index_a.load().is_none());

        let (_guard_b, latest_index_b) =
            listen_for_updates(&ns_b, 999, cache_b.clone(), &listener, &pool)
                .await
                .expect("listen");
        assert!(latest_index_b.load().is_none());

        change_revision_status(
            rev_a,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        sleep(Duration::from_secs(3)).await;
        assert_eq!(
            **latest_index_a.load(),
            Some(Revision {
                revision: rev_a,
                generated_at: DateTime::from(UNIX_EPOCH),
            }),
            "a: must load latest index"
        );
        assert!(
            latest_index_b.load().is_none(),
            "b: must not load latest index"
        );
        assert!(cache_a.invalidated(), "a: cache must be invalidated");
        assert!(!cache_b.invalidated(), "b: cache must not be invalidated");
        cache_a.reset();

        change_revision_status(
            rev_b,
            RevisionStatus::Live,
            Some(DateTime::from(UNIX_EPOCH)),
            &pool,
        )
        .await
        .expect("change status");
        sleep(Duration::from_secs(3)).await;
        assert_eq!(
            **latest_index_a.load(),
            Some(expected_rev_a),
            "a: remain the same"
        );
        assert_eq!(
            **latest_index_b.load(),
            Some(expected_rev_b),
            "b: must load latest index"
        );
        assert!(!cache_a.invalidated(), "a: cache must not be invalidated");
        assert!(cache_b.invalidated(), "b: cache must be invalidated");
    }
}
