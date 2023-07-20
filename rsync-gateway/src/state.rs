use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use eyre::Result;
use futures::FutureExt;
use sqlx::PgPool;
use tokio::time::interval;
use tracing::{info, info_span, warn, Instrument};

use crate::cache::NSCache;
use crate::listener::RevisionsChangeListener;
use crate::pg::{latest_live_revision, Revision};
use rsync_core::utils::AbortJoinHandle;

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

/// Listen for pg revisions change events and update the active revision.
pub async fn listen_for_updates<'a>(
    namespace: &'a str,
    update_interval: u64,
    cache: Arc<NSCache>,
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
                cell.store(Arc::new(None));
                cache.invalidate();
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

// TODO test
// #[cfg(test)]
// mod tests {
//     use std::sync::atomic::Ordering;
//     use std::time::{Duration, UNIX_EPOCH};
//
//     use tokio::time::sleep;
//
//     use rsync_core::metadata::Metadata;
//     use rsync_core::tests::{generate_random_namespace, redis_client, MetadataIndex};
//
//     use crate::state::listen_for_updates;
//
//     #[tokio::test]
//     async fn must_get_none() {
//         let client = redis_client();
//         let namespace = generate_random_namespace();
//
//         let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
//         assert_eq!(latest_index.load(Ordering::SeqCst), 0);
//     }
//
//     #[tokio::test]
//     async fn must_get_latest_index() {
//         let client = redis_client();
//         let namespace = generate_random_namespace();
//
//         let _idx_guard = MetadataIndex::new(
//             &client,
//             &format!("{namespace}:index:42"),
//             &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
//         );
//         let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
//         assert_eq!(latest_index.load(Ordering::SeqCst), 42);
//     }
//
//     #[tokio::test]
//     async fn must_update_latest_index() {
//         let client = redis_client();
//         let namespace = generate_random_namespace();
//
//         let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
//         sleep(Duration::from_millis(500)).await;
//         assert_eq!(latest_index.load(Ordering::SeqCst), 0);
//
//         let _idx_guard = MetadataIndex::new(
//             &client,
//             &format!("{namespace}:index:42"),
//             &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
//         );
//         sleep(Duration::from_millis(500)).await;
//         assert_eq!(dbg!(latest_index.load(Ordering::SeqCst)), 42);
//     }
//
//     #[tokio::test]
//     async fn must_differentiate_namespace() {
//         let client = redis_client();
//         let ns_a = generate_random_namespace();
//         let ns_b = generate_random_namespace();
//
//         let (_guard_a, latest_index_a) = listen_for_updates(&client, &ns_a, 999).await.unwrap();
//         assert_eq!(latest_index_a.load(Ordering::SeqCst), 0);
//
//         let (_guard_b, latest_index_b) = listen_for_updates(&client, &ns_b, 999).await.unwrap();
//         assert_eq!(latest_index_b.load(Ordering::SeqCst), 0);
//
//         let _idx_guard = MetadataIndex::new(
//             &client,
//             &format!("{ns_a}:index:42"),
//             &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
//         );
//         sleep(Duration::from_millis(500)).await;
//         assert_eq!(latest_index_a.load(Ordering::SeqCst), 42);
//         assert_eq!(latest_index_b.load(Ordering::SeqCst), 0);
//
//         let _idx_guard_2 = MetadataIndex::new(
//             &client,
//             &format!("{ns_b}:index:43"),
//             &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
//         );
//         sleep(Duration::from_millis(500)).await;
//         assert_eq!(latest_index_a.load(Ordering::SeqCst), 42);
//         assert_eq!(latest_index_b.load(Ordering::SeqCst), 43);
//     }
// }
