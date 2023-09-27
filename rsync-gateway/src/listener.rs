use std::collections::{BTreeMap, VecDeque};
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use eyre::{bail, Report, Result};
use futures::future::BoxFuture;
use futures::TryStreamExt;
use scopeguard::defer;
use serde::Deserialize;
use sqlx::postgres::PgListener;
use sqlx::PgPool;
use tokio::sync::RwLock;
use tracing::{debug, error};

use rsync_core::utils::AbortJoinHandle;

#[derive(Debug, Clone, Deserialize)]
pub struct RevisionsChangeEvent {
    pub revision: i32,
    pub repository: String,
}

pub type RevisionsChangeCallback =
    Box<dyn (Fn(RevisionsChangeEvent) -> BoxFuture<'static, Result<()>>) + Send + Sync>;

#[derive(Clone, Default)]
pub struct RevisionsChangeListener(Arc<RevisionsChangeListenerInner>);

impl RevisionsChangeListener {
    pub fn spawn(&self, pool: &PgPool) -> AbortJoinHandle<()> {
        let inner = self.0.clone();
        let pool = pool.clone();
        AbortJoinHandle::new(tokio::spawn(async move {
            // Panic if we crash 3 times in 10 seconds.
            let mut err_queue: VecDeque<Instant> = VecDeque::with_capacity(3);
            loop {
                if let Err(e) = inner.listen_task(&pool).await {
                    if let Some(t) = err_queue.pop_front() {
                        assert!(
                            t.elapsed().as_secs() >= 10,
                            "listener crashed 3 times in 10 seconds, exiting: {e}",
                        );
                    }
                    error!("listen task crashed: {}, restarting", e);
                    err_queue.push_back(Instant::now());
                }
            }
        }))
    }
}

impl Deref for RevisionsChangeListener {
    type Target = RevisionsChangeListenerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Default)]
pub struct RevisionsChangeListenerInner {
    callback_map: RwLock<BTreeMap<String, RevisionsChangeCallback>>,
    started: AtomicBool,
}

impl RevisionsChangeListenerInner {
    pub async fn register(
        &self,
        repository: &str,
        callback: RevisionsChangeCallback,
    ) -> Result<()> {
        self.callback_map
            .write()
            .await
            .insert(repository.to_string(), callback);
        Ok(())
    }
    pub async fn listen_task(&self, pool: &PgPool) -> Result<()> {
        if self
            .started
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |_x| Some(true))
            .unwrap()
        {
            bail!("listener already started")
        }
        defer! {
            // Restore to false if we exit due to an error, so we can restart.
            self.started.store(false, Ordering::SeqCst);
        }

        let mut conn = PgListener::connect_with(pool).await?;
        conn.listen("revisions_change").await?;
        let stream = conn.into_stream();
        #[allow(clippy::significant_drop_tightening)] // false positive
        stream
            .map_err(Report::from)
            .try_for_each_concurrent(None, |notification| async move {
                let payload = notification.payload();
                debug!(%payload, "received notification");
                let ev: RevisionsChangeEvent = serde_json::from_str(payload)?;

                let callback_map = self.callback_map.read().await;
                if let Some(callback) = callback_map.get(&ev.repository) {
                    callback(ev).await?;
                }
                Ok(())
            })
            .await?;
        Ok(())
    }
}
