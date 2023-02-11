use std::ffi::OsStr;
use std::future::ready;
use std::num::NonZeroU64;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use eyre::{bail, Result};
use futures::{pin_mut, StreamExt, TryStreamExt};
use redis::{aio, AsyncCommands, Client};
use scan_fmt::scan_fmt;
use tap::TapOptional;
use tokio::time::interval;
use tracing::{info, warn};

use rsync_core::metadata::Metadata;
use rsync_core::redis_::{follow_symlink, get_latest_index, recursive_resolve_dir_symlink};

use crate::utils::AbortJoinHandle;

/// Per-thread state.
pub struct State {
    // Each repository has its own connection.
    conn: aio::MultiplexedConnection,
    namespace: String,
    // Zero stands for not found.
    latest_index: Arc<AtomicU64>,
}

impl State {
    pub fn new(
        conn: aio::MultiplexedConnection,
        namespace: String,
        latest_index: Arc<AtomicU64>,
    ) -> Self {
        Self {
            conn,
            namespace,
            latest_index,
        }
    }
    /// Get the latest index.
    pub fn latest_index(&self) -> Option<NonZeroU64> {
        NonZeroU64::new(self.latest_index.load(Ordering::Relaxed))
    }
    /// Lookup the hash of a path.
    pub async fn lookup_hash_of_path(&self, key: &[u8]) -> Result<Option<[u8; 20]>> {
        let mut conn = self.conn.clone();

        let namespace = &self.namespace;
        let index = if let Some(index) = self.latest_index() {
            format!("{namespace}:index:{index}")
        } else {
            return Ok(None);
        };
        let meta: Option<Metadata> = conn.hget(&index, key).await?;

        // We follow the symlink instead of redirecting the client to avoid circular redirection.
        let key = Path::new(OsStr::from_bytes(key));
        follow_symlink(&mut conn, &index, key, meta.map(|meta| meta.extra)).await
    }
    pub async fn resolve_dir(&self, key: &[u8]) -> Result<Vec<u8>> {
        let mut conn = self.conn.clone();
        let namespace = &self.namespace;
        let index = if let Some(index) = self.latest_index() {
            format!("{namespace}:index:{index}")
        } else {
            return Ok(key.to_vec());
        };

        let key = Path::new(OsStr::from_bytes(key));
        recursive_resolve_dir_symlink(&mut conn, &index, key)
            .await
            .map(|p| p.into_os_string().into_vec())
    }
}

/// Listen for redis key rename events and update the latest index.
pub async fn listen_for_updates(
    client: &Client,
    namespace: &str,
    update_interval: u64,
) -> Result<(AbortJoinHandle<()>, Arc<AtomicU64>)> {
    let mut conn = client.get_async_connection().await?;
    let res: String = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("Eg")
        .query_async(&mut conn)
        .await?;
    if res != "OK" {
        bail!("Failed to set notify-keyspace-events: {}", res);
    }

    let namespace = namespace.to_string();

    let latest_idx = get_latest_index(&mut conn, &namespace)
        .await?
        .tap_none(|| warn!("no index found."));
    let cell = Arc::new(AtomicU64::new(latest_idx.unwrap_or_default()));

    let mut psub = conn.into_pubsub();
    psub.subscribe("__keyevent@0__:rename_to").await?;

    let handle = tokio::spawn({
        let mut conn = client.get_async_connection().await?;
        let cell = cell.clone();
        let mut interval = interval(Duration::from_secs(update_interval));

        async move {
            let filtered = psub
                .into_on_message()
                .try_filter(|msg| ready(msg.get_channel_name() == "__keyevent@0__:rename_to"))
                .try_filter_map(|msg| async move {
                    Ok(String::from_utf8(msg.get_payload_bytes().into()).ok())
                })
                .try_filter_map({
                    let namespace = namespace.clone();
                    move |k| {
                        ready(Ok(
                            scan_fmt!(&k, &format!("{namespace}:index:{{d}}"), u64).ok()
                        ))
                    }
                });
            pin_mut!(filtered);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        match get_latest_index(&mut conn, &namespace).await {
                            Ok(Some(idx)) => {
                                info!(?idx, "update latest index");
                                cell.store(idx, std::sync::atomic::Ordering::Relaxed);
                            },
                            Err(e) => warn!(?e, "failed to get latest index"),
                            _ => {}
                        }
                    },
                    Some(ev) = filtered.next() => {
                        match ev {
                            Ok(idx) => {
                                info!(?idx, "update latest index");
                                cell.store(idx, Ordering::Relaxed);
                            },
                            Err(e) => warn!(?e, "failed to parse update event")
                        }
                    },
                }
            }
        }
    });

    Ok((AbortJoinHandle::new(handle), cell))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, UNIX_EPOCH};

    use tokio::time::sleep;

    use rsync_core::metadata::Metadata;
    use rsync_core::tests::{generate_random_namespace, redis_client, MetadataIndex};

    use crate::state::listen_for_updates;

    #[tokio::test]
    async fn must_get_none() {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
        assert_eq!(latest_index.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn must_get_latest_index() {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let _idx_guard = MetadataIndex::new(
            &client,
            &format!("{namespace}:index:42"),
            &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
        );
        let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
        assert_eq!(latest_index.load(Ordering::SeqCst), 42);
    }

    #[tokio::test]
    async fn must_update_latest_index() {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
        sleep(Duration::from_millis(500)).await;
        assert_eq!(latest_index.load(Ordering::SeqCst), 0);

        let _idx_guard = MetadataIndex::new(
            &client,
            &format!("{namespace}:index:42"),
            &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
        );
        sleep(Duration::from_millis(500)).await;
        assert_eq!(dbg!(latest_index.load(Ordering::SeqCst)), 42);
    }

    #[tokio::test]
    async fn must_differentiate_namespace() {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let (_guard, latest_index) = listen_for_updates(&client, &namespace, 999).await.unwrap();
        assert_eq!(latest_index.load(Ordering::SeqCst), 0);

        let _idx_guard = MetadataIndex::new(
            &client,
            &format!("{namespace}_a:index:42"),
            &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
        );
        sleep(Duration::from_millis(500)).await;
        assert_eq!(latest_index.load(Ordering::SeqCst), 0);

        let _idx_guard_2 = MetadataIndex::new(
            &client,
            &format!("{namespace}:index:42"),
            &[("a".into(), Metadata::regular(0, UNIX_EPOCH, [0; 20]))],
        );
        sleep(Duration::from_millis(1500)).await;
        assert_eq!(latest_index.load(Ordering::SeqCst), 42);
    }
}
