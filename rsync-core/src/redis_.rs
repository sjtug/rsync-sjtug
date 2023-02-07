use std::collections::HashSet;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::time::Duration;

use clean_path::Clean;
use eyre::Result;
use eyre::{bail, ensure};
use futures::{stream, Stream, TryStreamExt};
use redis::{aio, AsyncCommands, AsyncIter, Client, Commands, FromRedisValue, Script};
use scan_fmt::scan_fmt;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{error, instrument, warn};

use crate::metadata::{MetaExtra, Metadata};

/// An instance lock based on Redis.
pub struct RedisLock {
    // Need to use a sync connection because the lock is released in the Drop impl.
    conn: redis::Connection,
    namespace: String,
    token: u64,
    refresh_handle: JoinHandle<()>,
}

impl RedisLock {
    /// Acquire a lock.
    ///
    /// # Errors
    ///
    /// Returns an error if another process is running, or failed to communicate with Redis.
    pub async fn new(client: &Client, namespace: String, lock_ttl: u64) -> Result<Self> {
        let token = rand::random();

        let mut conn = client.get_async_connection().await?;
        if !redis::cmd("SET")
            .arg(format!("{namespace}:lock"))
            .arg(token)
            .arg("NX")
            .arg("EX")
            .arg(lock_ttl)
            .query_async::<_, bool>(&mut conn)
            .await?
        {
            bail!("Failed to acquire lock. Another process is running?");
        }

        let refresh_handle = tokio::spawn({
            let namespace = namespace.clone();
            let mut interval = time::interval(Duration::from_secs(lock_ttl) / 4);
            async move {
                loop {
                    interval.tick().await;
                    match Script::new(
                        r#"
                        if redis.call("GET", KEYS[1]) == ARGV[1] then
                            return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2])
                        else
                            return 0
                        end
                    "#,
                    )
                    .key(format!("{namespace}:lock"))
                    .arg(token)
                    .arg(lock_ttl)
                    .invoke_async::<_, bool>(&mut conn)
                    .await
                    {
                        Err(e) => {
                            error!(?e, "failed to renew lock");
                        }
                        Ok(false) => {
                            error!(
                                "failed to renew lock, lock token mismatch. \
                                another process acquired the lock?"
                            );
                            break;
                        }
                        Ok(true) => {}
                    }
                }
            }
        });

        Ok(Self {
            conn: client.get_connection()?,
            namespace,
            token,
            refresh_handle,
        })
    }
    /// Force break an existing lock.
    ///
    /// # Errors
    ///
    /// Returns an error if failed to communicate with Redis.
    pub fn force_break(mut conn: redis::Connection, namespace: &str) -> Result<()> {
        conn.del(format!("{namespace}:lock"))?;
        Ok(())
    }
}

impl Drop for RedisLock {
    fn drop(&mut self) {
        self.refresh_handle.abort();
        match Script::new(
            r#"
            if redis.call("GET", KEYS[1]) == ARGV[1] then
                return redis.call("DEL", KEYS[1])
            else
                return 0
            end
        "#,
        )
        .key(format!("{}:lock", self.namespace))
        .arg(self.token)
        .invoke::<bool>(&mut self.conn)
        {
            Err(e) => error!(?e, "failed to release lock"),
            Ok(false) => error!(
                "failed to release lock, lock token mismatch. another process acquired the lock?"
            ),
            Ok(true) => {}
        }
    }
}

/// Helper function to acquire a lock.
///
/// # Errors
///
/// Returns an error if another process is running, or failed to communicate with Redis.
pub async fn acquire_instance_lock(
    client: &Client,
    namespace: &str,
    lock_ttl: u64,
    force_break: bool,
) -> Result<RedisLock> {
    let lock = loop {
        break match RedisLock::new(client, namespace.to_string(), lock_ttl).await {
            Ok(lock) => lock,
            Err(e) => {
                if force_break {
                    warn!("force breaking lock");
                    RedisLock::force_break(client.get_connection()?, namespace)?;
                    continue;
                }
                bail!(e)
            }
        };
    };

    Ok(lock)
}

/// Update metadata of a file, and return the old metadata if any.
///
/// # Errors
///
/// Returns an error if nothing is written, or failed to communicate with Redis.
pub async fn update_metadata(
    redis: &mut impl aio::ConnectionLike,
    index: &str,
    path: &[u8],
    metadata: Metadata,
) -> Result<Option<Metadata>> {
    // TODO rollback
    let (old_meta, h_added): (Option<Metadata>, usize) = redis::pipe()
        .atomic()
        .hget(index, path)
        .hset(index, path, metadata)
        .query_async(redis)
        .await?;

    ensure!(h_added == 1, "hset failed");

    Ok(old_meta)
}

/// Commit a completed transfer and put the new index into effect.
///
/// # Errors
///
/// Returns an error if target index already exists, failed to communicate with Redis.
pub async fn commit_transfer(
    redis: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    timestamp: u64,
) -> Result<()> {
    let old_index = format!("{namespace}:partial");
    let new_index = format!("{namespace}:index:{timestamp}");

    // TODO rollback
    let succ: bool = redis.rename(old_index, new_index).await?;
    ensure!(succ, "rename failed");

    Ok(())
}

pub fn async_iter_to_stream<T: FromRedisValue + Unpin>(
    it: AsyncIter<T>,
) -> impl Stream<Item = Result<T>> + '_ {
    stream::unfold(it, |mut it| async move {
        it.next_item()
            .await
            .map_err(eyre::Error::from)
            .transpose()
            .map(|i| (i, it))
    })
}

/// Get the latest production index.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn get_latest_index(
    redis: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
) -> Result<Option<u64>> {
    let keys = async_iter_to_stream(
        redis
            .scan_match::<_, Vec<u8>>(format!("{namespace}:index:*"))
            .await?,
    );
    let filtered = keys
        .try_filter_map(|k| async move { Ok(String::from_utf8(k).ok()) })
        .try_filter_map(|k| async move {
            Ok(scan_fmt!(&k, &format!("{namespace}:index:{{d}}"), u64).ok())
        });

    filtered
        .try_fold(None, |acc, x| async move {
            acc.map_or(Ok(Some(x)), |acc: u64| Ok(Some(acc.max(x))))
        })
        .await
}

#[instrument(skip(redis, items))]
pub async fn copy_index(
    redis: &mut (impl aio::ConnectionLike + Send),
    from: &str,
    to: &str,
    items: &[Vec<u8>],
) -> Result<()> {
    for item in items {
        let entry: Vec<u8> = redis.hget(from, item).await?;
        ensure!(
            !entry.is_empty(),
            "key or field not found: {}",
            String::from_utf8_lossy(item)
        );

        // TODO rollback
        let set: usize = redis.hset(to, item, entry).await?;

        ensure!(set == 1, "set failed");
    }
    Ok(())
}

#[instrument(skip(redis, items))]
pub async fn move_index(
    redis: &mut (impl aio::ConnectionLike + Send),
    from: &str,
    to: &str,
    items: &[Vec<u8>],
) -> Result<()> {
    for item in items {
        let entry: Vec<u8> = redis.hget(from, item).await?;
        ensure!(
            !entry.is_empty(),
            "key or field not found: {}",
            String::from_utf8_lossy(item)
        );

        // TODO rollback
        let (h_deleted, h_added): (usize, usize) = redis::pipe()
            .atomic()
            .hdel(from, item)
            .hset(to, item, entry)
            .query_async(redis)
            .await?;

        ensure!(h_deleted == 1, "hdel failed");
        ensure!(h_added == 1, "hset failed");
    }
    Ok(())
}

/// Get a specific index.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn get_index(
    redis: &mut (impl aio::ConnectionLike + Send),
    key: &str,
) -> Result<Vec<Vec<u8>>> {
    let mut filenames: Vec<_> =
        async_iter_to_stream(redis.hscan::<_, (Vec<u8>, Vec<u8>)>(key).await?)
            .map_ok(|(k, _)| k)
            .try_collect()
            .await?;
    filenames.sort_unstable();
    Ok(filenames)
}

/// Follow a symlink and return the hash of the target.
/// Also works if the given metadata is already a regular file.
///
/// Returns None if it's a dead or circular symlink.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn follow_symlink(
    conn: &mut (impl aio::ConnectionLike + Send),
    redis_index: &str,
    key: &[u8],
    mut meta_extra: MetaExtra,
) -> Result<Option<[u8; 20]>> {
    let filename_display = Path::new(OsStr::from_bytes(key)).display();

    let mut pwd = Path::new(OsStr::from_bytes(key))
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .to_path_buf();

    let mut visited = HashSet::new();
    let hash = loop {
        break match meta_extra {
            MetaExtra::Symlink { ref target } => {
                let new_loc = pwd.join(Path::new(OsStr::from_bytes(target))).clean();
                if visited.insert(new_loc.clone()) {
                    if let Some(new_meta) = conn
                        .hget(redis_index, new_loc.as_os_str().as_bytes())
                        .await?
                    {
                        let new_meta: Metadata = new_meta;
                        meta_extra = new_meta.extra;
                        pwd = new_loc
                            .parent()
                            .unwrap_or_else(|| Path::new(""))
                            .to_path_buf();
                        continue;
                    }
                    warn!(filename=%filename_display, target=%new_loc.display(), "symlink target not found");
                } else {
                    warn!(filename=%filename_display, "symlink loop detected");
                }
                None
            }
            MetaExtra::Regular { blake2b_hash } => Some(blake2b_hash),
        };
    };

    Ok(hash)
}
