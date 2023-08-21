//! DEPRECATED: Redis-based implementation of the metadata server.
//! Now only used in migration.

use std::collections::VecDeque;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use bincode::config::Configuration;
use clean_path::Clean;
use eyre::Result;
use eyre::{bail, ensure};
use futures::{stream, Stream, TryStreamExt};
use redis::{
    aio, AsyncCommands, AsyncIter, Client, Commands, FromRedisValue, RedisError, RedisResult,
    RedisWrite, Script, ToRedisArgs, Value,
};
use scan_fmt::scan_fmt;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{error, instrument, warn};

use crate::metadata::{MetaExtra, Metadata};

const BINCODE_CONFIG: Configuration = bincode::config::standard();

#[derive(Debug, Clone)]
pub struct RedisOpts {
    pub namespace: String,
    pub force_break: bool,
    pub lock_ttl: u64,
}

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
pub async fn acquire_instance_lock(client: &Client, opts: &RedisOpts) -> Result<RedisLock> {
    let RedisOpts {
        namespace,
        force_break,
        lock_ttl,
    } = opts;
    let lock = loop {
        break match RedisLock::new(client, namespace.to_string(), *lock_ttl).await {
            Ok(lock) => lock,
            Err(e) => {
                if *force_break {
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
/// Returns an error if the source field is not of type Metadata, or fails to communicate with Redis.
pub async fn update_metadata(
    redis: &mut impl aio::ConnectionLike,
    index: &str,
    path: &[u8],
    metadata: Metadata,
) -> Result<Option<Metadata>> {
    // No need to rollback because if the transaction fails, nothing is written.
    let (old_meta, _): (Option<Metadata>, usize) = redis::pipe()
        .atomic()
        .hget(index, path)
        .hset(index, path, metadata)
        .query_async(redis)
        .await?;

    Ok(old_meta)
}

/// Commit a completed transfer and put the new index into effect.
///
/// # Errors
///
/// Returns an error if target index already exists, or fails to communicate with Redis.
pub async fn commit_transfer(
    redis: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    timestamp: u64,
) -> Result<()> {
    let old_index = format!("{namespace}:partial");
    let new_index = format!("{namespace}:index:{timestamp}");

    let succ: bool = redis.rename(old_index, new_index).await?;
    ensure!(succ, "rename failed");

    Ok(())
}

pub fn async_iter_to_stream<T: FromRedisValue + Send + Unpin>(
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

/// Iterate through all live index.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn live_index<'a>(
    redis: &'a mut (impl aio::ConnectionLike + Send),
    namespace: &'a str,
) -> Result<impl Stream<Item = Result<u64>> + 'a> {
    let keys = async_iter_to_stream(
        redis
            .scan_match::<_, Vec<u8>>(format!("{namespace}:index:*"))
            .await?,
    );
    Ok(keys
        .try_filter_map(|k| async move { Ok(String::from_utf8(k).ok()) })
        .try_filter_map(move |k| async move {
            Ok(scan_fmt!(&k, &format!("{namespace}:index:{{d}}"), u64).ok())
        }))
}

/// Iterate through all stale index.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn stale_index<'a>(
    redis: &'a mut (impl aio::ConnectionLike + Send),
    namespace: &'a str,
) -> Result<impl Stream<Item = Result<u64>> + 'a> {
    let keys = async_iter_to_stream(
        redis
            .scan_match::<_, Vec<u8>>(format!("{namespace}:stale:*"))
            .await?,
    );
    Ok(keys
        .try_filter_map(|k| async move { Ok(String::from_utf8(k).ok()) })
        .try_filter_map(move |k| async move {
            Ok(scan_fmt!(&k, &format!("{namespace}:stale:{{d}}"), u64).ok())
        }))
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
    let index = live_index(redis, namespace).await?;

    index
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

pub const MAX_SYMLINK_LOOKUP: usize = 40;

macro_rules! guard_lookup {
    ($lookup: expr) => {
        $lookup += 1;
        if $lookup > MAX_SYMLINK_LOOKUP {
            warn!("symlink lookup limit exceeded");
            return Ok(None);
        }
    };
}

#[derive(Debug, Clone)]
pub enum Target {
    /// A regular file.
    File([u8; 20]),
    /// A directory.
    Directory(Vec<u8>),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum ComponentOwned {
    Prefix,
    RootDir,
    CurDir,
    ParentDir,
    Normal(OsString),
}

fn into_component_owned(comp: Component) -> ComponentOwned {
    match comp {
        Component::Prefix(_) => ComponentOwned::Prefix,
        Component::RootDir => ComponentOwned::RootDir,
        Component::CurDir => ComponentOwned::CurDir,
        Component::ParentDir => ComponentOwned::ParentDir,
        Component::Normal(s) => ComponentOwned::Normal(s.to_owned()),
    }
}

/// Follow a symlink and return the hash of the target.
/// Only works on files.
/// Also works if the given metadata is already a regular file.
///
/// Must give meta_extra if it's a direct link (hget key = Some _) to take advantage of the fast path.
///
/// Returns None if it's a dead or circular symlink.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
#[instrument(skip(conn, maybe_meta_extra))]
pub async fn follow_symlink(
    conn: &mut (impl aio::ConnectionLike + Send),
    redis_index: &str,
    key: &Path,
    maybe_meta_extra: Option<MetaExtra>,
) -> Result<Option<Target>> {
    // Fast path: it's a direct link.
    if let Some(meta_extra) = maybe_meta_extra {
        if let Some(target) = follow_direct_link(conn, redis_index, key, meta_extra).await? {
            return Ok(Some(target));
        }
    }
    // Slow path: there are symlinks in some intermediate directories.
    let mut lookup = 0;
    let mut remaining: VecDeque<_> = key.components().map(into_component_owned).collect();
    let mut cwd = PathBuf::new();
    while let Some(component) = remaining.pop_front() {
        match component {
            ComponentOwned::Prefix => {
                warn!("prefix is not supported, refusing to follow symlink");
                return Ok(None);
            }
            ComponentOwned::RootDir => {
                warn!("absolute path is not supported, refusing to follow symlink");
                return Ok(None);
            }
            ComponentOwned::CurDir => {
                continue;
            }
            ComponentOwned::ParentDir => {
                if !cwd.pop() {
                    warn!("path escapes root, refusing to follow symlink");
                    return Ok(None);
                }
            }
            ComponentOwned::Normal(name) => {
                let Some(entry): Option<Metadata> = conn.hget(redis_index, cwd.join(&name).as_os_str().as_bytes()).await? else {
                    warn!(?cwd, ?name, "broken symlink");
                    return Ok(None);
                };
                match entry.extra {
                    MetaExtra::Symlink { target } => {
                        guard_lookup!(lookup);
                        let path = Path::new(OsStr::from_bytes(&target));
                        let new_comps = path.components().map(into_component_owned);
                        for new_comp in new_comps.rev() {
                            remaining.push_front(new_comp);
                        }
                    }
                    MetaExtra::Regular { blake2b_hash } => {
                        if remaining.is_empty() {
                            // We've reached the end of the symlink and it's a file.
                            return Ok(Some(Target::File(blake2b_hash)));
                        }
                        warn!(
                            ?cwd,
                            ?name,
                            "symlink points to a file, but there are still components left"
                        );
                        return Ok(None);
                    }
                    MetaExtra::Directory => {
                        cwd.push(&name);
                    }
                }
            }
        }
    }

    // We've reached the end of the symlink and it's a directory.
    Ok(Some(Target::Directory(cwd.into_os_string().into_vec())))
}

// This function only resolves the symlink if there's no directory symlink in the path.
#[allow(unreachable_code)]
#[instrument(skip(conn, meta_extra))]
async fn follow_direct_link(
    conn: &mut (impl aio::ConnectionLike + Send),
    redis_index: &str,
    key: &Path,
    mut meta_extra: MetaExtra,
) -> Result<Option<Target>> {
    if key.is_absolute() {
        warn!(filename=%key.display(), "absolute path is not supported, refusing to follow");
        return Ok(None);
    }
    if key.starts_with("..") {
        warn!(filename=%key.display(), "path starts with .., refusing to follow");
        return Ok(None);
    }
    let key = key.clean();
    let mut basename = key.file_name().unwrap_or_else(|| OsStr::new("")).to_owned();
    let mut pwd = key.parent().unwrap_or_else(|| Path::new("")).to_path_buf();
    let mut lookup = 0;
    Ok(loop {
        match meta_extra {
            MetaExtra::Symlink { ref target } => {
                let new_loc = pwd.join(Path::new(OsStr::from_bytes(target))).clean();

                guard_lookup!(lookup);

                let Some(new_meta) = conn
                    .hget(redis_index, new_loc.as_os_str().as_bytes())
                    .await? else {
                    break None;
                };

                let new_meta: Metadata = new_meta;
                meta_extra = new_meta.extra;
                basename = new_loc
                    .file_name()
                    .unwrap_or_else(|| OsStr::new(""))
                    .to_owned();
                pwd = new_loc
                    .parent()
                    .unwrap_or_else(|| Path::new(""))
                    .to_path_buf();
            }
            MetaExtra::Regular { blake2b_hash } => break Some(Target::File(blake2b_hash)),
            MetaExtra::Directory => {
                break Some(Target::Directory(
                    pwd.join(basename).as_os_str().as_bytes().to_vec(),
                ));
            }
        }
    })
}

impl FromRedisValue for Metadata {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Data(data) => {
                let (metadata, len) =
                    bincode::decode_from_slice(data, BINCODE_CONFIG).map_err(|e| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Invalid metadata",
                            e.to_string(),
                        ))
                    })?;
                if data.len() != len {
                    return Err(RedisError::from((
                        redis::ErrorKind::TypeError,
                        "Invalid metadata (length mismatch)",
                    )));
                }
                Ok(metadata)
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
            ))),
        }
    }
}

impl ToRedisArgs for Metadata {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let buf = bincode::encode_to_vec(self, BINCODE_CONFIG).expect("bincode encode failed");
        out.write_arg(&buf);
    }
}
