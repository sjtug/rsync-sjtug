use std::time::SystemTime;

use eyre::Result;
use eyre::{bail, ensure};
use redis::{aio, Commands, Script};
use tracing::{error, info, warn};

use crate::opts::RedisOpts;
use crate::plan::Metadata;

/// An instance lock based on Redis.
pub struct RedisLock {
    // Need to use a sync connection because the lock is released in the Drop impl.
    conn: redis::Connection,
    namespace: String,
    token: u64,
}

impl RedisLock {
    /// Acquire a lock.
    pub fn new(mut conn: redis::Connection, opts: &RedisOpts) -> Result<Self> {
        let namespace = opts.namespace.clone();
        let token = rand::random();

        if !redis::cmd("SET")
            .arg(format!("{namespace}:lock"))
            .arg(token)
            .arg("NX")
            .query::<bool>(&mut conn)?
        {
            bail!("Failed to acquire lock. Another process is running?");
        }

        Ok(Self {
            conn,
            namespace,
            token,
        })
    }
    /// Force break an existing lock.
    pub fn force_break(mut conn: redis::Connection, opts: &RedisOpts) -> Result<()> {
        conn.del(format!("{}:lock", opts.namespace))?;
        Ok(())
    }
}

impl Drop for RedisLock {
    fn drop(&mut self) {
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
pub fn acquire_instance_lock(client: &redis::Client, opts: &RedisOpts) -> Result<RedisLock> {
    let lock = loop {
        break match RedisLock::new(client.get_connection()?, opts) {
            Ok(lock) => lock,
            Err(e) => {
                if opts.force_break {
                    warn!("force breaking lock");
                    RedisLock::force_break(client.get_connection()?, opts)?;
                    continue;
                }
                error!(?e, "failed to acquire lock");
                bail!(e)
            }
        };
    };

    Ok(lock)
}

pub async fn update_metadata(
    redis: &mut impl aio::ConnectionLike,
    prefix: &str,
    path: &[u8],
    metadata: Metadata,
) -> Result<()> {
    let (zset_key, hash_key) = (format!("{prefix}:zset"), format!("{prefix}:hash"));

    let (z_added, h_added): (usize, usize) = redis::pipe()
        .atomic()
        .zadd(zset_key, path, 0)
        .hset(hash_key, path, metadata)
        .query_async(redis)
        .await?;

    // TODO rollback
    ensure!(z_added == 1, "zadd failed");
    ensure!(h_added == 1, "hset failed");

    Ok(())
}

/// Commit a completed transfer and put the new index into effect.
pub async fn commit_transfer(redis: &mut impl aio::ConnectionLike, namespace: &str) -> Result<()> {
    info!("commit transfer");

    let (old_zset_key, old_hash_key) = (
        format!("{namespace}:partial:zset"),
        format!("{namespace}:partial:hash"),
    );

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system time is before UNIX epoch")
        .as_secs();
    let (new_zset_key, new_hash_key) = (
        format!("{namespace}:index:{timestamp}:zset"),
        format!("{namespace}:index:{timestamp}:hash"),
    );

    let (z_succ, h_succ): (bool, bool) = redis::pipe()
        .atomic()
        .rename(old_zset_key, new_zset_key)
        .rename(old_hash_key, new_hash_key)
        .query_async(redis)
        .await?;

    // TODO rollback
    ensure!(z_succ, "zset rename failed");
    ensure!(h_succ, "hash rename failed");

    Ok(())
}
