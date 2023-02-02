use crate::opts::RedisOpts;
use eyre::bail;
use eyre::Result;
use redis::{Commands, Script};
use tracing::{error, warn};

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
