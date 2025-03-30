use blake2::digest::consts::U8;
use blake2::Digest;
use eyre::bail;
use sqlx::PgConnection;

pub struct PgLock {
    key: i64,
    shared: bool,
}

pub struct PgLockGuard<'lock, C: AsMut<PgConnection>> {
    lock: &'lock PgLock,
    conn: Option<C>,
}

impl PgLock {
    /// Create a new lock with exclusive access.
    #[must_use]
    pub fn new_exclusive(s: &str) -> Self {
        Self {
            key: generate_key(s),
            shared: false,
        }
    }
    /// Create a new lock with shared access.
    #[must_use]
    pub fn new_shared(s: &str) -> Self {
        Self {
            key: generate_key(s),
            shared: true,
        }
    }
    /// Acquire the lock.
    ///
    /// # Errors
    /// Returns an error if there's a problem acquiring the lock.
    #[allow(clippy::missing_panics_doc)]
    pub async fn lock<C: AsMut<PgConnection>>(&self, mut conn: C) -> eyre::Result<PgLockGuard<C>> {
        if self.shared {
            sqlx::query!("SELECT pg_advisory_lock_shared($1)", self.key)
                .execute(conn.as_mut())
                .await?;
        } else {
            sqlx::query!("SELECT pg_advisory_lock($1)", self.key)
                .execute(conn.as_mut())
                .await?;
        }
        Ok(PgLockGuard {
            lock: self,
            conn: Some(conn),
        })
    }
}

impl<C: AsMut<PgConnection>> PgLockGuard<'_, C> {
    /// Unlock the lock.
    ///
    /// # Errors
    /// Returns an error if the lock is not acquired by the current session.
    #[allow(clippy::missing_panics_doc)]
    pub async fn unlock(self) -> eyre::Result<bool> {
        let mut conn = self.conn.expect("lock already unlocked");
        let result = if self.lock.shared {
            sqlx::query_scalar!(
                r#"SELECT pg_advisory_unlock_shared($1) as "_res!""#,
                self.lock.key
            )
            .fetch_one(conn.as_mut())
            .await?
        } else {
            sqlx::query_scalar!(r#"SELECT pg_advisory_unlock($1) as "_res!""#, self.lock.key)
                .fetch_one(conn.as_mut())
                .await?
        };
        if !result {
            bail!("lock is not acquired by current session")
        }
        Ok(result)
    }
}

fn generate_key(s: &str) -> i64 {
    let mut hasher = blake2::Blake2b::<U8>::default();
    hasher.update(s.as_bytes());
    let res = hasher.finalize();

    i64::from_le_bytes(res[..8].try_into().unwrap())
}
