use bstr::ByteSlice;
use chrono::{DateTime, Utc};
use eyre::Result;
use sqlx::postgres::types::PgInterval;
use sqlx::types::BigDecimal;
use sqlx::{Acquire, Postgres};
use tracing::instrument;

use rsync_core::pg::{FileType, RevisionStatus};

use crate::path_resolve::ListingEntry;

struct RawEntry {
    r#type: FileType,
    blake2b: Option<Vec<u8>>,
    target: Option<Vec<u8>>,
}

/// Entry information needed to resolve a symlink.
pub enum Entry {
    Regular { blake2b: [u8; 20] },
    Directory,
    Symlink { target: Vec<u8> },
}

impl From<RawEntry> for Entry {
    fn from(value: RawEntry) -> Self {
        match value.r#type {
            FileType::Directory => Self::Directory,
            FileType::Regular => Self::Regular {
                blake2b: value
                    .blake2b
                    .expect("regular blake2b")
                    .try_into()
                    .expect("blake2b length"),
            },
            FileType::Symlink => Self::Symlink {
                target: value.target.expect("symlink target"),
            },
        }
    }
}

/// Get the entry of a path at a specific revision.
/// Returns None if the path does not exist.
///
/// # Errors
/// Returns error if db query fails.
pub async fn entry_of_path<'a>(
    revision: i32,
    path: &[u8],
    conn: impl Acquire<'a, Database = Postgres>,
) -> Result<Option<Entry>> {
    Ok(
        sqlx::query_file_as!(RawEntry, "../sqls/realpath.sql", revision, path)
            .map(Entry::from)
            .fetch_optional(&mut *conn.acquire().await?)
            .await?,
    )
}

/// Revision.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Revision {
    pub revision: i32,
    pub generated_at: DateTime<Utc>,
}

/// Detailed information about a revision.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct RevisionStat {
    pub revision: i64,
    pub status: RevisionStatus,
    #[cfg_attr(
        test,
        proptest(strategy = "proptest_arbitrary_interop::arb::<DateTime<Utc>>()")
    )]
    pub created_at: DateTime<Utc>,
    #[cfg_attr(
        test,
        proptest(strategy = "proptest::option::of(test::arb_pg_interval())")
    )]
    pub elapsed: Option<PgInterval>,
    pub count: Option<i64>,
    #[cfg_attr(
        test,
        proptest(strategy = "proptest::option::of(test::arb_big_decimal())")
    )]
    pub sum: Option<BigDecimal>,
}

/// Get the latest live revision.
///
/// # Errors
/// Returns error if db query fails.
#[instrument(skip(db))]
pub async fn latest_live_revision<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Option<Revision>> {
    let mut conn = db.acquire().await?;
    Ok(
        sqlx::query_file!("../sqls/latest_live_revision.sql", namespace)
            .map(|row| Revision {
                revision: row.revision,
                generated_at: row.created_at,
            })
            .fetch_optional(&mut *conn)
            .await?,
    )
}

/// Get detailed information about revisions.
///
/// Some fields may be not available before materialized view is refreshed.
///
/// # Errors
/// Returns error if db query fails.
#[instrument(skip_all)]
pub async fn revision_stats<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<RevisionStat>> {
    Ok(
        sqlx::query_file_as!(RevisionStat, "../sqls/revision_stats.sql", namespace)
            .fetch_all(&mut *db.acquire().await?)
            .await?,
    )
}

struct RawResolveEntry {
    filename: Vec<u8>,
    len: i64,
    r#type: FileType,
    modify_time: DateTime<Utc>,
}

impl From<RawResolveEntry> for ListingEntry {
    fn from(
        RawResolveEntry {
            filename,
            len,
            r#type,
            modify_time,
        }: RawResolveEntry,
    ) -> Self {
        Self {
            filename: if let Some((_, name)) = filename.rsplit_once_str("/") {
                name.to_vec()
            } else {
                filename
            },
            len: if r#type == FileType::Regular {
                #[allow(clippy::cast_sign_loss)]
                Some(len as u64)
            } else {
                None
            },
            modify_time: Some(modify_time),
            is_dir: r#type == FileType::Directory,
        }
    }
}

/// List a directory
pub async fn list_directory<'a>(
    path: &[u8],
    revision: i32,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<ListingEntry>> {
    Ok(if path.is_empty() {
        sqlx::query_file_as!(RawResolveEntry, "../sqls/list_root.sql", revision)
            .map(Into::into)
            .fetch_all(&mut *db.acquire().await?)
            .await?
    } else {
        sqlx::query_file_as!(
            RawResolveEntry,
            "../sqls/list_directory.sql",
            path,
            revision
        )
        .map(Into::into)
        .fetch_all(&mut *db.acquire().await?)
        .await?
    })
}

#[cfg(test)]
mod test {
    use bigdecimal::num_bigint::BigInt;
    use proptest::prop_compose;
    use sqlx::postgres::types::PgInterval;
    use sqlx::types::BigDecimal;

    prop_compose! {
        pub fn arb_pg_interval()(months: i32, days: i32, microseconds: i64) -> PgInterval {
            PgInterval {
                months,
                days,
                microseconds,
            }
        }
    }

    prop_compose! {
        pub fn arb_big_decimal()(digits in proptest_arbitrary_interop::arb::<BigInt>(), scale in 0..8i64) -> BigDecimal {
            // scale must not be too large or the test will run forever
            BigDecimal::new(digits, scale)
        }
    }
}
