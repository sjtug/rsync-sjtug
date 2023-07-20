use eyre::Result;
use sqlx::{Acquire, Postgres};
use tracing::instrument;

use rsync_core::pg::RevisionStatus;

/// Calculate all th hashes in stale indices that are not in either alive or partial indices.
///
/// # Errors
/// Returns error if db query fails.
#[instrument(skip(db))]
pub async fn hashes_to_remove<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<[u8; 20]>> {
    Ok(sqlx::query_file!("../sqls/hashes_to_remove.sql", namespace)
        .map(|row| row.blake2b.try_into().expect("blake2b length"))
        .fetch_all(&mut *db.acquire().await?)
        .await?)
}

/// Set live indices to stale except the last `keep` ones. Return the indices that are set to stale.
///
/// # Errors
/// Returns error if db query fails.
#[instrument(skip(db))]
pub async fn keep_last_n<'a>(
    namespace: &str,
    keep: u32,
    status: RevisionStatus,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<i32>> {
    Ok(sqlx::query_file!(
        "../sqls/keep_last_n.sql",
        namespace,
        status as _,
        i64::from(keep)
    )
    .map(|row| row.revision)
    .fetch_all(&mut *db.acquire().await?)
    .await?)
}

/// Remove given revisions.
///
/// # Errors
/// Returns error if db query fails.
pub async fn remove_revisions<'a>(
    stale_revs: &[i32],
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    sqlx::query!(
        r#"
        DELETE FROM revisions
        WHERE revision = ANY($1::int[])
    "#,
        &stale_revs
    )
    .execute(&mut *db.acquire().await?)
    .await?;
    Ok(())
}
