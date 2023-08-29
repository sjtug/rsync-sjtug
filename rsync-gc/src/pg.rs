use eyre::Result;
use itertools::{Either, Itertools};
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

/// Set live indices to stale according to the policy stated below.
/// Return the indices that are set to stale.
///
/// # Policy
/// 1. Remove all partial indices before the last live index.
/// 2. Keep at most `keep_partial` partial indices after the last live index.
/// 3. Keep at most `keep_live` live indices.
///
/// # Errors
/// Returns error if db query fails.
#[instrument(skip(db))]
pub async fn mark_stale<'a>(
    namespace: &str,
    keep_live: usize,
    keep_partial: usize,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<i32>> {
    struct Revision {
        revision: i32,
        status: RevisionStatus,
    }

    let mut tx = db.begin().await?;

    // Lock the table to prevent concurrent read/write because there's a time gap between
    // we read revisions and we update them.
    sqlx::query!("LOCK TABLE revisions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await?;

    let revisions = sqlx::query_as!(
        Revision,
        r#"
        SELECT revision, status AS "status: _"
        FROM revisions
        WHERE repository in (SELECT id FROM repositories WHERE name = $1)
        ORDER BY revision DESC
        "#,
        namespace
    )
    .fetch_all(&mut *tx)
    .await?;

    let (live_revs, partial_revs): (Vec<_>, Vec<_>) = revisions
        .into_iter()
        .filter(|r| r.status == RevisionStatus::Live || r.status == RevisionStatus::Partial)
        .partition_map(|r| {
            if r.status == RevisionStatus::Live {
                Either::Left(r.revision)
            } else {
                Either::Right(r.revision)
            }
        });

    // Remove all partial indices before the last live index.
    // No partial index will be removed if there's no live index.
    let last_live_rev = live_revs.first().copied().unwrap_or_default();
    let mut to_stale_revs: Vec<_> = partial_revs
        .iter()
        .filter(|r| **r < last_live_rev)
        .copied()
        .collect();

    // Keep at most `keep_partial` partial indices after the last live index.
    to_stale_revs.extend(
        partial_revs
            .iter()
            .filter(|r| **r >= last_live_rev)
            .skip(keep_partial),
    );

    // Keep at most `keep_live` live indices.
    to_stale_revs.extend(live_revs.into_iter().skip(keep_live));

    // Set the indices to stale.
    sqlx::query!(
        r#"
        UPDATE revisions
        SET status = $1
        WHERE revision = ANY($2::int[])
        "#,
        RevisionStatus::Stale as _,
        &to_stale_revs
    )
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(to_stale_revs)
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
