//! Some common pg functions and types.
#![allow(clippy::missing_panics_doc)]

use std::time::Duration;

use eyre::Result;
use futures::FutureExt;
use itertools::multizip;
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::{Acquire, Postgres};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, instrument};

use crate::metadata::{MetaExtra, Metadata};

pub const INSERT_CHUNK_SIZE: usize = 10000;

const MAX_INSERT_PER_BATCH: usize = 1000;
const MAX_INSERT_INTERVAL: Duration = Duration::from_secs(10);

/// File type.
#[derive(Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(type_name = "filetype", rename_all = "lowercase")]
pub enum FileType {
    Regular,
    Directory,
    Symlink,
}

/// Revision status.
#[derive(sqlx::Type, Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "tests", derive(proptest_derive::Arbitrary))]
#[sqlx(type_name = "revision_status", rename_all = "lowercase")]
pub enum RevisionStatus {
    Partial,
    Live,
    Stale,
}

#[instrument(skip_all)]
pub async fn insert_task<'a>(
    rev: i32,
    mut rx: mpsc::Receiver<(Vec<u8>, Metadata)>,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    let mut timed_trigger = tokio::time::interval(MAX_INSERT_INTERVAL);
    timed_trigger.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let (mut keys, mut values, mut mtimes, mut types, mut hashes, mut targets) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);

    let mut conn = db.acquire().await?;
    loop {
        let (force, brk) = tokio::select! {
            _ = timed_trigger.tick() => (true, false),
            maybe_kv = rx.recv().fuse() => {
                if let Some((k, v)) = maybe_kv {
                    let (typ, blake2b, target) = match v.extra {
                        MetaExtra::Symlink { target } => (FileType::Symlink, None, Some(target)),
                        MetaExtra::Regular { blake2b_hash } => {
                            (FileType::Regular, Some(blake2b_hash), None)
                        }
                        MetaExtra::Directory => (FileType::Directory, None, None),
                    };
                    keys.push(k);
                    #[allow(clippy::cast_possible_wrap)]
                    values.push(v.len as i64);
                    mtimes.push(DateTime::<Utc>::from(v.modify_time));
                    types.push(typ);
                    hashes.push(blake2b);
                    targets.push(target);

                    (false, false)
                } else {
                    debug!("pg_tx closed, breaking");
                    (true, true)
                }
            }
        };
        if (force || keys.len() >= MAX_INSERT_PER_BATCH) && !keys.is_empty() {
            debug!(count = keys.len(), "inserting objects");

            let mut txn = conn.begin().await?;

            let mut affected = 0u64;
            for (ks, vs, ms, tys, hs, ts) in multizip((
                keys.chunks(INSERT_CHUNK_SIZE),
                values.chunks(INSERT_CHUNK_SIZE),
                mtimes.chunks(INSERT_CHUNK_SIZE),
                types.chunks(INSERT_CHUNK_SIZE),
                hashes.chunks(INSERT_CHUNK_SIZE),
                targets.chunks(INSERT_CHUNK_SIZE),
            )) {
                let slice_len = ks.len();
                let result = sqlx::query_file!(
                    "../sqls/insert_objects.sql",
                    rev,
                    &[
                        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                        {
                            slice_len as i32
                        }
                    ],
                    &ks[..],
                    &vs[..],
                    &ms[..],
                    &tys[..] as _,
                    &hs[..] as _,
                    &ts[..] as _,
                )
                .execute(&mut *txn)
                .await?;
                affected += result.rows_affected();
            }
            debug!(rev, affected, "rows affected");
            txn.commit().await?;

            keys.clear();
            values.clear();
            mtimes.clear();
            types.clear();
            hashes.clear();
            targets.clear();
            timed_trigger.reset();
        }
        if brk {
            break;
        }
    }

    Ok(())
}

#[instrument(skip(db))]
pub async fn ensure_repository<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    let mut conn = db.acquire().await?;
    let result = sqlx::query!(
        r#"
        INSERT INTO repositories (name)
        VALUES ($1)
        ON CONFLICT DO NOTHING
        "#,
        namespace
    )
    .execute(&mut *conn)
    .await?;
    if result.rows_affected() > 0 {
        info!(namespace, "created repository.");
    }
    Ok(())
}

#[instrument(skip(db))]
pub async fn create_revision<'a>(
    namespace: &str,
    status: RevisionStatus,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<i32> {
    let mut conn = db.acquire().await?;
    Ok(sqlx::query_scalar!(
        r#"
        INSERT INTO revisions (repository, created_at, status)
        VALUES ((SELECT id FROM repositories WHERE name = $1), now(), $2)
        RETURNING revision;
        "#,
        namespace,
        status as _
    )
    .fetch_one(&mut *conn)
    .await?)
}

#[cfg(feature = "tests")]
#[instrument(skip(db))]
pub async fn create_revision_at<'a>(
    namespace: &str,
    status: RevisionStatus,
    created_at: DateTime<Utc>,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<i32> {
    let mut conn = db.acquire().await?;
    Ok(sqlx::query_scalar!(
        r#"
        INSERT INTO revisions (repository, created_at, status)
        VALUES ((SELECT id FROM repositories WHERE name = $1), $2, $3)
        RETURNING revision;
        "#,
        namespace,
        created_at,
        status as _
    )
    .fetch_one(&mut *conn)
    .await?)
}

#[instrument(skip(db))]
pub async fn change_revision_status<'a>(
    revision: i32,
    status: RevisionStatus,
    completed_at: Option<DateTime<Utc>>,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    let mut conn = db.acquire().await?;
    if let Some(completed_at) = completed_at {
        sqlx::query!(
            r#"
        UPDATE revisions
        SET status = $1, completed_at = $2
        WHERE revision = $3;
        "#,
            status as _,
            completed_at,
            revision,
        )
        .execute(&mut *conn)
        .await?;
    } else {
        sqlx::query!(
            r#"
        UPDATE revisions
        SET status = $1
        WHERE revision = $2;
        "#,
            status as _,
            revision
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}
