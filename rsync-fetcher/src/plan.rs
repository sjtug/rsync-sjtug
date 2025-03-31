//! Transfer plan.
//!
//! Changes:
//! 1. No need to use partial stale to handle stale files caused by previous partial transfer, because we will mark those revisions as STALE.
//! 2. All previous live revisions are considered, not just the latest one.

use crate::utils::namespace_as_table;
use eyre::Result;
use sqlx::postgres::PgRow;
use sqlx::{Acquire, Error, FromRow, Postgres, Row};
use tap::Tap;
use tracing::{info, instrument};

/// Diff local and remote file list.
///
/// Returns a list of files that need to be transferred, and count of rows copied from previous revisions.
#[instrument(skip(db))]
pub async fn diff_and_apply<'a>(
    namespace: &'a str,
    target_revision: i32,
    db: impl Acquire<'a, Database = Postgres> + Clone,
) -> Result<(u64, Vec<TransferItem>)> {
    let mut unchanged_txn = db.clone().begin().await?;
    let mut dir_txn = db.clone().begin().await?;
    let mut link_txn = db.clone().begin().await?;
    let (download, unchanged_affected, dir_affected, link_affected) = tokio::try_join!(
        diff_changed_or_remote_only(namespace, db),
        diff_unchanged_and_copy(namespace, target_revision, &mut unchanged_txn),
        copy_directories(namespace, target_revision, &mut dir_txn),
        copy_symlinks(namespace, target_revision, &mut link_txn),
    )?;

    // Commit side effects after diff.
    unchanged_txn.commit().await?;
    dir_txn.commit().await?;
    link_txn.commit().await?;

    Ok((unchanged_affected + dir_affected + link_affected, download))
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct TransferItem {
    /// File idx in file list.
    pub idx: i32,
    /// If present, perform delta transfer on this file with given hash (address on S3).
    pub blake2b: Option<[u8; 20]>,
}

impl FromRow<'_, PgRow> for TransferItem {
    fn from_row(row: &'_ PgRow) -> std::result::Result<Self, Error> {
        Ok(Self {
            idx: row.try_get("idx").expect("idx"),
            blake2b: row
                .try_get::<Option<Vec<u8>>, _>("blake2b")
                .expect("idx")
                .map(|blake2b| blake2b.try_into().expect("blake2b length")),
        })
    }
}

/// Diff local and remote file list, and return a list of files
/// 1. only on remote
/// 2. filename exists on remote and local, but with no entry in local with len and mtime match
///    (in which case returns blake2b of newest revision with same filename).
#[instrument(skip(db))]
async fn diff_changed_or_remote_only<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Vec<TransferItem>> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    Ok(sqlx::query_as(
        &include_str!("../../sqls/diff_changed_or_remote_only.sql")
            .replace("rsync_filelist", &format!("{ns_table}_fl")),
    )
    .bind(namespace)
    .fetch_all(&mut *db.acquire().await?)
    .await?
    .tap(|items| info!(len = items.len(), "changed or remote only files")))
}

/// Diff local and remote file list, get a list of files existing on both sides but unchanged
/// (i.e. filename, len and mtime match), and copy them to the new revision. In case of multiple
/// matching entries, the one with the newest revision is used.
///
/// # Note
///
/// This function has side effects. It should only be called once per revision.
#[instrument(skip(db))]
async fn diff_unchanged_and_copy<'a>(
    namespace: &'a str,
    target_revision: i32,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<u64> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    let result = sqlx::query(
        &include_str!("../../sqls/diff_unchanged_and_copy.sql")
            .replace("rsync_filelist", &format!("{ns_table}_fl")),
    )
    .bind(namespace)
    .bind(target_revision)
    .execute(&mut *db.acquire().await?)
    .await?;
    let affected = result.rows_affected();
    info!(affected, "copied unchanged files to new revision");
    Ok(affected)
}

/// Copy directories to the new revision.
///
/// # Note
///
/// This function has side effects. It should only be called once per revision.
#[instrument(skip(db))]
async fn copy_directories<'a>(
    namespace: &'a str,
    target_revision: i32,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<u64> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    let result = sqlx::query(
        &include_str!("../../sqls/copy_directories.sql")
            .replace("rsync_filelist", &format!("{ns_table}_fl")),
    )
    .bind(target_revision)
    .execute(&mut *db.acquire().await?)
    .await?;
    let affected = result.rows_affected();
    info!(affected, "copied directories to new revision");
    Ok(affected)
}

/// Copy symlinks to the new revision.
///
/// # Note
///
/// This function has side effects. It should only be called once per revision.
#[instrument(skip(db))]
async fn copy_symlinks<'a>(
    namespace: &'a str,
    target_revision: i32,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<u64> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    let result = sqlx::query(
        &include_str!("../../sqls/copy_symlinks.sql")
            .replace("rsync_filelist", &format!("{ns_table}_fl")),
    )
    .bind(target_revision)
    .execute(&mut *db.acquire().await?)
    .await?;
    let affected = result.rows_affected();
    info!(affected, "copied directories to new revision");
    Ok(affected)
}

#[cfg(test)]
mod tests {
    mod db_required {
        use std::time::{Duration, UNIX_EPOCH};

        use chrono::{DateTime, Utc};
        use itertools::Itertools;
        use sqlx::{Acquire, PgPool, Postgres};

        use rsync_core::metadata::Metadata;
        use rsync_core::pg::{
            FileType, RevisionStatus, change_revision_status, create_revision, ensure_repository,
        };
        use rsync_core::tests::generate_random_namespace;
        use rsync_core::tests::insert_to_revision;

        use crate::pg::{create_fl_table, insert_file_list_to_db};
        use crate::plan::{TransferItem, diff_and_apply};
        use crate::rsync::file_list::FileEntry;

        async fn assert_entry_eq<'a>(
            source_revision: i32,
            target_revision: i32,
            keys: &[Vec<u8>],
            conn: impl Acquire<'a, Database = Postgres>,
        ) {
            let missing_or_different = sqlx::query_file!(
                "../sqls/test_missing_or_different.sql",
                source_revision,
                target_revision,
                keys
            )
            .fetch_all(&mut *conn.acquire().await.expect("pool"))
            .await
            .expect("query");
            assert!(
                missing_or_different.is_empty(),
                "missing or different: {missing_or_different:?}"
            );
        }

        struct DirLinkEntry {
            filename: Vec<u8>,
            modify_time: DateTime<Utc>,
            r#type: FileType,
            target: Option<Vec<u8>>,
        }

        async fn assert_dir_link_entries<'a>(
            revision: i32,
            entries: &'a [DirLinkEntry],
            conn: impl Acquire<'a, Database = Postgres>,
        ) {
            let filenames: Vec<_> = entries.iter().map(|e| e.filename.clone()).collect();
            sqlx::query_file_as!(
                DirLinkEntry,
                "../sqls/test_dir_link_entries.sql",
                revision,
                &filenames[..]
            )
            .fetch_all(&mut *conn.acquire().await.expect("pool"))
            .await
            .expect("query")
            .into_iter()
            .zip(entries.iter())
            .for_each(|(row, entry)| {
                assert_eq!(row.filename, entry.filename);
                assert_eq!(row.modify_time, entry.modify_time);
                assert_eq!(row.r#type, entry.r#type);
                assert_eq!(row.target, entry.target);
            });
        }

        async fn assert_plan(
            remote: &[FileEntry],
            live: &[(Vec<u8>, Metadata)],
            partial: &[(Vec<u8>, Metadata)],
            expect_downloads: &[TransferItem],
            expect_copy_live: &[Vec<u8>],
            expect_copy_partial: &[Vec<u8>],
            db: &PgPool,
        ) {
            let mut conn = db.acquire().await.expect("pool");
            let namespace = generate_random_namespace();

            create_fl_table(&namespace, db).await.expect("create table");
            insert_file_list_to_db(&namespace, remote, &mut conn)
                .await
                .expect("insert file list");

            ensure_repository(&namespace, &mut conn)
                .await
                .expect("ensure repository");
            let live_rev = create_revision(&namespace, RevisionStatus::Partial, &mut conn)
                .await
                .expect("create live");
            let partial_rev = create_revision(&namespace, RevisionStatus::Partial, &mut conn)
                .await
                .expect("create partial");

            insert_to_revision(live_rev, live, &mut conn).await;
            insert_to_revision(partial_rev, partial, &mut conn).await;

            change_revision_status(
                live_rev,
                RevisionStatus::Live,
                Some(DateTime::from(UNIX_EPOCH)),
                &mut conn,
            )
            .await
            .expect("change live status");

            let target_rev = create_revision(&namespace, RevisionStatus::Partial, &mut conn)
                .await
                .expect("create target");
            let (inserted, mut downloads) = diff_and_apply(&namespace, target_rev, db)
                .await
                .expect("diff and apply");

            let expected_dir_link: Vec<_> = remote
                .iter()
                .filter_map(|e| {
                    let kind = if unix_mode::is_symlink(e.mode) {
                        FileType::Symlink
                    } else if unix_mode::is_dir(e.mode) {
                        FileType::Directory
                    } else {
                        return None;
                    };
                    Some(DirLinkEntry {
                        filename: e.name.clone(),
                        modify_time: DateTime::from(e.modify_time),
                        r#type: kind,
                        target: e.link_target.clone(),
                    })
                })
                .collect();

            let expect_inserted =
                expect_copy_live.len() + expect_copy_partial.len() + expected_dir_link.len();
            assert_eq!(inserted, expect_inserted as u64);

            downloads.sort();
            let expect_downloads: Vec<_> = expect_downloads.iter().sorted().cloned().collect();
            assert_eq!(downloads, expect_downloads);
            assert_entry_eq(live_rev, target_rev, expect_copy_live, &mut conn).await;
            assert_entry_eq(partial_rev, target_rev, expect_copy_partial, &mut conn).await;
            assert_dir_link_entries(target_rev, &expected_dir_link, &mut conn).await;
        }

        // 1. test specific migrations
        // 2. move one test to here from plan, and check if it works
        // 3. implement compare copy and dir_link
        // 4. migrate remaining tests

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_plan_no_latest(pool: PgPool) {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 1, UNIX_EPOCH, 0),
                    FileEntry::regular("b".into(), 1, UNIX_EPOCH, 1),
                ],
                &[],
                &[],
                &[TransferItem::new(0, None), TransferItem::new(1, None)],
                &[],
                &[],
                &pool,
            )
            .await;
        }

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_plan_with_latest(pool: PgPool) {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                    FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                    FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                    FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
                ],
                &[
                    ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                    ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                    ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                    ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                ],
                &[],
                &[
                    TransferItem::new(0, Some([0; 20])), // len differs
                    TransferItem::new(1, Some([1; 20])), // time differs
                    TransferItem::new(3, None),          // new file
                ],
                &[
                    b"c".to_vec(), // up-to-date in latest but not exist in partial
                ],
                &[],
                &pool,
            )
            .await;
        }

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_plan_with_partial(pool: PgPool) {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                    FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                    FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                    FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
                ],
                &[],
                &[
                    ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                    ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                    ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                    ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                ],
                &[
                    TransferItem::new(0, Some([0; 20])), // len differs
                    TransferItem::new(1, Some([1; 20])), // time differs
                    TransferItem::new(3, None),          // new file
                ],
                &[], // in our test harness, partial revision is higher than latest
                &[
                    b"c".to_vec(), // same as partial
                                   // no 'd' because not in remote
                ],
                &pool,
            )
            .await;
        }

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_plan_with_partial_latest(pool: PgPool) {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                    FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                    FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                    FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
                    FileEntry::regular("f".into(), 1, UNIX_EPOCH, 4),
                    FileEntry::regular("g".into(), 1, UNIX_EPOCH, 5),
                    FileEntry::regular("h".into(), 1, UNIX_EPOCH, 6),
                ],
                &[
                    ("a".into(), Metadata::regular(2, UNIX_EPOCH, [0; 20])),
                    ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                    ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                    ("f".into(), Metadata::regular(2, UNIX_EPOCH, [4; 20])),
                    ("g".into(), Metadata::regular(1, UNIX_EPOCH, [6; 20])),
                    ("h".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                ],
                &[
                    ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                    (
                        "b".into(),
                        Metadata::regular(1, UNIX_EPOCH + Duration::from_secs(1), [1; 20]),
                    ),
                    ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                    ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                    ("f".into(), Metadata::regular(2, UNIX_EPOCH, [5; 20])),
                    ("g".into(), Metadata::regular(2, UNIX_EPOCH, [6; 20])),
                ],
                &[
                    // 0 is present in live
                    // 1 is present in partial
                    // 2 is present in partial
                    TransferItem::new(3, None), // new file
                    TransferItem::new(4, Some([5; 20])), // len differs, take partial as basis
                                                // 5 is present in live
                                                // 6 is present in live
                ],
                &[
                    b"a".to_vec(), // 0
                    b"g".to_vec(), // 5
                    b"h".to_vec(), // 6
                ],
                &[
                    b"b".to_vec(), // 1
                    b"c".to_vec(), // 2
                ],
                &pool,
            )
            .await;
        }

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_plan_copy_dir_link(pool: PgPool) {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 1, UNIX_EPOCH, 0),
                    FileEntry::directory("b".into(), 1, UNIX_EPOCH, 1),
                    FileEntry::symlink("c".into(), 1, "a".to_string(), UNIX_EPOCH, 2),
                    FileEntry::directory("d".into(), 1, UNIX_EPOCH, 3),
                    FileEntry::symlink("e".into(), 1, "d".to_string(), UNIX_EPOCH, 4),
                ],
                &[
                    ("a".into(), Metadata::directory(1, UNIX_EPOCH)),
                    ("b".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                    ("c".into(), Metadata::symlink(1, UNIX_EPOCH, "a")),
                    ("d".into(), Metadata::directory(2, UNIX_EPOCH)),
                ],
                &[("d".into(), Metadata::directory(1, UNIX_EPOCH))],
                &[TransferItem::new(0, None)],
                &[],
                &[],
                &pool,
            )
            .await;
        }
    }
}
