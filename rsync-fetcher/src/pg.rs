use chrono::{DateTime, Utc};
use eyre::Result;
use itertools::multizip;
use sqlx::{Acquire, Postgres};
use tracing::{info, instrument};

use rsync_core::pg::INSERT_CHUNK_SIZE;

use crate::rsync::file_list::FileEntry;
use crate::utils::namespace_as_table;

#[instrument(skip(db))]
pub async fn create_fl_table<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    // TODO
    // We are now creating rsync_filelist and an unlogged table instead of a temp one,
    // because we may need to access it from different connections due to pooling.
    // There are two alternatives:
    // 1. Fix this connection and pass it around. Quite verbose.
    // 2. Make it short-lived. No need to query it later because we have file_list in memory.
    // If we switch to temp table, we don't need namespacing any more.

    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    let mut txn = db.begin().await?;
    sqlx::query(&format!("DROP TABLE IF EXISTS {ns_table}_fl"))
        .execute(&mut *txn)
        .await?;
    sqlx::query(
        &include_str!("../../sqls/create_fl_table.sql")
            .replace("rsync_filelist", &format!("{ns_table}_fl")),
    )
    .execute(&mut *txn)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX {ns_table}_fl_nm_idx ON {ns_table}_fl (filename, mode)"
    ))
    .execute(&mut *txn)
    .await?;
    sqlx::query(&format!(
        "CREATE INDEX {ns_table}_fl_m_idx ON {ns_table}_fl (mode)"
    ))
    .execute(&mut *txn)
    .await?;
    txn.commit().await?;
    Ok(())
}

#[instrument(skip(db))]
pub async fn drop_fl_table<'a>(
    namespace: &str,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    let mut conn = db.acquire().await?;
    sqlx::query(&format!("DROP TABLE IF EXISTS {ns_table}_fl"))
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[instrument(skip(db, file_list))]
pub async fn insert_file_list_to_db<'a>(
    namespace: &str,
    file_list: &[FileEntry],
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    // Make sure the namespace is a valid table name.
    let ns_table = namespace_as_table(namespace);

    // TODO doubled memory usage, can be optimised if OOM
    let (mut names, mut lens, mut mtimes, mut modes, mut targets, mut ids) =
        (vec![], vec![], vec![], vec![], vec![], vec![]);
    #[allow(clippy::cast_possible_wrap)]
    for entry in file_list {
        let FileEntry {
            name,
            len,
            modify_time,
            mode,
            link_target,
            idx,
        } = entry;
        names.push(name.clone());
        lens.push(*len as i64);
        mtimes.push(DateTime::<Utc>::from(*modify_time));
        modes.push(*mode as i32);
        targets.push(link_target.clone());
        ids.push(*idx as i32);
    }

    let mut txn = db.begin().await?;

    let mut affected = 0u64;
    for (ns, ls, ms, mos, ts, is) in multizip((
        names.chunks(INSERT_CHUNK_SIZE),
        lens.chunks(INSERT_CHUNK_SIZE),
        mtimes.chunks(INSERT_CHUNK_SIZE),
        modes.chunks(INSERT_CHUNK_SIZE),
        targets.chunks(INSERT_CHUNK_SIZE),
        ids.chunks(INSERT_CHUNK_SIZE),
    )) {
        let result = sqlx::query(
            &include_str!("../../sqls/insert_filelist.sql")
                .replace("rsync_filelist", &format!("{ns_table}_fl")),
        )
        .bind(ns)
        .bind(ls)
        .bind(ms)
        .bind(mos)
        .bind(ts)
        .bind(is)
        .execute(&mut *txn)
        .await?;
        affected += result.rows_affected();
    }

    sqlx::query(&format!("ANALYZE {ns_table}_fl"))
        .execute(&mut *txn)
        .await?;
    txn.commit().await?;

    info!(affected, "inserted filelist to db");
    Ok(())
}

#[instrument(skip_all)]
pub async fn analyse_objects<'a>(conn: impl Acquire<'a, Database = Postgres>) -> Result<()> {
    let mut conn = conn.acquire().await?;
    sqlx::query!("ANALYZE objects").execute(&mut *conn).await?;
    Ok(())
}

#[instrument(skip(conn))]
pub async fn update_parent_ids<'a>(
    rev: i32,
    conn: impl Acquire<'a, Database = Postgres>,
) -> Result<()> {
    let mut conn = conn.acquire().await?;
    sqlx::query_file!("../sqls/update_parent.sql", rev)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

    mod db_required {
        use sqlx::PgPool;

        use rsync_core::pg::{FileType, RevisionStatus, create_revision, ensure_repository};
        use rsync_core::tests::generate_random_namespace;

        use crate::pg::update_parent_ids;

        #[sqlx::test(migrations = "../tests/migrations")]
        async fn must_update_parent_ids(pool: PgPool) {
            let mut conn = pool.acquire().await.expect("acquire");

            let namespace = generate_random_namespace();
            ensure_repository(&namespace, &mut conn)
                .await
                .expect("create repo");
            let rev = create_revision(&namespace, RevisionStatus::Partial, &mut conn)
                .await
                .expect("create rev");

            let entries: &[(&[u8], _)] = &[
                (b"a/d", FileType::Directory), // file inserted before directory is allowed
                (b"a/d/e", FileType::Regular),
                (b"a", FileType::Directory),
                (b"a/b", FileType::Regular),
                (b"a/c", FileType::Regular),
                (b"b", FileType::Regular),
                (b"d", FileType::Directory),
                (b"d/f", FileType::Directory),
                (b"d/f/g", FileType::Directory),
            ];
            for (idx, (filename, kind)) in entries.iter().enumerate() {
                let blake2b = if *kind == FileType::Regular {
                    Some(&[0u8; 20][..])
                } else {
                    None
                };
                sqlx::query!(
                    r#"
            INSERT INTO objects (id, revision, filename, len, modify_time, type, blake2b)
            OVERRIDING SYSTEM VALUE
            VALUES ($1, $2, $3, 0, now(), $4, $5)
            "#,
                    idx as i32,
                    rev,
                    *filename,
                    *kind as _,
                    blake2b
                )
                .execute(&mut *conn)
                .await
                .expect("insert");
            }

            update_parent_ids(rev, &mut conn)
                .await
                .expect("update_parent_ids");

            let expect_entries: &[(&[u8], _)] = &[
                (b"a", None),
                (b"a/b", Some(2i64)),
                (b"a/c", Some(2)),
                (b"a/d", Some(2)),
                (b"a/d/e", Some(0)),
                (b"b", None),
                (b"d", None),
                (b"d/f", Some(6)),
                (b"d/f/g", Some(7)),
            ];
            for (filename, expected_parent) in expect_entries {
                let actual = sqlx::query!(
                    r#"
            SELECT parent FROM objects WHERE revision = $1 AND filename = $2
            "#,
                    rev,
                    *filename
                )
                .fetch_one(&mut *conn)
                .await
                .expect("select")
                .parent;
                assert_eq!(
                    actual,
                    *expected_parent,
                    "parent of {:?} is wrong",
                    String::from_utf8_lossy(filename)
                );
            }
        }
    }
}
