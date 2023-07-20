use std::cmp::Ordering;

use clap::Parser;
use futures::future::ready;
use futures::{pin_mut, StreamExt, TryStreamExt};
use itertools::multizip;
use redis::AsyncCommands;
use scan_fmt::scan_fmt;
use sqlx::postgres::{PgHasArrayType, PgTypeInfo};
use sqlx::types::chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::info;
use url::Url;

use rsync_core::metadata::{MetaExtra, Metadata};
use rsync_core::pg_lock::PgLock;
use rsync_core::redis_::{acquire_instance_lock, async_iter_to_stream, RedisOpts};
use rsync_core::utils::{init_color_eyre, init_logger};

const INSERT_CHUNK_SIZE: usize = 10000;

#[derive(Parser)]
struct Args {
    #[clap(long, env = "DATABASE_URL")]
    db_url: String,
    #[clap(long)]
    redis: Option<Url>,
    #[clap(long)]
    namespace: Option<String>,
}

#[derive(sqlx::Type)]
#[sqlx(type_name = "filetype", rename_all = "lowercase")]
enum FileType {
    Regular,
    Directory,
    Symlink,
}

impl PgHasArrayType for FileType {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_filetype")
    }
}

#[derive(sqlx::Type, Debug, Eq, PartialEq)]
#[sqlx(type_name = "revision_status", rename_all = "lowercase")]
enum RevisionStatus {
    Partial,
    Live,
    Stale,
}

impl PgHasArrayType for RevisionStatus {
    fn array_type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("_revision_status")
    }
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> eyre::Result<()> {
    init_color_eyre()?;
    init_logger();
    dotenvy::dotenv()?;
    let args = Args::parse();

    let pool = PgPool::connect(&args.db_url).await?;
    // Only when there's no r/w access to the database can we change the schema.
    info!("waiting for _system lock");
    let system_lock = PgLock::new_exclusive("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;
    sqlx::migrate!("../migrations").run(&pool).await?;
    system_guard.unlock().await?;

    info!("db migrations applied successfully");

    if let Some(redis_url) = args.redis {
        let redis = redis::Client::open(redis_url)?;
        let mut redis_conn = redis.get_async_connection().await?;

        info!("waiting for redis lock");
        let namespace = args.namespace.expect("namespace is required");
        let _lock = acquire_instance_lock(
            &redis,
            &RedisOpts {
                namespace: namespace.clone(),
                force_break: false,
                lock_ttl: 3 * 60 * 60,
            },
        )
        .await?;

        let mut indices: Vec<_> = async_iter_to_stream(
            redis_conn
                .scan_match::<_, Vec<u8>>(format!("{namespace}:*"))
                .await?,
        )
        .and_then(|s| ready(String::from_utf8(s).map_err(Into::into)))
        .try_filter(|s| ready(!s.ends_with("lock")))
        .try_collect()
        .await?;
        indices.sort_unstable_by(|s, t| index_order(&namespace, s, t));
        info!(?indices, "found indices");

        // Exclusively lock namespace to ensure only one write operation(fetch, migrate, gc) is
        // running on the namespace.
        let repo_lock = PgLock::new_exclusive(&namespace);
        let repo_guard = repo_lock.lock(pool.acquire().await?).await?;
        let mut txn = pool.begin().await?;
        let namespace_id = sqlx::query_scalar!(
            r#"
            INSERT INTO repositories (name) VALUES ($1)
            ON CONFLICT (name) DO NOTHING RETURNING id
            "#,
            namespace
        )
        .fetch_one(&mut *txn)
        .await?;
        for index_key in indices {
            info!(index_key, "start migrating");

            info!(index_key, "fetching from redis");
            let (mut keys, mut values, mut mtimes, mut types, mut hashes, mut targets) =
                (vec![], vec![], vec![], vec![], vec![], vec![]);

            let stream = async_iter_to_stream({
                let mut cmd = redis::cmd("HSCAN");
                cmd.arg(&index_key).cursor_arg(0).arg("COUNT").arg(10000);
                cmd.iter_async::<(Vec<u8>, Metadata)>(&mut redis_conn)
                    .await?
            });
            pin_mut!(stream);
            while let Some(kv) = stream.next().await {
                let (k, v) = kv?;
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
            }

            info!(index_key, "writing to pg");
            let rev_state = if index_key.contains("stale") {
                RevisionStatus::Stale
            } else if index_key.contains("index") {
                RevisionStatus::Live
            } else {
                RevisionStatus::Partial
            };
            let _created_at = Utc::now();
            let rev = sqlx::query_scalar!(
                r#"
                INSERT INTO revisions(repository, created_at, status) VALUES ($1, now(), 'partial')
                RETURNING revision
                "#,
                namespace_id
            )
            .fetch_one(&mut *txn)
            .await?;
            if rev_state != RevisionStatus::Partial {
                sqlx::query!(
                    r#"
                    UPDATE revisions
                    SET status = $1, completed_at = now()
                    WHERE revision = $2
                    "#,
                    rev_state as _,
                    rev
                )
                .execute(&mut *txn)
                .await?;
            }
            // Batch entries into 1000 entries per query
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
            info!(index_key, rev, affected, "rows affected");

            info!(index_key, rev, "start updating parents");
            info!(index_key, rev, "analysing data pattern");
            sqlx::query!("ANALYZE objects").execute(&mut *txn).await?;
            info!(index_key, rev, "do actual update");
            sqlx::query_file!("../sqls/update_parent.sql", rev)
                .execute(&mut *txn)
                .await?;
            info!(index_key, affected, "rows affected");
        }
        txn.commit().await?;
        repo_guard.unlock().await?;

        info!("redis migrations applied successfully");
    }

    Ok(())
}

fn index_order(namespace: &str, s: &str, t: &str) -> Ordering {
    if s.ends_with("partial-stale") {
        return Ordering::Greater;
    }
    if t.ends_with("partial-stale") {
        return Ordering::Less;
    }
    if s.ends_with("partial") {
        return Ordering::Greater;
    }
    if t.ends_with("partial") {
        return Ordering::Less;
    }
    let s_ts = scan_fmt!(s, &format!("{namespace}:index:{{d}}"), u64)
        .or_else(|_| scan_fmt!(s, &format!("{namespace}:stale:{{d}}"), u64))
        .unwrap();
    let t_ts = scan_fmt!(t, &format!("{namespace}:index:{{d}}"), u64)
        .or_else(|_| scan_fmt!(t, &format!("{namespace}:stale:{{d}}"), u64))
        .unwrap();
    s_ts.cmp(&t_ts)
}
