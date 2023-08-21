use std::cmp::Ordering;

use clap::Parser;
use eyre::Result;
use futures::future::ready;
use futures::{pin_mut, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use redis::{aio, AsyncCommands};
use scan_fmt::scan_fmt;
use sqlx::types::chrono::Utc;
use sqlx::{PgPool, Postgres, Transaction};
use tokio::sync::mpsc;
use tracing::info;
use url::Url;

use rsync_core::metadata::Metadata;
use rsync_core::pg::{
    change_revision_status, create_revision, ensure_repository, insert_task, RevisionStatus,
};
use rsync_core::pg_lock::PgLock;
use rsync_core::redis_::{acquire_instance_lock, async_iter_to_stream, RedisOpts};

#[derive(Parser)]
pub struct Args {
    /// Database URL.
    #[clap(long, env = "DATABASE_URL")]
    db_url: String,
    /// Redis URL.
    #[clap(long)]
    redis: Url,
    /// Redis namespace.
    #[clap(long)]
    namespace: String,
}

pub async fn redis_to_pg(args: Args) -> Result<()> {
    let pool = PgPool::connect(&args.db_url).await?;

    let redis = redis::Client::open(args.redis)?;
    let mut redis_conn = redis.get_async_connection().await?;

    info!("waiting for redis lock");
    let namespace = args.namespace;
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

    ensure_repository(&namespace, &mut txn).await?;

    for index in indices {
        migrate_index(&namespace, &index, &mut redis_conn, &mut txn).await?;
    }

    txn.commit().await?;
    repo_guard.unlock().await?;

    info!("redis migrations applied successfully");
    Ok(())
}

async fn migrate_index<'a, 'b>(
    namespace: &'a str,
    index: &'a str,
    redis_conn: &'a mut aio::Connection,
    txn: &'a mut Transaction<'b, Postgres>,
) -> Result<()> {
    info!(index, "start migration");

    let rev = create_revision(namespace, RevisionStatus::Partial, &mut *txn).await?;
    info!(rev, "revision created");

    let total_count = redis_conn.hlen(index).await?;
    let pb = ProgressBar::new(total_count);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );

    let stream = async_iter_to_stream({
        let mut cmd = redis::cmd("HSCAN");
        cmd.arg(index).cursor_arg(0).arg("COUNT").arg(10000);
        cmd.iter_async::<(Vec<u8>, Metadata)>(redis_conn).await?
    });
    pin_mut!(stream);

    let (tx, rx) = mpsc::channel(20000);
    let consume_fut = insert_task(rev, rx, &mut *txn);
    let prod_fut = {
        let pb = pb.clone();
        async move {
            while let Some(item) = stream.try_next().await? {
                tx.send(item).await?;
                pb.inc(1);
            }
            Ok(())
        }
    };
    tokio::try_join!(consume_fut, prod_fut)?;
    pb.finish();

    let rev_state = if index.contains("stale") {
        RevisionStatus::Stale
    } else if index.contains("index") {
        RevisionStatus::Live
    } else {
        RevisionStatus::Partial
    };
    if rev_state != RevisionStatus::Partial {
        info!(rev, ?rev_state, "updating revision status");
        change_revision_status(rev, rev_state, Some(Utc::now()), &mut *txn).await?;
    }

    info!(index, rev, "analysing data pattern");
    sqlx::query!("ANALYZE objects").execute(&mut **txn).await?;
    info!(index, rev, "update parents");
    sqlx::query_file!("../sqls/update_parent.sql", rev)
        .execute(&mut **txn)
        .await?;

    info!(index, rev, "migration finished");

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
