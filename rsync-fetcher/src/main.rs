#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::await_holding_lock
)]
// Note: ensure that lock guards crossing await points does not overlap

use std::sync::Arc;

use chrono::Utc;
use eyre::Result;
use futures::FutureExt;
use sqlx::PgPool;
use tokio::sync::mpsc;
use tracing::info;

use rsync_core::logging::{LogFormat, LogTarget};
use rsync_core::logging::{init_color_eyre, init_logger};
use rsync_core::pg::{
    RevisionStatus, change_revision_status, create_revision, ensure_repository, insert_task,
};
use rsync_core::pg_lock::PgLock;
use rsync_core::s3::{S3Opts, build_operator};

use crate::pg::{
    analyse_objects, create_fl_table, drop_fl_table, insert_file_list_to_db, update_parent_ids,
};
use crate::plan::diff_and_apply;
use crate::rsync::{TaskBuilders, finalize, start_handshake};
use crate::utils::{flatten_err, plan_stat};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod consts;
mod opts;
mod pg;
mod plan;
mod rsync;
#[cfg(test)]
mod tests;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    init_color_eyre()?;
    init_logger(LogTarget::Stderr, LogFormat::Human);

    drop(dotenvy::dotenv());
    let (opts, rsync_opts) = opts::parse();
    let s3_opts = S3Opts::from(&opts);
    let namespace = &opts.namespace;

    let pool = Arc::new(PgPool::connect(&opts.pg_url).await?);
    let mut db_conn = pool.acquire().await?;
    // Shared lock table structure to prevent schema changes.
    let system_lock = PgLock::new_shared("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;
    // Exclusively lock namespace to ensure only one write operation(fetch, migrate, gc) is running
    // on the namespace.
    let ns_lock = PgLock::new_exclusive(namespace);
    let ns_guard = ns_lock.lock(pool.acquire().await?).await?;

    let s3 = build_operator(&s3_opts)?;

    let handshake = start_handshake(&opts.src).await?;
    let mut conn = handshake.finalize(&rsync_opts.filters).await?;

    info!("fetching file list from rsync server.");
    let file_list = Arc::new(conn.recv_file_list().await?);
    create_fl_table(namespace, &mut db_conn).await?;
    insert_file_list_to_db(namespace, &file_list, &mut db_conn).await?;

    info!("generating transfer plan.");
    ensure_repository(namespace, &mut db_conn).await?;
    let revision = create_revision(namespace, RevisionStatus::Partial, &mut db_conn).await?;
    // Run queries to
    // 1. diff files to be transferred
    // 2. copy unchanged entries from live indices to partial index
    // 3. copy directories and symlinks from file list to partial index.
    let (copied, transfer_items) = diff_and_apply(namespace, revision, &*pool).await?;
    info!(
        downloads=%transfer_items.len(),
        copied,
        "transfer plan generated."
    );

    let (pg_tx, pg_rx) = mpsc::channel(10240);
    let insert_handle = tokio::spawn({
        let pool = pool.clone();
        async move { insert_task(revision, pg_rx, &*pool).await }
    });

    // Start the transfer. The transfer model is basically the same as the original rsync impl.
    let TaskBuilders {
        downloader,
        generator,
        receiver,
        uploader,
        progress,
    } = conn.into_task_builders(
        s3.clone(),
        opts.s3_prefix,
        pg_tx,
        file_list.clone(),
        &opts.tmp_path,
    )?;
    let stat = plan_stat(&file_list, &transfer_items);
    info!(?stat, "transfer plan stat.");
    progress.set_length(stat.total_bytes);
    let transfer_items = Arc::new(transfer_items);
    let ((), mut generator, mut receiver, ()) = tokio::try_join!(
        tokio::spawn({
            let transfer_items = transfer_items.clone();
            async move { downloader.tasks(&transfer_items).await }
        })
        .map(flatten_err),
        tokio::spawn({
            let transfer_items = transfer_items.clone();
            async move { generator.generate_task(&transfer_items).await }
        })
        .map(flatten_err),
        tokio::spawn(receiver.recv_task()).map(flatten_err),
        tokio::spawn(uploader.upload_tasks()).map(flatten_err),
    )?;
    progress.finalize().await?;

    info!("waiting for database insertion to finish.");
    insert_handle.await??;

    info!("generating index for directory listing.");
    analyse_objects(&mut db_conn).await?;
    update_parent_ids(revision, &mut db_conn).await?;

    info!("committing transfer.");
    change_revision_status(
        revision,
        RevisionStatus::Live,
        Some(Utc::now()),
        &mut db_conn,
    )
    .await?;

    // Finalize rsync connection.
    let stats = finalize(&mut *generator, &mut *receiver).await?;
    info!(?stats, "transfer stats");

    // Finalize db.
    drop_fl_table(namespace, &mut db_conn).await?;
    system_guard.unlock().await?;
    ns_guard.unlock().await?;

    Ok(())
}
