#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines
)]

use std::sync::Arc;

use clap::Parser;
use eyre::Result;
use tracing::info;

use rsync_core::redis_::{
    acquire_instance_lock, commit_transfer, copy_index, get_latest_index, move_index, RedisOpts,
};
use rsync_core::s3::{create_s3_client, S3Opts};
use rsync_core::utils::init_logger;

use crate::index::generate_index_and_upload;
use crate::opts::{Opts, RsyncOpts};
use crate::plan::generate_transfer_plan;
use crate::rsync::{finalize, start_handshake, TaskBuilders};
use crate::symlink::apply_symlinks_n_directories;
use crate::utils::{plan_stat, timestamp};

mod consts;
mod index;
mod opts;
mod plan;
mod rsync;
mod symlink;
#[cfg(test)]
mod tests;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    color_eyre::install()?;

    let opts = Opts::parse();
    let rsync_opts = RsyncOpts::from(&opts);
    let redis_opts = RedisOpts::from(&opts);
    let s3_opts = S3Opts::from(&opts);
    let redis_namespace = redis_opts.namespace.clone();

    let redis = redis::Client::open(opts.redis)?;
    let mut redis_conn = redis.get_async_connection().await?;

    let _lock = acquire_instance_lock(&redis, &redis_opts).await?;

    let s3 = create_s3_client(&s3_opts).await;

    let handshake = start_handshake(&opts.src).await?;
    let mut conn = handshake.finalize(&rsync_opts.filters).await?;

    info!("fetching file list from rsync server.");
    let file_list = Arc::new(conn.recv_file_list().await?);

    info!("generating transfer plan.");
    let namespace = &redis_opts.namespace;
    let latest_index = get_latest_index(&mut redis_conn, namespace)
        .await?
        .map(|x| format!("{namespace}:index:{x}"));
    let transfer_plan = generate_transfer_plan(
        &redis,
        &file_list,
        &redis_opts,
        &latest_index,
        opts.no_delta,
    )
    .await?;
    info!(
        downloads=%transfer_plan.downloads.len(),
        copy_from_latest=%transfer_plan.copy_from_latest.len(),
        stale=%transfer_plan.stale.len(),
        "transfer plan generated."
    );

    if !transfer_plan.stale.is_empty() {
        // Move stale files in the partial index to the "partial-stale" index.
        // These files were uploaded to S3 during last partial transfer, but are removed from remote
        // rsync server.
        info!("moving outdated files from last partial sync to partial-stale index.");
        // On error this would leave some files in the partial index, but the partial index will be
        // updated anyway during next transfer.
        move_index(
            &mut redis_conn,
            &format!("{namespace}:partial"),
            &format!("{namespace}:partial-stale"),
            &transfer_plan.stale,
        )
        .await?;
    }

    if !transfer_plan.copy_from_latest.is_empty() {
        info!("copying up-to-date files from latest index to partial index.");
        // These files already exists in the latest index. Copy them to the partial index.
        // On error this would leave some files in the partial index, but the partial index will be
        // updated anyway during next transfer.
        copy_index(
            &mut redis_conn,
            latest_index
                .as_ref()
                .expect("copy_from_latest is not empty, so latest_index must be Some"),
            &format!("{namespace}:partial"),
            &transfer_plan.copy_from_latest,
        )
        .await?;
    }

    // Update symlinks. No real files are transferred yet.
    apply_symlinks_n_directories(
        &mut redis_conn,
        &redis_opts,
        &file_list,
        &transfer_plan.downloads,
    )
    .await?;

    // Start the transfer. The transfer model is basically the same as the original rsync impl.
    let TaskBuilders {
        downloader,
        mut generator,
        mut receiver,
        uploader,
        progress,
    } = conn.into_task_builders(
        s3.clone(),
        s3_opts.clone(),
        redis.get_multiplexed_async_connection().await?,
        redis_opts,
        file_list.clone(),
        &opts.tmp_path,
    )?;
    let stat = plan_stat(&file_list, &transfer_plan.downloads);
    info!(?stat, "transfer plan stat.");
    progress.set_length(stat.total_bytes);
    tokio::try_join!(
        downloader.tasks(&transfer_plan.downloads),
        generator.generate_task(&transfer_plan.downloads),
        receiver.recv_task(),
        uploader.upload_tasks(),
    )?;
    progress.finalize().await?;

    let timestamp = timestamp();

    info!("generating listings.");
    generate_index_and_upload(
        &redis,
        &redis_namespace,
        &s3,
        &s3_opts,
        &opts.gateway_base,
        &opts.repository,
        timestamp,
    )
    .await?;

    info!("committing transfer.");
    commit_transfer(
        &mut redis.get_async_connection().await?,
        &redis_namespace,
        timestamp,
    )
    .await?;

    // Finalize rsync connection.
    let stats = finalize(&mut *generator, &mut *receiver).await?;
    info!(?stats, "transfer stats");

    Ok(())
}
