#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send
)]

use std::sync::Arc;

use clap::Parser;
use eyre::Result;
use tracing::info;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::opts::{Opts, RedisOpts, RsyncOpts, S3Opts};
use crate::plan::generate_transfer_plan;
use crate::redis_::{
    acquire_instance_lock, commit_transfer, copy_index, get_latest_index, move_index,
};
use crate::rsync::{finalize, start_handshake};
use crate::s3::create_s3_client;
use crate::symlink::apply_symlinks;

mod opts;
mod plan;
mod redis_;
mod rsync;
mod s3;
mod set_ops;
mod symlink;
#[cfg(test)]
mod tests;
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::Registry::default()
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
    color_eyre::install()?;

    let opts = Opts::parse();
    let rsync_opts = RsyncOpts::from(&opts);
    let redis_opts = RedisOpts::from(&opts);
    let s3_opts = S3Opts::from(&opts);
    let redis_namespace = redis_opts.namespace.clone();

    let redis = redis::Client::open(opts.redis)?;
    let mut redis_conn = redis.get_async_connection().await?;

    let _lock = acquire_instance_lock(&redis, &redis_opts)?;

    let s3 = create_s3_client(&s3_opts).await;

    let handshake = start_handshake(&opts.src).await?;
    let mut conn = handshake.finalize(&rsync_opts.filters).await?;

    let file_list = Arc::new(conn.recv_file_list().await?);

    let namespace = &redis_opts.namespace;
    let latest_index = get_latest_index(&mut redis_conn, namespace).await?;
    let transfer_plan =
        generate_transfer_plan(&redis, &file_list, &redis_opts, &latest_index).await?;
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
        copy_index(
            &mut redis_conn,
            latest_index.as_ref().expect("latest index"),
            &format!("{namespace}:partial"),
            &transfer_plan.copy_from_latest,
        )
        .await?;
    }

    // Update symlinks. No real files are transferred yet.
    apply_symlinks(
        &mut redis_conn,
        &redis_opts,
        &file_list,
        &transfer_plan.downloads,
    )
    .await?;

    // Start the transfer. The transfer model is basically the same as the original rsync impl.
    let (mut generator, mut receiver) =
        conn.into_gen_recv(s3, s3_opts, redis_conn, redis_opts, file_list)?;
    tokio::try_join!(
        generator.generate_task(&transfer_plan.downloads),
        receiver.recv_task(),
    )?;

    info!("committing transfer.");
    // Commit the transfer.
    commit_transfer(&mut redis.get_async_connection().await?, &redis_namespace).await?;

    // Finalize rsync connection.
    let stats = finalize(&mut *generator, &mut *receiver).await?;
    info!(?stats, "transfer stats");

    Ok(())
}
