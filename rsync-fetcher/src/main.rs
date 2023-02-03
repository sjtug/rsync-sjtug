#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send
)]

use std::sync::Arc;

use clap::Parser;
use eyre::Result;
use tracing::info;

use crate::opts::{Opts, RedisOpts, RsyncOpts, S3Opts};
use crate::plan::generate_transfer_plan;
use crate::redis_::{acquire_instance_lock, commit_transfer};
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
mod utils;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

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
    let transfer_plan = generate_transfer_plan(&redis, &file_list, &redis_opts).await?;

    // First update symlinks. No real files are transferred yet.
    apply_symlinks(&mut redis_conn, &redis_opts, &file_list, &transfer_plan).await?;

    // Start the transfer. The transfer model is basically the same as the original rsync impl.
    let (mut generator, mut receiver) =
        conn.into_gen_recv(s3, s3_opts, redis_conn, redis_opts, file_list)?;
    tokio::try_join!(
        generator.generate_task(&transfer_plan),
        receiver.recv_task(),
    )?;

    // Commit the transfer.
    commit_transfer(&mut redis.get_async_connection().await?, &redis_namespace).await?;

    // Finalize rsync connection.
    let stats = finalize(&mut *generator, &mut *receiver).await?;
    info!(?stats, "transfer stats");

    Ok(())
}
