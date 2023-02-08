#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines
)]

use clap::Parser;
use eyre::Result;
use tracing::info;

use rsync_core::redis_::{acquire_instance_lock, RedisOpts};
use rsync_core::s3::{create_s3_client, S3Opts};
use rsync_core::utils::init_logger;

use crate::index::{commit_gc, rename_to_stale, scan_live_index, ScanIndex};
use crate::opts::Opts;
use crate::plan::hashes_to_remove;
use crate::s3::{bulk_delete_objs, delete_listing};

mod index;
mod opts;
mod plan;
mod s3;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    color_eyre::install()?;

    let opts = Opts::parse();
    let redis_opts = RedisOpts::from(&opts);
    let s3_opts = S3Opts::from(&opts);

    let redis = redis::Client::open(opts.redis)?;
    let mut redis_conn = redis.get_async_connection().await?;

    let _lock = acquire_instance_lock(&redis, &redis_opts).await?;

    let s3 = create_s3_client(&s3_opts).await;

    info!("scanning index...");
    let ScanIndex {
        delete_before,
        keep,
        delete,
    } = scan_live_index(&mut redis_conn, &redis_opts.namespace, opts.keep).await?;
    info!(keep = keep.len(), delete = delete.len(), "scanned index");

    rename_to_stale(&mut redis_conn, &redis_opts.namespace, &delete).await?;

    info!("deleting stale listings...");
    let deleted = delete_listing(&s3, &s3_opts, delete_before).await?;
    info!("deleted {} objects", deleted);

    info!("deleting stale objects...");
    let hashes = hashes_to_remove(&mut redis_conn, &redis_opts.namespace, &delete, &keep).await?;
    bulk_delete_objs(&s3, &s3_opts, &hashes).await?;
    info!("deleted {} objects", hashes.len());

    commit_gc(&mut redis_conn, &redis_opts.namespace, &delete).await?;

    Ok(())
}
