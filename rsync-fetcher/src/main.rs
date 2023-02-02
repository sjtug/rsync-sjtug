#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use clap::Parser;
use eyre::Result;

use crate::opts::{Opts, RedisOpts, RsyncOpts};
use crate::redis_::acquire_instance_lock;
use crate::rsync::start_socket_client;

mod filter;
mod opts;
mod redis_;
mod rsync;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let opts = Opts::parse();
    let rsync_opts = RsyncOpts::from(&opts);
    let redis_opts = RedisOpts::from(&opts);

    let redis = redis::Client::open(opts.redis)?;
    let conn = redis.get_async_connection().await?;

    let _lock = acquire_instance_lock(&redis, &redis_opts)?;

    start_socket_client(opts.src, &rsync_opts).await?;

    Ok(())
}
