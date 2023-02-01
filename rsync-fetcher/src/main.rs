#![allow(clippy::module_name_repetitions, clippy::default_trait_access)]

use clap::Parser;
use eyre::Result;

use crate::opts::{Opts, RsyncOpts};
use crate::rsync::start_socket_client;

mod filter;
mod opts;
mod rsync;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::init();

    let opts = Opts::parse();
    let rsync_opts = RsyncOpts::from(&opts);

    start_socket_client(opts.src, &rsync_opts).await?;

    Ok(())
}
