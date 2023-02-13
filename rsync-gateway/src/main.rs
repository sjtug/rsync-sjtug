#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines
)]

use std::sync::Arc;

use actix_web::middleware::{NormalizePath, TrailingSlash};
use actix_web::{App, HttpServer};
use eyre::Result;
use tracing_actix_web::TracingLogger;

use rsync_core::utils::init_logger;

use crate::app::configure;
use crate::opts::{load_config, validate_config};

mod app;
mod handler;
mod opts;
mod state;
#[cfg(test)]
mod tests;
mod utils;

#[actix_web::main]
pub async fn main() -> Result<()> {
    init_logger();
    color_eyre::install()?;

    let opts = Arc::new(load_config()?);
    validate_config(&opts)?;

    let cfg = configure(&opts).await?;

    let mut server = HttpServer::new({
        move || {
            App::new()
                .wrap(NormalizePath::new(TrailingSlash::Trim))
                .wrap(TracingLogger::default())
                .configure(cfg.clone())
        }
    });

    for bind in &opts.bind {
        server = server.bind(bind)?;
    }
    server.run().await?;

    Ok(())
}
