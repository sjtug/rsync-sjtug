#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines,
    clippy::redundant_pub_crate
)]

use actix_web::middleware::TrailingSlash;
use actix_web::web::Data;
use actix_web::{web, App, HttpServer};
use actix_web_lab::middleware::NormalizePath;
use clap::Parser;
use eyre::Result;
use sqlx::PgPool;
use tracing::{error, info, warn};
use tracing_actix_web::TracingLogger;

use rsync_core::logging::{init_color_eyre, init_logger};
use rsync_core::logging::{LogFormat, LogTarget};
use rsync_core::pg_lock::PgLock;

use crate::app::{configure, default_op_builder};
use crate::metrics::init_metrics;
use crate::opts::{load_config, patch_generated_config, validate_config, Config, Opts};

mod app;
mod cache;
mod handler;
mod listener;
mod metrics;
mod opts;
mod path_resolve;
mod pg;
mod realpath;
mod render;
mod state;
mod templates;
#[cfg(test)]
mod tests;
mod utils;

rust_i18n::i18n!();

#[actix_web::main]
pub async fn main() -> Result<()> {
    init_color_eyre()?;
    let mut logger_handle = init_logger(LogTarget::Stderr, LogFormat::Human);

    let opts = Opts::parse();
    if opts.generate_config {
        println!(
            "{}",
            patch_generated_config(doku::to_toml_fmt::<Config>(&doku::toml::Formatting {
                enums_style: doku::toml::EnumsStyle::Commented,
                ..Default::default()
            }))
        );
        return Ok(());
    }

    let metrics_handle = init_metrics()?;

    drop(dotenvy::dotenv());
    let cfg = load_config(&opts.config)?;
    logger_handle.set_target_format(cfg.log.target.clone(), cfg.log.format);
    validate_config(&cfg)?;

    info!(?cfg, "Loaded config");

    let pool = PgPool::connect(&cfg.database_url).await?;
    // Shared lock table structure to prevent schema changes.
    let system_lock = PgLock::new_shared("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;
    // No need to lock namespace because write operations can be performed in parallel with read.

    let (listener_handle, actix_cfg) = configure(&cfg, default_op_builder, pool.clone()).await?;

    let mut server = HttpServer::new({
        move || {
            App::new()
                .wrap(NormalizePath::new(TrailingSlash::MergeOnly).use_redirects())
                .wrap(TracingLogger::default())
                .configure(actix_cfg.clone())
                .route("/_metrics", web::get().to(metrics::metrics_handler))
                .app_data(Data::new(metrics_handle.clone()))
        }
    });

    for bind in &cfg.bind {
        server = server.bind(bind)?;
    }
    tokio::select! {
        result = listener_handle => {
            error!(?result, "Listener exited");
        }
        result = server.run() => {
            warn!(?result, "Server exited");
        }
    }

    system_guard.unlock().await?;

    Ok(())
}
