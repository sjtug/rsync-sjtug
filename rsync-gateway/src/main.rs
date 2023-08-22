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
use eyre::Result;
use sqlx::PgPool;
use tracing::{error, warn};
use tracing_actix_web::TracingLogger;

use rsync_core::pg_lock::PgLock;
use rsync_core::utils::{init_color_eyre, init_logger};

use crate::app::{configure, default_op_builder};
use crate::metrics::init_metrics;
use crate::opts::{load_config, validate_config};

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

#[actix_web::main]
pub async fn main() -> Result<()> {
    init_logger();
    init_color_eyre()?;

    let metrics_handle = init_metrics()?;

    drop(dotenvy::dotenv());
    let opts = load_config()?;
    validate_config(&opts)?;

    let pool = PgPool::connect(&opts.database_url).await?;
    // Shared lock table structure to prevent schema changes.
    let system_lock = PgLock::new_shared("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;
    // No need to lock namespace because write operations can be performed in parallel with read.

    let (listener_handle, cfg) = configure(&opts, default_op_builder, pool.clone()).await?;

    let mut server = HttpServer::new({
        move || {
            App::new()
                .wrap(NormalizePath::new(TrailingSlash::MergeOnly).use_redirects())
                .wrap(TracingLogger::default())
                .configure(cfg.clone())
                .route("/_metrics", web::get().to(metrics::metrics_handler))
                .app_data(Data::new(metrics_handle.clone()))
        }
    });

    for bind in &opts.bind {
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
