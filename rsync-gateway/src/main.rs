#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines
)]

use std::sync::Arc;

use actix_web::middleware::{NormalizePath, TrailingSlash};
use actix_web::web::Data;
use actix_web::{web, App, HttpServer};
use clap::Parser;
use eyre::{Report, Result};
use tracing_actix_web::TracingLogger;

use rsync_core::utils::init_logger;

use crate::opts::Opts;
use crate::state::{listen_for_updates, State};

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

    let opts = Arc::new(Opts::parse());

    let redis = redis::Client::open(opts.redis.clone())?;
    let (_guard, latest_index) =
        listen_for_updates(&redis, &opts.redis_namespace, opts.update_interval).await?;

    let state_factory = {
        let namespace = opts.redis_namespace.clone();
        move || {
            let latest_index = latest_index.clone();
            let namespace = namespace.clone();
            let redis = redis.clone();
            async move {
                // One connection per thread.
                let conn = redis.get_multiplexed_tokio_connection().await?;
                Ok(State::new(conn, namespace.clone(), latest_index))
            }
        }
    };

    let server = HttpServer::new({
        let opts = opts.clone();
        move || {
            App::new()
                .wrap(NormalizePath::new(TrailingSlash::MergeOnly))
                .wrap(TracingLogger::default())
                .app_data(Data::from(opts.clone()))
                .data_factory::<_, _, _, Report>(state_factory.clone())
                .service(web::resource("/{path:.*}").to(handler::handler))
        }
    });

    server.bind(&opts.bind)?.run().await?;

    Ok(())
}
