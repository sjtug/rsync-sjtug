use std::sync::Arc;

use actix_web::web;
use actix_web::web::{Data, ServiceConfig};
use eyre::{Report, Result};
use futures::future;

use crate::handler;
use crate::opts::Opts;
use crate::state::{listen_for_updates, State};

pub async fn configure(opts: &Opts) -> Result<impl Fn(&mut ServiceConfig) + Clone> {
    let prefix_state: Arc<Vec<_>> = Arc::new(
        future::try_join_all(opts.endpoints.iter().rev().map(|(prefix, endpoint)| async {
            let prefix = prefix.trim_end_matches('/').to_string();
            let redis = redis::Client::open(endpoint.redis.clone())?;
            let conn = redis.get_multiplexed_tokio_connection().await?;
            let (guard, latest_index) =
                listen_for_updates(&redis, &endpoint.redis_namespace, opts.update_interval).await?;
            Ok::<_, Report>((
                prefix,
                Arc::new(endpoint.clone()),
                conn,
                latest_index,
                Arc::new(guard),
            ))
        }))
        .await?,
    );

    Ok(assert_hrtb(move |cfg| {
        for (prefix, endpoint, conn, latest_index, guard) in &*prefix_state {
            let state = State::new(
                conn.clone(),
                endpoint.redis_namespace.clone(),
                latest_index.clone(),
                guard.clone(),
            );
            cfg.service(
                web::resource(&format!("/{prefix}{{path:(.|/)*}}"))
                    .app_data(Data::new(state))
                    .app_data(Data::from(endpoint.clone()))
                    .route(web::get().to(handler::handler)),
            );
        }
    }))
}

// Enforcing the HRTB is necessary to avoid a lifetime error.
const fn assert_hrtb<F: Fn(&mut ServiceConfig)>(f: F) -> F {
    f
}
