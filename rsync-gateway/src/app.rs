use std::ops::Deref;
use std::sync::Arc;

use actix_web::guard::Guard;
use actix_web::web::{Data, ServiceConfig};
use actix_web::{guard, web};
use eyre::{Report, Result};
use futures::future;
use opendal::Operator;
use sqlx::PgPool;

use rsync_core::s3::{S3Opts, build_operator};
use rsync_core::utils::AbortJoinHandle;

use crate::cache::NSCache;
use crate::handler;
use crate::listener::RevisionsChangeListener;
use crate::opts::{Config, Endpoint};
use crate::state::{State, listen_for_updates};

pub struct Prefix(pub String);

impl Deref for Prefix {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn get_or_head() -> impl Guard + 'static {
    guard::Any(guard::Get()).or(guard::Head())
}

pub fn default_op_builder(opts: &Config, ep: &Endpoint) -> Result<Operator> {
    #[allow(clippy::needless_question_mark)] // false positive
    build_operator(&S3Opts {
        region: opts.s3_region.clone(),
        url: opts.s3_url.clone(),
        bucket: ep.s3_bucket.clone(),
    })
}

pub async fn configure<B: for<'a> Fn(&'a Config, &'a Endpoint) -> Result<Operator>>(
    opts: &Config,
    op_builder: B,
    pool: PgPool,
) -> Result<(
    AbortJoinHandle<()>,
    impl Fn(&mut ServiceConfig) + Clone + use<B>,
)> {
    let listener = RevisionsChangeListener::default();
    let listener_handle = listener.spawn(&pool);

    let prefix_state: Arc<Vec<_>> = Arc::new(
        future::try_join_all(opts.endpoints.iter().map(|(prefix, endpoint)| async {
            let prefix = prefix.trim_end_matches('/').to_string();
            let cache = Arc::new(NSCache::new(opts.cache));
            let op = op_builder(opts, endpoint)?;
            let (guard, revision) = listen_for_updates(
                &endpoint.namespace,
                opts.update_interval,
                cache.clone(),
                &listener,
                &pool,
            )
            .await?;
            Ok::<_, Report>((
                prefix,
                Arc::new(endpoint.clone()),
                revision,
                Arc::new(guard),
                cache,
                op,
            ))
        }))
        .await?,
    );

    Ok((listener_handle, move |cfg: &mut ServiceConfig| {
        for (prefix, endpoint, revision, guard, cache, op) in &*prefix_state {
            let state = State::new(revision.clone(), guard.clone());
            cfg.service(
                web::scope(&format!("/{prefix}"))
                    .app_data(Data::new(Prefix(prefix.clone())))
                    .app_data(Data::new(state))
                    .app_data(Data::from(endpoint.clone()))
                    .app_data(Data::from(cache.clone()))
                    .app_data(Data::new(op.clone()))
                    .route(
                        "/_revisions",
                        web::route().guard(get_or_head()).to(handler::rev_handler),
                    )
                    .service(
                        web::resource(["", "/{path:(.|/)*}"])
                            .route(web::route().guard(get_or_head()).to(handler::main_handler)),
                    ),
            );
        }
        cfg.app_data(Data::new(pool.clone()));
    }))
}
