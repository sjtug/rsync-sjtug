use std::time::{Duration, Instant};

use actix_web::web::Data;
use actix_web::{Either, HttpRequest, Responder};
use chrono::Utc;
use eyre::eyre;
use futures::FutureExt;
use opendal::Operator;
use sqlx::PgPool;
use tracing_actix_web::RequestId;

use crate::app::Prefix;
use crate::cache::NSCache;
use crate::opts::Endpoint;
use crate::path_resolve::resolve;
use crate::pg::{revision_stats, Revision};
use crate::render::{render_internal_error, render_revision_stats};
use crate::state::State;

/// Main handler.
#[allow(clippy::too_many_arguments)]
pub async fn main_handler(
    endpoint: Data<Endpoint>,
    prefix: Data<Prefix>,
    state: Data<State>,
    db: Data<PgPool>,
    op: Data<Operator>,
    cache: Data<NSCache>,
    request_id: RequestId,
    req: HttpRequest,
) -> impl Responder {
    let prefix = prefix.as_str();
    let path = req.match_info().get("path").map_or_else(Vec::new, |path| {
        percent_encoding::percent_decode(path.trim_start_matches('/').as_bytes()).collect()
    });

    let namespace = &endpoint.namespace;
    let s3_prefix = &endpoint.s3_prefix;
    let Some(Revision { revision, generated_at }) = state.revision() else {
        return Either::Left(render_internal_error(
            &path,
            prefix,
            None,
            Utc::now(),
            Duration::default(),
            &eyre!("no revision available"),
            request_id,
        ));
    };

    let query_start = Instant::now();
    let resolved = match cache
        .get_or_insert(
            &path,
            resolve(namespace, &path, revision, s3_prefix, &**db, &op).boxed_local(),
        )
        .await
    {
        Ok(resolved) => resolved,
        Err(e) => {
            let query_time = query_start.elapsed();
            return Either::Left(render_internal_error(
                &path,
                prefix,
                Some(revision),
                Utc::now(),
                query_time,
                &e,
                request_id,
            ));
        }
    };
    let query_time = query_start.elapsed();

    Either::Right(resolved.to_responder(&path, prefix, revision, generated_at, query_time))
}

pub async fn rev_handler(
    endpoint: Data<Endpoint>,
    prefix: Data<Prefix>,
    db: Data<PgPool>,
    request_id: RequestId,
) -> impl Responder {
    let prefix = prefix.as_str();
    let mut conn = db.acquire().await.unwrap();

    let namespace = &endpoint.namespace;

    let query_start = Instant::now();
    let try_stats = revision_stats(namespace, &mut *conn).await;

    let elapsed = query_start.elapsed();

    match try_stats {
        Ok(stats) => Either::Left(render_revision_stats(&stats, Utc::now(), elapsed, prefix)),
        Err(e) => Either::Right(render_internal_error(
            b"_revisions",
            prefix,
            None,
            Utc::now(),
            elapsed,
            &e,
            request_id,
        )),
    }
}
