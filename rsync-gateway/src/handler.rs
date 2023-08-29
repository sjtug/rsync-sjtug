use std::time::{Duration, Instant};

use actix_web::http::header::{AcceptLanguage, Preference};
use actix_web::web::{Data, Header};
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

fn select_locale(accept_language: &AcceptLanguage) -> &'static str {
    let accepted = accept_language.ranked();
    let available = rust_i18n::available_locales!();
    accepted
        .iter()
        .find_map(|lang| match lang {
            Preference::Any => Some(available[0]),
            Preference::Specific(l) => available
                .iter()
                .find(|al| **al == l.as_str() || **al == l.primary_language())
                .copied(),
        })
        .unwrap_or(available[0])
}

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
    accepted_language: Header<AcceptLanguage>,
) -> impl Responder {
    let prefix = prefix.as_str();
    let full_path = req.path();
    let path = req.match_info().get("path").map_or_else(Vec::new, |path| {
        percent_encoding::percent_decode(path.trim_end_matches('/').as_bytes()).collect()
    });
    let req_path = if full_path.ends_with('/') {
        let mut p = path.clone();
        p.push(b'/');
        p
    } else {
        path.clone()
    };

    let locale = select_locale(&accepted_language);

    let namespace = &endpoint.namespace;
    let s3_prefix = &endpoint.s3_prefix;
    let Some(Revision { revision, generated_at }) = state.revision() else {
        return Either::Left(render_internal_error(
            &req_path,
            prefix,
            None,
            Utc::now(),
            Duration::default(),
            &eyre!("no revision available"),
            &request_id,
            locale
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
                &req_path,
                prefix,
                Some(revision),
                Utc::now(),
                query_time,
                &e,
                &request_id,
                locale,
            ));
        }
    };
    let query_time = query_start.elapsed();

    Either::Right(resolved.to_responder(
        &req_path,
        full_path,
        prefix,
        revision,
        generated_at,
        query_time,
        locale,
    ))
}

pub async fn rev_handler(
    endpoint: Data<Endpoint>,
    prefix: Data<Prefix>,
    db: Data<PgPool>,
    request_id: RequestId,
    accepted_language: Header<AcceptLanguage>,
) -> impl Responder {
    let prefix = prefix.as_str();
    let mut conn = db.acquire().await.unwrap();

    let namespace = &endpoint.namespace;

    let query_start = Instant::now();
    let try_stats = revision_stats(namespace, &mut *conn).await;

    let elapsed = query_start.elapsed();

    let locale = select_locale(&accepted_language);

    match try_stats {
        Ok(stats) => Either::Left(render_revision_stats(
            &stats,
            Utc::now(),
            elapsed,
            prefix,
            locale,
        )),
        Err(e) => Either::Right(render_internal_error(
            b"_revisions",
            prefix,
            None,
            Utc::now(),
            elapsed,
            &e,
            &request_id,
            locale,
        )),
    }
}
