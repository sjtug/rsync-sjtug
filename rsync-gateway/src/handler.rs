use actix_web::web::{Data, Path, Redirect};
use actix_web::{Either, HttpResponse, Responder};
use bstr::BStr;
use tracing::debug;

use rsync_core::redis_::Target;
use rsync_core::utils::{ToHex, PATH_ASCII_SET};

use crate::opts::Endpoint;
use crate::state::State;
use crate::utils::{ReportExt, ReportWrapper};

/// Main handler.
pub async fn handler(
    endpoint: Data<Endpoint>,
    state: Data<State>,
    path: Path<String>,
) -> impl Responder {
    let path: Vec<_> =
        percent_encoding::percent_decode(path.trim_start_matches('/').as_bytes()).collect();

    debug!(path=?BStr::new(&path), "incoming request");

    if path.is_empty() {
        Either::Left(index(&endpoint, &state))
    } else {
        Either::Right(file_handler(&endpoint, &state, &path).await)
    }
}

/// Handler for index.
fn index(endpoint: &Endpoint, state: &State) -> impl Responder {
    Ok::<_, ReportWrapper>(state.latest_index().map_or_else(
        || Either::Right(HttpResponse::NotFound()),
        |latest| {
            Either::Left(Redirect::to(format!(
                "{}/listing-{latest}/index.html",
                endpoint.s3_website
            )))
        },
    ))
}

/// Handler for file requests.
async fn file_handler(endpoint: &Endpoint, state: &State, path: &[u8]) -> impl Responder {
    let target = state.lookup_target_of_path(path).await.into_resp_err()?;
    Ok::<_, ReportWrapper>(match target {
        None => Either::Left(HttpResponse::NotFound()),
        Some(Target::File(hash)) => Either::Right(Redirect::to(format!(
            "{}/{:x}",
            endpoint.s3_website,
            hash.as_hex()
        ))),
        Some(Target::Directory(path)) => state.latest_index().map_or_else(
            || Either::Left(HttpResponse::NotFound()),
            |latest| {
                let path = percent_encoding::percent_encode(&path, PATH_ASCII_SET);
                Either::Right(Redirect::to(format!(
                    "{}/listing-{latest}/{path}/index.html",
                    endpoint.s3_website
                )))
            },
        ),
    })
}
