use actix_web::web::{Data, Path, Redirect};
use actix_web::{Either, HttpResponse, Responder};
use bstr::BStr;
use tracing::debug;

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
    let path: Vec<_> = percent_encoding::percent_decode(path.as_bytes()).collect();

    debug!(path=?BStr::new(&path), "incoming request");

    let listing = path.ends_with(b"/") || path.is_empty();
    if listing {
        Either::Left(listing_handler(&endpoint, &state, &path).await)
    } else {
        Either::Right(file_handler(&endpoint, &state, &path).await)
    }
}

/// Handler for listing requests.
async fn listing_handler(endpoint: &Endpoint, state: &State, path: &[u8]) -> impl Responder {
    let path = state.resolve_dir(path).await.into_resp_err()?;
    Ok::<_, ReportWrapper>(state.latest_index().map_or_else(
        || Either::Right(HttpResponse::NotFound()),
        |latest| {
            let path = percent_encoding::percent_encode(&path, PATH_ASCII_SET);
            Either::Left(Redirect::to(format!(
                "{}/listing-{latest}/{path}index.html",
                endpoint.s3_website
            )))
        },
    ))
}

/// Handler for file requests.
async fn file_handler(endpoint: &Endpoint, state: &State, path: &[u8]) -> impl Responder {
    let hash = state.lookup_hash_of_path(path).await.into_resp_err()?;
    Ok::<_, ReportWrapper>(hash.map_or_else(
        || Either::Right(HttpResponse::NotFound()),
        |hash| {
            Either::Left(Redirect::to(format!(
                "{}/{:x}",
                endpoint.s3_website,
                hash.as_hex()
            )))
        },
    ))
}
