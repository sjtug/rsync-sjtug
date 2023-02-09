use actix_web::web::{Data, Redirect};
use actix_web::{Either, HttpRequest, HttpResponse, Responder};
use bstr::{BStr, ByteSlice};
use tracing::debug;

use rsync_core::utils::ToHex;

use crate::opts::Opts;
use crate::state::State;
use crate::utils::{ReportExt, ReportWrapper};

/// Main handler.
pub async fn handler(opts: Data<Opts>, state: Data<State>, req: HttpRequest) -> impl Responder {
    let path = urlencoding::decode_binary(req.path().as_bytes());

    debug!(path=?BStr::new(&path), "incoming request");

    let listing = path.ends_with(b"/");
    let path = path.as_ref().trim_start_with(|c| c == '/');
    if listing {
        Either::Left(listing_handler(&opts, &state, path))
    } else {
        Either::Right(file_handler(&opts, &state, path).await)
    }
}

/// Handler for listing requests.
fn listing_handler(opts: &Opts, state: &State, path: &[u8]) -> impl Responder {
    state.latest_index().map_or_else(
        || Either::Right(HttpResponse::NotFound()),
        |latest| {
            let path = urlencoding::encode_binary(path);
            let s3_base = opts.s3_base.trim_end_matches('/');
            Either::Left(Redirect::to(format!(
                "{s3_base}/listing-{latest}/{path}index.html"
            )))
        },
    )
}

/// Handler for file requests.
async fn file_handler(opts: &Opts, state: &State, path: &[u8]) -> impl Responder {
    let s3_base = opts.s3_base.trim_end_matches('/');
    let hash = state.lookup_hash_of_path(path).await.into_resp_err()?;
    Ok::<_, ReportWrapper>(hash.map_or_else(
        || Either::Right(HttpResponse::NotFound()),
        |hash| Either::Left(Redirect::to(format!("{s3_base}/{:x}", hash.as_hex()))),
    ))
}
