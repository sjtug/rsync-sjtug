use std::time::Instant;

use bstr::ByteSlice;
use chrono::{DateTime, Utc};
use eyre::{Result, bail};
use get_size::GetSize;
use metrics::counter;
use opendal::Operator;
use rkyv::{Archive, Deserialize, Serialize};
use sqlx::{Acquire, Postgres};
use tracing::{error, instrument};

use rsync_core::utils::{ATTR_CHAR, ToHex};

use crate::cache::PRESIGN_TIMEOUT;
use crate::metrics::{
    COUNTER_RESOLVED_ERROR, COUNTER_RESOLVED_LISTING, COUNTER_RESOLVED_MISSING,
    COUNTER_RESOLVED_REGULAR,
};
use crate::pg::list_directory;
use crate::realpath::{RealpathError, ResolveError, Target, realpath};
use crate::utils::SkipRkyv;

/// A resolved result that can be rendered or redirected to.
#[derive(Debug, Clone, Eq, PartialEq, GetSize, Archive, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum Resolved {
    Directory {
        entries: Vec<ListingEntry>,
    },
    Regular {
        url: String,
        // Duration since UNIX epoch.
        #[with(SkipRkyv)]
        expired_at: Instant,
    },
    NotFound {
        reason: ResolveError,
    },
}

/// Resolve a path to a result that can be rendered or redirected to.
#[instrument(skip(path, db, op), fields(path = % String::from_utf8_lossy(path)))]
pub async fn resolve<'a>(
    namespace: &str,
    path: &[u8],
    revision: i32,
    s3_prefix: &str,
    list_hidden: bool,
    db: impl Acquire<'a, Database = Postgres> + Clone,
    op: &Operator,
) -> Result<Resolved> {
    Ok(match realpath(path, revision, db.clone()).await {
        Ok(Target::Directory(path)) => {
            let resolved = resolve_listing(&path, revision, list_hidden, db).await?;
            counter!(COUNTER_RESOLVED_LISTING).increment(1);
            resolved
        }
        Ok(Target::Regular(blake2b)) => {
            let filename = path.rsplit_once_str(b"/").map_or(path, |(_, s)| s);
            let encoded_name = percent_encoding::percent_encode(filename, ATTR_CHAR);
            let content_disposition =
                format!("attachment; filename=\"{encoded_name}\"; filename*=UTF-8''{encoded_name}");
            let s3_path = if s3_prefix.is_empty() {
                format!("{:x}", blake2b.as_hex())
            } else {
                format!("{s3_prefix}/{:x}", blake2b.as_hex())
            };
            // Expire the pre-signed URL halfway through its lifetime.
            let expired_at = Instant::now() + PRESIGN_TIMEOUT / 2;
            let presigned = op
                .presign_read_with(&s3_path, PRESIGN_TIMEOUT)
                .override_content_disposition(&content_disposition)
                .await?;
            counter!(COUNTER_RESOLVED_REGULAR).increment(1);
            Resolved::Regular {
                url: presigned.uri().to_string(),
                expired_at,
            }
        }
        Err(RealpathError::Resolve(e)) => {
            counter!(COUNTER_RESOLVED_MISSING).increment(1);
            Resolved::NotFound { reason: e }
        }
        Err(e) => {
            counter!(COUNTER_RESOLVED_ERROR).increment(1);
            error!(%e, "realpath error");
            bail!(e);
        }
    })
}

/// Resolve a directory listing.
async fn resolve_listing<'a>(
    path: &[u8],
    revision: i32,
    list_hidden: bool,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Resolved> {
    let mut entries = list_directory(path, revision, list_hidden, db).await?;
    entries.shrink_to_fit(); // shrink as much as we can to reduce cache memory usage
    Ok(Resolved::Directory { entries })
}

/// A single entry in a listing.
#[derive(Debug, Clone, Eq, PartialEq, GetSize, Archive, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct ListingEntry {
    #[cfg_attr(test, proptest(regex = "[^/\0]+"))]
    pub filename: Vec<u8>,
    pub len: Option<u64>,
    #[get_size(ignore)]
    #[cfg_attr(
        test,
        proptest(strategy = "proptest::option::of(crate::tests::datetime_strategy())")
    )]
    pub modify_time: Option<DateTime<Utc>>,
    pub is_dir: bool,
}
