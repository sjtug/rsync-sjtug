use eyre::Result;
use futures::{future, Stream, TryStreamExt};
use redis::{aio, AsyncCommands};

use rsync_core::redis_::async_iter_to_stream;

/// Iterate through all index.
///
/// # Errors
///
/// Returns an error if failed to communicate with Redis.
pub async fn scan_index<'a>(
    redis: &'a mut (impl aio::ConnectionLike + Send),
    namespace: &'a str,
) -> Result<impl Stream<Item = Result<String>> + 'a> {
    let keys = async_iter_to_stream(
        redis
            .scan_match::<_, Vec<u8>>(format!("{namespace}:*"))
            .await?,
    );
    Ok(keys
        .try_filter_map(|k| async move { Ok(String::from_utf8(k).ok()) })
        .try_filter(|k| future::ready(!k.ends_with(":lock"))))
}
