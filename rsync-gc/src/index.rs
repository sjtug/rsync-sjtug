use eyre::Result;
use futures::TryStreamExt;
use redis::aio;

use rsync_core::redis_::live_index;

pub struct ScanIndex {
    pub delete_before: u64,
    pub keep: Vec<u64>,
    pub delete: Vec<u64>,
}

/// Iterate through all live index and classify them into two groups: to be kept and to be deleted.
pub async fn scan_live_index(
    conn: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    keep_count: usize,
) -> Result<ScanIndex> {
    let mut index: Vec<_> = live_index(conn, namespace).await?.try_collect().await?;

    index.sort_unstable();

    let (delete, keep) = index.split_at(index.len().saturating_sub(keep_count));
    let delete_before = keep.first().copied().unwrap_or(u64::MAX);

    Ok(ScanIndex {
        delete_before,
        keep: keep.to_vec(),
        delete: delete.to_vec(),
    })
}

/// Mark all index to be deleted as stale.
pub async fn rename_to_stale(
    conn: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    index: &[u64],
) -> Result<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();

    // No need to rollback because this won't break consistency.
    for i in index {
        pipe.rename_nx(
            format!("{namespace}:index:{i}"),
            format!("{namespace}:stale:{i}"),
        );
    }

    let result: Vec<bool> = pipe.query_async(conn).await?;
    for idx in result
        .into_iter()
        .enumerate()
        .filter_map(|(i, r)| if r { None } else { Some(i) })
    {
        tracing::warn!("failed to rename index {} to stale", index[idx]);
    }

    Ok(())
}

pub async fn commit_gc(
    conn: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    index: &[u64],
) -> Result<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();

    // No need to rollback because this won't break consistency.
    for i in index {
        pipe.del(format!("{namespace}:stale:{i}"));
    }
    pipe.del(format!("{namespace}:partial-stale"));

    let result: Vec<bool> = pipe.query_async(conn).await?;
    for idx in result
        .into_iter()
        .enumerate()
        .filter_map(|(i, r)| if r { None } else { Some(i) })
    {
        // It's okay if we failed to delete partial-stale because it may not exist.
        if idx < index.len() {
            tracing::warn!("failed to delete stale index {}", index[idx]);
        }
    }

    Ok(())
}
