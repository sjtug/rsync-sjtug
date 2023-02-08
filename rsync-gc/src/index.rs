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
    let delete_before = keep.first().copied().unwrap_or(0);

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

    // TODO rollback
    for i in index {
        pipe.rename_nx(
            format!("{namespace}:index:{i}"),
            format!("{namespace}:stale:{i}"),
        );
    }

    pipe.query_async::<_, Vec<bool>>(conn).await?;
    Ok(())
}

pub async fn commit_gc(
    conn: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    index: &[u64],
) -> Result<()> {
    let mut pipe = redis::pipe();
    pipe.atomic();

    // TODO rollback
    for i in index {
        pipe.del(format!("{namespace}:stale:{i}"));
    }
    // Partial-stale index might not exist.
    pipe.del(format!("{namespace}:partial-stale"));

    pipe.query_async::<_, Vec<bool>>(conn).await?;
    Ok(())
}
