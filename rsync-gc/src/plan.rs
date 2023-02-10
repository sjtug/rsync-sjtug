use std::collections::HashSet;
use std::iter;

use eyre::Result;
use itertools::Either;
use redis::{aio, AsyncCommands};

use rsync_core::metadata::{MetaExtra, Metadata};

pub async fn hashes_to_remove(
    redis: &mut (impl aio::ConnectionLike + Send),
    namespace: &str,
    stale_indices: &[u64],
    alive_indices: &[u64],
    delete_partial: bool,
) -> Result<HashSet<[u8; 20]>> {
    let stale_indices = stale_indices
        .iter()
        .map(|index| format!("{namespace}:stale:{index}"))
        // We also need to remove partial-stale index.
        .chain(iter::once(format!("{namespace}:partial-stale")))
        // May need to delete partial.
        .chain(if delete_partial {
            Either::Left(iter::once(format!("{namespace}:partial")))
        } else {
            Either::Right(iter::empty())
        });
    let alive_indices = alive_indices
        .iter()
        .map(|index| format!("{namespace}:index:{index}"));

    // to_remove = Sigma_(stale) (key.hash) - Sigma_(alive) (key.hash)
    let mut to_remove = HashSet::new();

    for index in stale_indices {
        let mut stream = redis.hscan::<_, (Vec<u8>, Metadata)>(index).await?;
        while let Some((_, entry)) = stream.next_item().await? {
            if let MetaExtra::Regular { blake2b_hash } = entry.extra {
                to_remove.insert(blake2b_hash);
            }
        }
    }

    for index in alive_indices {
        let mut stream = redis.hscan::<_, (Vec<u8>, Metadata)>(index).await?;
        while let Some((_, entry)) = stream.next_item().await? {
            if let MetaExtra::Regular { blake2b_hash } = entry.extra {
                to_remove.remove(&blake2b_hash);
            }
        }
    }

    Ok(to_remove)
}
