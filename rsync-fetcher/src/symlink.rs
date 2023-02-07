use eyre::Result;
use redis::aio;
use tracing::info;

use rsync_core::metadata::{MetaExtra, Metadata};
use rsync_core::redis_::update_metadata;

use crate::opts::RedisOpts;
use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;

pub async fn apply_symlinks(
    redis: &mut impl aio::ConnectionLike,
    opts: &RedisOpts,
    file_list: &[FileEntry],
    plan: &[TransferItem],
) -> Result<()> {
    info!("writing symlinks to metadata index");
    for item in plan {
        let entry = &file_list[item.idx as usize];
        if let Some(link_target) = &entry.link_target {
            let metadata = Metadata {
                len: entry.len,
                modify_time: entry.modify_time,
                extra: MetaExtra::Symlink {
                    target: link_target.clone(),
                },
            };
            update_metadata(
                redis,
                &format!("{}:partial", opts.namespace),
                &entry.name,
                metadata,
            )
            .await?;
        }
    }
    Ok(())
}
