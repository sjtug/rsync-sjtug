use eyre::Result;
use redis::aio;
use tracing::info;

use rsync_core::metadata::{MetaExtra, Metadata};
use rsync_core::redis_::{update_metadata, RedisOpts};

use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;

pub async fn apply_symlinks_n_directories(
    redis: &mut impl aio::ConnectionLike,
    opts: &RedisOpts,
    file_list: &[FileEntry],
    plan: &[TransferItem],
) -> Result<()> {
    info!("writing symlinks & directories to metadata index");
    for item in plan {
        let entry = &file_list[item.idx as usize];
        let metadata = Metadata {
            len: entry.len,
            modify_time: entry.modify_time,
            extra: if let Some(link_target) = &entry.link_target {
                MetaExtra::Symlink {
                    target: link_target.clone(),
                }
            } else if unix_mode::is_dir(entry.mode) {
                MetaExtra::Directory
            } else {
                continue;
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
    Ok(())
}
