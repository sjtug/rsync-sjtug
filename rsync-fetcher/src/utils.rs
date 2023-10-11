use std::fmt::{Debug, Formatter};
use std::ops::Add;

use blake2::Blake2b;
use bytesize::ByteSize;
use digest::consts::U20;
use digest::Digest;
use tracing::warn;

use rsync_core::utils::ToHex;

use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;

pub fn hash(data: &[u8]) -> [u8; 20] {
    let mut hasher = Blake2b::<U20>::default();
    hasher.update(data);
    hasher.finalize().into()
}

pub fn ignore_mode(mode: u32, path: Option<impl Debug>) -> bool {
    if unix_mode::is_dir(mode) {
        // No need to create dir in S3 storage.
        return true;
    }

    // We skip all non-regular files, including directories and symlinks.
    if !unix_mode::is_file(mode) {
        if !(unix_mode::is_symlink(mode) || unix_mode::is_dir(mode)) {
            if let Some(path) = path {
                warn!(?path, mode = format!("{mode:o}"), "skip special file");
            }
        }
        return true;
    }

    false
}

#[derive(Copy, Clone, Default)]
pub struct PlannedTransfer {
    pub total_bytes: u64,
    pub total_count: u64,
    pub full_bytes: u64,
    pub full_count: u64,
    pub partial_bytes: u64,
    pub partial_count: u64,
}

impl Debug for PlannedTransfer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannedTransfer")
            .field("total_bytes", &ByteSize::b(self.total_bytes).to_string())
            .field("total_count", &self.total_count)
            .field("full_bytes", &ByteSize::b(self.full_bytes).to_string())
            .field("full_count", &self.full_count)
            .field(
                "partial_bytes",
                &ByteSize::b(self.partial_bytes).to_string(),
            )
            .field("partial_count", &self.partial_count)
            .finish()
    }
}

impl Add for PlannedTransfer {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            total_bytes: self.total_bytes + rhs.total_bytes,
            total_count: self.total_count + rhs.total_count,
            full_bytes: self.full_bytes + rhs.full_bytes,
            full_count: self.full_count + rhs.full_count,
            partial_bytes: self.partial_bytes + rhs.partial_bytes,
            partial_count: self.partial_count + rhs.partial_count,
        }
    }
}

pub fn plan_stat(files: &[FileEntry], transfer_items: &[TransferItem]) -> PlannedTransfer {
    transfer_items
        .iter()
        .filter_map(|item| {
            #[allow(clippy::cast_sign_loss)]
            let entry = &files[item.idx as usize];
            let path = entry.name_lossy();
            (!ignore_mode(entry.mode, Some(path))).then(|| {
                if item.blake2b.is_some() {
                    PlannedTransfer {
                        total_bytes: entry.len,
                        total_count: 1,
                        full_bytes: 0,
                        full_count: 0,
                        partial_bytes: entry.len,
                        partial_count: 1,
                    }
                } else {
                    PlannedTransfer {
                        total_bytes: entry.len,
                        total_count: 1,
                        full_bytes: entry.len,
                        full_count: 1,
                        partial_bytes: 0,
                        partial_count: 0,
                    }
                }
            })
        })
        .reduce(Add::add)
        .unwrap_or_default()
}

pub fn flatten_err<T, E1, E2>(t: Result<Result<T, E1>, E2>) -> eyre::Result<T>
where
    E1: Into<eyre::Report> + Send + Sync + 'static,
    E2: Into<eyre::Report> + Send + Sync + 'static,
{
    t.map_err(Into::into)?.map_err(Into::into)
}

/// Clean namespace name as a valid SQL table name.
pub fn namespace_as_table(namespace: &str) -> String {
    format!("h{:x}", hash(namespace.as_bytes()).as_hex())
}
