use std::time::SystemTime;

use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;

impl TransferItem {
    pub const fn new(idx: i32, hash: Option<[u8; 20]>) -> Self {
        Self { idx, blake2b: hash }
    }
}

impl FileEntry {
    pub fn regular(name: String, len: u64, modify_time: SystemTime, idx: u32) -> Self {
        Self {
            name: name.into_bytes(),
            len,
            modify_time,
            mode: 0o0_100_777,
            link_target: None,
            idx,
        }
    }
    pub fn directory(name: String, len: u64, modify_time: SystemTime, idx: u32) -> Self {
        Self {
            name: name.into_bytes(),
            len,
            modify_time,
            mode: 0o0_040_777,
            link_target: None,
            idx,
        }
    }
    pub fn symlink(
        name: String,
        len: u64,
        link_target: String,
        modify_time: SystemTime,
        idx: u32,
    ) -> Self {
        Self {
            name: name.into_bytes(),
            len,
            modify_time,
            mode: 0o0_120_777,
            link_target: Some(link_target.into_bytes()),
            idx,
        }
    }
}
