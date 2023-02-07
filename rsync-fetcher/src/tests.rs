use std::convert::Infallible;
use std::str::FromStr;
use std::time::SystemTime;

use crate::opts::RedisOpts;
use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;

impl TransferItem {
    pub const fn new(idx: u32, hash: Option<[u8; 20]>) -> Self {
        Self {
            idx,
            blake2b_hash: hash,
        }
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
}

impl FromStr for RedisOpts {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            namespace: s.to_string(),
            force_break: false,
            lock_ttl: 5,
        })
    }
}
