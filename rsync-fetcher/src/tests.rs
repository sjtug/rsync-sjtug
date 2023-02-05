use std::convert::Infallible;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use redis::Connection;

use crate::opts::RedisOpts;
use crate::plan::{Metadata, MetaExtra, TransferItem};
use crate::rsync::file_list::FileEntry;

pub fn generate_random_namespace() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let random = rand::random::<u64>();
    format!("test_{timestamp}_{random}")
}

pub struct MetadataIndex {
    conn: Connection,
    prefix: String,
}

impl Drop for MetadataIndex {
    fn drop(&mut self) {
        let prefix = &self.prefix;
        let _ = redis::pipe()
            .del(format!("{prefix}:hash"))
            .del(format!("{prefix}:zset"))
            .query::<()>(&mut self.conn);
    }
}

impl MetadataIndex {
    pub fn new(client: &redis::Client, prefix: &str, items: &[(String, Metadata)]) -> Self {
        let mut conn = client.get_connection().unwrap();

        let hash_key = format!("{prefix}:hash");
        let zset_key = format!("{prefix}:zset");

        let mut pipe = redis::pipe();
        for (path, metadata) in items {
            pipe.hset(&hash_key, path, metadata);
            pipe.zadd(&zset_key, path, 0);
        }

        pipe.query::<()>(&mut conn).unwrap();

        Self {
            conn,
            prefix: prefix.to_string(),
        }
    }
}

impl TransferItem {
    pub const fn new(idx: u32, hash: Option<[u8; 20]>) -> Self {
        Self {
            idx,
            blake2b_hash: hash,
        }
    }
}

impl Metadata {
    pub const fn regular(len: u64, modify_time: SystemTime, hash: [u8; 20]) -> Self {
        Self {
            len,
            modify_time,
            extra: MetaExtra::Regular { blake2b_hash: hash },
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
        })
    }
}