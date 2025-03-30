use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub len: u64,
    pub modify_time: SystemTime,
    pub extra: MetaExtra,
}

impl Metadata {
    #[must_use]
    pub const fn regular(len: u64, modify_time: SystemTime, hash: [u8; 20]) -> Self {
        Self {
            len,
            modify_time,
            extra: MetaExtra::Regular { blake2b_hash: hash },
        }
    }
    #[must_use]
    pub fn symlink(len: u64, modify_time: SystemTime, target: &str) -> Self {
        Self {
            len,
            modify_time,
            extra: MetaExtra::Symlink {
                target: target.as_bytes().into(),
            },
        }
    }

    #[must_use]
    pub const fn directory(len: u64, modify_time: SystemTime) -> Self {
        Self {
            len,
            modify_time,
            extra: MetaExtra::Directory,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MetaExtra {
    Symlink { target: Vec<u8> },
    Regular { blake2b_hash: [u8; 20] },
    Directory,
}

impl MetaExtra {
    #[must_use]
    pub const fn regular(hash: [u8; 20]) -> Self {
        Self::Regular { blake2b_hash: hash }
    }
    #[must_use]
    pub const fn directory() -> Self {
        Self::Directory
    }
    #[must_use]
    pub fn symlink(target: &str) -> Self {
        Self::Symlink {
            target: target.as_bytes().into(),
        }
    }
}
