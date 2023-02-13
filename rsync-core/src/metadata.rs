use std::time::SystemTime;

use bincode::config::Configuration;
use bincode::{Decode, Encode};
use redis::{FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value};

const BINCODE_CONFIG: Configuration = bincode::config::standard();

#[derive(Debug, Clone, Encode, Decode)]
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

#[derive(Debug, Clone, Encode, Decode)]
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

impl FromRedisValue for Metadata {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Data(data) => {
                let (metadata, _) =
                    bincode::decode_from_slice(data, BINCODE_CONFIG).map_err(|e| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Response data not valid metadata",
                            e.to_string(),
                        ))
                    })?;
                Ok(metadata)
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
            ))),
        }
    }
}

impl ToRedisArgs for Metadata {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let buf = bincode::encode_to_vec(self, BINCODE_CONFIG).expect("bincode encode failed");
        out.write_arg(&buf);
    }
}
