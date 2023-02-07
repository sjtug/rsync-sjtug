use std::time::{SystemTime, UNIX_EPOCH};

use blake2::Blake2b;
use digest::consts::U20;
use digest::Digest;

pub fn hash(data: &[u8]) -> [u8; 20] {
    let mut hasher = Blake2b::<U20>::default();
    hasher.update(data);
    hasher.finalize().into()
}

pub fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is before UNIX epoch")
        .as_secs()
}
