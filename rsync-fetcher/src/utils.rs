use std::fmt::LowerHex;
use std::time::{SystemTime, UNIX_EPOCH};

use blake2::Blake2b;
use digest::consts::U20;
use digest::Digest;

pub trait ToHex {
    fn as_hex(&self) -> HexWrapper<'_>;
}

impl ToHex for [u8] {
    fn as_hex(&self) -> HexWrapper<'_> {
        HexWrapper(self)
    }
}

pub struct HexWrapper<'a>(&'a [u8]);

impl LowerHex for HexWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

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
