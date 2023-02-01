use std::cmp::max;

use eyre::Result;
use md4::{Digest, Md4};
use num::integer::Roots;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Debug, Default)]
pub struct SumHead {
    pub checksum_count: i32,
    pub block_len: i32,
    pub checksum_len: i32,
    pub remainder_len: i32,
}

const BLOCK_SIZE: u64 = 700;

impl SumHead {
    pub fn sum_sizes_sqroot(len: u64) -> Self {
        let block_len = if len <= BLOCK_SIZE * BLOCK_SIZE {
            BLOCK_SIZE
        } else {
            // Note: this is different from original implementation.
            // We use floor sqrt instead of ceil sqrt.
            let b = len.sqrt();

            // For unknown reason this value is rounded up to a multiple of 8.
            // This won't overflow because sqrt of u64 must be much smaller than u64::MAX.
            let b = (b + 7) & !7;

            max(b, BLOCK_SIZE)
        };

        // TODO blocksum_bits = BLOCKSUM_EXP + 2*log2(file_len) - log2(block_len)
        let checksum_len = 16;

        Self {
            checksum_count: i32::try_from((len + block_len - 1) / block_len).expect("overflow"),
            block_len: i32::try_from(block_len).expect("overflow"),
            checksum_len,
            remainder_len: i32::try_from(len % block_len).expect("overflow"),
        }
    }
    pub async fn read_from(mut rx: impl AsyncRead + Unpin) -> Result<Self> {
        let checksum_count = rx.read_i32_le().await?;
        let block_len = rx.read_i32_le().await?;
        let checksum_len = rx.read_i32_le().await?;
        let remainder_len = rx.read_i32_le().await?;
        Ok(Self {
            checksum_count,
            block_len,
            checksum_len,
            remainder_len,
        })
    }
    pub async fn write_to(&self, mut tx: impl AsyncWrite + Unpin) -> Result<()> {
        tx.write_i32_le(self.checksum_count).await?;
        tx.write_i32_le(self.block_len).await?;
        tx.write_i32_le(self.checksum_len).await?;
        tx.write_i32_le(self.remainder_len).await?;
        Ok(())
    }
}

#[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
const fn sign_extend(x: u8) -> u32 {
    // The original implementation converts signed char to uint32_t, and this conversion uses sign extension in C.
    // This might not be conscious, but we need to follow the original implementation.
    x as i8 as u32
}

pub fn checksum_1(buf: &[u8]) -> u32 {
    // Reference: https://github.com/kristapsdz/openrsync/blob/c83683ab5d4fb8d4c466fc74affe005784220d23/hash.c
    let mut s1: u32 = 0;
    let mut s2: u32 = 0;

    for chunk in buf.chunks(4) {
        if chunk.len() == 4 {
            s2 = s2.wrapping_add(
                (s1.wrapping_add(sign_extend(chunk[0])).wrapping_mul(4))
                    .wrapping_add(sign_extend(chunk[1]).wrapping_mul(3))
                    .wrapping_add(sign_extend(chunk[2]).wrapping_mul(2))
                    .wrapping_add(sign_extend(chunk[3])),
            );
            s1 = s1.wrapping_add(
                sign_extend(chunk[0])
                    .wrapping_add(sign_extend(chunk[1]))
                    .wrapping_add(sign_extend(chunk[2]))
                    .wrapping_add(sign_extend(chunk[3])),
            );
        } else {
            for b in chunk.iter() {
                s1 = s1.wrapping_add(sign_extend(*b));
                s2 = s2.wrapping_add(s1);
            }
        }
    }

    (s1 & 0xffff) + (s2 << 16)
}

pub fn checksum_2(seed: i32, buf: &[u8]) -> Vec<u8> {
    let mut hasher = Md4::default();
    hasher.update(buf);
    hasher.update(seed.to_le_bytes());
    hasher.finalize().to_vec()
}
