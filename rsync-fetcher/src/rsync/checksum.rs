use std::cmp::min;
use std::fs::File;
use std::io::Read;

use eyre::Result;
use md4::{Digest, Md4};
use multiversion::multiversion;
use num::integer::Roots;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

mod md4_simd;

#[derive(Debug, Copy, Clone, Default)]
pub struct SumHead {
    pub checksum_count: i32,
    pub block_len: i32,
    pub checksum_len: i32,
    pub remainder_len: i32,
}

const BLOCK_SIZE: u64 = 700;
const MAX_BLOCK_SIZE: u64 = 1 << 29;

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

            b.clamp(BLOCK_SIZE, MAX_BLOCK_SIZE)
        };

        // TODO blocksum_bits = BLOCKSUM_EXP + 2*log2(file_len) - log2(block_len)
        let checksum_len = 16;

        Self {
            checksum_count: i32::try_from(len.div_ceil(block_len)).expect("overflow"),
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

#[multiversion(targets = "simd")]
pub fn checksum_1(buf: &[u8]) -> u32 {
    let mut s1: u32 = 0;
    let mut s2: u32 = 0;

    let len = u32::try_from(buf.len()).expect("overflow");

    for (idx, b) in buf.iter().enumerate() {
        let b = sign_extend(*b);
        s1 = s1.wrapping_add(b);
        s2 = s2.wrapping_add(b.wrapping_mul(len.wrapping_sub(
            #[allow(clippy::cast_possible_truncation)] // checked above
            {
                idx as u32
            },
        )));
    }

    (s1 & 0xffff) + (s2 << 16)
}

pub fn checksum_2(seed: i32, buf: &[u8]) -> Vec<u8> {
    let mut hasher = Md4::default();
    hasher.update(buf);
    hasher.update(seed.to_le_bytes());
    hasher.finalize().to_vec()
}

pub fn checksum_payload(sum_head: SumHead, seed: i32, file: &mut File, file_len: u64) -> Vec<u8> {
    checksum_payload_(sum_head, seed, file, file_len, true)
}

#[cfg(test)]
pub fn checksum_payload_basic(
    sum_head: SumHead,
    seed: i32,
    file: &mut File,
    file_len: u64,
) -> Vec<u8> {
    checksum_payload_(sum_head, seed, file, file_len, false)
}

#[inline]
#[allow(clippy::cast_sign_loss)] // block_len and checksum_count are always positive.
fn checksum_payload_(
    sum_head: SumHead,
    seed: i32,
    file: &mut File,
    file_len: u64,
    enable_simd: bool,
) -> Vec<u8> {
    let mut file_remaining = file_len;

    let mut buf_sum = Vec::with_capacity(sum_head.checksum_count as usize * 20);
    let mut block_remaining = sum_head.checksum_count as usize;

    if enable_simd {
        if let Some(simd_impl) = md4_simd::simd::Md4xN::select() {
            // Sqrt of usize can't be negative.
            let mut bufs: [_; md4_simd::simd::MAX_LANES] =
                array_init::array_init(|_| vec![0u8; sum_head.block_len as usize + 4]);

            while block_remaining >= simd_impl.lanes() {
                if file_remaining < sum_head.block_len as u64 * simd_impl.lanes() as u64 {
                    // not enough data for simd
                    break;
                }

                let mut datas: [&[u8]; md4_simd::simd::MAX_LANES] =
                    [&[]; md4_simd::simd::MAX_LANES];
                for (idx, buf) in bufs[0..simd_impl.lanes()].iter_mut().enumerate() {
                    // let buf = &mut bufs[idx];

                    // Sqrt of usize must be in u32 range.
                    #[allow(clippy::cast_possible_truncation)]
                    let n1 = sum_head.block_len as usize;

                    file.read_exact(&mut buf[..n1]).expect("IO error");

                    file_remaining -= n1 as u64;
                    let buf_slice = &mut buf[..n1 + 4];
                    buf_slice[n1..].copy_from_slice(&seed.to_le_bytes());

                    datas[idx] = buf_slice;
                }

                let md4_hashes = simd_impl.md4(&datas);

                for (idx, block) in datas[..simd_impl.lanes()].iter().enumerate() {
                    // fast checksum (must remove seed from buffer)
                    buf_sum.extend_from_slice(&checksum_1(&block[..block.len() - 4]).to_le_bytes());
                    // slow checksum
                    buf_sum.extend_from_slice(&md4_hashes[idx]);
                }

                block_remaining -= simd_impl.lanes();
            }
            // final block shares the code with non-simd implementation
        }
    }

    // Sqrt of usize can't be negative.
    let mut buf = vec![0u8; sum_head.block_len as usize + 4];
    for _ in 0..block_remaining {
        // Sqrt of usize must be in u32 range.
        #[allow(clippy::cast_possible_truncation)]
        let n1 = min(sum_head.block_len as u64, file_remaining) as usize;
        let buf_slice = &mut buf[..n1];

        file.read_exact(buf_slice).expect("IO error");

        file_remaining -= n1 as u64;

        buf_sum.extend_from_slice(&checksum_1(buf_slice).to_le_bytes());
        buf_sum.extend_from_slice(&checksum_2(seed, buf_slice));
    }

    buf_sum
}

#[cfg(test)]
mod tests {
    #![allow(clippy::ignored_unit_patterns)]
    use std::io::{Seek, SeekFrom, Write};

    use proptest::prop_assert_eq;
    use tempfile::tempfile;
    use test_strategy::proptest;

    use crate::rsync::checksum::{SumHead, checksum_payload, checksum_payload_basic};

    #[inline]
    fn must_checksum_payload_basic_eq_simd_(data: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let file_len = data.len() as u64;

        let mut f = tempfile().expect("tempfile");
        f.write_all(data).expect("write_all");
        f.seek(SeekFrom::Start(0)).expect("seek");

        let sum_head = SumHead::sum_sizes_sqroot(file_len);

        let chksum_simd = checksum_payload(sum_head, 0, &mut f, file_len);
        f.seek(SeekFrom::Start(0)).expect("seek");
        let chksum_basic = checksum_payload_basic(sum_head, 0, &mut f, file_len);
        (chksum_simd, chksum_basic)
    }

    #[proptest]
    fn must_checksum_payload_basic_eq_simd(data: Vec<u8>) {
        let (chksum_simd, chksum_basic) = must_checksum_payload_basic_eq_simd_(&data);
        prop_assert_eq!(chksum_simd, chksum_basic);
    }

    #[test]
    fn checksum_payload_simd_regression_1() {
        let data = vec![0u8; 11199];
        let (chksum_simd, chksum_basic) = must_checksum_payload_basic_eq_simd_(&data);
        assert_eq!(chksum_simd, chksum_basic);
    }
}
