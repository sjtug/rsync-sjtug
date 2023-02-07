use std::cmp::min;
use std::ffi::OsStr;
use std::io::SeekFrom;
use std::ops::{Deref, DerefMut};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use eyre::Result;
use indicatif::ProgressBar;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{debug, info, warn};

use rsync_core::utils::ToHex;

use crate::opts::S3Opts;
use crate::plan::TransferItem;
use crate::rsync::checksum::{checksum_1, checksum_2, SumHead};
use crate::rsync::file_list::FileEntry;
use crate::utils::hash;

/// Generator sends requests to rsync server.
pub struct Generator {
    tx: OwnedWriteHalf,
    file_list: Arc<Vec<FileEntry>>,
    seed: i32,
    s3: aws_sdk_s3::Client,
    s3_opts: S3Opts,
    basis_dir: PathBuf,
}

impl Generator {
    pub const fn new(
        tx: OwnedWriteHalf,
        file_list: Arc<Vec<FileEntry>>,
        seed: i32,
        s3: aws_sdk_s3::Client,
        s3_opts: S3Opts,
        basis_dir: PathBuf,
    ) -> Self {
        Self {
            tx,
            file_list,
            seed,
            s3,
            s3_opts,
            basis_dir,
        }
    }
}

impl Deref for Generator {
    type Target = OwnedWriteHalf;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl DerefMut for Generator {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl Generator {
    pub async fn generate_task(
        &mut self,
        transfer_plan: &[TransferItem],
        pb: ProgressBar,
    ) -> Result<()> {
        info!("generator started.");

        info!("generate file phase 1");
        for entry in transfer_plan {
            self.recv_generator(entry, &pb).await?;
        }

        self.write_i32_le(-1).await?;

        // TODO phase 2: re-do failed files
        info!("generate file phase 2");
        self.write_i32_le(-1).await?;

        info!("generator finish");
        Ok(())
    }
    async fn recv_generator(&mut self, item: &TransferItem, pb: &ProgressBar) -> Result<()> {
        let entry = &self.file_list[item.idx as usize];
        let idx = entry.idx;
        let path = Path::new(OsStr::from_bytes(&entry.name));

        if unix_mode::is_dir(entry.mode) {
            // No need to create dir in S3 storage.
            return Ok(());
        }

        // We skip all non-regular files. Symlinks are skipped too.
        if !unix_mode::is_file(entry.mode) {
            warn!(?path, "skip non-regular file");
            return Ok(());
        }

        pb.inc_length(entry.len);
        if let Some(blake2b_hash) = &item.blake2b_hash {
            // We have a basis file. Use it to initiate a delta transfer
            debug!(?path, idx, "requesting partial file");

            debug!(?path, hash=%format!("{:x}", blake2b_hash.as_hex()), "download basis file");
            let basis_file = self.download_basis_file(blake2b_hash, &entry.name).await?;

            self.write_u32_le(idx).await?;
            self.generate_and_send_sums(basis_file).await?;
        } else {
            debug!(?path, idx, "requesting full file");

            self.write_u32_le(idx).await?;
            SumHead::default().write_to(&mut **self).await?;
        }

        Ok(())
    }
    async fn download_basis_file(&self, blake2b_hash: &[u8; 20], path: &[u8]) -> Result<File> {
        let basis_path = self.basis_dir.join(format!("{:x}", hash(path).as_hex()));
        let mut basis_file = File::create(basis_path).await?;
        let obj = self
            .s3
            .get_object()
            .bucket(&self.s3_opts.bucket)
            .key(format!(
                "{}{:x}",
                self.s3_opts.prefix,
                blake2b_hash.as_hex()
            ))
            .send()
            .await?;
        let mut body = BufReader::new(obj.body.into_async_read());
        tokio::io::copy(&mut body, &mut basis_file).await?;

        basis_file.seek(SeekFrom::Start(0)).await?;
        Ok(basis_file)
    }
    async fn generate_and_send_sums(&mut self, mut file: File) -> Result<()> {
        let file_len = file.metadata().await?.size();
        let sum_head = SumHead::sum_sizes_sqroot(file_len);
        sum_head.write_to(&mut **self).await?;

        // Sqrt of usize can't be negative.
        #[allow(clippy::cast_sign_loss)]
        let mut buf = vec![0u8; sum_head.block_len as usize];
        let mut remaining = file_len;

        for _ in 0..sum_head.checksum_count {
            // Sqrt of usize must be in u32 range.
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let n1 = min(sum_head.block_len as u64, remaining) as usize;
            let buf_slice = &mut buf[..n1];
            file.read_exact(buf_slice).await?;

            let sum1 = checksum_1(buf_slice);
            let sum2 = checksum_2(self.seed, buf_slice);
            // The original implementation reads sum1 as i32, but casting it to i32 is a no-op anyway.
            self.write_u32_le(sum1).await?;
            self.write_all(&sum2).await?;

            remaining -= n1 as u64;
        }

        Ok(())
    }
}
