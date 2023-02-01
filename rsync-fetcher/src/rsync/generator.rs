use std::cmp::min;
use std::ffi::OsStr;
use std::ops::{Deref, DerefMut};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use eyre::Result;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::WriteHalf;
use tracing::info;

use crate::rsync::checksum::{checksum_1, checksum_2, SumHead};
use crate::rsync::file_list::FileEntry;

pub struct Generator<'a> {
    tx: WriteHalf<'a>,
    seed: i32,
}

impl<'a> Generator<'a> {
    pub const fn new(tx: WriteHalf<'a>, seed: i32) -> Self {
        Self { tx, seed }
    }
}

impl<'a> Deref for Generator<'a> {
    type Target = WriteHalf<'a>;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<'a> DerefMut for Generator<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

impl<'a> Generator<'a> {
    pub async fn generate_task(&mut self, file_list: &[FileEntry]) -> Result<()> {
        for entry in file_list {
            self.recv_generator(entry).await?;
        }

        info!("generate file phase 1");
        self.write_i32_le(-1).await?;

        // TODO phase 2: re-do failed files
        info!("generate file phase 2");
        self.write_i32_le(-1).await?;

        info!("generator finish");
        Ok(())
    }
    async fn recv_generator(&mut self, entry: &FileEntry) -> Result<()> {
        let filename = Path::new(OsStr::from_bytes(&entry.name));
        // TODO s3 impl: merge s3 file index and local partial index, compare to s3, and generate missing files.

        // NOTE the following impl doesn't consider
        // 1. expect file, but dir exists
        // 2. permission incorrect
        // 3. soft links & hardlinks
        // 4. non regular files

        if unix_mode::is_dir(entry.mode) {
            // No need to create dir in S3 storage.
            return Ok(());
        }

        // TODO we skip all non-regular files
        if !unix_mode::is_file(entry.mode) {
            return Ok(());
        }

        // TODO check if skip file by metadata
        // check if skip file
        // let meta = tokio::fs::metadata(opts.dest.join(filename))
        //     .await
        //     .map(|m| Some(m))
        //     .or_else(|e| {
        //         if e.kind() == std::io::ErrorKind::NotFound {
        //             Ok(None)
        //         } else {
        //             Err(e)
        //         }
        //     })?;
        // if let Some(meta) = meta {
        //     if meta.size() == entry.len && mod_time_eq(meta.modified()?, entry.modify_time) {
        //         return Ok(());
        //     }
        // }

        // TODO if delta transfer is enabled, download file from s3 and generate hash.
        // if let Some(f) = File::open(opts.dest.join(filename)).await.ok() {
        //     info!(?filename, idx = entry.idx, "requesting partial file");
        //     // incremental mode
        //     self.write_i32_le(entry.idx).await?;
        //     self.generate_and_send_sums(f).await?;
        // } else {
        info!(?filename, idx = entry.idx, "requesting full file");
        // full mode
        self.write_i32_le(entry.idx).await?;
        SumHead::default().write_to(&mut **self).await?;
        // }

        Ok(())
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
