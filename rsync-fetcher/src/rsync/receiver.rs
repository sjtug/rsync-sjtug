use std::cmp::Ordering;
use std::io::SeekFrom;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use blake2::Blake2b;
use digest::consts::U20;
use digest::Digest;
use eyre::{ensure, Result};
use indicatif::ProgressBar;
use md4::Md4;
use tempfile::{tempfile_in, TempDir};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tracing::{debug, info, instrument};

use rsync_core::utils::ToHex;

use crate::rsync::checksum::SumHead;
use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::file_list::FileEntry;
use crate::rsync::uploader::UploadTask;
use crate::utils::hash;

pub struct Receiver {
    rx: EnvelopeRead<BufReader<OwnedReadHalf>>,
    file_list: Arc<Vec<FileEntry>>,
    seed: i32,
    basis_dir: TempDir,
}

impl Receiver {
    // We are fine with this because it's a private constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rx: EnvelopeRead<BufReader<OwnedReadHalf>>,
        file_list: Arc<Vec<FileEntry>>,
        seed: i32,
        basis_dir: TempDir,
    ) -> Self {
        Self {
            rx,
            file_list,
            seed,
            basis_dir,
        }
    }
}

impl Deref for Receiver {
    type Target = EnvelopeRead<BufReader<OwnedReadHalf>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for Receiver {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

enum FileToken {
    Data(Vec<u8>),
    Copied(u32),
    Done,
}

#[derive(Debug)]
struct RecvResult {
    target_file: File,
    blake2b_hash: [u8; 20],
}

impl Receiver {
    pub async fn recv_task(
        &mut self,
        pb: ProgressBar,
        tx: flume::Sender<UploadTask>,
    ) -> Result<()> {
        info!("receiver started.");
        let mut phase = 0;
        loop {
            let idx = self.read_i32_le().await?;

            if idx == -1 {
                if phase == 0 {
                    phase += 1;
                    info!("recv file phase {}", phase);
                    continue;
                }
                break;
            }

            // Intended sign loss.
            #[allow(clippy::cast_sign_loss)]
            let idx = idx as usize;
            self.recv_file(idx, &pb, &tx).await?;
        }

        info!("recv finish");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn recv_file(
        &mut self,
        idx: usize,
        pb: &ProgressBar,
        tx: &flume::Sender<UploadTask>,
    ) -> Result<()> {
        let entry = &self.file_list[idx];
        debug!(file=%entry.name_lossy(), "receive file");

        // Get basis file if exists. It should be created by generator if delta transfer is
        // enabled and an old version of the file exists.
        let basis_file = self.try_get_basis_file(&entry.name).await?;

        // Receive file data.
        let RecvResult {
            target_file,
            blake2b_hash,
        } = self.recv_data(basis_file, pb).await?;

        tx.send_async(UploadTask {
            idx,
            blake2b_hash,
            file: target_file,
        })
        .await?;

        Ok(())
    }

    async fn try_get_basis_file(&self, path: &[u8]) -> Result<Option<File>> {
        let basis_path = self
            .basis_dir
            .path()
            .join(format!("{:x}", hash(path).as_hex()));
        Ok(File::open(&basis_path).await.map(Some).or_else(|f| {
            if f.kind() == std::io::ErrorKind::NotFound {
                Ok(None)
            } else {
                Err(f)
            }
        })?)
    }

    async fn recv_data(
        &mut self,
        mut local_basis: Option<File>,
        pb: &ProgressBar,
    ) -> Result<RecvResult> {
        let SumHead {
            checksum_count,
            block_len,
            checksum_len: _,
            remainder_len,
        } = SumHead::read_from(&mut **self).await?;

        let mut target_file = File::from_std(tempfile_in(&self.basis_dir)?);

        // Hasher for final file consistency check.
        let mut md4_hasher = Md4::default();
        md4_hasher.update(self.seed.to_le_bytes());
        // Hasher for content addressing. Hash function is blake2b-160.
        let mut blake2b_hasher = Blake2b::<U20>::default();

        let (mut transferred, mut copied) = (0u64, 0u64);
        loop {
            let token = self.recv_token().await?;
            match token {
                FileToken::Data(data) => {
                    transferred += data.len() as u64;
                    pb.inc(data.len() as u64);

                    md4_hasher.update(&data);
                    blake2b_hasher.update(&data);
                    target_file.write_all(&data).await?;
                }
                // We interpret sum head values as unsigned ints anyway.
                #[allow(clippy::cast_sign_loss)]
                FileToken::Copied(block_offset) => {
                    let offset = u64::from(block_offset) * block_len as u64;
                    let data_len =
                        if block_offset == checksum_count as u32 - 1 && remainder_len != 0 {
                            remainder_len
                        } else {
                            block_len
                        };
                    copied += data_len as u64;
                    pb.inc(data_len as u64);

                    let mut buf = vec![0; data_len as usize];
                    let local_basis = local_basis.as_mut().expect("incremental");
                    local_basis.seek(SeekFrom::Start(offset)).await?;
                    local_basis.read_exact(&mut buf).await?;

                    md4_hasher.update(&buf);
                    blake2b_hasher.update(&buf);
                    target_file.write_all(&buf).await?;
                }
                FileToken::Done => break,
            }
        }

        let local_checksum = md4_hasher.finalize(); // we don't need constant time comparison here
        let mut remote_checksum = vec![0; local_checksum.len()];

        self.read_exact(&mut remote_checksum).await?;
        ensure!(*local_checksum == remote_checksum, "checksum mismatch");

        // A debug log anyway.
        #[allow(clippy::cast_precision_loss)]
        let (transferred, total) = (transferred as f64, (transferred + copied) as f64);
        debug!(ratio = transferred / total, "transfer ratio");

        // No need to set perms because we'll upload it to s3.

        let hash: [u8; 20] = blake2b_hasher.finalize().into();

        target_file.seek(SeekFrom::Start(0)).await?;
        Ok(RecvResult {
            target_file,
            blake2b_hash: hash,
        })
    }

    async fn recv_token(&mut self) -> Result<FileToken> {
        let token = self.read_i32_le().await?;
        match token.cmp(&0) {
            Ordering::Equal => Ok(FileToken::Done),
            Ordering::Greater => {
                // Token is guaranteed to be positive.
                #[allow(clippy::cast_sign_loss)]
                let mut buf = vec![0; token as usize];
                self.read_exact(&mut buf).await?;
                Ok(FileToken::Data(buf))
            }
            // value < 0 => value + 1 <= 0 => -(value + 1) >= 0
            #[allow(clippy::cast_sign_loss)]
            Ordering::Less => Ok(FileToken::Copied(-(token + 1) as u32)),
        }
    }
}
