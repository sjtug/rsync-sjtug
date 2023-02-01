use std::cmp::Ordering;
use std::io::SeekFrom;
use std::ops::{Deref, DerefMut};

use blake2::Blake2b;
use digest::consts::U20;
use digest::Digest;
use eyre::{ensure, Result};
use md4::Md4;
use tempfile::tempfile;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::ReadHalf;
use tracing::info;

use crate::rsync::checksum::SumHead;
use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::file_list::FileEntry;

pub struct Receiver<'a> {
    rx: EnvelopeRead<BufReader<ReadHalf<'a>>>,
    seed: i32,
}

impl<'a> Receiver<'a> {
    pub const fn new(rx: EnvelopeRead<BufReader<ReadHalf<'a>>>, seed: i32) -> Self {
        Self { rx, seed }
    }
}

impl<'a> Deref for Receiver<'a> {
    type Target = EnvelopeRead<BufReader<ReadHalf<'a>>>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl<'a> DerefMut for Receiver<'a> {
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
    hash: [u8; 20],
}

impl<'a> Receiver<'a> {
    pub async fn recv_task(&mut self, file_list: &[FileEntry]) -> Result<()> {
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
            let entry = &file_list[idx as usize];
            info!("recv file #{} ({})", idx, entry.name_lossy());

            // TODO s3 impl download file from storage in this step.
            // let basis_path = opts.dest.join(Path::new(OsStr::from_bytes(&entry.name)));
            // let basis_file = File::open(&basis_path)
            //     .await
            //     .map(|f| Some(f))
            //     .or_else(|f| {
            //         if f.kind() == std::io::ErrorKind::NotFound {
            //             Ok(None)
            //         } else {
            //             Err(f)
            //         }
            //     })?;

            // let RecvResult{ target_file, hash } = self.recv_data(seed, basis_file).await?;

            // TODO s3 impl upload file to storage in this step.
        }

        info!("recv finish");
        Ok(())
    }

    async fn recv_data(&mut self, mut local_basis: Option<File>) -> Result<RecvResult> {
        let SumHead {
            checksum_count,
            block_len,
            checksum_len: _,
            remainder_len,
        } = SumHead::read_from(&mut **self).await?;

        // TODO security fix: this file should not be accessible to other users.
        let mut target_file = File::from_std(tempfile()?);

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
        info!(ratio = transferred / total, "transfer ratio");

        // No need to set perms because we'll upload it to s3.

        let hash: [u8; 20] = blake2b_hasher.finalize().into();

        target_file.seek(SeekFrom::Start(0)).await?;
        Ok(RecvResult { target_file, hash })
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
