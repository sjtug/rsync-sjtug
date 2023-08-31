use std::cmp::Ordering;
use std::fs::File;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use blake2::Blake2b;
use digest::consts::U20;
use digest::Digest;
use eyre::{ensure, Result};
use md4::Md4;
use tempfile::{tempfile_in, TempDir, TempPath};
use tokio::fs;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, info, instrument};

use rsync_core::utils::ToHex;

use crate::rsync::checksum::SumHead;
use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::file_list::FileEntry;
use crate::rsync::progress_display::ProgressDisplay;
use crate::rsync::uploader::UploadTask;
use crate::utils::hash;

pub struct Receiver {
    rx: EnvelopeRead<BufReader<OwnedReadHalf>>,
    upload_tx: Option<flume::Sender<UploadTask>>,
    file_list: Arc<Vec<FileEntry>>,
    seed: i32,
    basis_dir: TempDir,
    permits: Arc<Semaphore>,
    pb: ProgressDisplay,
}

impl Receiver {
    // We are fine with this because it's a private constructor.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rx: EnvelopeRead<BufReader<OwnedReadHalf>>,
        upload_tx: flume::Sender<UploadTask>,
        file_list: Arc<Vec<FileEntry>>,
        seed: i32,
        basis_dir: TempDir,
        permits: Arc<Semaphore>,
        pb: ProgressDisplay,
    ) -> Self {
        Self {
            rx,
            upload_tx: Some(upload_tx),
            file_list,
            seed,
            basis_dir,
            permits,
            pb,
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

enum IOChunk {
    Data(Vec<u8>),
    Copied { offset: u64, data_len: i32 },
}

#[derive(Debug)]
struct RecvResult {
    target_file: File,
    blake2b_hash: [u8; 20],
}

struct BasisFile {
    _path: TempPath,
    file: File,
    pb: ProgressDisplay,
}

impl Deref for BasisFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for BasisFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl Drop for BasisFile {
    fn drop(&mut self) {
        self.pb.dec_basis(1);
    }
}

impl Receiver {
    pub async fn recv_task(mut self) -> Result<Self> {
        self.recv_task_mut().await?;
        Ok(self)
    }
    pub async fn recv_task_mut(&mut self) -> Result<()> {
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
            self.recv_file(idx).await?;
        }

        info!("recv finish");
        self.upload_tx.take().unwrap();
        Ok(())
    }

    #[instrument(skip(self))]
    async fn recv_file(&mut self, idx: usize) -> Result<()> {
        let entry = &self.file_list[idx];
        debug!(file=%entry.name_lossy(), "receive file");

        // Get basis file if exists. It should be created by generator if delta transfer is
        // enabled and an old version of the file exists.
        let basis_file = self.try_get_basis_file(&entry.name)?;

        // Receive file data.
        let RecvResult {
            target_file,
            blake2b_hash,
        } = self.recv_data(basis_file).await?;

        // Release permit.
        self.permits.add_permits(1);
        self.pb.dec_pending(1);

        self.upload_tx
            .as_ref()
            .expect("task can be run only once")
            .send_async(UploadTask {
                idx,
                blake2b_hash,
                file: fs::File::from_std(target_file),
            })
            .await?;
        self.pb.inc_uploading(1);

        Ok(())
    }

    fn try_get_basis_file(&self, path: &[u8]) -> Result<Option<BasisFile>> {
        let basis_path = self
            .basis_dir
            .path()
            .join(format!("{:x}", hash(path).as_hex()));
        Ok(File::open(&basis_path)
            .map(|f| {
                Some(BasisFile {
                    _path: TempPath::from_path(basis_path),
                    file: f,
                    pb: self.pb.clone(),
                })
            })
            .or_else(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(e)
                }
            })?)
    }

    async fn recv_data(&mut self, mut local_basis: Option<BasisFile>) -> Result<RecvResult> {
        let SumHead {
            checksum_count,
            block_len,
            checksum_len: _,
            remainder_len,
        } = SumHead::read_from(&mut **self).await?;

        let mut target_file = tempfile_in(&self.basis_dir)?;

        // Hasher for final file consistency check.
        let mut md4_hasher = Md4::default();
        md4_hasher.update(self.seed.to_le_bytes());
        // Hasher for content addressing. Hash function is blake2b-160.
        let mut blake2b_hasher = Blake2b::<U20>::default();

        let (tx, mut rx) = mpsc::channel(1024);

        #[allow(clippy::read_zero_byte_vec)] // false positive
        let io_task = tokio::task::spawn_blocking(move || {
            let mut buf = vec![];
            while let Some(token) = rx.blocking_recv() {
                match token {
                    IOChunk::Data(data) => {
                        md4_hasher.update(&data);
                        blake2b_hasher.update(&data);
                        target_file.write_all(&data)?;
                    }
                    IOChunk::Copied { offset, data_len } => {
                        #[allow(clippy::cast_sign_loss)] // data_len is always positive.
                        if buf.len() != data_len as usize {
                            buf.resize(data_len as usize, 0);
                        }

                        let local_basis = local_basis.as_mut().expect("incremental");
                        local_basis.seek(SeekFrom::Start(offset))?;
                        local_basis.read_exact(&mut buf)?;

                        md4_hasher.update(&buf);
                        blake2b_hasher.update(&buf);
                        target_file.write_all(&buf)?;
                    }
                }
            }

            let md4: [u8; 16] = md4_hasher.finalize().into();
            let blake2b: [u8; 20] = blake2b_hasher.finalize().into();

            target_file.seek(SeekFrom::Start(0))?;
            Ok::<_, io::Error>((md4, blake2b, target_file))
        });

        let (mut transferred, mut copied) = (0u64, 0u64);
        loop {
            let token = self.recv_token().await?;
            match token {
                FileToken::Data(data) => {
                    // Protect against malicious input.
                    // The maximum memory usage is 128 * 1024 * 1024 = 128 MiB.
                    assert!(data.len() <= 128 * 1024, "data chunk too large");

                    transferred += data.len() as u64;
                    self.pb.inc(data.len() as u64);

                    tx.send(IOChunk::Data(data)).await?;
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
                    self.pb.inc(data_len as u64);

                    tx.send(IOChunk::Copied { offset, data_len }).await?;
                }
                FileToken::Done => break,
            }
        }

        drop(tx);
        let (md4, blake2b, mut target_file) = io_task.await??;

        let mut remote_md4 = [0; 16];

        self.read_exact(&mut remote_md4).await?;
        ensure!(md4 == remote_md4, "checksum mismatch");

        // A debug log anyway.
        #[allow(clippy::cast_precision_loss)]
        let (transferred, total) = (transferred as f64, (transferred + copied) as f64);
        debug!(ratio = transferred / total, "transfer ratio");

        // No need to set perms because we'll upload it to s3.

        target_file.seek(SeekFrom::Start(0))?;
        Ok(RecvResult {
            target_file,
            blake2b_hash: blake2b,
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
