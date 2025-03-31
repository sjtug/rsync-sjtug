use std::ffi::OsStr;
use std::ops::{Deref, DerefMut};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;

use eyre::Result;
use futures::{StreamExt, stream};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info};

use crate::plan::TransferItem;
use crate::rsync::checksum::{SumHead, checksum_payload};
use crate::rsync::file_list::FileEntry;
use crate::rsync::progress_display::ProgressDisplay;
use crate::utils::ignore_mode;

/// Generator sends requests to rsync server.
pub struct Generator {
    tx: OwnedWriteHalf,
    file_list: Arc<Vec<FileEntry>>,
    seed: i32,
    permits: Arc<Semaphore>,
    basis_rx: mpsc::Receiver<(u32, File)>,
    pb: ProgressDisplay,
}

impl Generator {
    pub const fn new(
        tx: OwnedWriteHalf,
        file_list: Arc<Vec<FileEntry>>,
        seed: i32,
        permits: Arc<Semaphore>,
        basis_rx: mpsc::Receiver<(u32, File)>,
        pb: ProgressDisplay,
    ) -> Self {
        Self {
            tx,
            file_list,
            seed,
            permits,
            basis_rx,
            pb,
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
    pub async fn generate_task(mut self, transfer_plan: &[TransferItem]) -> Result<Self> {
        self.generate_task_mut(transfer_plan).await?;
        Ok(self)
    }
    pub async fn generate_task_mut(&mut self, transfer_plan: &[TransferItem]) -> Result<()> {
        info!("generator started.");

        info!("generate file phase 1");
        self.recv_generator(transfer_plan).await?;

        self.write_i32_le(-1).await?;

        // TODO phase 2: re-do failed files
        info!("generate file phase 2");
        self.write_i32_le(-1).await?;

        info!("generator finish");
        Ok(())
    }
    async fn recv_generator(&mut self, transfer_plan: &[TransferItem]) -> Result<()> {
        let file_list = self.file_list.clone();
        let it = transfer_plan.iter().filter(|item| {
            #[allow(clippy::cast_sign_loss)]
            let entry = &file_list[item.idx as usize];
            item.blake2b.is_none() && !ignore_mode(entry.mode, None::<()>)
        });
        let mut async_it = stream::iter(it);

        loop {
            // NOTE: permits are refilled after the corresponding files are received.
            self.permits.acquire().await?.forget();

            tokio::select! { biased;
                Some((idx, file)) = self.basis_rx.recv() => {
                    let entry = &self.file_list[idx as usize];
                    let path = Path::new(OsStr::from_bytes(&entry.name));

                    debug!(?path, idx, "requesting partial file");
                    self.write_u32_le(idx).await?;
                    self.generate_and_send_sums(file).await?;

                    self.pb.inc_pending(1);
                }
                Some(item) = async_it.next() => {
                    #[allow(clippy::cast_sign_loss)]
                    let entry = &self.file_list[item.idx as usize];
                    let idx = entry.idx;
                    let path = Path::new(OsStr::from_bytes(&entry.name));

                    debug!(?path, idx, "requesting full file");
                    self.write_u32_le(idx).await?;
                    SumHead::default().write_to(&mut **self).await?;

                    self.pb.inc_pending(1);
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn generate_and_send_sums(&mut self, file: File) -> Result<()> {
        let file_len = file.metadata().await?.size();
        let sum_head = SumHead::sum_sizes_sqroot(file_len);
        sum_head.write_to(&mut **self).await?;

        let mut file = file.into_std().await;
        let seed = self.seed;

        let sum_bytes = tokio::task::spawn_blocking(move || {
            checksum_payload(sum_head, seed, &mut file, file_len)
        })
        .await?;

        self.write_all(&sum_bytes).await?;

        Ok(())
    }
}
