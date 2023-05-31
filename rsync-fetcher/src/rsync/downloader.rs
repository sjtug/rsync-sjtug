use std::fmt::{Debug, Display, Formatter};
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use eyre::{bail, eyre, Result};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, TryStreamExt};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, BufReader};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use rsync_core::s3::S3Opts;
use rsync_core::utils::ToHex;

use crate::consts::{BASIS_BUFFER_LIMIT, DOWNLOAD_CONN};
use crate::plan::TransferItem;
use crate::rsync::file_list::FileEntry;
use crate::rsync::progress_display::ProgressDisplay;
use crate::utils::{hash, ignore_mode};

pub struct Downloader {
    file_list: Arc<Vec<FileEntry>>,
    s3: aws_sdk_s3::Client,
    s3_opts: S3Opts,
    basis_dir: PathBuf,
    basis_tx: mpsc::Sender<(u32, File)>,
    pb: ProgressDisplay,
}

impl Downloader {
    pub fn new(
        file_list: Arc<Vec<FileEntry>>,
        s3: aws_sdk_s3::Client,
        s3_opts: S3Opts,
        basis_dir: PathBuf,
        basis_tx: mpsc::Sender<(u32, File)>,
        pb: ProgressDisplay,
    ) -> Self {
        Self {
            file_list,
            s3,
            s3_opts,
            basis_dir,
            basis_tx,
            pb,
        }
    }
}

struct DownloadEntry<'a> {
    idx: u32,
    blake2b_hash: &'a [u8],
    path: &'a [u8],
}

impl<'a> Debug for DownloadEntry<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DownloadEntry")
            .field("idx", &self.idx)
            .field("blake2b_hash", &format!("{:x}", self.blake2b_hash.as_hex()))
            .field("path", &String::from_utf8_lossy(self.path))
            .finish()
    }
}

impl Downloader {
    pub async fn tasks(self, transfer_plan: &[TransferItem]) -> Result<()> {
        let (tx, rx) = flume::bounded(BASIS_BUFFER_LIMIT);
        let tasks: FuturesUnordered<_> = (0..DOWNLOAD_CONN)
            .map(|id| self.download_task(id, rx.clone()).left_future())
            .collect();
        tasks.push(self.gen_task(transfer_plan, tx).right_future());
        tasks.try_collect::<()>().await?;
        Ok(())
    }
    async fn gen_task<'a>(
        &'a self,
        transfer_plan: &'a [TransferItem],
        tx: flume::Sender<DownloadEntry<'a>>,
    ) -> Result<()> {
        debug!("basis generator started");

        for item in transfer_plan {
            let Some(blake2b_hash) = &item.blake2b_hash else { continue };

            let entry = &self.file_list[item.idx as usize];
            if ignore_mode(entry.mode, None::<()>) {
                continue;
            }

            tx.send_async(DownloadEntry {
                idx: item.idx,
                blake2b_hash,
                path: &entry.name,
            })
            .await
            .map_err(|e| eyre!(e.to_string()))?;
        }

        debug!("basis generator finished");
        Ok(())
    }
    async fn download_task<'a>(
        &'a self,
        id: usize,
        rx: flume::Receiver<DownloadEntry<'a>>,
    ) -> Result<()> {
        debug!("basis downloader {} started", id);

        while let Ok(entry) = rx.recv_async().await {
            let permit = self.basis_tx.reserve().await?;
            self.pb.inc_basis(1);
            self.pb.inc_basis_downloading(1);
            let basis_path = self
                .basis_dir
                .join(format!("{:x}", hash(entry.path).as_hex()));
            let mut basis_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&basis_path)
                .await?;
            let try_obj = self
                .s3
                .get_object()
                .bucket(&self.s3_opts.bucket)
                .key(format!(
                    "{}{:x}",
                    self.s3_opts.prefix,
                    entry.blake2b_hash.as_hex()
                ))
                .send()
                .await;
            match try_obj {
                Ok(obj) => {
                    let mut body = BufReader::new(obj.body.into_async_read());
                    tokio::io::copy(&mut body, &mut basis_file).await?;
                }
                Err(e) => {
                    let e = e.into_service_error();
                    if e.is_no_such_key() {
                        warn!(
                            ?entry,
                            "INCONSISTENCY: basis file exists in metadata but not present in S3. \
                            Fallback to full file download"
                        );
                    } else {
                        bail!(e);
                    }
                }
            }

            basis_file.seek(SeekFrom::Start(0)).await?;
            permit.send((entry.idx, basis_file));
            self.pb.dec_basis_downloading(1);
        }

        debug!("basis downloader {} finished", id);
        Ok(())
    }
}
