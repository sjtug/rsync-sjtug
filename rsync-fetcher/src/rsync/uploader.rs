use std::ffi::OsStr;
use std::io::SeekFrom;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use dashmap::DashSet;
use eyre::Result;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use opendal::{ErrorKind, Operator};
use tap::TapOptional;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::mpsc;
use tokio_util::compat::TokioAsyncReadCompatExt;
use tracing::{debug, info, warn};

use rsync_core::metadata::{MetaExtra, Metadata};
use rsync_core::utils::{ToHex, ATTR_CHAR};

use crate::consts::UPLOAD_CONN;
use crate::rsync::file_list::FileEntry;
use crate::rsync::progress_display::ProgressDisplay;

pub struct Uploader {
    rx: flume::Receiver<UploadTask>,
    file_list: Arc<Vec<FileEntry>>,
    s3: Operator,
    s3_prefix: String,
    pg_tx: mpsc::Sender<(Vec<u8>, Metadata)>,
    uploaded: DashSet<[u8; 20]>,
    pb: ProgressDisplay,
}

pub struct UploadTask {
    pub idx: usize,
    pub blake2b_hash: [u8; 20],
    pub file: File,
}

impl Uploader {
    pub fn new(
        rx: flume::Receiver<UploadTask>,
        file_list: Arc<Vec<FileEntry>>,
        s3: Operator,
        s3_prefix: String,
        pg_tx: mpsc::Sender<(Vec<u8>, Metadata)>,
        pb: ProgressDisplay,
    ) -> Self {
        Self {
            rx,
            file_list,
            s3,
            s3_prefix,
            pg_tx,
            uploaded: DashSet::new(),
            pb,
        }
    }

    pub async fn upload_tasks(self) -> Result<()> {
        let futs: FuturesUnordered<_> = (0..UPLOAD_CONN).map(|id| self.upload_task(id)).collect();
        futs.try_collect().await?;
        Ok(())
    }

    async fn upload_task(&self, id: usize) -> Result<()> {
        debug!("upload task {} started", id);

        while let Ok(task) = self.rx.recv_async().await {
            let UploadTask {
                idx,
                blake2b_hash,
                file,
            } = task;

            // Avoid repeatedly uploading the same file to address
            // https://stackoverflow.com/questions/63238344/amazon-s3-how-parallel-puts-to-the-same-key-are-resolved-in-versioned-buckets
            if !self.uploaded.contains(&blake2b_hash) {
                // Upload file to S3.
                let entry = &self.file_list[idx];
                let key = Path::new(OsStr::from_bytes(&entry.name));
                let filename = key
                    .file_name()
                    .tap_none(|| warn!(?key, "missing filename of entry"))
                    .map(OsStrExt::as_bytes);

                // REMARK: If a file is soft/hard linked, users may see a content-disposition with a
                // different filename instead of the one they expect.
                // TODO since we no longer need static file path, we can use presign on the gateway.
                let mut file = file.try_clone().await.expect("unable to dup file");
                file.seek(SeekFrom::Start(0))
                    .await
                    .expect("unable to seek file");
                self.upload_s3(filename, file, &blake2b_hash).await?;
                self.uploaded.insert(blake2b_hash);
            }
            self.pb.dec_uploading(1);

            // Update metadata in pg.
            self.update_metadata(idx, blake2b_hash).await?;
        }

        debug!("upload task {} finished", id);
        Ok(())
    }

    async fn upload_s3(
        &self,
        filename: Option<&[u8]>,
        target_file: File,
        blake2b_hash: &[u8; 20],
    ) -> Result<()> {
        // File is content addressed by its blake2b hash.
        let key = format!("{}{:x}", self.s3_prefix, blake2b_hash.as_hex());

        // Check if file already exists. Might happen if the file is the same between two syncs,
        // or it's hard linked.
        let exists = self
            .s3
            .stat(&key)
            .await
            .map(|_| true)
            .or_else(|e| match e.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(e),
            })?;

        // Only upload if it doesn't exist.
        if !exists {
            let content_disposition = filename.map(|filename| {
                let encoded_name = percent_encoding::percent_encode(filename, ATTR_CHAR);
                format!("attachment; filename=\"{encoded_name}\"; filename*=UTF-8''{encoded_name}")
            });
            let file_size = target_file.metadata().await?.len();

            let mut writer = content_disposition
                .map_or_else(
                    || self.s3.writer_with(&key).content_length(file_size),
                    |content_disposition| {
                        self.s3
                            .writer_with(&key)
                            .content_length(file_size)
                            .content_disposition(&content_disposition)
                    },
                )
                .await?;
            writer.copy(file_size, target_file.compat()).await?;
            writer.close().await?;
        }
        Ok(())
    }

    async fn update_metadata(&self, idx: usize, blake2b_hash: [u8; 20]) -> Result<()> {
        let entry = &self.file_list[idx];
        let metadata = Metadata {
            len: entry.len,
            modify_time: entry.modify_time,
            extra: MetaExtra::Regular { blake2b_hash },
        };

        // Queue metadata insertion to Postgres.
        self.pg_tx.send((entry.name.clone(), metadata)).await?;
        Ok(())
    }
}

impl Drop for Uploader {
    fn drop(&mut self) {
        info!("uploader dropped");
    }
}
