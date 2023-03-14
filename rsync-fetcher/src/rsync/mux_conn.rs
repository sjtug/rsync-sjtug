use std::path::Path;
use std::sync::Arc;

use eyre::{Context, Result};
use redis::aio;
use tempfile::TempDir;
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, Semaphore};

use rsync_core::redis_::RedisOpts;
use rsync_core::s3::S3Opts;

use crate::consts::{BASIS_BUFFER_LIMIT, UPLOAD_CONN};
use crate::rsync::downloader::Downloader;
use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::file_list::FileEntry;
use crate::rsync::generator::Generator;
use crate::rsync::progress_display::ProgressDisplay;
use crate::rsync::receiver::Receiver;
use crate::rsync::uploader::Uploader;
use crate::rsync::TaskBuilders;

pub struct MuxConn {
    tx: OwnedWriteHalf,
    rx: EnvelopeRead<BufReader<OwnedReadHalf>>,
    seed: i32,
}

impl MuxConn {
    pub fn new(tx: OwnedWriteHalf, rx: EnvelopeRead<BufReader<OwnedReadHalf>>, seed: i32) -> Self {
        Self { tx, rx, seed }
    }
    pub async fn recv_file_list(&mut self) -> Result<Vec<FileEntry>> {
        self.rx.recv_file_list().await
    }
    pub fn into_task_builders(
        self,
        s3: aws_sdk_s3::Client,
        s3_opts: S3Opts,
        redis: aio::MultiplexedConnection,
        redis_opts: RedisOpts,
        file_list: Arc<Vec<FileEntry>>,
        temp_dir: &Path,
    ) -> Result<TaskBuilders> {
        let basis_dir = TempDir::new_in(temp_dir).context("failed to create temp dir")?;
        let permits = Arc::new(Semaphore::new(BASIS_BUFFER_LIMIT));
        let (basis_tx, basis_rx) = mpsc::channel(BASIS_BUFFER_LIMIT);
        let (upload_tx, upload_rx) = flume::bounded(UPLOAD_CONN * 2);
        let progress = ProgressDisplay::new();
        let downloader = Downloader::new(
            file_list.clone(),
            s3.clone(),
            s3_opts.clone(),
            basis_dir.path().to_path_buf(),
            basis_tx,
            progress.clone(),
        );
        let generator = Generator::new(
            self.tx,
            file_list.clone(),
            self.seed,
            permits.clone(),
            basis_rx,
            progress.clone(),
        );
        let receiver = Receiver::new(
            self.rx,
            upload_tx,
            file_list.clone(),
            self.seed,
            basis_dir,
            permits,
            progress.clone(),
        );
        let uploader = Uploader::new(
            upload_rx,
            file_list,
            s3,
            s3_opts,
            redis,
            redis_opts,
            progress.clone(),
        );
        Ok(TaskBuilders {
            downloader,
            generator,
            receiver,
            uploader,
            progress,
        })
    }
}
