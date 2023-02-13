use std::sync::Arc;

use eyre::{Context, Result};
use tempfile::TempDir;
use tokio::io::BufReader;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use rsync_core::s3::S3Opts;

use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::file_list::FileEntry;
use crate::rsync::generator::Generator;
use crate::rsync::receiver::Receiver;

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
    pub fn into_gen_recv(
        self,
        s3: aws_sdk_s3::Client,
        s3_opts: S3Opts,
        file_list: Arc<Vec<FileEntry>>,
    ) -> Result<(Generator, Receiver)> {
        let basis_dir = TempDir::new().context("failed to create temp dir")?;
        Ok((
            Generator::new(
                self.tx,
                file_list.clone(),
                self.seed,
                s3,
                s3_opts,
                basis_dir.path().to_path_buf(),
            ),
            Receiver::new(self.rx, file_list, self.seed, basis_dir),
        ))
    }
}
