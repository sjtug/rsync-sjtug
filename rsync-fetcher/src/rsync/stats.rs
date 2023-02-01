use eyre::Result;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::rsync::envelope::RsyncReadExt;

const BYE: i32 = -1;

/// Stats returned by server at the end of transmission.
#[derive(Debug, Copy, Clone)]
pub struct Stats {
    /// Bytes read.
    pub read: i64,
    /// Bytes written.
    pub written: i64,
    /// Total size of files.
    pub size: i64,
}

pub async fn finalize(
    mut tx: impl AsyncWrite + Unpin,
    mut rx: impl AsyncRead + Unpin + Send,
) -> Result<Stats> {
    let read = rx.read_rsync_long().await?;
    let written = rx.read_rsync_long().await?;
    let size = rx.read_rsync_long().await?;

    tx.write_i32_le(BYE).await?;
    tx.shutdown().await?;

    Ok(Stats {
        read,
        written,
        size,
    })
}
