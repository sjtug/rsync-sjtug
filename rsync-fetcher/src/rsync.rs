use eyre::{Context, ContextCompat, Result};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use url::Url;

use crate::rsync::envelope::RsyncReadExt;
use crate::rsync::handshake::HandshakeConn;
use crate::rsync::stats::Stats;

mod checksum;
mod envelope;
pub mod file_list;
pub mod filter;
mod generator;
mod handshake;
mod mux_conn;
mod receiver;
pub mod stats;
mod version;

const BYE: i32 = -1;

pub async fn start_handshake(url: &Url) -> Result<HandshakeConn> {
    let port = url.port().unwrap_or(873);
    let path = url.path().trim_start_matches('/');
    let module = path.split('/').next().context("empty remote path")?;

    let stream = TcpStream::connect(format!(
        "{}:{}",
        url.host_str().context("missing remote host")?,
        port
    ))
    .await
    .context("rsync server refused connection. Is it running?")?;

    let mut handshake = HandshakeConn::new(stream);
    handshake.start_inband_exchange(module, path).await?;

    Ok(handshake)
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
