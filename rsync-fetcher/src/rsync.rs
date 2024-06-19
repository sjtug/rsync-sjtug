use eyre::{bail, Context, ContextCompat, Result};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_socks::tcp::Socks5Stream;
use tracing::info;
use url::Url;

use crate::rsync::downloader::Downloader;
use crate::rsync::envelope::RsyncReadExt;
use crate::rsync::generator::Generator;
use crate::rsync::handshake::{Auth, HandshakeConn};
use crate::rsync::progress_display::ProgressDisplay;
use crate::rsync::receiver::Receiver;
use crate::rsync::stats::Stats;
use crate::rsync::uploader::Uploader;

mod checksum;
mod downloader;
mod envelope;
pub mod file_list;
pub mod filter;
mod generator;
mod handshake;
mod mux_conn;
mod progress_display;
mod receiver;
pub mod stats;
pub mod uploader;
mod version;

const BYE: i32 = -1;

pub struct TaskBuilders {
    pub downloader: Downloader,
    pub generator: Generator,
    pub receiver: Receiver,
    pub uploader: Uploader,
    pub progress: ProgressDisplay,
}

async fn connect_with_proxy(target: &str) -> Result<TcpStream> {
    let proxy = std::env::var("SOCKS5_PROXY")
        .ok()
        .and_then(|s| (!s.is_empty()).then_some(s))
        .or_else(|| {
            std::env::var("socks5_proxy")
                .ok()
                .and_then(|s| (!s.is_empty()).then_some(s))
        });

    if let Some(proxy) = proxy {
        let proxy = Url::parse(&proxy).context("invalid proxy URL")?;
        if proxy.scheme().to_lowercase() != "socks5" {
            bail!("unsupported proxy scheme: {}", proxy.scheme());
        }
        let proxy_addr = proxy.host_str().context("missing proxy host")?;
        let proxy_port = proxy.port().unwrap_or(1080);
        let proxy_username = proxy.username();
        let proxy_password = proxy.password().unwrap_or_default();

        let stream = if proxy_username.is_empty() {
            info!("connecting to {} via SOCKS5 proxy {}", target, proxy);
            Socks5Stream::connect((proxy_addr, proxy_port), target)
                .await
                .context("proxy or rsync server refused connection. Are they running?")?
        } else {
            info!(
                "connecting to {} via SOCKS5 proxy {} as {}",
                target, proxy, proxy_username
            );
            Socks5Stream::connect_with_password(
                (proxy_addr, proxy_port),
                target,
                proxy_username,
                proxy_password,
            )
            .await
            .context("proxy or rsync server refused connection. Are they running?")?
        };

        Ok(stream.into_inner())
    } else {
        TcpStream::connect(target)
            .await
            .context("rsync server refused connection. Is it running?")
    }
}

pub async fn start_handshake(url: &Url) -> Result<HandshakeConn> {
    let port = url.port().unwrap_or(873);
    let path = url.path().trim_start_matches('/');
    let auth = Auth::from_url_and_env(url);
    let module = path.split('/').next().context("empty remote path")?;

    let stream = connect_with_proxy(&format!(
        "{}:{}",
        url.host_str().context("missing remote host")?,
        port
    ))
    .await?;

    let mut handshake = HandshakeConn::new(stream);
    handshake.start_inband_exchange(module, path, auth).await?;

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
