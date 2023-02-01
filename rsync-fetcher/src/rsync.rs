use eyre::Result;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{info, warn};
use url::Url;

use crate::opts::RsyncOpts;
use crate::rsync::handshake::HandshakeConn;
use crate::rsync::stats::finalize;

mod checksum;
mod envelope;
pub mod file_list;
mod filter;
mod generator;
mod handshake;
mod receiver;
mod stats;
mod version;

pub async fn start_socket_client(url: Url, opts: &RsyncOpts) -> Result<()> {
    let port = url.port().unwrap_or(873);
    let path = url.path().trim_start_matches('/');
    let module = path.split('/').next().unwrap_or("must have module");

    let mut stream = TcpStream::connect(format!("{}:{}", url.host_str().expect("has host"), port))
        .await
        .expect("connect success");

    let mut handshake = HandshakeConn::new(&mut stream);
    handshake.start_inband_exchange(module, path).await?;

    let (mut generator, mut receiver) = handshake.finalize(&opts.filters).await?;

    let file_list = receiver.recv_file_list().await?;

    let io_errors = receiver.read_i32_le().await?;
    if io_errors != 0 {
        warn!("server reported IO errors: {}", io_errors);
    }

    tokio::try_join!(
        generator.generate_task(&file_list),
        receiver.recv_task(&file_list),
    )?;

    let stats = finalize(&mut *generator, &mut *receiver).await?;
    info!(?stats, "transfer stats");

    Ok(())
}
