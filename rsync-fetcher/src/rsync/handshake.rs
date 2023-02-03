//! Rsync handshake phase.
//!
//! In this stage, the client and server exchange information about the protocol version, server
//! sends the motd message, and client sends the module name, path name, options, and filter rules.

use eyre::{bail, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::{debug, instrument};

use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::filter::Rule;
use crate::rsync::mux_conn::MuxConn;
use crate::rsync::version::{Version, SUPPORTED_VERSION};

/// Represents a connection that is in the handshake phase.
///
/// Note that in this stage no multiplexing is done.
#[derive(Debug)]
pub struct HandshakeConn {
    pub tx: OwnedWriteHalf,
    pub rx: BufReader<OwnedReadHalf>,
}

impl HandshakeConn {
    pub fn new(stream: TcpStream) -> Self {
        let (rx, tx) = stream.into_split();
        Self {
            tx,
            rx: BufReader::with_capacity(256 * 1024, rx),
        }
    }

    #[instrument(skip(self))]
    pub async fn start_inband_exchange(&mut self, module: &str, path: &str) -> Result<()> {
        debug!("negotiate protocol version");
        SUPPORTED_VERSION.write_to(&mut self.tx).await?;

        let remote_protocol = Version::read_from(&mut self.rx).await?;
        if remote_protocol.major < 27 {
            bail!("server protocol version too old: {}", remote_protocol);
        }

        debug!(%remote_protocol, local_protocol = 27, "protocol negotiated");

        debug!(module, "send module name");
        self.tx.write_all(format!("{module}\n").as_bytes()).await?;

        debug!("reading motd");
        loop {
            let mut line = String::new();
            (&mut self.rx).take(1024).read_line(&mut line).await?;

            if line.starts_with("@ERROR") {
                bail!("server error: {}", line);
            } else if line.starts_with("@RSYNCD: AUTHREQD ") {
                bail!("server requires authentication");
            } else if line.starts_with("@RSYNCD: OK") {
                break;
            } else {
                println!("{}", line.trim_end());
            }
        }

        // -l preserve_links -t preserve_times -r recursive -p perms
        let options = ["--server", "--sender", "-ltpr", ".", path];
        debug!(?options, "send options");
        for opt in options {
            self.tx.write_all(format!("{opt}\n").as_bytes()).await?;
        }
        self.tx.write_all(b"\n").await?;

        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn finalize(mut self, rules: &[Rule]) -> Result<MuxConn> {
        let seed = self.rx.read_i32_le().await?;
        debug!(seed);

        self.send_filter_rules(rules).await?;

        let rx = EnvelopeRead::new(self.rx);

        Ok(MuxConn::new(self.tx, rx, seed))
    }
}
