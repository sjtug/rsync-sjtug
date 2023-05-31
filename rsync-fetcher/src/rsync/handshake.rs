//! Rsync handshake phase.
//!
//! In this stage, the client and server exchange information about the protocol version, server
//! sends the motd message, and client sends the module name, path name, options, and filter rules.

use std::fmt::{Debug, Formatter};

use base64::engine::general_purpose;
use base64::Engine;
use digest::Digest;
use eyre::{bail, Result};
use md4::Md4;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tracing::{debug, instrument};
use url::Url;
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::rsync::envelope::EnvelopeRead;
use crate::rsync::filter::Rule;
use crate::rsync::mux_conn::MuxConn;
use crate::rsync::version::{Version, SUPPORTED_VERSION};

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct Auth {
    username: String,
    password: String,
}

impl Debug for Auth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Auth")
            .field("username", &self.username)
            .field("password", &"********")
            .finish()
    }
}

impl Auth {
    pub fn from_url_and_env(url: &Url) -> Self {
        let username = if url.username().is_empty() {
            "nobody".to_string()
        } else {
            url.username().to_string()
        };
        let password = url
            .password()
            .map(ToString::to_string)
            .or_else(|| std::env::var("RSYNC_PASSWORD").ok())
            .unwrap_or_default();
        Self { username, password }
    }
    fn challenge(&self, challenge: &str) -> String {
        let mut hasher = Md4::default();
        hasher.update([0; 4]);
        hasher.update(&self.password);
        hasher.update(challenge);
        let hash = hasher.finalize();
        let response = general_purpose::STANDARD_NO_PAD.encode(hash);
        format!("{} {}", self.username, response)
    }
}

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
    pub async fn start_inband_exchange(
        &mut self,
        module: &str,
        path: &str,
        auth: Auth,
    ) -> Result<()> {
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
                let challenge = line
                    .strip_prefix("@RSYNCD: AUTHREQD ")
                    .expect("challenge")
                    .trim();
                let resp = auth.challenge(challenge);
                self.tx.write_all(format!("{resp}\n").as_bytes()).await?;
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
