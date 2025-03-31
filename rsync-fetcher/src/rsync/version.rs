use std::fmt::Display;

use eyre::{Context, Result, eyre};
use scan_fmt::scan_fmt;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::debug;

/// The version of the rsync protocol that is supported by this implementation.
pub const SUPPORTED_VERSION: Version = Version {
    major: 27,
    minor: None,
};

/// Version of the rsync protocol.
#[derive(Debug, Copy, Clone)]
pub struct Version {
    pub major: i32,
    pub minor: Option<i32>,
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(minor) = self.minor {
            write!(f, "{}.{}", self.major, minor)
        } else {
            write!(f, "{}", self.major)
        }
    }
}

impl Version {
    pub async fn read_from(mut rx: impl AsyncBufRead + Unpin) -> Result<Self> {
        let mut greeting = String::new();
        (&mut rx).take(128).read_line(&mut greeting).await?;
        debug!(greeting, "greeting");

        let protocol_header = greeting
            .trim()
            .strip_prefix("@RSYNCD: ")
            .ok_or_else(|| eyre!("invalid greeting"))?
            .to_string();

        let (major, minor) = scan_fmt!(&protocol_header, "{}.{}", i32, i32)
            .map(|(protocol, sub)| (protocol, Some(sub)))
            .or_else(|_| scan_fmt!(&protocol_header, "{}", i32).map(|protocol| (protocol, None)))
            .context("invalid greeting: no server version")?;

        Ok(Self { major, minor })
    }
    pub async fn write_to(self, mut tx: impl AsyncWrite + Unpin) -> Result<()> {
        let msg = self.minor.map_or_else(
            || format!("@RSYNCD: {}\n", self.major),
            |minor| format!("@RSYNCD: {}.{}\n", self.major, minor),
        );

        tx.write_all(msg.as_bytes()).await?;
        Ok(())
    }
}
