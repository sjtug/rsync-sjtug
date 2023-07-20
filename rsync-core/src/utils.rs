use std::convert::Infallible;
use std::env;
use std::fmt::LowerHex;
use std::future::Future;
use std::ops::Deref;
use std::task::{Context, Poll};

use futures::FutureExt;
#[cfg(feature = "percent-encoding")]
use percent_encoding::{AsciiSet, CONTROLS};
use tokio::task::JoinHandle;
#[cfg(feature = "tests")]
use tracing::level_filters::LevelFilter;
use tracing_error::ErrorLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

#[cfg(feature = "percent-encoding")]
pub const PATH_ASCII_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}');

#[cfg(feature = "percent-encoding")]
pub const ATTR_CHAR: &AsciiSet = &CONTROLS
    .add(b'(')
    .add(b')')
    .add(b'<')
    .add(b'>')
    .add(b'@')
    .add(b',')
    .add(b';')
    .add(b':')
    .add(b'\\')
    .add(b'"')
    .add(b'/')
    .add(b'[')
    .add(b']')
    .add(b'?')
    .add(b'=')
    .add(b'{')
    .add(b'}')
    .add(b' ')
    .add(b'\t')
    .add(b'*')
    .add(b'\'')
    .add(b'%');

pub fn init_logger() {
    tracing_subscriber::Registry::default()
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
}

#[cfg(feature = "tests")]
pub fn test_init_logger() {
    tracing_subscriber::Registry::default()
        .with(LevelFilter::DEBUG)
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
}

/// Initialize color-eyre error handling, with `NO_COLOR` support.
///
/// # Errors
/// Returns an error if `color-eyre` has already been initialized.
pub fn init_color_eyre() -> eyre::Result<()> {
    if env::var("NO_COLOR").is_ok() {
        color_eyre::config::HookBuilder::new()
            .theme(color_eyre::config::Theme::new())
            .install()?;
    } else {
        color_eyre::install()?;
    }
    Ok(())
}

pub trait ToHex {
    fn as_hex(&self) -> HexWrapper<'_>;
}

impl ToHex for [u8] {
    fn as_hex(&self) -> HexWrapper<'_> {
        HexWrapper(self)
    }
}

pub struct HexWrapper<'a>(&'a [u8]);

impl LowerHex for HexWrapper<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_ensure_end_slash(s: &str) -> Result<String, Infallible> {
    Ok(if s.ends_with('/') {
        s.to_string()
    } else {
        format!("{s}/")
    })
}

/// Wrapper around `tokio::task::JoinHandle` that aborts the task when dropped.
pub struct AbortJoinHandle<T>(JoinHandle<T>);

impl<T> AbortJoinHandle<T> {
    #[must_use]
    pub fn new(handle: JoinHandle<T>) -> Self {
        Self(handle)
    }
}

impl<T> Deref for AbortJoinHandle<T> {
    type Target = JoinHandle<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> Future for AbortJoinHandle<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_unpin(cx)
    }
}

impl<T> Drop for AbortJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
