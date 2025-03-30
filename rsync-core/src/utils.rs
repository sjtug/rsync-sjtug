use std::convert::Infallible;
use std::fmt::LowerHex;
use std::future::Future;
use std::ops::Deref;
use std::task::{Context, Poll};

use futures::FutureExt;
#[cfg(feature = "percent-encoding")]
use percent_encoding::AsciiSet;
#[cfg(feature = "percent-encoding")]
use percent_encoding::NON_ALPHANUMERIC;
use tokio::task::JoinHandle;
#[cfg(feature = "percent-encoding")]
use url_escape::COMPONENT;

// COMPONENT set without '/'
#[cfg(feature = "percent-encoding")]
pub const PATH_ASCII_SET: &AsciiSet = &COMPONENT.remove(b'/');

// https://github.com/seanmonstar/reqwest/blob/61b1b2b5e6dace3733cdba291801378dd974386a/src/async_impl/multipart.rs#L438
#[cfg(feature = "percent-encoding")]
pub const ATTR_CHAR: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'!')
    .remove(b'#')
    .remove(b'$')
    .remove(b'&')
    .remove(b'+')
    .remove(b'-')
    .remove(b'.')
    .remove(b'^')
    .remove(b'_')
    .remove(b'`')
    .remove(b'|')
    .remove(b'~');

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
    pub const fn new(handle: JoinHandle<T>) -> Self {
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
        self.0.poll_unpin(cx)
    }
}

impl<T> Drop for AbortJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
