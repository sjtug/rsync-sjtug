use std::fmt::LowerHex;

#[cfg(feature = "percent-encoding")]
use percent_encoding::{AsciiSet, CONTROLS};
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

pub fn init_logger() {
    tracing_subscriber::Registry::default()
        .with(EnvFilter::from_default_env())
        .with(ErrorLayer::default())
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .init();
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