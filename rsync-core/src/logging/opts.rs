use std::fmt::Display;
use std::str::FromStr;
use std::{fmt, io};

use eyre::bail;

use crate::logging::either::EitherMakeWriter;
use crate::logging::tcp::TcpWriter;

/// Log format.
#[cfg_attr(feature = "doku", derive(doku::Document))]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LogFormat {
    /// Format log in human readable format.
    Human,
    /// Format log in JSON format.
    JSON,
}

/// Log target.
#[derive(Debug, Clone)]
pub enum LogTarget {
    /// Log to stderr.
    Stderr,
    /// Log to TCP socket.
    TCP(String),
}

#[cfg(feature = "doku")]
impl doku::Document for LogTarget {
    fn ty() -> doku::Type {
        doku::Type::from(doku::TypeKind::Enum {
            tag: doku::Tag::External,
            variants: vec![
                doku::Variant {
                    id: "stderr",
                    title: "stderr",
                    comment: Some("Log to stderr."),
                    serializable: true,
                    deserializable: true,
                    fields: doku::Fields::Unit,
                    aliases: &[],
                },
                doku::Variant {
                    id: "tcp://localhost:7070",
                    title: "tcp://localhost:7070",
                    comment: Some("Log to tcp socket."),
                    serializable: true,
                    deserializable: true,
                    fields: doku::Fields::Unit,
                    aliases: &[],
                },
            ],
        })
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for LogTarget {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for LogTarget {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FromStr::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl Display for LogTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stderr => write!(f, "stderr"),
            Self::TCP(addr) => write!(f, "tcp://{addr}"),
        }
    }
}

impl FromStr for LogTarget {
    type Err = eyre::Report;

    fn from_str(s: &str) -> eyre::Result<Self> {
        Ok(if s.to_lowercase() == "stderr" {
            Self::Stderr
        } else if let Some(addr) = s.strip_prefix("tcp://") {
            Self::TCP(addr.to_string())
        } else {
            bail!("invalid log target: {}", s)
        })
    }
}

impl LogTarget {
    pub fn into_make_writer(self) -> EitherMakeWriter<fn() -> io::Stderr, TcpWriter> {
        match self {
            Self::Stderr => EitherMakeWriter::Left(io::stderr),
            Self::TCP(addr) => EitherMakeWriter::Right(TcpWriter::connect(addr)),
        }
    }
}
