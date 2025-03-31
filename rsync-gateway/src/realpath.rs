use std::ffi::{OsStr, OsString};
use std::fmt::{Debug, Formatter};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Component, Path, PathBuf};

use eyre::eyre;
use get_size::GetSize;
use rkyv::{Archive, Deserialize, Serialize};
use sqlx::{Acquire, Postgres};
use thiserror::Error;
use tracing::instrument;

use crate::pg::{Entry, entry_of_path};

/// Max allowed symlink lookup depth. Same as linux.
pub const MAX_SYMLINK_LOOKUP: usize = 40;

#[derive(Debug, Clone)]
pub enum Target {
    /// A regular file.
    Regular([u8; 20], i64),
    /// A directory.
    Directory(Vec<u8>),
}

#[derive(Debug, Error)]
pub enum RealpathError {
    #[error("{0}")]
    Resolve(#[from] ResolveError),
    #[error("other error: {0:?}")]
    Other(#[from] eyre::Report),
}

#[derive(Debug, Clone, Eq, PartialEq, Error, GetSize, Archive, Serialize, Deserialize)]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub enum ResolveError {
    #[error("wrong path or broken symlink")]
    Broken { trace: Vec<String> },
    #[error("max symlink depth reached")]
    Loop { trace: Vec<String> },
    #[error("path contains absolute path")]
    Absolute { trace: Vec<String> },
    #[error("path escaped root")]
    Unsafe { trace: Vec<String> },
}

// For cache eviction decision.
impl GetSize for RealpathError {
    fn get_heap_size(&self) -> usize {
        match self {
            Self::Resolve(e) => e.get_heap_size(),
            Self::Other(_) => 0,
        }
    }
}

impl ResolveError {
    pub fn trace(&self) -> &[String] {
        match self {
            Self::Broken { trace }
            | Self::Loop { trace }
            | Self::Absolute { trace }
            | Self::Unsafe { trace } => trace,
        }
    }
}

/// An owned version of `std::path::Component`.
///
/// We need to deal with `Components` in a buffer with some of them allocated during queries.
/// It's easier to let the buffer just own them.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComponentOwned {
    Prefix,
    RootDir,
    CurDir,
    ParentDir,
    Normal(OsString),
}

impl<'a> From<Component<'a>> for ComponentOwned {
    fn from(value: Component<'a>) -> Self {
        match value {
            Component::Prefix(_) => Self::Prefix,
            Component::RootDir => Self::RootDir,
            Component::CurDir => Self::CurDir,
            Component::ParentDir => Self::ParentDir,
            Component::Normal(s) => Self::Normal(s.to_owned()),
        }
    }
}

impl Debug for ComponentOwned {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prefix => "<windows prefix>".fmt(f),
            Self::RootDir => "/".fmt(f),
            Self::CurDir => ".".fmt(f),
            Self::ParentDir => "..".fmt(f),
            Self::Normal(s) => s.fmt(f),
        }
    }
}

macro_rules! explain {
    ($resolved: expr, $comp: expr, $reason: expr, $explain: expr) => {
        $explain.push(format!("{:?} / {:?}: {}", $resolved, $comp, $reason));
    };
}

macro_rules! giveup {
    ($err_variant: ident, $path: expr, $explain: expr) => {
        tracing::error!(path=?$path, explain=?$explain, "failed to solve symlink");
        return Err(RealpathError::Resolve(ResolveError::$err_variant{trace: $explain}));
    }
}

#[instrument(skip(path, db), fields(path = % String::from_utf8_lossy(path)))]
pub async fn realpath<'a>(
    path: &[u8],
    revision: i32,
    db: impl Acquire<'a, Database = Postgres>,
) -> Result<Target, RealpathError> {
    let mut conn = db.acquire().await.map_err(|e| eyre!(e))?;

    // Explain trace.
    let mut explain = vec![];

    if let Some(entry) = entry_of_path(revision, path, &mut *conn).await? {
        // Fast path: the entry exists.
        match entry {
            Entry::Regular { blake2b, len } => return Ok(Target::Regular(blake2b, len)),
            Entry::Directory => return Ok(Target::Directory(path.to_vec())),
            Entry::Symlink { .. } => {}
        }
    };
    let path = Path::new(OsStr::from_bytes(path)).to_path_buf();

    // Lookup depth guard.
    let mut lookup = 0;
    // Buffer of components to be resolved. Note that it's in reverse order so we can push & pop
    // from the front.
    let mut left: Vec<ComponentOwned> = path.components().map(Into::into).rev().collect();
    // Resolved path.
    let mut resolved = PathBuf::new();

    while let Some(next_comp) = left.pop() {
        match &next_comp {
            ComponentOwned::Prefix => {
                explain!(resolved, next_comp, "prefix not supported, abort", explain);
                giveup!(Broken, path, explain);
            }
            ComponentOwned::RootDir => {
                explain!(resolved, next_comp, "refuse to follow root", explain);
                giveup!(Absolute, path, explain);
            }
            ComponentOwned::CurDir => {
                explain!(resolved, next_comp, "unchanged", explain);
                continue;
            }
            ComponentOwned::ParentDir => {
                explain!(resolved, next_comp, "go up", explain);
                if !resolved.pop() {
                    explain!(resolved, next_comp, "escaped root, abort", explain);
                    giveup!(Unsafe, path, explain);
                }
            }
            ComponentOwned::Normal(name) => {
                let next = resolved.join(name);
                let Some(entry) =
                    entry_of_path(revision, next.as_os_str().as_bytes(), &mut *conn).await?
                else {
                    explain!(resolved, next_comp, "not found, abort", explain);
                    giveup!(Broken, path, explain);
                };
                match entry {
                    Entry::Regular { blake2b, len } => {
                        if !left.is_empty() {
                            explain!(
                                resolved,
                                next_comp,
                                "regular file in the middle, abort",
                                explain
                            );
                            giveup!(Broken, path, explain);
                        }
                        // explain!(resolved, next_comp, "resolved to regular file, done", explain);
                        return Ok(Target::Regular(blake2b, len));
                    }
                    Entry::Directory => {
                        explain!(resolved, next_comp, "followed directory", explain);
                        resolved.push(name);
                    }
                    Entry::Symlink { target } => {
                        if lookup >= MAX_SYMLINK_LOOKUP {
                            explain!(
                                resolved,
                                next_comp,
                                format!("too many symlinks ({MAX_SYMLINK_LOOKUP}), abort"),
                                explain
                            );
                            giveup!(Loop, path, explain);
                        }
                        lookup += 1;

                        let target = Path::new(OsStr::from_bytes(&target));
                        explain!(
                            resolved,
                            next_comp,
                            format!("following symlink -> {target:?}"),
                            explain
                        );
                        let mut target = target.components().map(Into::into).rev().collect();
                        left.append(&mut target);
                    }
                }
            }
        }
    }

    // Done, a directory.
    Ok(Target::Directory(resolved.into_os_string().into_vec()))
}
