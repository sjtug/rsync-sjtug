use std::ffi::OsString;

use clap::{ArgAction, Parser};
use url::Url;

use crate::filter::Rule;

#[derive(Parser)]
pub struct Opts {
    /// Rsync remote url.
    pub src: Url,
    /// S3 storage url.
    /// Format: https://your.domain/bucket_name
    /// For specifying authentication, use environment variables:
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    pub dest: Url,
    /// Metadata storage url. (Redis)
    pub redis: Url,
    /// Metadata namespace. Need to be unique for each repository.
    pub namespace: String,
    /// Force break existing lock.
    /// Only use this if you are sure there's no other fetch process running on the same namespace.
    pub force_break: bool,
    /// Exclude files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub exclude: Vec<OsString>,
    /// Include files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub include: Vec<OsString>,
}

#[derive(Debug, Clone)]
pub struct RedisOpts {
    pub namespace: String,
    pub force_break: bool,
}

impl From<&Opts> for RedisOpts {
    fn from(opts: &Opts) -> Self {
        Self {
            namespace: opts.namespace.clone(),
            force_break: opts.force_break,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RsyncOpts {
    pub filters: Vec<Rule>,
}

impl From<&Opts> for RsyncOpts {
    fn from(opts: &Opts) -> Self {
        let mut filters = Vec::new();
        for pattern in &opts.exclude {
            filters.push(Rule::Exclude(pattern.clone()));
        }
        for pattern in &opts.include {
            filters.push(Rule::Include(pattern.clone()));
        }
        Self { filters }
    }
}
