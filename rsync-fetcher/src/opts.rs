use std::ffi::OsString;

use clap::{ArgAction, Parser};
use url::Url;

use crate::rsync::filter::Rule;

const LOCK_TIMEOUT: u64 = 3 * 60;

#[derive(Parser)]
pub struct Opts {
    /// Rsync remote url.
    #[clap(long)]
    pub src: Url,
    /// S3 endpoint url.
    /// For specifying authentication, use environment variables:
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    #[clap(long)]
    pub s3_url: Url,
    /// S3 storage region.
    #[clap(long)]
    pub s3_region: String,
    /// S3 storage bucket.
    #[clap(long)]
    pub s3_bucket: String,
    /// S3 storage prefix.
    #[clap(long)]
    pub s3_prefix: String,
    /// Metadata storage url. (Redis)
    #[clap(long)]
    pub redis: Url,
    /// Metadata namespace. Need to be unique for each repository.
    #[clap(long)]
    pub redis_namespace: String,
    /// Force break existing lock.
    /// Only use this if you are sure there's no other fetch process running on the same namespace.
    #[clap(long)]
    pub force_break: bool,
    /// Exclude files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub exclude: Vec<OsString>,
    /// Include files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub include: Vec<OsString>,
}

#[derive(Debug, Clone)]
pub struct S3Opts {
    pub region: String,
    pub url: Url,
    pub bucket: String,
    // With end slash.
    pub prefix: String,
}

impl From<&Opts> for S3Opts {
    fn from(opts: &Opts) -> Self {
        let prefix = if opts.s3_prefix.ends_with('/') {
            opts.s3_prefix.clone()
        } else {
            format!("{}/", opts.s3_prefix)
        };
        Self {
            region: opts.s3_region.clone(),
            url: opts.s3_url.clone(),
            bucket: opts.s3_bucket.clone(),
            prefix,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RedisOpts {
    pub namespace: String,
    pub force_break: bool,
    pub lock_ttl: u64,
}

impl From<&Opts> for RedisOpts {
    fn from(opts: &Opts) -> Self {
        Self {
            namespace: opts.redis_namespace.clone(),
            force_break: opts.force_break,
            lock_ttl: LOCK_TIMEOUT,
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
