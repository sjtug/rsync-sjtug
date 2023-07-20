use std::ffi::OsString;
use std::path::PathBuf;

use clap::{ArgAction, Parser};
use url::Url;

use rsync_core::s3::S3Opts;
use rsync_core::utils::parse_ensure_end_slash;

use crate::rsync::filter::Rule;

#[derive(Parser)]
pub struct Opts {
    /// Rsync remote url.
    #[clap(long)]
    pub src: Url,
    /// S3 endpoint url.
    /// For specifying authentication, use environment variables:
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    #[clap(long)]
    pub s3_url: String,
    /// S3 storage region.
    #[clap(long)]
    pub s3_region: String,
    /// S3 storage bucket.
    #[clap(long)]
    pub s3_bucket: String,
    /// S3 storage prefix.
    #[clap(long, value_parser = parse_ensure_end_slash)]
    pub s3_prefix: String,
    /// Postgres database URL.
    #[clap(long, env = "DATABASE_URL")]
    pub pg_url: String,
    /// Metadata namespace. Need to be unique for each repository.
    #[clap(long)]
    pub namespace: String,
    /// Exclude files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub exclude: Vec<OsString>,
    /// Include files matching given pattern.
    #[clap(long, action = ArgAction::Append)]
    pub include: Vec<OsString>,
    /// Disable delta transfer.
    #[clap(long)]
    pub no_delta: bool,
    /// Temporary directory.
    #[clap(long, default_value = "/tmp")]
    pub tmp_path: PathBuf,
}

impl From<&Opts> for S3Opts {
    fn from(opts: &Opts) -> Self {
        Self {
            region: opts.s3_region.clone(),
            url: opts.s3_url.clone(),
            bucket: opts.s3_bucket.clone(),
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
