use std::ffi::OsString;
use std::path::PathBuf;

use clap::{ArgAction, CommandFactory, FromArgMatches, Parser};
use itertools::Itertools;
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
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
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

fn format_error(e: clap::Error) -> clap::Error {
    let mut cmd = Opts::command();
    e.format(&mut cmd)
}

pub fn parse() -> (Opts, RsyncOpts) {
    let mut matches = Opts::command().get_matches();

    let exclude_indices = matches
        .indices_of("exclude")
        .map_or(vec![], Iterator::collect);
    let include_indices = matches
        .indices_of("include")
        .map_or(vec![], Iterator::collect);

    let res = Opts::from_arg_matches_mut(&mut matches).map_err(format_error);

    let opts = match res {
        Ok(s) => s,
        Err(e) => {
            // Since this is more of a development-time error, we aren't doing as fancy of a quit
            // as `get_matches`
            e.exit()
        }
    };

    let exclude_patterns = opts
        .exclude
        .clone()
        .into_iter()
        .map(Rule::Exclude)
        .zip(exclude_indices);
    let include_patterns = opts
        .include
        .clone()
        .into_iter()
        .map(Rule::Include)
        .zip(include_indices);

    let filters = exclude_patterns
        .chain(include_patterns)
        .sorted_by_key(|(_, i)| *i)
        .map(|(p, _)| p)
        .collect();

    (opts, RsyncOpts { filters })
}
