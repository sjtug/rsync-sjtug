use clap::Parser;
use url::Url;

use rsync_core::redis_::RedisOpts;
use rsync_core::s3::S3Opts;

const LOCK_TIMEOUT: u64 = 3 * 60;

/// Garbage collect old & unused files in S3.
#[derive(Parser)]
pub struct Opts {
    /// How many revisions to keep.
    #[clap(short, long)]
    pub keep: usize,
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

impl From<&Opts> for RedisOpts {
    fn from(opts: &Opts) -> Self {
        Self {
            namespace: opts.redis_namespace.clone(),
            force_break: opts.force_break,
            lock_ttl: LOCK_TIMEOUT,
        }
    }
}
