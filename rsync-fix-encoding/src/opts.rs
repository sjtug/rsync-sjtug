use clap::Parser;
use url::Url;

use rsync_core::redis_::RedisOpts;

const LOCK_TIMEOUT: u64 = 3 * 60;

/// Garbage collect old & unused files in S3.
#[derive(Parser)]
pub struct Opts {
    /// Metadata storage url. (Redis)
    #[clap(long)]
    pub redis: Url,
    /// Metadata namespace. Need to be unique for each repository.
    #[clap(long)]
    pub redis_namespace: String,
    /// Do actual fix work. Will touch metadata server.
    #[clap(long)]
    pub fix: bool,
}

impl From<&Opts> for RedisOpts {
    fn from(opts: &Opts) -> Self {
        Self {
            namespace: opts.redis_namespace.clone(),
            force_break: false,
            lock_ttl: LOCK_TIMEOUT,
        }
    }
}
