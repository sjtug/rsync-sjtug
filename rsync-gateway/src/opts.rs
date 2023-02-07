use clap::Parser;
use url::Url;

/// Serves rsync repository on S3 over HTTP.
#[derive(Debug, Clone, Parser)]
pub struct Opts {
    /// Bind address.
    pub bind: String,
    /// S3 HTTP endpoint.
    #[clap(long)]
    pub s3_base: String,
    /// Metadata storage url. (Redis)
    #[clap(long)]
    pub redis: Url,
    /// Metadata namespace. Need to be unique for each repository.
    #[clap(long)]
    pub redis_namespace: String,
    /// Interval to update metadata. (seconds)
    ///
    /// Note that metadata will be updated if a corresponding redis key is updated.
    /// This interval is just for force update.
    #[clap(long, default_value = "300")]
    pub update_interval: u64,
}
