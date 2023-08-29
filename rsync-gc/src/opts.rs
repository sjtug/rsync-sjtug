use clap::Parser;

use rsync_core::s3::S3Opts;
use rsync_core::utils::parse_ensure_end_slash;

/// Garbage collect old & unused files in S3.
///
/// # GC Policy
///
/// 1. Remove all partial indices before the last live index.
/// 2. Keep at most `keep_partial` partial indices after the last live index.
/// 3. Keep at most `keep_live` live indices.
#[derive(Parser)]
#[clap(verbatim_doc_comment)]
pub struct Opts {
    /// How many live revisions to keep. Check `--help` for details.
    #[clap(short, long, default_value = "3")]
    pub keep: usize,
    /// How many partial revisions to keep. Check `--help` for details.
    #[clap(short, long, default_value = "5")]
    pub partial: usize,
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
}

impl From<&Opts> for S3Opts {
    fn from(value: &Opts) -> Self {
        Self {
            region: value.s3_region.clone(),
            url: value.s3_url.clone(),
            bucket: value.s3_bucket.clone(),
        }
    }
}
