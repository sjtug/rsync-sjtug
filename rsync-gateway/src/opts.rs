use std::collections::BTreeMap;
use std::env;

use eyre::{bail, Context, Result};
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use itertools::Itertools;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;

/// Serves rsync repository on S3 over HTTP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Opts {
    /// Bind address.
    pub bind: Vec<String>,
    /// Interval to update metadata. (seconds)
    ///
    /// Note that metadata will be updated if a corresponding revision is updated.
    /// This interval is just for force update.
    pub update_interval: u64,
    /// S3 endpoint url.
    /// For specifying authentication, use environment variables:
    /// AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    pub s3_url: String,
    /// S3 storage region.
    pub s3_region: String,
    /// PostgreSQL database url.
    pub database_url: String,
    /// Gateway endpoints.
    pub endpoints: BTreeMap</* http prefix */ String, Endpoint>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            bind: vec!["127.0.0.1:8080".into(), "[::1]:8080".into()],
            update_interval: 300,
            s3_url: String::new(),
            s3_region: String::new(),
            database_url: String::from("postgres://postgres@localhost:5432/rsync-sjtug"),
            endpoints: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    /// S3 bucket.
    #[serde(deserialize_with = "de_trim_end_slash")]
    pub s3_bucket: String,
    /// S3 directory prefix.
    #[serde(default, deserialize_with = "de_trim_end_slash")]
    pub s3_prefix: String,
    /// Metadata namespace. Need to be unique for each repository.
    /// Can be different from endpoint name.
    ///
    /// But no two endpoints can share the same namespace.
    pub namespace: String,
}

fn de_trim_end_slash<'de, D>(de: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(de)?;
    Ok(s.trim_end_matches('/').to_string())
}

pub fn load_config() -> Result<Opts> {
    let conf_path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    let _conf = std::fs::read_to_string(&conf_path)
        .context(format!("Failed to read config file: {conf_path}"))?;

    let opts: Opts = Figment::new()
        .merge(Serialized::defaults(Opts::default()))
        .merge(Toml::file(&conf_path))
        .merge(Env::raw())
        .extract()?;

    Ok(opts)
}

pub fn validate_config(opts: &Opts) -> Result<()> {
    if opts.endpoints.is_empty() {
        warn!("No endpoints configured");
    }

    if opts
        .endpoints
        .keys()
        .map(|prefix| prefix.trim_end_matches('/'))
        .any(str::is_empty)
    {
        bail!("Endpoint prefix cannot be empty");
    }

    if opts.endpoints.keys().any(|prefix| prefix.starts_with('/')) {
        bail!("Endpoint prefix cannot start with '/'");
    }

    if !opts.endpoints.keys().all_unique() {
        bail!("Endpoint prefix must be unique");
    }

    if !opts.endpoints.values().map(|ep| &ep.namespace).all_unique() {
        bail!("Endpoint namespace must be unique");
    }

    Ok(())
}
