use std::collections::BTreeMap;
use std::env;

use eyre::{bail, Context, Result};
use serde::{Deserialize, Deserializer};
use tracing::warn;
use url::Url;

/// Serves rsync repository on S3 over HTTP.
#[derive(Debug, Clone, Deserialize)]
pub struct Opts {
    /// Bind address.
    pub bind: Vec<String>,
    /// Interval to update metadata. (seconds)
    ///
    /// Note that metadata will be updated if a corresponding redis key is updated.
    /// This interval is just for force update.
    #[serde(default = "default_update_interval")]
    pub update_interval: u64,
    /// Gateway endpoints.
    pub endpoints: BTreeMap</* http prefix */ String, Endpoint>,
}

const fn default_update_interval() -> u64 {
    300
}

#[derive(Debug, Clone, Deserialize)]
pub struct Endpoint {
    /// S3 website endpoint.
    #[serde(deserialize_with = "de_trim_end_slash")]
    pub s3_website: String,
    /// Metadata storage url. (Redis)
    pub redis: Url,
    /// Metadata namespace. Need to be unique for each repository.
    pub redis_namespace: String,
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

    let conf = std::fs::read_to_string(&conf_path)
        .context(format!("Failed to read config file: {conf_path}"))?;

    let opts: Opts = toml::from_str(&conf).context("Failed to parse config file")?;

    Ok(opts)
}

pub fn validate_config(opts: &Opts) -> Result<()> {
    if opts.endpoints.is_empty() {
        warn!("No endpoints configured");
    }

    if opts.endpoints.len() > 1
        && opts
            .endpoints
            .keys()
            .map(|prefix| prefix.trim_end_matches('/'))
            .any(str::is_empty)
    {
        bail!("Only one endpoint can be configured without prefix");
    }

    Ok(())
}
