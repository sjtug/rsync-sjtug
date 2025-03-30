use std::collections::BTreeMap;

use bytesize::ByteSize;
use clap::Parser;
use doku::Document;
use eyre::{bail, Context, Result};
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use itertools::Itertools;
use lazy_regex::regex;
use line_span::LineSpans;
use serde::{Deserialize, Deserializer, Serialize};
use tracing::warn;

use rsync_core::logging::{LogFormat, LogTarget};

#[derive(Parser)]
pub struct Opts {
    /// Config file path.
    #[arg(default_value = "config.toml")]
    pub config: String,
    /// Generate a default config file to stdout.
    #[arg(long)]
    pub generate_config: bool,
}

/// Serves rsync repository on S3 over HTTP.
#[derive(Debug, Clone, Serialize, Deserialize, Document)]
pub struct Config {
    /// Bind address.
    #[doku(example = r#"127.0.0.1:8080", "[::1]:8080"#)]
    pub bind: Vec<String>,
    /// Log options.
    pub log: LogOpts,
    /// Log format.
    /// Interval to update metadata. (seconds)
    ///
    /// Note that normally metadata update can be triggered on demand.
    /// This interval only affects the fallback behavior.
    #[doku(example = "300")]
    pub update_interval: u64,
    /// S3 endpoint url.
    ///
    /// To specify credentials, use environment variables:
    /// `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
    pub s3_url: String,
    /// S3 storage region.
    pub s3_region: String,
    /// `PostgreSQL` database url.
    #[doku(example = "postgres://postgres@localhost:5432/rsync-sjtug")]
    pub database_url: String,
    /// Cache options.
    pub cache: CacheOpts,
    /// Gateway endpoints. At least one endpoint is required.
    ///
    /// The key is the http prefix, and it must:
    /// 1. Not be empty.
    /// 2. Not start with '/'.
    /// 3. Be unique.
    pub endpoints: BTreeMap</* http prefix */ String, Endpoint>,
}

/// Log options.
#[derive(Debug, Clone, Serialize, Deserialize, Document)]
pub struct LogOpts {
    /// Log target.
    pub target: LogTarget,
    /// Log format.
    pub format: LogFormat,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, Document)]
pub struct CacheOpts {
    /// L1 cache size.
    #[doku(as = "String", example = "128MB")]
    pub l1_size: ByteSize,
    /// L2 cache size.
    #[doku(as = "String", example = "1GB")]
    pub l2_size: ByteSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: vec!["127.0.0.1:8080".into(), "[::1]:8080".into()],
            log: LogOpts {
                target: LogTarget::Stderr,
                format: LogFormat::Human,
            },
            update_interval: 300,
            cache: CacheOpts {
                l1_size: ByteSize::mb(128),
                l2_size: ByteSize::gb(1),
            },
            s3_url: String::new(),
            s3_region: String::new(),
            database_url: String::from("postgres://postgres@localhost:5432/rsync-sjtug"),
            endpoints: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Document)]
pub struct Endpoint {
    /// S3 bucket.
    #[serde(deserialize_with = "de_trim_end_slash")]
    #[doku(example = "mirror")]
    pub s3_bucket: String,
    /// S3 directory prefix.
    #[serde(default, deserialize_with = "de_trim_end_slash")]
    #[doku(example = "rsync/fedora")]
    pub s3_prefix: String,
    /// Metadata namespace. Need to be unique for each repository.
    ///
    /// Can be different from endpoint name.
    /// But it must be unique across all endpoints.
    #[doku(example = "fedora")]
    pub namespace: String,
    /// Whether to list hidden files.
    #[serde(default)]
    #[doku(example = "false")]
    pub list_hidden: bool,
}

fn de_trim_end_slash<'de, D>(de: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(de)?;
    Ok(s.trim_end_matches('/').to_string())
}

pub fn load_config(conf_path: &str) -> Result<Config> {
    // There's a TOCTOU problem here, but it's not a big deal.
    let _conf = std::fs::read_to_string(conf_path)
        .context(format!("Failed to read config file: {conf_path}"))?;

    let opts: Config = Figment::new()
        .merge(Serialized::defaults(Config::default()))
        .merge(Toml::file(conf_path))
        .merge(Env::raw())
        .extract()?;

    Ok(opts)
}

pub fn validate_config(opts: &Config) -> Result<()> {
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

/// There's a bug in doku that it doesn't support map with struct type.
/// So we have to patch the generated config.
pub fn patch_generated_config(mut generated: String) -> String {
    loop {
        let mut last_table_header = None;

        // Search for clues indicating a map with struct type.
        // Example:
        // string = key = "value"
        if let Some(malformed_field) = generated.line_spans().find_map(|line| {
            if regex!(r#"^\[[\s\S]+\]$"#).is_match(line.as_str()) {
                last_table_header = Some(line.range());
            }
            regex!(r#"^string = [^"]+ ="#)
                .is_match(line.as_str())
                .then_some(line.range())
        }) {
            let last_table_header = last_table_header
                .expect("a malformed field is found but no parent table header is present");

            // Fix the malformed field.
            generated.replace_range(malformed_field.start..malformed_field.start + 9, "");
            // Fix the table header.
            generated.insert(last_table_header.end, '\n');
            generated.insert_str(last_table_header.end - 1, r#"."example""#);
        } else {
            break;
        }
    }

    generated
}
