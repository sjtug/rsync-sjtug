#![allow(clippy::module_name_repetitions)]

use std::time::Duration;

use clap::Parser;
use eyre::Result;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use redis::AsyncCommands;
use tracing::{info, warn};

use rsync_core::redis_::{acquire_instance_lock, RedisOpts};
use rsync_core::utils::init_logger;

use crate::index::scan_index;
use crate::metadata::{try_parse, CFG_STD};
use crate::opts::Opts;

mod index;
mod metadata;
mod opts;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    color_eyre::install()?;

    let opts = Opts::parse();
    let redis_opts = RedisOpts::from(&opts);

    let redis = redis::Client::open(opts.redis)?;
    let mut redis_conn = redis.get_async_connection().await?;
    let mut redis_write_conn = redis.get_async_connection().await?;

    let _lock = acquire_instance_lock(&redis, &redis_opts).await?;

    info!("scanning index...");
    let indices = scan_index(&mut redis_conn, &redis_opts.namespace)
        .await?
        .try_collect::<Vec<_>>()
        .await?;

    for index in &indices {
        info!(index, "processing index");
        let l = redis_conn.hlen::<_, u64>(index).await?;

        let pb = ProgressBar::new(l);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));

        let mut new_meta = 0usize;
        let mut old_meta = 0usize;
        let mut stream = redis_conn.hscan::<_, (Vec<u8>, Vec<u8>)>(index).await?;
        while let Some((name, entry)) = stream.next_item().await? {
            match try_parse(&entry) {
                Some((parsed, is_new)) => {
                    if is_new {
                        new_meta += 1;
                    } else {
                        old_meta += 1;
                    }
                    if opts.fix && !is_new {
                        let buf =
                            bincode::encode_to_vec(parsed, CFG_STD).expect("bincode encode failed");
                        redis_write_conn.hset(index, name, buf).await?;
                    }
                }
                None => {
                    warn!(name = ?String::from_utf8_lossy(&name), "failed to parse metadata");
                }
            }
            pb.inc(1);
        }
        info!(index, new_meta, old_meta, "done");
    }

    info!(fix = opts.fix, "all done");
    Ok(())
}
