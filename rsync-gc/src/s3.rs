use std::time::Duration;

use eyre::Result;
use futures::{stream, StreamExt, TryFutureExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use opendal::Operator;

use rsync_core::utils::ToHex;

use crate::opts::Opts;

const ITER_BATCH: usize = 1000;

const DELETE_CONN: usize = 16;

pub async fn bulk_delete_objs(op: &Operator, opts: &Opts, hashes: &[[u8; 20]]) -> Result<()> {
    let prefix = &opts.s3_prefix;

    let pb = ProgressBar::new(hashes.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    stream::iter(hashes.chunks(ITER_BATCH).map(|chunk| {
        let keys = chunk
            .iter()
            .map(|hash| format!("{prefix}{:x}", hash.as_hex()));
        op.delete_iter(keys).inspect_ok(|()| {
            pb.inc(chunk.len() as u64);
        })
    }))
    .buffer_unordered(DELETE_CONN)
    .try_collect::<()>()
    .await?;

    Ok(())
}
