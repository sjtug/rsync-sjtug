use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aws_sdk_s3::model::{Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use backon::{BackoffBuilder, Retryable};
use eyre::Result;
use futures::stream::FuturesUnordered;
use futures::{FutureExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools;
use scan_fmt::scan_fmt_some;
use tracing::warn;

use rsync_core::s3::S3Opts;
use rsync_core::utils::{policy, ToHex};

#[cfg(not(test))]
const ITER_BATCH: i32 = 1000;

#[cfg(test)]
const ITER_BATCH: i32 = 5;

const DELETE_CONN: usize = 16;

async fn delete_task(
    rx: flume::Receiver<Delete>,
    pb: &ProgressBar,
    pb_spinner: bool,
    policy: &impl BackoffBuilder,
    client: &Client,
    bucket: &str,
    count: &AtomicU64,
) -> Result<()> {
    while let Ok(batch) = rx.recv_async().await {
        (|| {
            let batch = batch.clone();
            let batch_len = batch.objects().unwrap_or_default().len();
            async move {
                let resp = client
                    .delete_objects()
                    .bucket(bucket)
                    .delete(batch)
                    .send()
                    .await?;
                let deleted = resp.deleted().unwrap_or_default().len() as u64;
                let prev = count.fetch_add(batch_len as u64, Ordering::Relaxed);
                if pb_spinner {
                    pb.set_message(format!("{} objects", prev + deleted));
                } else {
                    pb.inc(batch_len as u64);
                }
                Ok::<_, eyre::Report>(())
            }
        })
        .retry(policy)
        .await?;
    }
    Ok::<_, eyre::Report>(())
}

pub async fn delete_listing(client: &Client, opts: &S3Opts, delete_before: u64) -> Result<u64> {
    let bucket = &opts.bucket;

    let prefix = format!("{}listing-", opts.prefix);
    let scan_pat = format!("{prefix}{{d}}/");

    let spinner = ProgressBar::new_spinner();
    spinner.enable_steady_tick(Duration::from_millis(50));

    let deleted = AtomicU64::new(0);

    let policy = policy();
    let (tx, rx) = flume::bounded::<Delete>(DELETE_CONN * 2);
    let futs: FuturesUnordered<_> = (0..DELETE_CONN)
        .map(|_| {
            delete_task(
                rx.clone(),
                &spinner,
                true,
                &policy,
                client,
                bucket,
                &deleted,
            )
            .left_future()
        })
        .collect();
    let gen_fut = async move {
        let mut maybe_cached_prefix = None;
        let mut continuation_token = None;
        loop {
            let objects = client
                .list_objects_v2()
                .bucket(&opts.bucket)
                .prefix(&prefix)
                .max_keys(ITER_BATCH)
                .set_continuation_token(continuation_token.clone())
                .send()
                .await?;
            let mut delete_batch = Delete::builder();
            let mut end_of_scan = false;
            for obj in objects.contents().unwrap_or_default() {
                let Some(key) = obj.key() else { continue; };
                if maybe_cached_prefix
                    .as_ref()
                    .map_or(false, |prefix| key.starts_with(prefix))
                {
                    // Starts with known stale index, delete
                    delete_batch =
                        delete_batch.objects(ObjectIdentifier::builder().key(key).build());
                    continue;
                }
                let Some(timestamp) = scan_fmt_some!(key, &scan_pat, u64) else {
                    warn!(key, "Unexpected key in listing bucket");
                    continue;
                };
                if timestamp < delete_before {
                    // Update known stale index
                    maybe_cached_prefix = Some(format!("{prefix}{timestamp}"));
                    // Delete
                    delete_batch =
                        delete_batch.objects(ObjectIdentifier::builder().key(key).build());
                } else {
                    // Stop scanning.
                    end_of_scan = true;
                    break;
                }
            }

            let delete_batch = delete_batch.build();

            // Execute delete batch.
            if delete_batch.objects().is_some() {
                tx.send_async(delete_batch).await?;
            }

            // If end of scan, break.
            if end_of_scan {
                break;
            }

            // Update continuation token.
            if let Some(token) = objects.next_continuation_token() {
                continuation_token = Some(token.to_string());
            } else {
                // end of stream.
                break;
            }
        }
        Ok::<_, eyre::Report>(())
    };

    futs.push(gen_fut.right_future());
    futs.try_collect::<()>().await?;

    spinner.finish_and_clear();

    Ok(deleted.load(Ordering::Relaxed))
}

pub async fn bulk_delete_objs(
    client: &Client,
    opts: &S3Opts,
    hashes: &HashSet<[u8; 20]>,
) -> Result<u64> {
    let S3Opts { prefix, bucket, .. } = opts;

    let pb = ProgressBar::new(hashes.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    let deleted = AtomicU64::new(0);

    let policy = policy();
    let (tx, rx) = flume::bounded::<Delete>(DELETE_CONN * 2);
    let futs: FuturesUnordered<_> = (0..DELETE_CONN)
        .map(|_| {
            delete_task(rx.clone(), &pb, false, &policy, client, bucket, &deleted).left_future()
        })
        .collect();
    let chunks = hashes.iter().chunks(ITER_BATCH as usize);
    let gen_fut = async move {
        for chunk in &chunks {
            let mut delete_batch = Delete::builder();
            for hash in chunk {
                let key = format!("{prefix}{:x}", hash.as_hex());
                delete_batch = delete_batch.objects(ObjectIdentifier::builder().key(key).build());
            }
            let delete_batch = delete_batch.build();
            tx.send_async(delete_batch).await?;
        }
        Ok::<_, eyre::Report>(())
    };

    futs.push(gen_fut.right_future());
    futs.try_collect::<()>().await?;

    Ok(deleted.load(Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::{format, vec};

    use rsync_core::s3::tests::with_s3;

    use crate::s3::delete_listing;

    #[tokio::test]
    async fn must_remove() {
        for idx in 8..10 {
            for blk in 8..10 {
                for del in 0..idx {
                    test_remove(blk, idx, del as u64).await;
                }
            }
        }
    }

    async fn test_remove(block_size: usize, index_count: usize, delete_before: u64) {
        let mut objs = vec![];
        for idx in 0..index_count {
            for blk in 0..block_size {
                objs.push((format!("test/listing-{idx}/{blk}.html"), vec![]));
            }
        }
        let (_guard, client, opts) = with_s3(&objs).await;
        delete_listing(&client, &opts, delete_before)
            .await
            .expect("delete listing");

        let objs = client
            .list_objects_v2()
            .bucket(&opts.bucket)
            .send()
            .await
            .expect("list objects");

        let mut expects = BTreeSet::new();
        #[allow(clippy::cast_possible_truncation)]
        for idx in delete_before as usize..index_count {
            for blk in 0..block_size {
                expects.insert(format!("test/listing-{idx}/{blk}.html"));
            }
        }

        for obj in objs.contents().unwrap_or_default() {
            let key = obj.key().expect("key");
            assert!(expects.remove(key), "unexpected key: {key}");
        }
        assert!(expects.is_empty(), "missing keys: {expects:?}");
    }
}
