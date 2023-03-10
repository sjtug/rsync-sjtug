use std::ffi::OsStr;
use std::io::SeekFrom;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;

use aws_sdk_s3::error::{HeadObjectError, HeadObjectErrorKind};
use aws_sdk_s3::types::ByteStream;
use backon::Retryable;
use dashmap::DashSet;
use eyre::Result;
use futures::stream::FuturesUnordered;
use futures::TryStreamExt;
use redis::aio;
use tap::TapOptional;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tracing::warn;

use rsync_core::metadata::{MetaExtra, Metadata};
use rsync_core::redis_::{update_metadata, RedisOpts};
use rsync_core::s3::S3Opts;
use rsync_core::utils::{policy, ToHex, ATTR_CHAR};

use crate::rsync::file_list::FileEntry;

pub const UPLOAD_CONN: usize = 4;

pub struct Uploader {
    file_list: Arc<Vec<FileEntry>>,
    s3: aws_sdk_s3::Client,
    s3_opts: S3Opts,
    redis: aio::MultiplexedConnection,
    redis_opts: RedisOpts,
    uploaded: DashSet<[u8; 20]>,
}

pub struct UploadTask {
    pub idx: usize,
    pub blake2b_hash: [u8; 20],
    pub file: File,
}

impl Uploader {
    pub fn new(
        file_list: Arc<Vec<FileEntry>>,
        s3: aws_sdk_s3::Client,
        s3_opts: S3Opts,
        redis: aio::MultiplexedConnection,
        redis_opts: RedisOpts,
    ) -> Self {
        Self {
            file_list,
            s3,
            s3_opts,
            redis,
            redis_opts,
            uploaded: DashSet::new(),
        }
    }

    pub async fn upload_tasks(&self, rx: flume::Receiver<UploadTask>) -> Result<()> {
        let futs: FuturesUnordered<_> = (0..UPLOAD_CONN).map(|_| self.upload_task(&rx)).collect();
        futs.try_collect().await?;
        Ok(())
    }

    async fn upload_task(&self, rx: &flume::Receiver<UploadTask>) -> Result<()> {
        let policy = policy();
        while let Ok(task) = rx.recv_async().await {
            let UploadTask {
                idx,
                blake2b_hash,
                file,
            } = task;

            (|| {
                let file = &file;
                async {
                    // Avoid repeatedly uploading the same file to address
                    // https://stackoverflow.com/questions/63238344/amazon-s3-how-parallel-puts-to-the-same-key-are-resolved-in-versioned-buckets
                    if !self.uploaded.contains(&blake2b_hash) {
                        // Upload file to S3.
                        let entry = &self.file_list[idx];
                        let key = Path::new(OsStr::from_bytes(&entry.name));
                        let filename = key
                            .file_name()
                            .tap_none(|| warn!(?key, "missing filename of entry"))
                            .map(OsStrExt::as_bytes);

                        // REMARK: If a file is soft/hard linked, users may see a content-disposition with a
                        // different filename instead of the one they expect.
                        let mut file = file.try_clone().await.expect("unable to dup file");
                        file.seek(SeekFrom::Start(0))
                            .await
                            .expect("unable to seek file");
                        self.upload_s3(filename, file, &blake2b_hash).await?;
                        self.uploaded.insert(blake2b_hash);
                    }
                    Ok::<_, eyre::Report>(())
                }
            })
            .retry(&policy)
            .await?;

            // Update metadata in Redis.
            self.update_metadata(idx, blake2b_hash).await?;
        }
        Ok(())
    }

    async fn upload_s3(
        &self,
        filename: Option<&[u8]>,
        target_file: File,
        blake2b_hash: &[u8; 20],
    ) -> Result<()> {
        // File is content addressed by its blake2b hash.
        let key = format!("{}{:x}", self.s3_opts.prefix, blake2b_hash.as_hex());

        // Check if file already exists. Might happen if the file is the same between two syncs,
        // or it's hard linked.
        let exists = self
            .s3
            .head_object()
            .bucket(&self.s3_opts.bucket)
            .key(&key)
            .send()
            .await
            .map(|_| true)
            .or_else(|e| match e.into_service_error() {
                HeadObjectError {
                    kind: HeadObjectErrorKind::NotFound(_),
                    ..
                } => Ok(false),
                e => Err(e),
            })?;

        // Only upload if it doesn't exist.
        if !exists {
            let body = ByteStream::read_from().file(target_file).build().await?;
            let content_disposition = filename.map(|filename| {
                let encoded_name = percent_encoding::percent_encode(filename, ATTR_CHAR);
                format!("attachment; filename=\"{encoded_name}\"; filename*=UTF-8''{encoded_name}")
            });

            self.s3
                .put_object()
                .bucket(&self.s3_opts.bucket)
                .key(key)
                .set_content_disposition(content_disposition)
                .body(body)
                .send()
                .await?;
        }
        Ok(())
    }

    async fn update_metadata(&self, idx: usize, blake2b_hash: [u8; 20]) -> Result<()> {
        let entry = &self.file_list[idx];
        let metadata = Metadata {
            len: entry.len,
            modify_time: entry.modify_time,
            extra: MetaExtra::Regular { blake2b_hash },
        };

        // Update metadata in Redis.
        let mut conn = self.redis.clone();
        let maybe_old_meta = update_metadata(
            &mut conn,
            &format!("{}:partial", self.redis_opts.namespace),
            &entry.name,
            metadata,
        )
        .await?;

        // If a previous version of the file exists, add it to partial-stale.
        if let Some(Metadata {
            extra: MetaExtra::Regular {
                blake2b_hash: old_hash,
            },
            ..
        }) = maybe_old_meta
        {
            if old_hash != blake2b_hash {
                drop(
                    update_metadata(
                        &mut conn,
                        &format!("{}:partial-stale", self.redis_opts.namespace),
                        &entry.name,
                        maybe_old_meta.expect("checked above"),
                    )
                    .await?,
                );
            }
        }

        Ok(())
    }
}
