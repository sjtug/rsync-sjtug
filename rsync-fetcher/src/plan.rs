//! Transfer plan.
use std::ops::ControlFlow;
use std::time::{SystemTime, UNIX_EPOCH};

use bincode::config::Configuration;
use bincode::{Decode, Encode};
use either::Either;
use eyre::Result;
use futures::{pin_mut, stream, StreamExt};
use redis::{
    AsyncCommands, Client, FromRedisValue, RedisError, RedisResult, RedisWrite, ToRedisArgs, Value,
};
use tracing::{info, instrument};

use crate::opts::RedisOpts;
use crate::redis_::get_index;
use crate::rsync::file_list::FileEntry;
use crate::set_ops::{into_union, with_sorted_iter, Case};

const BINCODE_CONFIG: Configuration = bincode::config::standard();

#[derive(Debug, Eq, PartialEq)]
pub struct TransferItem {
    pub idx: u32,
    pub blake2b_hash: Option<[u8; 20]>,
}

pub struct TransferPlan {
    /// Files need to be downloaded from rsync server.
    pub downloads: Vec<TransferItem>,
    /// Files need to be copied from latest index.
    pub copy_from_latest: Vec<Vec<u8>>,
    /// Files need to be moved from partial to partial-stale index.
    pub stale: Vec<Vec<u8>>,
}

enum TransferKind {
    // The latest index has up-to-date file.
    Latest(Vec<u8>),
    // Stale file to be removed from partial.
    Stale(Vec<u8>),
    // Need to download from rsync server.
    Remote(TransferItem),
}

#[instrument(skip_all)]
pub async fn generate_transfer_plan(
    client: &Client,
    remote: &[FileEntry],
    opts: &RedisOpts,
    latest_index: &Option<String>,
) -> Result<TransferPlan> {
    let namespace = &opts.namespace;

    let mux_conn = client.get_multiplexed_async_connection().await?;
    let partial_index = format!("{namespace}:partial");
    info!(
        latest_index,
        "generating transfer plan: remote Δ (latest + partial)"
    );

    let (mut latest_conn, mut partial_conn) = (mux_conn.clone(), mux_conn.clone());
    let (latest_iter, partial_iter) = (
        if let Some(latest_index) = latest_index {
            // If there are index in production, fetch the latest one.
            stream::iter(get_index(&mut latest_conn, latest_index).await?)
                .map(Ok)
                .left_stream()
        } else {
            // Otherwise, we use an empty stream.
            stream::empty().right_stream()
        },
        stream::iter(get_index(&mut partial_conn, &partial_index).await?).map(Ok),
    );
    let remote_iter = stream::iter(remote.iter()).map(Ok);

    pin_mut!(latest_iter, partial_iter);

    // local = latest (if any) + partial.
    // Note that entries in partial have higher priority than latest.
    let local = into_union(partial_iter, latest_iter, Ord::cmp);
    pin_mut!(local);

    // Generate the transfer plan.
    // The plan is a stream of TransferItem, which contains the index of the file in the remote file
    // list, and optionally the blake2b hash of the file on s3 (so that we may fetch the basis file
    // and perform delta transfer, noting that files are content addressed on s3).
    let plan_stream = with_sorted_iter(
        remote_iter,
        local,
        |x, y| x.name.cmp(&y.as_ref()),
        |case| {
            let mut mux_conn = mux_conn.clone();
            let partial_index = &partial_index;
            let latest_index = latest_index.as_ref();
            async move {
                Ok(match case {
                    // File only exists on remote, needs to be transferred
                    Case::Left(remote) => ControlFlow::Break(TransferKind::Remote(TransferItem {
                        idx: remote.idx,
                        blake2b_hash: None,
                    })),
                    // File only exists in latest or partial.
                    Case::Right(local) => {
                        let in_partial = matches!(local, Either::Left(_));
                        if in_partial {
                            // File only exists in partial, needs to be removed.
                            ControlFlow::Break(TransferKind::Stale(local.into_inner()))
                        } else {
                            // File only exists in latest. It would be GCed but we don't care now.
                            ControlFlow::Continue(())
                        }
                    }
                    // File exists on both, check if it's the same
                    Case::Both(remote, local) => {
                        let in_partial = matches!(local, Either::Left(_));
                        let name = local.into_inner();

                        let index = if in_partial {
                            partial_index
                        } else {
                            latest_index.as_ref().expect("right is latest index")
                        };
                        let metadata: Metadata = mux_conn.hget(index, &name).await?;
                        if remote.len == metadata.len
                            && mod_time_eq(remote.modify_time, metadata.modify_time)
                        {
                            // File is the same.
                            if in_partial {
                                ControlFlow::Continue(())
                            } else {
                                // But we still to update metadata in partial.
                                ControlFlow::Break(TransferKind::Latest(name))
                            }
                        } else {
                            // File differs.
                            // We book the old hash so that we may download the file on s3 and perform delta
                            // transfer.
                            // TODO: if delta transfer is disabled, set blake2b_hash to None
                            // TODO: or we can't set it to None, because we need to detect partial-stale?
                            let blake2b_hash = match metadata.extra {
                                MetaExtra::Symlink { .. } => None,
                                MetaExtra::Regular { blake2b_hash } => Some(blake2b_hash),
                            };
                            ControlFlow::Break(TransferKind::Remote(TransferItem {
                                idx: remote.idx,
                                blake2b_hash,
                            }))
                        }
                    }
                })
            }
        },
    );

    // TODO we may pass down the stream to the caller, so that we may perform the transfer in parallel
    // But there might be a problem with the size of the future, and may cause a stack overflow.
    // Also we need to clone the channel for `apply_symlink`, `index_copy` and `generator`, so might
    // not be a good idea anyway.
    let (mut downloads, mut copy_from_latest, mut stale) = (vec![], vec![], vec![]);

    pin_mut!(plan_stream);
    while let Some(item) = plan_stream.next().await.transpose()? {
        match item {
            TransferKind::Remote(item) => downloads.push(item),
            TransferKind::Latest(name) => copy_from_latest.push(name),
            TransferKind::Stale(name) => stale.push(name),
        }
    }

    Ok(TransferPlan {
        downloads,
        copy_from_latest,
        stale,
    })
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Metadata {
    pub len: u64,
    pub modify_time: SystemTime,
    pub extra: MetaExtra,
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum MetaExtra {
    Symlink {
        // maybe PathBuf?
        target: Vec<u8>,
    },
    Regular {
        blake2b_hash: [u8; 20],
    },
}

impl FromRedisValue for Metadata {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        match v {
            Value::Data(data) => {
                let (metadata, _) =
                    bincode::decode_from_slice(data, BINCODE_CONFIG).map_err(|e| {
                        RedisError::from((
                            redis::ErrorKind::TypeError,
                            "Response data not valid metadata",
                            e.to_string(),
                        ))
                    })?;
                Ok(metadata)
            }
            _ => Err(RedisError::from((
                redis::ErrorKind::TypeError,
                "Response was of incompatible type",
            ))),
        }
    }
}

impl ToRedisArgs for Metadata {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        let buf = bincode::encode_to_vec(self, BINCODE_CONFIG).expect("bincode encode failed");
        out.write_arg(&buf);
    }
}

pub fn mod_time_eq(x: SystemTime, y: SystemTime) -> bool {
    x.duration_since(UNIX_EPOCH)
        .expect("time before unix epoch")
        .as_secs()
        == y.duration_since(UNIX_EPOCH)
            .expect("time before unix epoch")
            .as_secs()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::plan::{generate_transfer_plan, Metadata, TransferItem};
    use crate::rsync::file_list::FileEntry;
    use crate::tests::{generate_random_namespace, redis_client, MetadataIndex};

    async fn assert_plan(
        remote: &[FileEntry],
        use_latest: bool,
        latest: &[(String, Metadata)],
        partial: &[(String, Metadata)],
        expect_downloads: Option<&[TransferItem]>,
        expect_copy_from_latest: Option<&[&[u8]]>,
        expect_stale: Option<&[&[u8]]>,
    ) {
        let client = redis_client();
        let namespace = generate_random_namespace();

        let latest_index = format!("{namespace}:latest");
        let _partial_guard = MetadataIndex::new(&client, &format!("{namespace}:partial"), partial);
        let _latest_guard = MetadataIndex::new(&client, &latest_index, latest);

        let plan = generate_transfer_plan(
            &client,
            remote,
            &namespace.parse().unwrap(),
            &use_latest.then_some(latest_index),
        )
        .await
        .unwrap();

        assert_eq!(plan.downloads, expect_downloads.unwrap_or(&[]));
        assert_eq!(
            plan.copy_from_latest,
            expect_copy_from_latest.unwrap_or(&[])
        );
        assert_eq!(plan.stale, expect_stale.unwrap_or(&[]));
    }

    #[tokio::test]
    async fn must_plan_no_latest() {
        for use_latest in [false, true] {
            assert_plan(
                &[
                    FileEntry::regular("a".into(), 1, UNIX_EPOCH, 0),
                    FileEntry::regular("b".into(), 1, UNIX_EPOCH, 1),
                ],
                use_latest,
                &[],
                &[],
                Some(&[TransferItem::new(0, None), TransferItem::new(1, None)]),
                None,
                None,
            )
            .await;
        }
    }

    #[tokio::test]
    async fn must_plan_with_latest() {
        assert_plan(
            &[
                FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
            ],
            true,
            &[
                ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
            ],
            &[],
            Some(&[
                TransferItem::new(0, Some([0; 20])), // len differs
                TransferItem::new(1, Some([1; 20])), // time differs
                TransferItem::new(3, None),          // new file
            ]),
            Some(&[
                b"c", // up-to-date in latest but not exist in partial
            ]),
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn must_plan_with_partial() {
        assert_plan(
            &[
                FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
            ],
            false,
            &[],
            &[
                ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
            ],
            Some(&[
                TransferItem::new(0, Some([0; 20])), // len differs
                TransferItem::new(1, Some([1; 20])), // time differs
                TransferItem::new(3, None),          // new file
            ]),
            None,
            Some(&[
                b"d", // exists in partial but not in remote
                     // For files with different len and time between partial and remote, we can't be
                     // sure whether they are stale until recv, so we don't mark them as stale here.
            ]),
        )
        .await;
    }

    #[tokio::test]
    async fn must_plan_with_partial_latest() {
        assert_plan(
            &[
                FileEntry::regular("a".into(), 2, UNIX_EPOCH, 0),
                FileEntry::regular("b".into(), 1, UNIX_EPOCH + Duration::from_secs(1), 1),
                FileEntry::regular("c".into(), 1, UNIX_EPOCH, 2),
                FileEntry::regular("e".into(), 1, UNIX_EPOCH, 3),
                FileEntry::regular("f".into(), 1, UNIX_EPOCH, 4),
                FileEntry::regular("g".into(), 1, UNIX_EPOCH, 5),
                FileEntry::regular("h".into(), 1, UNIX_EPOCH, 6),
            ],
            true,
            &[
                ("a".into(), Metadata::regular(2, UNIX_EPOCH, [0; 20])),
                ("b".into(), Metadata::regular(1, UNIX_EPOCH, [1; 20])),
                ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                ("f".into(), Metadata::regular(2, UNIX_EPOCH, [4; 20])),
                ("g".into(), Metadata::regular(1, UNIX_EPOCH, [6; 20])),
                ("h".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
            ],
            &[
                ("a".into(), Metadata::regular(1, UNIX_EPOCH, [0; 20])),
                (
                    "b".into(),
                    Metadata::regular(1, UNIX_EPOCH + Duration::from_secs(1), [1; 20]),
                ),
                ("c".into(), Metadata::regular(1, UNIX_EPOCH, [2; 20])),
                ("d".into(), Metadata::regular(1, UNIX_EPOCH, [3; 20])),
                ("f".into(), Metadata::regular(2, UNIX_EPOCH, [5; 20])),
                ("g".into(), Metadata::regular(2, UNIX_EPOCH, [6; 20])),
            ],
            Some(&[
                TransferItem::new(0, Some([0; 20])), // len differs, partial prio
                // 1 is up-to-date in partial but outdated in latest, partial prio
                // 2 is up-to-date in partial but not exist in latest, partial prio
                TransferItem::new(3, None),          // new file
                TransferItem::new(4, Some([5; 20])), // len differs, partial prio (hash)
                // 5 is outdated in partial but up-to-date in latest, partial prio
                // TODO is this reasonable? anyway this case should be rare
                TransferItem::new(5, Some([6; 20])),
            ]),
            Some(&[
                b"h", // up-to-date in latest but not exist in partial
            ]),
            Some(&[
                b"d", // exists in partial but not in remote
            ]),
        )
        .await;
    }
}
