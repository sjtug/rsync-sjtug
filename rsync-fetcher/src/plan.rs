//! Transfer plan.
use std::ops::ControlFlow;
use std::time::SystemTime;

use bincode::config::Configuration;
use bincode::{Decode, Encode};
use either::Either;
use eyre::Result;
use futures::{pin_mut, stream, Stream, StreamExt, TryStream, TryStreamExt};
use redis::{
    AsyncCommands, AsyncIter, Client, FromRedisValue, RedisError, RedisResult, RedisWrite,
    ToRedisArgs, Value,
};

use crate::opts::RedisOpts;
use crate::rsync::file_list::FileEntry;
use crate::set_ops::{into_union, with_sorted_iter, Case};

const BINCODE_CONFIG: Configuration = bincode::config::standard();

pub struct TransferItem {
    idx: i32,
    blake2b_hash: Option<[u8; 20]>,
}

fn async_iter_to_stream<T: FromRedisValue + Unpin>(
    it: AsyncIter<T>,
) -> impl Stream<Item = Result<T>> + '_ {
    stream::unfold(it, |mut it| async move {
        it.next_item()
            .await
            .map_err(eyre::Error::from)
            .transpose()
            .map(|i| (i, it))
    })
}

pub async fn generate_transfer_plan(
    client: &Client,
    remote: &[FileEntry],
    opts: &RedisOpts,
) -> Result<Vec<TransferItem>> {
    let namespace = &opts.namespace;

    let mux_conn = client.get_multiplexed_async_connection().await?;
    let (mut latest_conn, mut partial_conn) = (mux_conn.clone(), mux_conn.clone());
    let (latest_iter, partial_iter) = (
        async_iter_to_stream(
            latest_conn
                .zscan::<_, Vec<u8>>(format!("{namespace}:latest:zset"))
                .await?,
        ),
        async_iter_to_stream(
            partial_conn
                .zscan::<_, Vec<u8>>(format!("{namespace}:partial:zset"))
                .await?,
        ),
    );
    let remote_iter = stream::iter(remote.iter()).map(Ok);

    pin_mut!(latest_iter, partial_iter);

    // local = latest + partial
    let local = into_union(partial_iter, latest_iter, |x, y| x.cmp(&y));
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
            async move {
                Ok(match case {
                    // File only exists on remote, needs to be transferred
                    Case::Left(remote) => ControlFlow::Break(TransferItem {
                        idx: remote.idx,
                        blake2b_hash: None,
                    }),
                    // File only exists on local, no need to transfer
                    Case::Right(_) => ControlFlow::Continue(()),
                    // File exists on both, check if it's the same
                    Case::Both(remote, local) => {
                        let local_loc = if matches!(local, Either::Left(_)) {
                            "partial"
                        } else {
                            "latest"
                        };
                        let metadata: Metadata = mux_conn
                            .hget(format!("{namespace}:{local_loc}:hash"), local.into_inner())
                            .await?;
                        if remote.len == metadata.len && remote.modify_time == metadata.modify_time
                        {
                            // File is the same, no need to transfer
                            ControlFlow::Continue(())
                        } else {
                            // We book the old hash so that we may download the file on s3 and perform delta
                            // transfer.
                            // TODO: if delta transfer is disabled, set blake2b_hash to None
                            ControlFlow::Break(TransferItem {
                                idx: remote.idx,
                                blake2b_hash: Some(metadata.blake2b_hash),
                            })
                        }
                    }
                })
            }
        },
    );

    // TODO we may pass down the stream to the caller, so that we may perform the transfer in parallel
    // But there might be a problem with the size of the future, and may cause a stack overflow.
    plan_stream.try_collect().await
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Metadata {
    pub len: u64,
    pub modify_time: SystemTime,
    // maybe PathBuf?
    pub link_target: Option<Vec<u8>>,
    pub blake2b_hash: [u8; 20],
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
