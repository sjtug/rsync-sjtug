use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use eyre::{eyre, Report, Result};
use futures::{FutureExt, TryFutureExt};
use get_size::GetSize;
use metrics::{counter, histogram};
use moka::future::Cache;
use moka::Expiry;
use rkyv::{Deserialize, Infallible};
use tracing::{debug, instrument, warn};

use crate::metrics::{
    COUNTER_L1_HIT, COUNTER_L2_HIT, COUNTER_MISS, COUNTER_POOR, COUNTER_SMALL,
    HISTOGRAM_COMPRESSION_RATIO, HISTOGRAM_L1_QUERY_TIME, HISTOGRAM_L2_QUERY_TIME,
    HISTOGRAM_MISS_QUERY_TIME, HISTOGRAM_RESOLVED_SIZE,
};
use crate::opts::CacheOpts;
use crate::path_resolve::{ArchivedResolved, Resolved};

/// Pre-sign timeout is 24 hours.
pub const PRESIGN_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 24);

/// 2-level NINE cache for resolved results in a single namespace.
///
/// # Benefits
/// 1. Less space usage than normal cache by compressing less frequently accessed paths
/// 2. Less access time than storing certain patterns compressed in a single cache, ignoring
///    their access patterns. Can probably reduce the risk of `DoS` attacks by deliberately
///    requesting compressed paths very frequently.
pub struct NSCache {
    inner: ArcSwap<NSCacheInner>,
    sync_l2: bool, // do not return until L2 cache is synced
    opts: CacheOpts,
}

impl NSCache {
    /// Create a new cache.
    pub fn new(opts: CacheOpts) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(NSCacheInner::new(
                opts.l1_size.as_u64(),
                opts.l2_size.as_u64(),
            ))),
            sync_l2: false,
            opts,
        }
    }
    /// Create a new cache.
    ///
    /// Note: the cache created by this function forces L2 cache to be synced before returning
    /// from `get_or_insert`. In other words, all inserted value are guaranteed to be in L2 cache
    /// when `get_or_insert` returns.
    #[cfg(test)]
    pub fn new_with_sync_l2(opts: CacheOpts) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(NSCacheInner::new(
                opts.l1_size.as_u64(),
                opts.l2_size.as_u64(),
            ))),
            sync_l2: true,
            opts,
        }
    }
    /// Invalidate the cache. Takes effect immediately.
    pub fn invalidate(&self) {
        self.inner.store(Arc::new(NSCacheInner::new(
            self.opts.l1_size.as_u64(),
            self.opts.l2_size.as_u64(),
        )));
    }
    /// Invalidate L1 cache only.
    /// Debug only.
    pub fn invalidate_l1(&self) {
        let guard = self.inner.load();
        guard.l1.invalidate_all();
    }
    pub async fn get_or_insert(
        &self,
        key: &[u8],
        init: impl Future<Output = Result<Resolved>>,
    ) -> Result<Arc<Resolved>, Arc<Report>> {
        // We have this weird return type to avoid losing information during error propagation.

        let guard = self.inner.load();
        guard.get_or_insert(key, init, self.sync_l2).await
    }
}

pub struct NSCacheInner {
    /// L1 cache stores uncompressed resolved results.
    l1: RawCache,
    /// L2 cache stores (maybe) compressed resolved results.
    l2: RawCompressedCache,
}

impl NSCacheInner {
    fn new(l1_capacity: u64, l2_capacity: u64) -> Self {
        Self {
            l1: Cache::builder()
                .max_capacity(l1_capacity)
                // expire file entries after pre-sign timeout
                .expire_after(ExpiryPolicy)
                // we use heap size as weight
                .weigher(|k: &Vec<u8>, v: &Arc<Resolved>| {
                    u32::try_from(k.get_heap_size() + v.get_heap_size()).unwrap_or(u32::MAX)
                })
                .build(),
            l2: Cache::builder()
                .max_capacity(l2_capacity)
                // expire file entries after pre-sign timeout
                .expire_after(ExpiryPolicy)
                // we use heap size as weight
                .weigher(|k: &Vec<u8>, v: &Arc<MaybeCompressed>| {
                    u32::try_from(k.get_heap_size() + v.get_heap_size()).unwrap_or(u32::MAX)
                })
                .build(),
        }
    }
    #[allow(clippy::cast_precision_loss)]
    #[instrument(skip_all, fields(key = % String::from_utf8_lossy(key)))]
    async fn get_or_insert(
        &self,
        key: &[u8],
        init: impl Future<Output = Result<Resolved>>,
        sync_l2: bool,
    ) -> Result<Arc<Resolved>, Arc<Report>> {
        // We have this weird return type to avoid losing information during error propagation.

        let started = Instant::now();

        // Fast path: already exists in L1 cache
        if let Some(resolved) = self.l1.get(key).await {
            let elapsed = started.elapsed();
            histogram!(HISTOGRAM_L1_QUERY_TIME).record(elapsed.as_millis() as f64);
            counter!(COUNTER_L1_HIT).increment(1);
            return Ok(resolved);
        }

        // Now there might be multiple threads trying to get_or_insert the same key.
        // We'll deal with this later. No calculation done yet.
        let init = self.l2.get(key).await.map_or_else(
            || {
                counter!(COUNTER_MISS).increment(1);
                init.map_ok(|resolved| {
                    let elapsed = started.elapsed();
                    histogram!(HISTOGRAM_MISS_QUERY_TIME).record(elapsed.as_millis() as f64);
                    let resolved_size = resolved.get_heap_size();
                    #[allow(clippy::cast_precision_loss)]
                    {
                        histogram!(HISTOGRAM_RESOLVED_SIZE).record(resolved_size as f64);
                    }
                    Arc::new(resolved)
                })
                .right_future()
            },
            |maybe_compressed| {
                // Data still in L2 cache, but not in L1 cache.
                // Then we need to decompress it to L1 cache.
                counter!(COUNTER_L2_HIT).increment(1);
                let current_span = tracing::Span::current();
                async move {
                    tokio::task::spawn_blocking(move || {
                        let _guard = current_span.enter();
                        let decompressed = maybe_compressed.decompress();

                        let elapsed = started.elapsed();
                        histogram!(HISTOGRAM_L2_QUERY_TIME).record(elapsed.as_millis() as f64);

                        decompressed
                    })
                    .await
                    .map_err(|e| eyre!("decompress task failed: {}", e))
                }
                .left_future()
            },
        );

        // We get_or_insert results into L1 cache. This is when real calculation happens.
        // Only one thread doing the actual job, others will wait for it. No risk of cache stampede.
        let resolved = self.l1.try_get_with_by_ref(key, init).await?;

        // Now we've got the resolved object, we need to compress it and put it into L2 cache in
        // case it's not there already.
        // We do this asynchronously even though there might be a short period of time when the
        // entry only exists in L1 cache but not in L2 cache. This should be okay as long as we are
        // a NINE cache, and in most cases subsequent requests will hit L1 cache.
        let handle = tokio::spawn({
            let l2 = self.l2.clone();
            let key = key.to_vec();
            let resolved = resolved.clone();
            async move {
                let current_span = tracing::Span::current();
                // Must be wrapped in a future, otherwise the task will start immediately.
                let fut = async move {
                    tokio::task::spawn_blocking(move || {
                        let _guard = current_span.enter();
                        Arc::new(MaybeCompressed::compress_from(resolved))
                    })
                    .await
                };
                // Also, only one thread doing the actual job.
                if let Err(e) = l2.clone().try_get_with(key, fut).await {
                    warn!(?e, "error occur when putting into L2 cache");
                }
            }
        });

        if sync_l2 {
            handle.await.expect("L2 cache task failed");
        }

        Ok(resolved)
    }
}

/// Maybe compressed resolved object.
///
/// Some resolved objects are not responding well to compression, so we can store them as a
/// reference to uncompressed resolved object in the L2 cache.
#[derive(GetSize)]
pub enum MaybeCompressed {
    /// Owned compressed bytes.
    Compressed { data: Vec<u8>, len: usize },
    /// Reference to uncompressed resolved object.
    Uncompressed(Arc<Resolved>),
}

impl MaybeCompressed {
    #[allow(clippy::cognitive_complexity)] // why? this function is quite simple...
    #[instrument(skip_all, fields(resolved_size = % resolved.get_heap_size()))]
    pub fn compress_from(resolved: Arc<Resolved>) -> Self {
        if matches!(&*resolved, Resolved::Regular { .. }) || resolved.get_heap_size() < 2048 {
            // Too small, not worth compressing.
            counter!(COUNTER_SMALL).increment(1);
            debug!(
                size = resolved.get_heap_size(),
                "too small, not worth compressing"
            );
            return Self::Uncompressed(resolved);
        }

        // Maybe another scratch space or serializer?
        let bytes = rkyv::to_bytes::<_, 8192>(&*resolved).expect("can't be serialized");
        let mut compressed = zstd::bulk::compress(&bytes, 0).expect("can't be compressed");
        compressed.shrink_to_fit();
        if compressed.len() > bytes.len() * 3 / 4 {
            // Not responding well to compression.
            counter!(COUNTER_POOR).increment(1);
            debug!("not responding well to compression, give up");
            return Self::Uncompressed(resolved);
        }

        let ratio = 100 - compressed.len() * 100 / bytes.len();
        #[allow(clippy::cast_precision_loss)]
        {
            histogram!(HISTOGRAM_COMPRESSION_RATIO).record(ratio as f64);
        }
        Self::Compressed {
            data: compressed,
            len: bytes.len(),
        }
    }
    /// Decompress the object.
    #[instrument(skip_all)]
    pub fn decompress(&self) -> Arc<Resolved> {
        match self {
            Self::Compressed { data, len } => {
                let decompressed =
                    zstd::bulk::decompress(data, *len).expect("incorrect decompress capacity");
                let archived = unsafe { rkyv::archived_root::<Resolved>(&decompressed) };
                let deserialized =
                    <ArchivedResolved as Deserialize<Resolved, Infallible>>::deserialize(
                        archived,
                        &mut Infallible,
                    )
                    .expect("infallible");
                Arc::new(deserialized)
            }
            Self::Uncompressed(resolved) => resolved.clone(),
        }
    }
}

pub type RawCache = Cache<Vec<u8>, Arc<Resolved>>;
pub type RawCompressedCache = Cache<Vec<u8>, Arc<MaybeCompressed>>;

struct ExpiryPolicy;

impl Expiry<Vec<u8>, Arc<Resolved>> for ExpiryPolicy {
    fn expire_after_create(
        &self,
        _: &Vec<u8>,
        value: &Arc<Resolved>,
        current_time: Instant,
    ) -> Option<Duration> {
        if let Resolved::Regular { expired_at, .. } = &**value {
            Some(current_time - *expired_at)
        } else {
            None
        }
    }
}

impl Expiry<Vec<u8>, Arc<MaybeCompressed>> for ExpiryPolicy {
    fn expire_after_create(
        &self,
        key: &Vec<u8>,
        value: &Arc<MaybeCompressed>,
        current_time: Instant,
    ) -> Option<Duration> {
        match &**value {
            MaybeCompressed::Compressed { .. } => None,
            MaybeCompressed::Uncompressed(resolved) => {
                self.expire_after_create(key, resolved, current_time)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::explicit_deref_methods, clippy::ignored_unit_patterns)]

    use std::future::ready;

    use bytesize::ByteSize;
    use eyre::eyre;
    use proptest::strategy::{Just, Strategy};
    use proptest::{prop_assert_eq, prop_assume, prop_oneof};
    use proptest_derive::Arbitrary;
    use rstest::rstest;
    use test_strategy::proptest;
    use tokio::sync::oneshot;

    use crate::cache::NSCache;
    use crate::opts::CacheOpts;
    use crate::path_resolve::Resolved;

    const CACHE_CONFIG: CacheOpts = CacheOpts {
        l1_size: ByteSize::mib(32),
        l2_size: ByteSize::mib(128),
    };

    #[derive(Debug, Copy, Clone, Eq, PartialEq, Arbitrary)]
    enum InvalidateLevel {
        L1,
        All,
        None,
    }

    impl InvalidateLevel {
        fn execute_on(self, cache: &NSCache) -> bool {
            match self {
                Self::L1 => {
                    cache.invalidate_l1();
                    false
                }
                Self::All => {
                    cache.invalidate();
                    true
                }
                Self::None => false,
            }
        }
    }

    #[proptest(async = "tokio")]
    async fn must_ok_refl_prop(key: Vec<u8>, resolved: Resolved, invalidate: InvalidateLevel) {
        let cache = NSCache::new_with_sync_l2(CACHE_CONFIG);
        let result = cache
            .get_or_insert(&key, ready(Ok(resolved.clone())))
            .await
            .expect("no error");
        prop_assert_eq!(&*result, &resolved);

        let purged = invalidate.execute_on(&cache);

        let input = ready(if purged {
            Ok(resolved.clone())
        } else {
            Err(eyre!("boom"))
        });
        let result = cache.get_or_insert(&key, input).await.expect("no error");
        prop_assert_eq!(&*result, &resolved);
    }

    fn level_no_all_strategy() -> impl Strategy<Value = InvalidateLevel> {
        prop_oneof![Just(InvalidateLevel::L1), Just(InvalidateLevel::None)]
    }

    #[proptest(async = "tokio")]
    async fn must_ok2_refl_prop(
        key1: Vec<u8>,
        resolved1: Resolved,
        key2: Vec<u8>,
        resolved2: Resolved,
        insert_in_first_pass: bool,
        #[strategy(level_no_all_strategy())] invalidate: InvalidateLevel,
    ) {
        prop_assume!(key1 != key2);
        let cache = NSCache::new_with_sync_l2(CACHE_CONFIG);
        cache
            .get_or_insert(&key1, ready(Ok(resolved1)))
            .await
            .expect("no error");
        if insert_in_first_pass {
            cache
                .get_or_insert(&key2, ready(Ok(resolved2.clone())))
                .await
                .expect("no error");
        }

        let purged = !insert_in_first_pass || invalidate.execute_on(&cache);

        let input = ready(if purged {
            Ok(resolved2.clone())
        } else {
            Err(eyre!("boom"))
        });
        let result = cache.get_or_insert(&key2, input).await.expect("no error");
        prop_assert_eq!(&*result, &resolved2);
    }

    #[rstest]
    #[case(InvalidateLevel::L1)]
    #[case(InvalidateLevel::All)]
    #[case(InvalidateLevel::None)]
    #[tokio::test]
    async fn must_err_refl(#[case] invalidate: InvalidateLevel) {
        let cache = NSCache::new_with_sync_l2(CACHE_CONFIG);

        let result = cache.get_or_insert(&[1], ready(Err(eyre!("boom1")))).await;
        if let Err(e) = &result {
            assert_eq!(e.to_string(), "boom1");
        } else {
            panic!("must be error");
        }

        invalidate.execute_on(&cache);

        let result = cache.get_or_insert(&[1], ready(Err(eyre!("boom2")))).await;
        if let Err(e) = &result {
            assert_eq!(e.to_string(), "boom2");
        } else {
            panic!("must be error");
        }
    }

    #[tokio::test]
    async fn must_race() {
        let cache = NSCache::new(CACHE_CONFIG);

        let (tx, rx) = oneshot::channel();
        let resolved = Resolved::Directory { entries: vec![] };

        let result_1 = cache.get_or_insert(&[1], {
            let resolved = resolved.clone();
            async move {
                rx.await.expect("recv");
                Ok(resolved)
            }
        });
        let result_2 = cache.get_or_insert(&[1], {
            async move {
                panic!("must not be called");
            }
        });
        tx.send(()).expect("send");

        assert_eq!(*result_1.await.expect("no error"), resolved);
        assert_eq!(*result_2.await.expect("no error"), resolved);
    }
}
