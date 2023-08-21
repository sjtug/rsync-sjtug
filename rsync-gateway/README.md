# rsync-gateway

`rsync-gateway` serves the rsync repository on S3 over HTTP, using the metadata stored in the database.

## Implementation Details

1. Connect to Postgres.
2. Spawn a watcher task to watch for the latest revision.
3. For each request, check if there's a cache hit. Return the cached response if there is.
4. Otherwise, try to resolve the path to in the revision. If the path is a directory, render the directory listing. If 
the path is a file, pre-sign the file on S3 and redirect to the pre-signed URL. Symlinks are followed.

## More details on the cache

There are two layers of cache: L1 and L2. Both of them are in-memory cache implemented using `moka`, a concurrent LRU
cache.

L1 cache is raw resolved entries, while L2 cache is compressed entries. The L2 cache is used to reduce memory
usage, since the raw resolved entries can be quite large when there are many files in a directory.

The size of the L1 cache is 32MB, and the size of the L2 cache is 128MB. There's a TTL for pre-signed URLs in both
caches, which is half the pre-signed URL expiration time.

It's a NINE cache. The eviction of L1 and L2 caches are independent and asynchronous.