# rsync-gc

`rsync-gc` garbage collect old & unused files in S3.

## Implementation Details

1. Connect to Redis and S3, check if there's already another instance (fetcher, gc) running.
2. Enumerate all production indexes and filter out ones to be removed.
3. Rename the indexes to be removed to `stale:<timestamp>`.
4. Delete all listing files in S3 belonging to the indexes to be removed.
5. Delete object files that are not referenced by any live index.
   > Note that this is calculated by
   > 
   > Sigma_(stale) (key.hash) - Sigma_(alive) (key.hash)
   > 
   > Because we don't have a way to get the universe set of all keys in S3.
6. Remove stale indexes from Redis.