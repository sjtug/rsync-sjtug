# rsync-gc

`rsync-gc` garbage collect old & unused files in S3.

## Implementation Details

1. Connect to Postgres and S3, check if there's already another instance (fetcher, gc) running.
2. Enumerate all production revisions and filter out ones to be removed. Mark them as stale.
3. Delete object files that are not referenced by any live or partial revision.
   > Note that this is calculated by
   >
   > Sigma_(stale) (key.hash) - Sigma_(live) (key.hash) - Sigma_(partial) (key.hash)
4. Remove stale revisions from Postgres.