# rsync-fetcher

This is a rsync receiver implementation. Simply put, it's much like rsync, but saves the files to s3 and metadata to
redis instead of to a filesystem.

## Features

- Incremental update
- Keep partial files
- Delta transfer
- Atomic commit

## Non-features

- Won't delete extraneous files, this is handled by *rsync-gc*
- Won't try to keep a standard directory structure on S3, you need *rsync-gateway* to serve the files on HTTP

## Implementation Details

1. Connect to Redis and S3, check if there's already another instance running.
2. Fetch file list from rsync server.
3. Calculate the delta between the remote file list and the local index, which is
   the union of current production index and last partial index (if any).
4. Start generator and receiver task.
5. After both tasks completed, commit the partial index to production.

Generator task:

1. Generates a list of files to be fetched, and sends them to the rsync server.
2. If any file exists in the local index, it downloads the file, calculate the rolling checksum, and additionally sends
   the checksum to rsync server.

Receiver task:

1. The receiver task receives files from rsync server, and upload them to S3.
2. If received a delta, it patches the existing local file previously downloaded and upload the patched file to S3.
3. After uploading a file, updates the partial index. If the file already exists in the partial index, check if the
   checksum matches. If not, put the old metadata into the partial-stale index, and update the partial index with the
   new metadata.