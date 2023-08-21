# rsync-fetcher

This is a rsync receiver implementation. Simply put, it's much like rsync, but saves the files to s3 and metadata to
the database instead of to a filesystem.

## Features

- Incremental update
- Keep partial files
- Delta transfer
- Atomic commit

## Non-features

- Won't delete extraneous files, this is handled by *rsync-gc*
- Won't try to keep a standard directory structure on S3, you need *rsync-gateway* to serve the files on HTTP

## Implementation Details

1. Connect to Postgres and S3, check if there's already another instance (fetcher, gc) running.
2. Fetch file list from rsync server.
3. Calculate the delta between the remote file list and local files, which is the union of files in all live and partial
revisions.
4. Create a new partial revision.
5. Start generator and receiver task.
6. After both tasks completed, update some metadata (parents link) to speedup directory listing.
7. Commit the partial revision to production.

Generator task:

1. Generates a list of files to be fetched, and sends them to the rsync server.
2. If any file exists in an existing live or partial revision, it downloads the file, calculate the rolling checksum,
and additionally sends the checksum to rsync server.

Receiver task:

1. The receiver task receives files from rsync server.
2. If received a delta, it patches the existing local file previously downloaded.
3. Sends the file to the uploader task.

Uploader task:

1. Take files downloaded by receiver task, and upload them to S3.
2. After uploading a file, updates the partial revision.