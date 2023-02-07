# rsync-gateway

`rsync-gateway` serves the rsync repository on S3 over HTTP, using the metadata stored in redis.

## Implementation Details

1. Connect to Redis.
2. Spawn a watcher task to watch for the latest index.
3. For each request, if the path ends with a trailing slash, it's a directory listing request. Otherwise, it's a file
   request.
4. For directory listing requests, redirect to `<path>/index.html` on S3.
5. For file requests, check if the file exists in the index. If not, return 404. Otherwise, redirect to the file on S3.
   Symlinks are resolved on the gateway side.