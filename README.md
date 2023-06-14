# rsync-sjtug

rsync-sjtug is an open-source project designed to provide an efficient method of mirroring remote repositories to s3
storage, with atomic updates and periodic garbage collection.

This project implements the rsync wire protocol, and is compatible with the rsync protocol version 27. All rsyncd
versions older than 2.6.0 are supported.

`rsync-sjtug` is currently powering the [sjtug mirror](https://mirror.sjtu.edu.cn/).

## Features

* Atomic repository update: users never see a partially updated repository.
* Periodic garbage collection: old versions of files can be removed from the storage.
* Delta transfer: only the changed parts of files are transferred.

## Commands

* **rsync-fetcher** - fetches the repository from the remote server, and uploads it to s3.
* **rsync-gateway** - serves the mirrored repository from s3 in **http** protocol.
* **rsync-gc** - periodically removes old versions of files from s3.
* **rsync-fix-encoding** - see "Migrating from v0.2.11 to older versions" section.

## Example

1. Sync rsync repository to S3.
    ```bash
    $ RUST_LOG=info RUST_BACKTRACE=1 AWS_ACCESS_KEY_ID=<ID> AWS_SECRET_ACCESS_KEY=<KEY> \
      rsync-fetcher \
        --src rsync://upstream/path \
        --s3-url https://s3_api_endpoint --s3-region region --s3-bucket bucket --s3-prefix repo_name \
        --redis redis://localhost --redis-namespace repo_name \ 
        --repository repo_name
        --gateway-base http://localhost:8081/repo_name
    ```
2. Serve the repository over HTTP.
    ```bash
    $ cat > config.toml <<-EOF
    bind = ["localhost:8081"]

    [endpoints."out"]
    redis = "redis://localhost"
    redis_namespace = "test"
    s3_website = "http://localhost:8080/test/test-prefix"
   
    EOF

    $ RUST_LOG=info RUST_BACKTRACE=1 rsync-gateway <optional config file>
    ```

3. GC old versions of files periodically.
    ```bash
    $ RUST_LOG=info RUST_BACKTRACE=1 AWS_ACCESS_KEY_ID=<ID> AWS_SECRET_ACCESS_KEY=<KEY> \
      rsync-gc \
        --s3-url https://s3_api_endpoint --s3-region region --s3-bucket bucket --s3-prefix repo_name \
        --redis redis://localhost --redis-namespace repo_name \ 
        --keep 2
    ```
   > It's recommended to keep at least 2 versions of files in case a gateway is still using an old revision.

## Design

File data and their metadata are stored separately.

### Data

Files are stored in S3 storage, named by their blake2b-160 hash (`<namespace/<hash>`).

Listing html pages are stored in `<namespace>/listing-<timestamp>/<path>/index.html`.

### Metadata

Metadata is stored in Redis for fast access.

Note that there are more than one file index in Redis.

- `<namespace>:index:<timestamp>` - an index of the repository synced at `<timestamp>`.
- `<namespace>:partial` - a partial index that is still being updated and not committed yet.
- `<namespace>:partial-stale` - a temporary index that is used to store outdated files when updating the partial index.
  This might happen if you interrupt a synchronization, restart it, and some files downloaded in the first run are
  already outdated. It's ready to be garbage collected.
- `<namespace>:stale:<timestamp>` - an index that is taken out of production, and is ready to be garbage collected.

> Not all files in partial index should be removed. For example, if a file exists both in a stale index and a "live"
> index, it should not be removed.

## Migrating from v0.2.11 to older versions

There's a bug affecting all versions before v0.3.0 and after v0.2.11, which causes the file metadata to be read in a
wrong format and silently corrupting the index. Note that no data is lost, but the gateway will fail to direct users to
the correct file. `rsync-fix-encoding` can be used to fix this issue.

After v0.3.0, all commands are using the new encoding. You can still use this tool to migrate old data to the new
encoding. Trying to use the new commands on old data will now fail.