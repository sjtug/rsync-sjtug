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
* **rsync-migration** - see [Migration](#migration) section for more details.

## Example

1. Sync rsync repository to S3.
    ```bash
    $ RUST_LOG=info RUST_BACKTRACE=1 AWS_ACCESS_KEY_ID=<ID> AWS_SECRET_ACCESS_KEY=<KEY> \
      rsync-fetcher \
        --src rsync://upstream/path \
        --s3-url https://s3_api_endpoint --s3-region region --s3-bucket bucket --s3-prefix prefix \
        --pg-url postgres://user@localhost/db \
        --namespace repo_name
    ```
2. Serve the repository over HTTP.
    ```bash
    $ cat > config.toml <<-EOF
    bind = ["localhost:8081"]
    s3_url = "https://s3_api_endpoint"
    s3_region = "region"

    [endpoints."out"]
    namespace = "repo_name"
    s3_bucket = "bucket"
    s3_prefix = "prefix"
   
    EOF

    $ RUST_LOG=info RUST_BACKTRACE=1 rsync-gateway <optional config file>
    ```
3. GC old versions of files manually.
    ```bash
    $ RUST_LOG=info RUST_BACKTRACE=1 AWS_ACCESS_KEY_ID=<ID> AWS_SECRET_ACCESS_KEY=<KEY> \
      rsync-gc \
        --s3-url https://s3_api_endpoint --s3-region region --s3-bucket bucket --s3-prefix repo_name \
        --pg-url postgres://user@localhost/db
    ```
   > It's recommended to keep at least 2 revisions in case a gateway is still using an old revision.

## Design

File data and their metadata are stored separately.

### Data

Files are stored in S3 storage, named by their blake2b-160 hash (`<prefix>/<namespace>/<hash>`).

### Metadata

Metadata is stored in Postgres.

An object is the smallest unit of metadata. There are three types of objects:
- **File** - a regular file, with its hash, size and mtime
- **Directory** - a directory, and its size and mtime
- **Symlink** - a symlink, with its size, mtime and target

Objects (files, directories and symlinks) are organized into revisions, which are immutable. Each revision has a unique
id, while an object may appear in multiple revisions. Revisions are further organized into repositories (namespaces),
like `debian`, `ubuntu`, etc. Repositories are mutable.

A revision can be in one of the following states:

- **Live** - a live revision is a revision in production, which is ready to be served. There can be multiple live 
    revisions, but only the latest one is served by the gateway.
- **Partial** - a partial revision is a revision that is still being updated. It's not ready to be served yet.
- **Stale** - a stale revision is a revision that is no longer in production, and is ready to be garbage collected.

## Migration

### Migration from v0.3.x to v0.4.x

v0.4.x switched from Redis to Postgres for storing metadata, greatly improving the performance of many operations and
reducing the storage usage.

Use `rsync-migration redis-to-pg` to migrate old metadata to the new database. Note that you can only migrate from
v0.3.x to v0.4.x, and you can't migrate from v0.2.x to v0.4.x directly.

The old Redis database is not modified.

### Migrating from v0.2.x to v0.3.x

v0.3.x uses a new encoding for file metadata, which is incompatible with v0.2.x. Trying to use v0.3.x on old data will
fail.

Use `rsync-migration upgrade-encoding` to upgrade the encoding.

This is a destructive operation, so make sure you have a backup of the database before running it. It does nothing
without the `--do` flag.

The new encoding is actually introduced in v0.2.12 by accident. `rsync-gateway` between v0.2.12 and v0.3.0 can't parse
old metadata correctly and return garbage data. No data is lost though, so if you used any version between v0.2.12 and
v0.3.0, you can still use `rsync-migration` to migrate to the new encoding.