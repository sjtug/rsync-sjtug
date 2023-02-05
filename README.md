# rsync-sjtug

> WIP: This project is still under development, and is not ready for production use.

rsync-sjtug is an open-source project designed to provide an efficient method of mirroring remote repositories to s3
storage, with atomic updates and periodic garbage collection.

This project implements the rsync wire protocol, and is compatible with the rsync protocol version 27. All rsyncd
versions older than 2.6.0 are supported.

## Features

* Atomic repository update: users never see a partially updated repository.
* Periodic garbage collection: old versions of files can be removed from the storage.
* Delta transfer: only the changed parts of files are transferred. Please see the [Delta Transfer]() section below for
  details.

## Commands

* **rsync-fetcher** - fetches the repository from the remote server, and uploads it to s3.
* **rsync-gateway** - serves the mirrored repository from s3 in **http** protocol.
* **rsync-gc** - periodically removes old versions of files from s3.

## Design

File data and their metadata are stored separately.

### Data

Files are stored in S3 storage, named by their blake2b-160 hash.

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

## Delta Transfer

rsync-sjtug implements the delta transfer algorithm described in the rsync protocol specification, which can reduce the
amount of data transferred from remote server.

However, because the basis file is not available locally, it needs to be fetched before a delta can be calculated.
What's more, S3 doesn't support random writes, so the patched file must be uploaded completely.

Therefore, if your S3 storage is not close (e.g. in the same network) to your rsync server, you may want to disable it.