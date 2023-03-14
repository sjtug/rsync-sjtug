/// Max follow depth of symlinks.
pub const MAX_DEPTH: usize = 64;
/// Max number of concurrent connections for uploading to S3.
pub const UPLOAD_CONN: usize = 8;
/// Max number of concurrent connections for downloading from S3.
pub const DOWNLOAD_CONN: usize = UPLOAD_CONN;
/// Max number of basis buffer files allowed to be open, or to be pending at the same time.
/// NOTE: therefore, at most `BASIS_BUFFER_LIMIT * 2` buffer files can be exist at the same time.
pub const BASIS_BUFFER_LIMIT: usize = UPLOAD_CONN * 2;
/// Lock timeout of redis lock.
pub const LOCK_TIMEOUT: u64 = 3 * 60;
