/// Stats returned by server at the end of transmission.
#[derive(Debug, Copy, Clone)]
pub struct Stats {
    /// Bytes read.
    pub read: i64,
    /// Bytes written.
    pub written: i64,
    /// Total size of files.
    pub size: i64,
}
