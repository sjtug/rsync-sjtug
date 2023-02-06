use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::time::{Duration, SystemTime};

use eyre::{bail, Result};
use indicatif::ProgressBar;
use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;
use tracing::{debug, info, warn};

use crate::rsync::envelope::{EnvelopeRead, RsyncReadExt};

const XMIT_SAME_MODE: u8 = 1 << 1;
const XMIT_SAME_NAME: u8 = 1 << 5;
const XMIT_LONG_NAME: u8 = 1 << 6;
const XMIT_SAME_TIME: u8 = 1 << 7;

const PATH_MAX: u32 = 4096;

#[derive(Clone)]
pub struct FileEntry {
    // maybe PathBuf?
    pub name: Vec<u8>,
    pub len: u64,
    pub modify_time: SystemTime,
    pub mode: u32,
    // maybe PathBuf?
    pub link_target: Option<Vec<u8>>,
    // int32 in rsync, but it couldn't be negative yes?
    pub idx: u32,
}

impl FileEntry {
    pub fn name_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.name)
    }
}

impl Debug for FileEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileEntry")
            .field("name", &self.name_lossy())
            .field("len", &self.len)
            .field("modify_time", &self.modify_time)
            .field("mode", &self.mode)
            .field(
                "link_target",
                &(self
                    .link_target
                    .as_ref()
                    .map(|s| String::from_utf8_lossy(s))),
            )
            .field("idx", &self.idx)
            .finish()
    }
}

impl EnvelopeRead<BufReader<OwnedReadHalf>> {
    pub async fn recv_file_list(&mut self) -> Result<Vec<FileEntry>> {
        let mut list = vec![];

        let spinner = ProgressBar::new_spinner();
        spinner.enable_steady_tick(Duration::from_millis(50));
        let mut name_scratch = Vec::new();
        loop {
            let b = self.read_u8().await?;
            if b == 0 {
                break;
            }

            let entry = self
                .recv_file_entry(b, &mut name_scratch, list.last())
                .await?;
            debug!(?entry, "recv file entry");
            list.push(entry);
            if list.len() % 100 == 0 {
                spinner.set_message(format!("{} files", list.len()));
            }
        }
        spinner.finish_and_clear();

        list.sort_unstable_by(|x, y| x.name.cmp(&y.name));
        list.dedup_by(|x, y| x.name == y.name);

        info!("{} files", list.len());

        // Now we mark their idx
        for (idx, entry) in list.iter_mut().enumerate() {
            entry.idx = u32::try_from(idx).expect("file list too long");
        }

        // enveloped_conn.consume_uid_mapping().await?;
        let io_errors = self.read_i32_le().await?;
        if io_errors != 0 {
            warn!("server reported IO errors: {}", io_errors);
        }

        Ok(list)
    }

    async fn recv_file_entry(
        &mut self,
        flags: u8,
        name_scratch: &mut Vec<u8>,
        prev: Option<&FileEntry>,
    ) -> Result<FileEntry> {
        let same_name = flags & XMIT_SAME_NAME != 0;
        let long_name = flags & XMIT_LONG_NAME != 0;
        let same_time = flags & XMIT_SAME_TIME != 0;
        let same_mode = flags & XMIT_SAME_MODE != 0;

        let inherit_name_len = if same_name { self.read_u8().await? } else { 0 };
        let name_len = if long_name {
            self.read_u32_le().await?
        } else {
            u32::from(self.read_u8().await?)
        };

        if name_len > PATH_MAX - u32::from(inherit_name_len) {
            bail!("path too long");
        }

        assert!(
            inherit_name_len as usize <= name_scratch.len(),
            "file list inconsistency"
        );
        name_scratch.resize(inherit_name_len as usize + name_len as usize, 0);
        self.read_exact(&mut name_scratch[inherit_name_len as usize..])
            .await?;
        // TODO: does rsync’s clean_fname() and sanitize_path() combination do
        // anything more than Go’s filepath.Clean()?
        let name = name_scratch.clone();

        // File length should always be positive right?
        #[allow(clippy::cast_sign_loss)]
        let len = self.read_rsync_long().await? as u64;

        let modify_time = if same_time {
            prev.expect("prev must exist").modify_time
        } else {
            // To avoid Y2038 problem, newer versions of rsync daemon treat mtime as u32 when
            // speaking protocol version 27.
            let secs = self.read_u32_le().await?;
            SystemTime::UNIX_EPOCH + Duration::new(u64::from(secs), 0)
        };

        let mode = if same_mode {
            prev.expect("prev must exist").mode
        } else {
            self.read_u32_le().await?
        };

        let is_link = unix_mode::is_symlink(mode);

        // Preserve uid is disabled
        // Preserve gid is disabled
        // Preserve dev and special are disabled

        // Preserve links
        let link_target = if is_link {
            let len = self.read_u32_le().await?;
            let mut buf = vec![0u8; len as usize];
            self.read_exact(&mut buf).await?;
            Some(buf)
        } else {
            None
        };

        Ok(FileEntry {
            name,
            len,
            modify_time,
            mode,
            link_target,
            idx: u32::MAX, // to be filled later
        })
    }
}
