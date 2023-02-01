use std::ffi::OsString;
use std::os::unix::ffi::OsStrExt;

use eyre::Result;
use tokio::io::AsyncWriteExt;

use crate::filter::Rule;
use crate::rsync::handshake::HandshakeConn;

const EXCLUSION_LIST_END: i32 = 0;

impl Rule {
    fn to_command(&self) -> OsString {
        match self {
            Self::Exclude(path) => {
                let mut cmd = OsString::from("-");
                cmd.push(path);
                cmd
            }
            Self::Include(path) => {
                let mut cmd = OsString::from("+");
                cmd.push(path);
                cmd
            }
        }
    }
}

impl<'a> HandshakeConn<'a> {
    pub async fn send_filter_rules(&mut self, rules: &[Rule]) -> Result<()> {
        for rule in rules {
            let cmd = rule.to_command();
            self.tx
                .write_i32_le(i32::try_from(cmd.len()).expect("rule too long"))
                .await?;
            self.tx.write_all(cmd.as_bytes()).await?;
        }
        self.tx.write_i32_le(EXCLUSION_LIST_END).await?;
        Ok(())
    }
}
