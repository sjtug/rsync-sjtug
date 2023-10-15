use std::io;
use std::sync::Arc;

use stubborn_io::{ReconnectOptions, StubbornTcpStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::fmt::MakeWriter;

use crate::utils::AbortJoinHandle;

const MAX_BACKLOG: usize = 1024;

#[derive(Clone)]
pub struct TcpWriter {
    tx: mpsc::Sender<Vec<u8>>,
    _handler: Arc<AbortJoinHandle<()>>,
}

impl TcpWriter {
    pub fn connect(addr: String) -> Self {
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(MAX_BACKLOG);

        let handler = AbortJoinHandle::new(tokio::spawn(async move {
            let mut stream = StubbornTcpStream::connect_with_options(
                addr,
                ReconnectOptions::new().with_exit_if_first_connect_fails(false),
            )
            .await
            .expect("never gonna give you up");
            info!(force_stderr = true, "connected to TCP endpoint");

            while let Some(buf) = rx.recv().await {
                if let Err(err) = stream.write_all(&buf).await {
                    // We do not use tracing here because we are in middle of a logging operation.
                    error!(force_stderr=true, %err, "error writing to TCP stream");
                }
            }
        }));

        Self {
            tx,
            _handler: Arc::new(handler),
        }
    }
}

impl io::Write for TcpWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let size = buf.len();
        self.tx
            .try_send(buf.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
        Ok(size)
    }

    fn flush(&mut self) -> io::Result<()> {
        // We do not block the main thread.
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for TcpWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
