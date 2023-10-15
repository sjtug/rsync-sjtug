use std::io;
use std::sync::Arc;

use stubborn_io::{ReconnectOptions, StubbornTcpStream};
use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use tracing_subscriber::fmt::MakeWriter;

use crate::utils::AbortJoinHandle;

const MAX_BACKLOG: usize = 1024;

#[derive(Clone)]
pub struct TcpWriter {
    tx: broadcast::Sender<Vec<u8>>,
    _handler: Arc<AbortJoinHandle<()>>,
}

impl TcpWriter {
    pub fn connect(addr: String) -> Self {
        let (tx, mut rx) = broadcast::channel::<Vec<u8>>(MAX_BACKLOG);

        let handler = AbortJoinHandle::new(tokio::spawn(async move {
            let mut stream = StubbornTcpStream::connect_with_options(
                addr.clone(),
                ReconnectOptions::new().with_exit_if_first_connect_fails(false),
            )
            .await
            .expect("never gonna give you up");
            info!(force_stderr = true, addr, "connected to TCP endpoint");

            loop {
                match rx.recv().await {
                    Ok(buf) => {
                        if let Err(err) = stream.write_all(&buf).await {
                            error!(force_stderr = true, %err, "error writing to TCP stream");
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(missed)) => {
                        // The sink is lagging behind, and we have missed some messages.
                        // This prints to stderr immediately.
                        warn!(
                            force_stderr = true,
                            missed, "missed messages due to sink lag"
                        );
                        // And this prints to the sink when it catches up.
                        warn!(
                            missed,
                            "missed messages due to sink lag, sink is now caught up"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        // The sender is dropped, we can exit.
                        break;
                    }
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
            .send(buf.to_vec())
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
