use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use eyre::Result;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::sync::mpsc;

#[derive(Debug, Default)]
struct Ctx {
    basis_downloading: AtomicUsize,
    basis_buf: AtomicUsize,
    pending: AtomicUsize,
    uploading: AtomicUsize,
}

impl Display for Ctx {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let basis_downloading = self.basis_downloading.load(Ordering::Relaxed);
        let basis_buf = self.basis_buf.load(Ordering::Relaxed);
        let pending = self.pending.load(Ordering::Relaxed);
        let uploading = self.uploading.load(Ordering::Relaxed);

        // let max_basis_buf = BASIS_BUFFER_LIMIT * 2;
        // let max_pending = BASIS_BUFFER_LIMIT * 2;
        // let max_uploading = UPLOAD_CONN * 3;

        // write!(f, "[B{basis_buf:0>2}/{max_basis_buf} P{pending:0>2}/{max_pending} U{uploading:0>2}/{max_uploading}]")
        write!(
            f,
            "[B{basis_downloading:0>2}/{basis_buf:0>2} P{pending:0>2} U{uploading:0>2}]"
        )
    }
}

#[derive(Debug, Clone)]
pub struct ProgressDisplay {
    ctx: Arc<Ctx>,
    pb: ProgressBar,
    stop_tx: mpsc::Sender<()>,
}

impl Deref for ProgressDisplay {
    type Target = ProgressBar;

    fn deref(&self) -> &Self::Target {
        &self.pb
    }
}

async fn progress_task(pb: ProgressBar, ctx: Arc<Ctx>, mut stop_rx: mpsc::Receiver<()>) {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = stop_rx.recv() => break,
            _ = interval.tick() => {
                pb.tick();
                pb.set_message(format!("{ctx}"));
            }
        }
    }
    pb.finish_with_message("done");
}

impl ProgressDisplay {
    pub fn new() -> Self {
        let ctx = Arc::new(Ctx::default());
        let pb = ProgressBar::new(0);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta}) {msg}",
            )
                .unwrap()
                .progress_chars("#>-"),
        );
        pb.enable_steady_tick(Duration::from_secs(u64::MAX)); // disable ticking
        pb.set_message(format!("{ctx}"));
        let (stop_tx, stop_rx) = mpsc::channel(1);
        tokio::spawn(progress_task(pb.clone(), ctx.clone(), stop_rx));
        Self { ctx, pb, stop_tx }
    }
    pub fn inc_basis_downloading(&self, i: usize) {
        self.ctx.basis_downloading.fetch_add(i, Ordering::SeqCst);
    }
    pub fn dec_basis_downloading(&self, i: usize) {
        self.ctx.basis_downloading.fetch_sub(i, Ordering::SeqCst);
    }
    pub fn inc_basis(&self, i: usize) {
        self.ctx.basis_buf.fetch_add(i, Ordering::SeqCst);
    }
    pub fn dec_basis(&self, i: usize) {
        self.ctx.basis_buf.fetch_sub(i, Ordering::SeqCst);
    }
    pub fn inc_pending(&self, i: usize) {
        self.ctx.pending.fetch_add(i, Ordering::SeqCst);
    }
    pub fn dec_pending(&self, i: usize) {
        self.ctx.pending.fetch_sub(i, Ordering::SeqCst);
    }
    pub fn inc_uploading(&self, i: usize) {
        self.ctx.uploading.fetch_add(i, Ordering::SeqCst);
    }
    pub fn dec_uploading(&self, i: usize) {
        self.ctx.uploading.fetch_sub(i, Ordering::SeqCst);
    }
    pub async fn finalize(&self) -> Result<()> {
        self.stop_tx.send(()).await?;
        Ok(())
    }
}
