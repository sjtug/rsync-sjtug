#![allow(
    clippy::module_name_repetitions,
    clippy::default_trait_access,
    clippy::future_not_send,
    clippy::too_many_lines
)]

use std::sync::Arc;

use clap::Parser;
use eyre::Result;
use sqlx::PgPool;
use tracing::info;

use rsync_core::pg::RevisionStatus;
use rsync_core::pg_lock::PgLock;
use rsync_core::s3::{build_operator, S3Opts};
use rsync_core::utils::{init_color_eyre, init_logger};

use crate::opts::Opts;
use crate::pg::{hashes_to_remove, keep_last_n, remove_revisions};
use crate::s3::bulk_delete_objs;

mod opts;
mod pg;
mod s3;
#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<()> {
    init_logger();
    init_color_eyre()?;

    dotenvy::dotenv()?;
    let opts = Opts::parse();
    let namespace = &opts.namespace;

    let pool = Arc::new(PgPool::connect(&opts.pg_url).await?);
    // Shared lock table structure to prevent schema changes.
    let system_lock = PgLock::new_shared("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;
    // Exclusively lock namespace to ensure only one write operation(fetch, migrate, gc) is running
    // on the namespace.
    let ns_lock = PgLock::new_exclusive(namespace);
    let ns_guard = ns_lock.lock(pool.acquire().await?).await?;

    let op = build_operator(&S3Opts::from(&opts))?;

    info!("marking stale indices...");
    let mut stale_revs = keep_last_n(namespace, opts.keep, RevisionStatus::Live, &*pool).await?;
    stale_revs.extend(keep_last_n(namespace, opts.partial, RevisionStatus::Stale, &*pool).await?);
    info!(?stale_revs, "marked as stale");

    info!("querying stale objects...");
    let hashes = hashes_to_remove(namespace, &*pool).await?;

    bulk_delete_objs(&op, &opts, &hashes).await?;
    info!("deleted {} objects", hashes.len());

    remove_revisions(&stale_revs, &*pool).await?;

    system_guard.unlock().await?;
    ns_guard.unlock().await?;

    Ok(())
}
