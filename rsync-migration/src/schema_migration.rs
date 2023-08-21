use clap::Parser;
use eyre::Result;
use rsync_core::pg_lock::PgLock;
use sqlx::PgPool;
use tracing::info;

#[derive(Parser)]
pub struct Args {
    /// Database URL.
    #[clap(long, env = "DATABASE_URL")]
    db_url: String,
}

pub async fn schema_migration(args: Args) -> Result<()> {
    let pool = PgPool::connect(&args.db_url).await?;

    // Only when there's no r/w access to the database can we change the schema.
    info!("waiting for _system lock");
    let system_lock = PgLock::new_exclusive("_system");
    let system_guard = system_lock.lock(pool.acquire().await?).await?;

    sqlx::migrate!("../migrations").run(&pool).await?;

    system_guard.unlock().await?;

    info!("db migrations applied successfully");
    Ok(())
}
