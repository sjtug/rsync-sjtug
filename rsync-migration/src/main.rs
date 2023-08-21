use clap::{Parser, Subcommand};

use rsync_core::utils::{init_color_eyre, init_logger};

use crate::redis_to_pg::redis_to_pg;
use crate::schema_migration::schema_migration;

mod redis_to_pg;
mod schema_migration;

/// rsync-sjtug migration tool.
#[derive(Parser)]
pub struct Args {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Apply database schema migrations.
    SchemaMigration(schema_migration::Args),
    /// Migrate data from redis to postgresql (pre 0.4 -> 0.4).
    RedisToPg(redis_to_pg::Args),
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> eyre::Result<()> {
    init_color_eyre()?;
    init_logger();
    dotenvy::dotenv()?;

    let args = Args::parse();

    match args.cmd {
        Command::SchemaMigration(args) => schema_migration(args).await?,
        Command::RedisToPg(args) => redis_to_pg(args).await?,
    }

    Ok(())
}
