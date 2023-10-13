use clap::{Parser, Subcommand};

use rsync_core::logging::{init_color_eyre, init_logger};
use rsync_core::logging::{LogFormat, LogTarget};

use crate::redis_to_pg::redis_to_pg;
use crate::schema_migration::schema_migration;
use crate::upgrade_encoding::upgrade_encoding;

mod redis_to_pg;
mod schema_migration;
mod upgrade_encoding;

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
    /// Migrate data from redis to postgresql (0.3 -> 0.4).
    RedisToPg(redis_to_pg::Args),
    /// Upgrade Redis index encoding (0.2 -> 0.3).
    UpgradeEncoding(upgrade_encoding::Args),
}

#[tokio::main]
#[allow(clippy::too_many_lines)]
async fn main() -> eyre::Result<()> {
    init_color_eyre()?;
    init_logger(LogTarget::Stderr, LogFormat::Human);
    drop(dotenvy::dotenv());

    let args = Args::parse();

    match args.cmd {
        Command::SchemaMigration(args) => schema_migration(args).await?,
        Command::RedisToPg(args) => redis_to_pg(args).await?,
        Command::UpgradeEncoding(args) => upgrade_encoding(args).await?,
    }

    Ok(())
}
