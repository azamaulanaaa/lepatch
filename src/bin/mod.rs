use std::{
    fs,
    io::{self, Write},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use lepatch::{command::backup::backup, reader::ChunkerConfig, storage};
use tracing::level_filters::LevelFilter;

#[derive(Debug, Clone, Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    Backup {
        source: PathBuf,
        output: PathBuf,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
    },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    let log_level = if args.verbose {
        LevelFilter::TRACE
    } else {
        LevelFilter::INFO
    };

    tracing_subscriber::fmt().with_max_level(log_level).init();

    match args.command {
        Commands::Backup {
            source,
            output,
            overwrite,
        } => {
            let mut index_file = {
                let mut path = output.clone();
                path.add_extension("idx");

                let file = if overwrite {
                    fs::File::create(&path)?
                } else {
                    fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&path)?
                };

                file
            };

            let config = ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            };
            let storage = storage::BlobFileStorage::new(output, overwrite).await?;

            let key = backup(source, storage, config).await?;

            index_file.write_all(key.as_bytes())?;
            index_file.flush()?;
        }
    }

    Ok(())
}
