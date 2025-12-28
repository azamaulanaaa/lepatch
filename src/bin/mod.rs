use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{backup::backup, restore::restore},
    reader::ChunkerConfig,
    storage,
};
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
        name: String,
        #[arg(long, default_value_t = false)]
        overwrite: bool,
    },
    Restore {
        destination: PathBuf,
        name: String,
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
            name,
            overwrite,
        } => {
            let index_path = PathBuf::from(&name).with_extension("idx");

            let mut index_file = if overwrite {
                fs::File::create(index_path)?
            } else {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(index_path)?
            };

            let config = ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            };

            let storage_path = PathBuf::from(&name).with_extension("bin");
            let storage = storage::BlobFileStorage::new(storage_path, overwrite).await?;

            let key = backup(source, storage, config).await?;

            index_file.write_all(key.as_bytes())?;
            index_file.flush()?;
        }
        Commands::Restore { destination, name } => {
            let index_path = PathBuf::from(&name).with_extension("idx");
            let mut index_file = fs::File::open(index_path)?;

            let mut key = String::new();
            index_file.read_to_string(&mut key)?;

            let storage_path = PathBuf::from(&name).with_extension("bin");
            let storage = storage::BlobFileStorage::<false>::new(storage_path, false).await?;

            restore(destination, key, storage).await?;
        }
    }

    Ok(())
}
