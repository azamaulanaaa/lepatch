use std::{
    fmt::Debug,
    io::{self, SeekFrom},
    path::PathBuf,
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt},
    sync::RwLock,
};
use tracing::instrument;

use crate::{reader, storage};

#[derive(Serialize, Deserialize, Debug)]
struct BlobEntry {
    offset: u64,
    length: u64,
}

#[derive(Debug)]
pub struct BlobFileStorage {
    file_path: PathBuf,
    lock: RwLock<()>,
}

impl BlobFileStorage {
    #[instrument(err)]
    pub async fn new<P: Into<PathBuf> + Debug>(path: P, allow_overwrite: bool) -> io::Result<Self> {
        let file_path = path.into();

        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }

        if allow_overwrite {
            fs::File::create(&file_path).await?;
        } else {
            fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file_path)
                .await?;
        }

        Ok(Self {
            file_path,
            lock: RwLock::new(()),
        })
    }
}

#[async_trait]
impl storage::Storage for BlobFileStorage {
    #[instrument(err)]
    async fn get(&self, key: &str) -> io::Result<reader::StreamReader> {
        let entry: BlobEntry = serde_json::from_str(key).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid blob key: {}", e),
            )
        })?;

        let _guard = self.lock.read().await;

        let mut file = fs::File::open(&self.file_path).await?;

        file.seek(SeekFrom::Start(entry.offset)).await?;
        let limited_reader = file.take(entry.length);

        Ok(Box::new(limited_reader))
    }

    #[instrument(skip(reader), ret, err)]
    async fn put(&self, mut reader: reader::StreamReader, _len: u64) -> io::Result<String> {
        let _guard = self.lock.write().await;

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file_path)
            .await?;

        let offset = file.metadata().await?.len();

        let length = tokio::io::copy(&mut reader, &mut file).await?;

        let entry = BlobEntry { offset, length };
        let key = serde_json::to_string(&entry).map_err(|e| io::Error::other(e))?;

        Ok(key)
    }
}
