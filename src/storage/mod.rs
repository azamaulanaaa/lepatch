use std::io;

use async_trait::async_trait;

use crate::reader;
pub use blob::BlobFileStorage;

mod blob;

#[async_trait]
pub trait StorageGet: Send + Sync {
    async fn get(&self, key: &str) -> io::Result<reader::StreamReadSeeker>;
}

#[async_trait]
pub trait StoragePut: Send + Sync {
    async fn put(&self, reader: reader::StreamReadSeeker, len: u64) -> io::Result<String>;
}
