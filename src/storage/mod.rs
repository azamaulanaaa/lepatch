use std::io;

use async_trait::async_trait;

use crate::reader;
pub use blob::BlobFileStorage;

mod blob;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn get(&self, key: &str) -> io::Result<reader::StreamReader>;

    async fn put(&self, reader: reader::StreamReader, len: u64) -> io::Result<String>;
}
