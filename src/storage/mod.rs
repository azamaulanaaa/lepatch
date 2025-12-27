use std::io;

use async_trait::async_trait;
use tokio::io::AsyncRead;

pub use blob::BlobFileStorage;

pub type StreamReader = Box<dyn AsyncRead + Unpin + Send>;

mod blob;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn get(&self, key: &str) -> io::Result<StreamReader>;

    async fn put(&self, reader: StreamReader, len: u64) -> io::Result<String>;
}
