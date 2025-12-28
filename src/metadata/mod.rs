use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub files: Vec<File>,
    pub chunks: Vec<Chunk>,
    pub file_chunks: Vec<FileChunk>,
    pub file_symlink: Vec<FileSymlink>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub hash: [u8; 32],
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSymlink {
    pub path: PathBuf,
    pub source: PathBuf,
    pub is_hard: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileChunk {
    pub chunk_index: u32,
    pub file_index: u32,
    pub chunk_offset: u32,
    pub file_offset: u64,
    pub length: u32,
}
