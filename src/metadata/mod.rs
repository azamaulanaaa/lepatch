use std::{
    io::{self, Read, Write},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub name: String,
    pub version: u8,
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

pub trait MetadataStore {
    fn open<R: Read>(&self, reader: R) -> io::Result<Snapshot>;
    fn save<W: Write>(&self, snapshot: &Snapshot, writer: W) -> io::Result<()>;
}

pub struct BincodeStore;

impl MetadataStore for BincodeStore {
    fn open<R: Read>(&self, reader: R) -> io::Result<Snapshot> {
        bincode::deserialize_from(reader).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    fn save<W: Write>(&self, snapshot: &Snapshot, writer: W) -> io::Result<()> {
        bincode::serialize_into(writer, snapshot)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
