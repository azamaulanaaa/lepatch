use std::{
    io::{self, Read, Write},
    path::PathBuf,
};

pub struct Snapshot {
    pub name: String,
    pub version: u8,
    pub files: Vec<File>,
    pub chunks: Vec<Chunk>,
    pub file_chunks: Vec<FileChunk>,
    pub file_symlink: Vec<FileSymlink>,
}

pub struct Chunk {
    pub hash: Vec<u8>,
    pub location: String,
}

pub struct FileSymlink {
    pub path: PathBuf,
    pub source: PathBuf,
    pub is_hard: bool,
}

pub struct File {
    pub path: PathBuf,
}

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
