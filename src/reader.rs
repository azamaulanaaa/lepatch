#[cfg(windows)]
use std::os::windows::fs::FileExt as WinFileExt;

use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Read},
    ops::{Deref, DerefMut},
    path::PathBuf,
    sync::Arc,
};

use fastcdc::v2020::StreamCDC;
use fs2::FileExt;

pub struct FileLock {
    inner: File,
}

impl FileLock {
    pub fn new<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path_buf = path.into();

        let file = File::open(&path_buf)?;

        FileExt::lock_shared(&file)?;

        Ok(Self { inner: file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.inner.unlock();
    }
}

impl Deref for FileLock {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for FileLock {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct SliceReader {
    file: Arc<FileLock>,
    offset: u64,
    remaining: u64,
}

impl SliceReader {
    pub fn new(file: Arc<FileLock>, offset: u64, length: u64) -> io::Result<Self> {
        Ok(Self {
            file,
            offset,
            remaining: length,
        })
    }
}

impl Read for SliceReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }

        let max_len = self.remaining.min(buf.len() as u64) as usize;
        let read_buf = &mut buf[..max_len];

        #[cfg(unix)]
        let n = self.file.read_at(read_buf, self.offset)?;

        #[cfg(windows)]
        let n = self.file.seek_read(read_buf, self.offset)?;

        if n == 0 {
            self.remaining = 0;
            return Ok(0);
        }

        self.offset += n as u64;
        self.remaining -= n as u64;

        Ok(n)
    }
}

pub struct ChainReader<R: Read> {
    inners: VecDeque<R>,
}

impl<R: Read> ChainReader<R> {
    pub fn new<I: Into<VecDeque<R>>>(inners: I) -> Self {
        Self {
            inners: inners.into(),
        }
    }
}

impl<R: Read> Read for ChainReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let current_reader = match self.inners.front_mut() {
                Some(slice) => slice,
                None => return Ok(0),
            };

            match current_reader.read(buf) {
                Ok(0) => {
                    self.inners.pop_front();
                    continue;
                }
                Ok(n) => {
                    return Ok(n);
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkSource {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u32,
}

pub struct Chunk {
    pub sources: Vec<ChunkSource>,
    pub reader: ChainReader<SliceReader>,
}

struct EntryFileRegistry {
    path: PathBuf,
    length: u64,
    global_offset: u64,
}

pub struct FileRegistry {
    entries: Vec<EntryFileRegistry>,
}

impl FileRegistry {
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> io::Result<Self> {
        let mut global_offset = 0;

        let entries = paths
            .map(|path| {
                let path_buf = path.into();
                let length = std::fs::metadata(&path_buf)?.len();

                let entry = EntryFileRegistry {
                    path: path_buf,
                    length,
                    global_offset,
                };

                global_offset += length;

                Ok(entry)
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self { entries })
    }

    pub fn resolve_chunk(&self, global_start: u64, length: u32) -> Vec<ChunkSource> {
        let global_end = global_start + length as u64;
        let mut mappings = Vec::new();

        let start_index = self
            .entries
            .partition_point(|entry| (entry.global_offset + entry.length) <= global_start);

        for entry in self.entries.iter().skip(start_index) {
            if entry.global_offset >= global_end {
                break;
            }

            let file_end_global = entry.global_offset + entry.length;

            let overlap_start_global = std::cmp::max(global_start, entry.global_offset);
            let overlap_end_global = std::cmp::min(global_end, file_end_global);

            let overlap_len = overlap_end_global.saturating_sub(overlap_start_global) as u32;

            if overlap_len > 0 {
                let file_local_offset = overlap_start_global - entry.global_offset;

                mappings.push(ChunkSource {
                    path: entry.path.clone(),
                    offset: file_local_offset,
                    length: overlap_len,
                });
            }
        }

        mappings
    }
}

pub struct GlobalStream {
    paths: VecDeque<PathBuf>,
    current_file: Option<FileLock>,
}

impl GlobalStream {
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> Self {
        let paths = paths.map(|v| v.into()).collect();

        Self {
            paths,
            current_file: None,
        }
    }
}

impl Read for GlobalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(ref mut file) = self.current_file {
                match file.read(buf) {
                    Ok(0) => {
                        self.current_file = None;
                        continue;
                    }
                    Ok(n) => return Ok(n),
                    Err(e) => return Err(e),
                }
            }

            match self.paths.pop_front() {
                Some(path) => {
                    let lock = FileLock::new(path)?;
                    self.current_file = Some(lock);
                }
                None => return Ok(0),
            }
        }
    }
}

pub struct ChunkerConfig {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

pub struct Chunker {
    registry: Arc<FileRegistry>,
    cdc_iter: StreamCDC<GlobalStream>,
}

impl Chunker {
    pub fn new(paths: Vec<PathBuf>, config: ChunkerConfig) -> io::Result<Self> {
        let registry = Arc::new(FileRegistry::new(paths.iter())?);

        let stream = GlobalStream::new(paths.iter());
        let cdc = StreamCDC::new(stream, config.min_size, config.avg_size, config.max_size);

        Ok(Self {
            registry,
            cdc_iter: cdc,
        })
    }
}

impl Iterator for Chunker {
    type Item = io::Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        let cdc_chunk = self.cdc_iter.next()?;
        let cdc_chunk = match cdc_chunk {
            Ok(v) => v,
            Err(e) => return Some(Err(io::Error::other(e))),
        };

        let sources = self
            .registry
            .resolve_chunk(cdc_chunk.offset as u64, cdc_chunk.length as u32);

        let mut slices = VecDeque::new();

        for map in &sources {
            let lock = match FileLock::new(&map.path) {
                Ok(l) => Arc::new(l),
                Err(e) => return Some(Err(e)),
            };

            match SliceReader::new(lock, map.offset, map.length as u64) {
                Ok(slice) => slices.push_back(slice),
                Err(e) => return Some(Err(e)),
            }
        }

        Some(Ok(Chunk {
            sources,
            reader: ChainReader::new(slices),
        }))
    }
}
