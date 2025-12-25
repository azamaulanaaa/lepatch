#[cfg(windows)]
use std::os::windows::fs::FileExt as WinFileExt;

use std::{
    collections::VecDeque,
    fs::File,
    io::{self, Read},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use fs2::FileExt;

pub struct FileLock {
    inner: File,
    pub path: PathBuf,
}

impl FileLock {
    pub fn new<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
        let path_buf = path.into();

        let file = File::open(&path_buf)?;

        FileExt::lock_shared(&file)?;

        Ok(Self {
            inner: file,
            path: path_buf,
        })
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
pub struct ChunkMapping {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

pub struct Chunk {
    pub metadata: Vec<ChunkMapping>,
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

    pub fn resolve_chunk(&self, global_start: u64, length: u64) -> Vec<ChunkMapping> {
        let global_end = global_start + length;
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

            let overlap_len = overlap_end_global.saturating_sub(overlap_start_global);

            if overlap_len > 0 {
                let file_local_offset = overlap_start_global - entry.global_offset;

                mappings.push(ChunkMapping {
                    path: entry.path.clone(),
                    offset: file_local_offset,
                    length: overlap_len,
                });
            }
        }

        mappings
    }
}

pub struct Chunker {
    pending_paths: VecDeque<PathBuf>,
    current_file: Option<Arc<FileLock>>,
    offset: u64,
    chunk_size: u64,
}

impl Chunker {
    pub fn new<P: AsRef<Path>, I: Iterator<Item = P>>(paths: I, chunk_size: u64) -> Self {
        let pending_paths = paths
            .map(|p| p.as_ref().to_path_buf())
            .collect::<VecDeque<_>>();

        Self {
            pending_paths,
            current_file: None,
            offset: 0,
            chunk_size,
        }
    }
}

impl Iterator for Chunker {
    type Item = io::Result<Chunk>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut slices = VecDeque::new();
        let mut mappings = Vec::new();
        let mut bytes_needed = self.chunk_size;

        while bytes_needed > 0 {
            if self.current_file.is_none() {
                match self.pending_paths.pop_front() {
                    Some(path) => {
                        let locked = match FileLock::new(path) {
                            Ok(l) => Arc::new(l),
                            Err(e) => return Some(Err(e)),
                        };
                        self.current_file = Some(locked);
                    }
                    None => break,
                }
            }

            let current_lock = self.current_file.as_ref().unwrap();

            let file_len = match current_lock.inner.metadata() {
                Ok(m) => m.len(),
                Err(e) => return Some(Err(e)),
            };

            if self.offset >= file_len {
                self.current_file = None;
                self.offset = 0;
                continue;
            }

            let available = file_len - self.offset;
            let to_read = std::cmp::min(available, bytes_needed);

            mappings.push(ChunkMapping {
                path: current_lock.path.clone(),
                offset: self.offset,
                length: to_read,
            });

            match SliceReader::new(Arc::clone(current_lock), self.offset, to_read) {
                Ok(slice) => slices.push_back(slice),
                Err(e) => return Some(Err(e)),
            }

            self.offset += to_read;
            bytes_needed -= to_read;
        }

        if slices.is_empty() {
            None
        } else {
            let reader = ChainReader::new(slices);

            Some(Ok(Chunk {
                metadata: mappings,
                reader,
            }))
        }
    }
}
