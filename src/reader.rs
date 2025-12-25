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
}

impl FileLock {
    pub fn new(file: File) -> io::Result<Self> {
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

pub struct SliceReader {
    file: Arc<FileLock>,
    offset: u64,
    remaining: u64,
}

impl SliceReader {
    pub fn new(file: Arc<FileLock>, offset: u64, limit: u64) -> io::Result<Self> {
        Ok(Self {
            file,
            offset,
            remaining: limit,
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

pub struct ChunkReader {
    slices: VecDeque<SliceReader>,
}

impl ChunkReader {
    pub fn new(slices: VecDeque<SliceReader>) -> Self {
        Self { slices }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            let current_slice = match self.slices.front_mut() {
                Some(slice) => slice,
                None => return Ok(0),
            };

            match current_slice.read(buf) {
                Ok(0) => {
                    self.slices.pop_front();
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
    type Item = io::Result<ChunkReader>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut slices = VecDeque::new();
        let mut bytes_needed = self.chunk_size;

        while bytes_needed > 0 {
            if self.current_file.is_none() {
                match self.pending_paths.pop_front() {
                    Some(path) => {
                        let file = match File::open(&path) {
                            Ok(f) => f,
                            Err(e) => return Some(Err(e)),
                        };

                        let locked = match FileLock::new(file) {
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
            Some(Ok(ChunkReader::new(slices)))
        }
    }
}
