#[cfg(windows)]
use std::os::windows::fs::FileExt as WinFileExt;

use std::{
    fs::File,
    io::{self, Read},
    ops::Deref,
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
