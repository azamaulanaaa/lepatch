use std::{fs::File, io, ops::Deref};

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
