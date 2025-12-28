use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::AsyncWrite;
use tracing::instrument;

pub struct SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    inner: W,
    remaining: u64,
}

impl<W> SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    #[instrument(level = "trace", skip(writer))]
    pub fn new(writer: W, limit: u64) -> Self {
        Self {
            inner: writer,
            remaining: limit,
        }
    }
}

impl<W> AsyncWrite for SliceAsyncWriter<W>
where
    W: AsyncWrite + Unpin,
{
    #[instrument(level = "trace", skip(self, cx, buf), ret)]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if self.remaining == 0 {
            return Poll::Ready(Ok(0));
        }

        let limit = std::cmp::min(self.remaining, buf.len() as u64) as usize;

        let result = Pin::new(&mut self.inner).poll_write(cx, &buf[..limit]);

        if let Poll::Ready(Ok(n)) = &result {
            self.remaining -= *n as u64;
        }

        result
    }

    #[instrument(level = "trace", skip(self, cx), ret)]
    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    #[instrument(level = "trace", skip(self, cx), ret)]
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
