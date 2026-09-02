use std::io::IoSliceMut;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::Bytes;
use nix::sys::uio::{process_vm_readv, RemoteIoVec};
use nix::unistd::Pid;
use overlaybd::virtual_file::VirtualFile;

/// Reads memory from a remote process using `process_vm_readv`.
pub(super) struct ProcessVmReader {
    pid: Pid,
}

impl ProcessVmReader {
    pub(super) fn new(pid: Pid) -> Self {
        Self { pid }
    }

    fn read_exact_remote(&self, remote_addr: u64, dst: &mut [u8]) -> Result<()> {
        Self::read_exact_remote_from(self.pid, remote_addr, dst)
    }

    /// `process_vm_readv` is synchronous, so the owned-buffer caller hands this
    /// to the blocking pool. `read_at_into` cannot: `&mut [u8]` does not
    /// outlive the call, so it stays on the polling thread. Nothing on the
    /// memory-capture path uses it — `compact_to` reads through `read_at`.
    fn read_exact_remote_from(pid: Pid, remote_addr: u64, dst: &mut [u8]) -> Result<()> {
        if dst.is_empty() {
            return Ok(());
        }

        let mut remote_addr = remote_addr;
        let mut remaining = dst;

        while !remaining.is_empty() {
            let remote_base = usize::try_from(remote_addr).with_context(|| {
                format!(
                    "remote address {remote_addr:#x} for pid {} does not fit usize",
                    pid.as_raw()
                )
            })?;
            let remote_iovs = [RemoteIoVec {
                base: remote_base,
                len: remaining.len(),
            }];
            let read = {
                let mut local_iovs = [IoSliceMut::new(remaining)];
                match process_vm_readv(pid, &mut local_iovs, &remote_iovs) {
                    Ok(read) => read,
                    Err(nix::errno::Errno::EINTR) => continue,
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!(
                                "process_vm_readv pid {} remote address {remote_addr:#x}",
                                pid.as_raw()
                            )
                        });
                    }
                }
            };

            if read == 0 {
                bail!(
                    "process_vm_readv for pid {} returned 0 bytes at {remote_addr:#x}",
                    pid.as_raw()
                );
            }
            if read > remaining.len() {
                bail!(
                    "process_vm_readv for pid {} returned {} bytes for {} byte buffer at {remote_addr:#x}",
                    pid.as_raw(),
                    read,
                    remaining.len()
                );
            }

            remote_addr = remote_addr.checked_add(read as u64).with_context(|| {
                format!(
                    "process_vm_readv remote address overflow for pid {}",
                    pid.as_raw()
                )
            })?;
            remaining = &mut remaining[read..];
        }
        Ok(())
    }
}

#[async_trait]
impl VirtualFile for ProcessVmReader {
    /// `offset` is a host virtual address (HVA), not a file offset.
    async fn read_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        // The buffer is owned here, so the copy can leave the polling thread.
        // The memory-capture path reads the whole guest through this method
        // with `concurrency = 32`; run inline it occupies up to 32 runtime
        // workers for the length of a pause and starves concurrent creates.
        let pid = self.pid;
        let data = tokio::task::spawn_blocking(move || {
            let mut data = vec![0u8; len];
            Self::read_exact_remote_from(pid, offset, &mut data)?;
            Ok::<_, anyhow::Error>(data)
        })
        .await
        .context("process_vm_readv blocking task")??;
        Ok(Bytes::from(data))
    }

    /// `offset` is a host virtual address (HVA), not a file offset.
    async fn read_at_into(&self, offset: u64, dst: &mut [u8]) -> Result<usize> {
        self.read_exact_remote(offset, dst)?;
        Ok(dst.len())
    }

    async fn write_at(&self, _offset: u64, _data: &[u8]) -> Result<usize> {
        bail!("ProcessVmReader does not support write_at")
    }

    async fn size(&self) -> Result<u64> {
        bail!("ProcessVmReader does not have a meaningful size")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reads_current_process_memory() -> Result<()> {
        let source = b"process-vm-reader";
        let reader = ProcessVmReader::new(Pid::this());

        let bytes = reader.read_at(source.as_ptr() as u64, source.len()).await?;
        assert_eq!(&bytes[..], source);

        let mut dst = vec![0u8; source.len()];
        let read = reader
            .read_at_into(source.as_ptr() as u64, &mut dst)
            .await?;
        assert_eq!(read, source.len());
        assert_eq!(dst, source);

        Ok(())
    }

    /// The capture path reads the whole guest through `read_at` at
    /// `concurrency = 32`. Run inline it parks the worker polling it for the
    /// length of the copy; on a single-worker runtime that is the whole
    /// runtime, and concurrent sandbox creates stop dead.
    #[tokio::test(flavor = "current_thread", start_paused = false)]
    async fn read_at_leaves_the_polling_thread_free() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        // Large enough that the copy outlasts several canary ticks.
        let source = vec![7u8; 128 * 1024 * 1024];
        let reader = ProcessVmReader::new(Pid::this());

        let ticks = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ticks);
        let canary = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(1)).await;
                counter.fetch_add(1, Ordering::SeqCst);
            }
        });
        tokio::task::yield_now().await;

        let bytes = reader
            .read_at(source.as_ptr() as u64, source.len())
            .await
            .expect("read own memory");
        canary.abort();

        assert_eq!(bytes.len(), source.len());
        assert!(
            ticks.load(Ordering::SeqCst) > 0,
            "the runtime made no progress while the read ran"
        );
    }
}
