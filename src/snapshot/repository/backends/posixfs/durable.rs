//! Crash-durable file publication for the POSIX snapshot catalog.
//!
//! `rename(2)` makes a file visible but not durable: until the containing
//! directory is fsynced, a power loss inside the journal-commit window can roll
//! the directory entry back while the blobs the file names survive. A publish is
//! acknowledged to the client as soon as `commit_publish` returns, so the
//! records, aliases and commit markers it writes go through here rather than
//! through a bare write-and-rename.

use std::fs;
use std::io::Write;
use std::path::Path;

use crate::snapshot::{RepositoryError, RepositoryResult};

/// Writes `contents` to `path` through a temp file in the same directory,
/// returning only once both the bytes and the directory entry are on disk.
pub(super) fn write_file_durably(path: &Path, contents: &[u8]) -> RepositoryResult<()> {
    let parent = path.parent().ok_or_else(|| RepositoryError::Backend {
        message: format!("resolve parent for '{}'", path.display()),
        source: None,
    })?;
    create_dir_durably(parent)?;

    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(|error| {
        RepositoryError::backend(format!("create temp file in '{}'", parent.display()), error)
    })?;
    temp.write_all(contents).map_err(|error| {
        RepositoryError::backend(
            format!("write temp file '{}'", temp.path().display()),
            error,
        )
    })?;
    temp.as_file().sync_all().map_err(|error| {
        RepositoryError::backend(format!("sync temp file '{}'", temp.path().display()), error)
    })?;
    #[cfg(test)]
    probe::record(probe::Synced::File(path.to_path_buf()));

    let temp_path = temp.path().to_path_buf();
    temp.persist(path).map_err(|error| {
        RepositoryError::backend(
            format!("persist '{}' -> '{}'", temp_path.display(), path.display()),
            error.error,
        )
    })?;
    sync_dir(parent)
}

/// Creates `dir` and any missing ancestors, syncing the parent when the
/// directory is new so the entry survives a crash like the files published
/// into it.
pub(super) fn create_dir_durably(dir: &Path) -> RepositoryResult<()> {
    if dir.is_dir() {
        return Ok(());
    }
    fs::create_dir_all(dir)
        .map_err(|error| RepositoryError::backend(format!("create '{}'", dir.display()), error))?;
    match dir.parent() {
        Some(parent) => sync_dir(parent),
        None => Ok(()),
    }
}

/// Syncs a directory, making the renames and unlinks inside it durable.
pub(super) fn sync_dir(path: &Path) -> RepositoryResult<()> {
    fs::File::open(path)
        .map_err(|error| RepositoryError::backend(format!("open '{}'", path.display()), error))?
        .sync_all()
        .map_err(|error| RepositoryError::backend(format!("sync '{}'", path.display()), error))?;
    #[cfg(test)]
    probe::record(probe::Synced::Dir(path.to_path_buf()));
    Ok(())
}

/// fsync leaves nothing user space can read back, so tests observe the calls
/// through this per-thread log.
#[cfg(test)]
pub(super) mod probe {
    use std::cell::RefCell;
    use std::path::PathBuf;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Synced {
        /// The contents that will live at this path were synced.
        File(PathBuf),
        /// This directory was synced.
        Dir(PathBuf),
    }

    thread_local! {
        static LOG: RefCell<Vec<Synced>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn record(event: Synced) {
        LOG.with(|log| log.borrow_mut().push(event));
    }

    /// Drains the calls recorded on this thread so far.
    pub(crate) fn take() -> Vec<Synced> {
        LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
    }
}
