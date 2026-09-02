use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::errno::Errno;
use nix::fcntl::{Flock, FlockArg};
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::durable::{create_dir_durably, write_file_durably};
use super::layout::PosixFsSnapshotArtifactLayout;
use crate::snapshot::repository::SnapshotListFilter;
use crate::snapshot::{
    CommittedSnapshot, RepositoryError, RepositoryResult, SnapshotAlias, SnapshotId,
    SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSource,
    SnapshotSourceKind, TemplateBuildErrorReason, TemplateBuildInfo, TemplateBuildStatus,
};
const FILE_LOCK_TIMEOUT: Option<Duration> = Some(Duration::from_secs(10));

/// How old a `create_new` lock file must be before the fallback strategy will
/// steal it. Only reachable when the file was already this old at the first
/// attempt: a contender gives up after FILE_LOCK_TIMEOUT, which is shorter, so
/// it can never age a live holder's lock into the steal branch itself.
const CREATE_NEW_STALE_AGE: Duration = Duration::from_secs(60);

/// How the catalog takes its alias and record locks.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PosixFsLockStrategy {
    /// `flock(LOCK_EX | LOCK_NB)`. The kernel owns the lock and releases it
    /// when the descriptor closes, including on process death, so there is no
    /// staleness to judge and nothing to steal.
    #[default]
    Flock,
    /// `create_new` plus an ownership token, for filesystems where `flock` is
    /// not honoured across the writers that share the repository — some NFS
    /// and FUSE mounts. It is strictly weaker: a holder that dies leaves its
    /// lock behind until it ages out, and everything waiting on it waits that
    /// long. Choose it only when `flock` does not work.
    CreateNew,
}

impl std::fmt::Display for PosixFsLockStrategy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Flock => "flock",
            Self::CreateNew => "create_new",
        })
    }
}

impl std::str::FromStr for PosixFsLockStrategy {
    type Err = String;

    fn from_str(raw: &str) -> std::result::Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "flock" => Ok(Self::Flock),
            "create_new" => Ok(Self::CreateNew),
            other => Err(format!(
                "unsupported posix_fs lock strategy {other:?}; expected \"flock\" or \"create_new\""
            )),
        }
    }
}

pub struct PosixFsCatalogStore {
    root: PathBuf,
    lock_strategy: PosixFsLockStrategy,
    lock_timeout: Option<Duration>,
}

/// Identifies one acquisition well enough that a second one cannot be mistaken
/// for it.
///
/// The pid alone cannot: pids are recycled, and on a shared filesystem two
/// hosts hand out the same numbers, so a stale lock from one host reads as
/// alive on another. The boot id scopes the pid to one running kernel, and the
/// uuid distinguishes two acquisitions by the same process — which is what
/// makes "is this still my lock?" answerable at drop time.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct LockToken {
    pid: u32,
    boot_id: String,
    uuid: String,
}

impl LockToken {
    fn new() -> Self {
        Self {
            pid: std::process::id(),
            boot_id: boot_id(),
            uuid: uuid::Uuid::now_v7().to_string(),
        }
    }
}

/// This kernel's boot identifier, or a per-process substitute.
///
/// The substitute is deliberately per-process rather than a constant: if the
/// boot id cannot be read, two processes must not be able to produce equal
/// tokens, because an equal token is what lets a guard delete a lock.
fn boot_id() -> String {
    static BOOT_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    BOOT_ID
        .get_or_init(|| {
            fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .map(|value| value.trim().to_string())
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("no-boot-id-{}", uuid::Uuid::now_v7()))
        })
        .clone()
}

#[derive(Debug)]
pub(crate) struct PublishSession {
    pub(crate) snapshot_id: SnapshotId,
}

/// Holds an exclusive advisory lock on a catalog lock file.
///
/// Ownership is kernel-enforced: dropping the guard closes the descriptor,
/// which releases the `flock`, and the kernel releases it just the same if the
/// process dies. There is no staleness heuristic to get wrong, and no window in
/// which two holders both believe they own the lock.
///
/// The lock file is deliberately never unlinked. Unlinking it would let one
/// holder remove a file another holder already has open and locked, so the next
/// contender would create a fresh inode and lock that instead, admitting two
/// writers. The files are empty, bounded by the number of aliases and records,
/// and reused on every acquire.
#[derive(Debug)]
enum PosixFileLockGuard {
    /// The kernel holds the lock; the descriptor is never read, and dropping
    /// it is what releases the lock.
    Flock(#[allow(dead_code)] Box<Flock<fs::File>>),
    /// The lock is the existence of a file, so releasing it means removing
    /// that file — but only while it still carries this guard's token. A
    /// guard whose lock was stolen must not delete the thief's file: that
    /// admits a third writer, and the failure compounds instead of settling.
    Token { path: PathBuf, token: LockToken },
}

impl Drop for PosixFileLockGuard {
    fn drop(&mut self) {
        let Self::Token { path, token } = self else {
            return;
        };
        match read_lock_token(path) {
            Some(current) if current == *token => {
                let _ = fs::remove_file(path);
            }
            // Either the lock is somebody else's now, or it is already gone.
            // Both mean this guard has nothing left to release.
            _ => {}
        }
    }
}

/// Reads the token a lock file carries, or `None` when it has none to read.
///
/// An unreadable or malformed file is treated as tokenless rather than as an
/// error: it can only have been written by an older build or a torn write, and
/// in both cases the answer to "is this mine?" is no.
fn read_lock_token(path: &Path) -> Option<LockToken> {
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

impl PosixFsCatalogStore {
    /// Creates a catalog store rooted at the repository's durable POSIX directory.
    pub fn new(root: PathBuf) -> Self {
        Self::with_lock_strategy(root, PosixFsLockStrategy::default())
    }

    /// Creates a catalog store that takes its locks the given way.
    pub fn with_lock_strategy(root: PathBuf, lock_strategy: PosixFsLockStrategy) -> Self {
        Self {
            root,
            lock_strategy,
            lock_timeout: FILE_LOCK_TIMEOUT,
        }
    }

    /// Shortens the acquisition timeout so a test that deliberately contends
    /// does not spend ten seconds proving it.
    #[cfg(test)]
    fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = Some(timeout);
        self
    }

    fn layout(&self, snapshot_id: &SnapshotId) -> PosixFsSnapshotArtifactLayout {
        PosixFsSnapshotArtifactLayout::new(&self.root, snapshot_id)
    }

    fn commit_marker_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        self.layout(snapshot_id)
            .path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
    }

    fn record_path(&self, snapshot_id: &SnapshotId) -> PathBuf {
        PosixFsSnapshotArtifactLayout::record_path(&self.root, snapshot_id)
    }

    /// Starts a publish session by creating the snapshot directory under the durable catalog root.
    pub(crate) fn begin_publish(
        &self,
        snapshot_id: &SnapshotId,
    ) -> RepositoryResult<PublishSession> {
        self.ensure_layout()?;
        create_dir_durably(&self.layout(snapshot_id).snapshot_dir())?;
        Ok(PublishSession {
            snapshot_id: snapshot_id.clone(),
        })
    }

    /// Commits one imported snapshot into the catalog and makes it visible via the commit marker.
    ///
    /// Flow:
    /// 1. acquire the alias lock when an alias is present
    /// 2. bind the alias
    /// 3. write the commit marker
    /// 4. write the committed snapshot record
    pub(crate) fn commit_publish(
        &self,
        session: &PublishSession,
        metadata: SnapshotPublishMetadata,
        committed: CommittedSnapshot,
    ) -> RepositoryResult<SnapshotRecord> {
        let now = now_unix_ms();
        let snapshot_id = metadata.id.clone();
        let write_result = if let Some(alias) = metadata.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let record = store.committed_record_unlocked(&metadata, committed.clone(), now)?;
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                if let Some(existing) = store.load_alias_target(alias)? {
                    if existing != snapshot_id {
                        if store.load_record_by_id_unlocked(&existing)?.is_some() {
                            return Err(RepositoryError::AliasConflict {
                                alias: alias.to_string(),
                                existing,
                                new_id: snapshot_id.clone(),
                            });
                        }
                        store.remove_file_if_exists(&alias_path)?;
                    }
                }
                store.write_json(&alias_path, &snapshot_id)?;
                store.write_commit_marker(&session.snapshot_id)?;
                store.write_committed_record_unlocked(&record)?;
                Ok(record)
            })
        } else {
            (|| {
                let record = self.committed_record_unlocked(&metadata, committed.clone(), now)?;
                self.write_commit_marker(&session.snapshot_id)?;
                self.write_committed_record_unlocked(&record)?;
                Ok(record)
            })()
        };

        match write_result {
            Ok(record) => Ok(record),
            Err(error) => {
                if let Some(alias) = metadata.alias.as_ref() {
                    let _ = self.with_alias_lock(alias, |store| {
                        let alias_path =
                            PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                        if store.load_alias_target(alias)?.as_ref() == Some(&snapshot_id) {
                            store.remove_file_if_exists(&alias_path)?;
                        }
                        Ok(())
                    });
                }
                let _ = self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id);
                Err(error)
            }
        }
    }

    /// Cleans up an unfinished publish session that never reached the committed marker.
    pub(crate) fn abort_publish(&self, session: &PublishSession) -> RepositoryResult<()> {
        self.cleanup_uncommitted_snapshot_dir(&session.snapshot_id)
    }

    pub(crate) fn create(&self, record: SnapshotRecord) -> RepositoryResult<SnapshotRecord> {
        self.ensure_layout()?;
        if !matches!(record.source, SnapshotSource::Template { .. }) {
            return Err(RepositoryError::InvalidRequest {
                reason: "only template snapshots can be pre-created".to_string(),
            });
        }
        if record.committed.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: "pre-created template snapshots must not already be committed".to_string(),
            });
        }
        if self.load_record_by_id_unlocked(&record.id)?.is_some() {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{}' already exists", record.id),
            });
        }

        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                store.ensure_alias_available(alias, &record.id)?;
                store.write_record_unlocked(&record)?;
                store.write_json(
                    &PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias),
                    &record.id,
                )
            })?;
        } else {
            self.write_record_unlocked(&record)?;
        }
        Ok(record)
    }

    pub(crate) fn get(&self, id_or_alias: &str) -> RepositoryResult<Option<SnapshotRecord>> {
        self.ensure_layout()?;
        if let Ok(direct_id) = SnapshotId::parse(id_or_alias) {
            if let Some(record) = self.load_record_by_id_unlocked(&direct_id)? {
                return Ok(Some(record));
            }
        }

        let alias =
            SnapshotAlias::parse(id_or_alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            match store.load_record_by_id_unlocked(&id)? {
                Some(record) => Ok(Some(record)),
                None => {
                    store.remove_file_if_exists(&PosixFsSnapshotArtifactLayout::alias_path(
                        &store.root,
                        &alias,
                    ))?;
                    Ok(None)
                }
            }
        })
    }

    pub(crate) fn list(&self, filter: SnapshotListFilter) -> RepositoryResult<Vec<SnapshotRecord>> {
        self.ensure_layout()?;
        let records_dir = self.records_dir();
        let mut records = Vec::new();
        for entry in fs::read_dir(&records_dir).map_err(|error| {
            RepositoryError::backend(
                format!("read records dir '{}'", records_dir.display()),
                error,
            )
        })? {
            let entry = entry.map_err(|error| {
                RepositoryError::backend(
                    format!("read entry in '{}'", records_dir.display()),
                    error,
                )
            })?;
            if !entry
                .file_type()
                .map_err(|error| {
                    RepositoryError::backend(
                        format!("inspect file type '{}'", entry.path().display()),
                        error,
                    )
                })?
                .is_file()
            {
                continue;
            }
            if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let record: SnapshotRecord = self.read_json(&entry.path())?;
            if Self::matches_record_filter(&record, &filter) {
                records.push(record);
            }
        }
        records.sort_by(|left, right| {
            right
                .created_at_unix_ms
                .cmp(&left.created_at_unix_ms)
                .then_with(|| left.id.to_string().cmp(&right.id.to_string()))
        });
        Ok(records)
    }

    pub(crate) fn delete_record(&self, id: &SnapshotId) -> RepositoryResult<()> {
        let Some(record) = self.load_record_by_id_unlocked(id)? else {
            // Idempotent: already doesn't exist
            return Ok(());
        };
        if let Some(alias) = record.alias.as_ref() {
            self.with_alias_lock(alias, |store| {
                let snapshot_layout = PosixFsSnapshotArtifactLayout::new(&store.root, id);
                let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, alias);
                store.remove_file_if_exists(
                    &snapshot_layout.path(super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER),
                )?;
                if store.load_alias_target(alias)?.as_ref() == Some(id) {
                    store.remove_file_if_exists(&alias_path)?;
                }
                if record.committed.is_some() {
                    store.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
                }
                store.remove_file_if_exists(&store.record_path(id))
            })?;
            return Ok(());
        }
        let snapshot_layout = self.layout(id);
        self.remove_file_if_exists(&self.commit_marker_path(id))?;
        if record.committed.is_some() {
            self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())?;
        }
        self.remove_file_if_exists(&self.record_path(id))?;
        Ok(())
    }

    /// Resolves one alias to a committed snapshot id and drops stale alias entries on the way.
    pub(crate) fn resolve_alias(&self, alias: &str) -> RepositoryResult<Option<SnapshotId>> {
        let alias =
            SnapshotAlias::parse(alias).map_err(|error| RepositoryError::InvalidRequest {
                reason: error.to_string(),
            })?;
        self.with_alias_lock(&alias, |store| {
            let Some(id) = store.load_alias_target(&alias)? else {
                return Ok(None);
            };
            if store.load_record_by_id_unlocked(&id)?.is_some() {
                return Ok(Some(id));
            }
            let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&store.root, &alias);
            store.remove_file_if_exists(&alias_path)?;
            Ok(None)
        })
    }

    fn aliases_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::aliases_dir(&self.root)
    }

    fn records_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::records_dir(&self.root)
    }

    fn snapshots_dir(&self) -> PathBuf {
        PosixFsSnapshotArtifactLayout::snapshots_dir(&self.root)
    }

    fn ensure_layout(&self) -> RepositoryResult<()> {
        let catalog_dir = PosixFsSnapshotArtifactLayout::catalog_dir(&self.root);
        let aliases_dir = self.aliases_dir();
        let records_dir = self.records_dir();
        let snapshots_dir = self.snapshots_dir();
        for dir in [&catalog_dir, &aliases_dir, &records_dir, &snapshots_dir] {
            fs::create_dir_all(dir).map_err(|error| {
                RepositoryError::backend(format!("create catalog dir '{}'", dir.display()), error)
            })?;
        }
        Ok(())
    }

    pub(crate) fn try_start(&self, id: &SnapshotId) -> RepositoryResult<SnapshotRecord> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        if build.status != TemplateBuildStatus::Waiting {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("template build '{id}' is not in waiting state"),
            });
        }
        build.status = TemplateBuildStatus::Building;
        build.started_at_unix_ms = Some(now);
        build.error_reason = None;
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)?;
        Ok(record)
    }

    pub(crate) fn mark_error(
        &self,
        id: &SnapshotId,
        reason: TemplateBuildErrorReason,
    ) -> RepositoryResult<()> {
        let _guard = self.acquire_record_lock(id)?;
        let mut record = self.load_record_by_id_unlocked(id)?.ok_or_else(|| {
            RepositoryError::SnapshotNotFound {
                lookup: id.to_string(),
            }
        })?;
        let now = now_unix_ms();
        let SnapshotSource::Template { build } = &mut record.source else {
            return Err(RepositoryError::InvalidRequest {
                reason: format!("snapshot '{id}' is not a template build"),
            });
        };
        build.status = TemplateBuildStatus::Error;
        build.finished_at_unix_ms = Some(now);
        build.error_reason = Some(reason);
        record.updated_at_unix_ms = now;
        self.write_record_unlocked(&record)
    }

    fn read_json<T>(&self, path: &Path) -> RepositoryResult<T>
    where
        T: DeserializeOwned,
    {
        let bytes = fs::read(path).map_err(|error| {
            RepositoryError::backend(format!("read '{}'", path.display()), error)
        })?;
        serde_json::from_slice(&bytes).map_err(|error| {
            RepositoryError::backend(format!("parse json '{}'", path.display()), error)
        })
    }

    fn write_json<T>(&self, path: &Path, value: &T) -> RepositoryResult<()>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec_pretty(value).map_err(|error| {
            RepositoryError::backend(format!("serialize json '{}'", path.display()), error)
        })?;
        write_file_durably(path, &bytes)
    }

    /// The marker is what makes a snapshot visible on the next startup, so it
    /// has to reach the platter with the record that describes it.
    fn write_commit_marker(&self, id: &SnapshotId) -> RepositoryResult<()> {
        write_file_durably(&self.commit_marker_path(id), b"committed")
    }

    fn remove_file_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn remove_dir_if_exists(&self, path: &Path) -> RepositoryResult<()> {
        match fs::remove_dir_all(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(RepositoryError::backend(
                format!("remove '{}'", path.display()),
                error,
            )),
        }
    }

    fn is_committed(&self, id: &SnapshotId) -> bool {
        self.commit_marker_path(id).exists()
            && self
                .load_record_by_id_unlocked(id)
                .ok()
                .flatten()
                .is_some_and(|record| record.committed.is_some())
    }

    fn cleanup_uncommitted_snapshot_dir(&self, id: &SnapshotId) -> RepositoryResult<()> {
        if self.is_committed(id) {
            return Ok(());
        }
        let snapshot_layout = self.layout(id);
        self.remove_dir_if_exists(&snapshot_layout.snapshot_dir())
    }

    fn load_record_by_id_unlocked(
        &self,
        id: &SnapshotId,
    ) -> RepositoryResult<Option<SnapshotRecord>> {
        let path = self.record_path(id);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    fn load_alias_target(&self, alias: &SnapshotAlias) -> RepositoryResult<Option<SnapshotId>> {
        let path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if !path.exists() {
            return Ok(None);
        }
        self.read_json(&path).map(Some)
    }

    /// Takes an exclusive advisory lock on `lock_path`, retrying until the
    /// timeout and then deferring to `on_locked`.
    ///
    /// This previously used `create_new` as the mutex and treated any lock file
    /// older than a fixed age as abandoned, deleting it and retrying. The age
    /// came from an mtime written once at creation and never refreshed, so a
    /// holder still inside its critical section past that age had its lock
    /// deleted and both parties proceeded. The guard then unlinked on drop with
    /// no ownership check, so the original holder removed the thief's lock and
    /// admitted a third — the failure compounded rather than settling.
    ///
    /// `flock` removes the staleness question entirely: the kernel releases the
    /// lock when the descriptor closes, including on process death.
    fn acquire_file_lock(
        &self,
        lock_path: PathBuf,
        contents: String,
        label: &'static str,
        on_locked: impl Fn() -> RepositoryResult<PosixFileLockGuard>,
    ) -> RepositoryResult<PosixFileLockGuard> {
        if let Some(parent) = lock_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                RepositoryError::backend(
                    format!("create {label} lock dir '{}'", parent.display()),
                    error,
                )
            })?;
        }

        let deadline = self.lock_timeout.map(|timeout| Instant::now() + timeout);
        loop {
            let acquired = match self.lock_strategy {
                PosixFsLockStrategy::Flock => Self::try_flock(&lock_path, &contents, label)?,
                PosixFsLockStrategy::CreateNew => Self::try_create_new(&lock_path, label)?,
            };
            if let Some(guard) = acquired {
                return Ok(guard);
            }
            if let Some(deadline) = deadline {
                if Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(25));
                    continue;
                }
            }
            return on_locked();
        }
    }

    /// One `flock` attempt. `Ok(None)` means somebody else holds it.
    fn try_flock(
        lock_path: &Path,
        contents: &str,
        label: &'static str,
    ) -> RepositoryResult<Option<PosixFileLockGuard>> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(lock_path)
            .map_err(|error| {
                RepositoryError::backend(
                    format!("open {label} lock '{}'", lock_path.display()),
                    error,
                )
            })?;

        match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
            Ok(mut lock) => {
                // Contents are diagnostic only; ownership is the flock.
                let _ = lock
                    .set_len(0)
                    .and_then(|()| lock.write_all(contents.as_bytes()));
                let _ = lock.flush();
                Ok(Some(PosixFileLockGuard::Flock(Box::new(lock))))
            }
            Err((_, Errno::EWOULDBLOCK | Errno::EINTR)) => Ok(None),
            Err((_, errno)) => Err(RepositoryError::backend(
                format!("lock {label} lock '{}'", lock_path.display()),
                std::io::Error::from(errno),
            )),
        }
    }

    /// One `create_new` attempt, stealing the lock if it is old enough to have
    /// been abandoned. `Ok(None)` means somebody else holds it.
    fn try_create_new(
        lock_path: &Path,
        label: &'static str,
    ) -> RepositoryResult<Option<PosixFileLockGuard>> {
        let token = LockToken::new();
        match Self::install_lock(lock_path, &token) {
            Ok(()) => {
                return Ok(Some(PosixFileLockGuard::Token {
                    path: lock_path.to_path_buf(),
                    token,
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(RepositoryError::backend(
                    format!("create {label} lock '{}'", lock_path.display()),
                    error,
                ))
            }
        }

        let Some(stale) = Self::stale_lock_token(lock_path) else {
            return Ok(None);
        };
        if !Self::win_steal_claim(lock_path, &stale) {
            return Ok(None);
        }

        // The claim is keyed on the token that was observed to be stale, so
        // exactly one contender can be here for that token. Re-read before
        // removing anything: a steal that already completed replaced the file,
        // and unlinking the winner's fresh lock would put two writers inside.
        let result = if read_lock_token(lock_path).as_ref() == Some(&stale) {
            let _ = fs::remove_file(lock_path);
            match Self::install_lock(lock_path, &token) {
                Ok(()) => Some(PosixFileLockGuard::Token {
                    path: lock_path.to_path_buf(),
                    token,
                }),
                Err(_) => None,
            }
        } else {
            None
        };

        // Released only after the new lock is installed, so a contender that
        // then wins this claim re-reads a token that has already changed.
        let _ = fs::remove_file(Self::steal_claim_path(lock_path, &stale));
        Ok(result)
    }

    /// Writes `token` into a lock file that must not already exist.
    fn install_lock(lock_path: &Path, token: &LockToken) -> std::io::Result<()> {
        let encoded = serde_json::to_vec(token).map_err(std::io::Error::other)?;
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(lock_path)?;
        file.write_all(&encoded)?;
        file.flush()
    }

    /// The token of a lock old enough to be treated as abandoned.
    ///
    /// The age and the token are taken from one open descriptor, so both
    /// describe the same inode. Statting the path and then reading it back
    /// describes two different files whenever the lock is stolen in between:
    /// a contender times the abandoned lock's mtime, the thief unlinks it and
    /// installs a fresh one, and the contender reads the *thief's* token as
    /// the one it just proved stale — then claims it and steals a lock that is
    /// microseconds old. Two holders, from an inode that was never abandoned.
    /// `a_stale_create_new_lock_admits_one_contender_at_a_time` reproduces it
    /// in 8 rounds out of 40 against the two-syscall version.
    ///
    /// Age comes from the mtime, which is written once at creation and never
    /// refreshed, so it measures how long ago the lock was taken rather than
    /// how long it has been idle. That is only safe because the acquisition
    /// timeout is far shorter than the stale age: a contender gives up long
    /// before it could age a live holder into this branch.
    fn stale_lock_token(lock_path: &Path) -> Option<LockToken> {
        let mut file = fs::File::open(lock_path).ok()?;
        let modified = file.metadata().ok()?.modified().ok()?;
        if SystemTime::now().duration_since(modified).ok()? < CREATE_NEW_STALE_AGE {
            return None;
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).ok()?;
        // Malformed is tokenless, as in `read_lock_token`: a torn or older
        // write cannot answer "whose lock is this?", so it is not stealable
        // through the token-keyed claim.
        serde_json::from_slice(&bytes).ok()
    }

    /// Where the right to steal the lock held by `stale` is claimed.
    fn steal_claim_path(lock_path: &Path, stale: &LockToken) -> PathBuf {
        let mut name = lock_path.as_os_str().to_os_string();
        name.push(format!(".steal.{}", stale.uuid));
        PathBuf::from(name)
    }

    /// Claims the right to steal one particular stale lock.
    ///
    /// `create_new` on a name derived from the observed token is what makes the
    /// steal exclusive. Deleting the stale file and re-creating it cannot be:
    /// two contenders would both delete, both create, and both proceed — which
    /// is the defect this strategy exists to have fixed.
    ///
    /// The known cost: a contender that dies between winning the claim and
    /// installing its lock leaves the claim file behind, and since the claim is
    /// keyed on the observed token, that stale lock can then never be stolen.
    /// Ageing the claim out is not a fix — two contenders would both find it
    /// expired, both remove it, both re-create it, and both steal the same
    /// lock, which is exactly the race above. Recovering that state means
    /// deleting the two files, and it is one more reason `flock` is the
    /// shipped default and this strategy is for filesystems that cannot.
    fn win_steal_claim(lock_path: &Path, stale: &LockToken) -> bool {
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(Self::steal_claim_path(lock_path, stale))
            .is_ok()
    }

    fn acquire_alias_lock(&self, alias: &SnapshotAlias) -> RepositoryResult<PosixFileLockGuard> {
        let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(&self.root, alias);
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            "alias",
            || {
                Err(RepositoryError::Backend {
                    message: format!("timed out waiting for alias lock '{}'", lock_path.display()),
                    source: None,
                })
            },
        )
    }

    fn acquire_record_lock(&self, id: &SnapshotId) -> RepositoryResult<PosixFileLockGuard> {
        let lock_path = PosixFsSnapshotArtifactLayout::record_lock_path(&self.root, id);
        self.acquire_file_lock(
            lock_path.clone(),
            std::process::id().to_string(),
            "record",
            || {
                Err(RepositoryError::Backend {
                    message: format!(
                        "timed out waiting for record lock '{}'",
                        lock_path.display()
                    ),
                    source: None,
                })
            },
        )
    }

    fn with_alias_lock<T>(
        &self,
        alias: &SnapshotAlias,
        action: impl FnOnce(&Self) -> RepositoryResult<T>,
    ) -> RepositoryResult<T> {
        let _guard = self.acquire_alias_lock(alias)?;
        action(self)
    }

    fn ensure_alias_available(
        &self,
        alias: &SnapshotAlias,
        new_id: &SnapshotId,
    ) -> RepositoryResult<()> {
        let alias_path = PosixFsSnapshotArtifactLayout::alias_path(&self.root, alias);
        if let Some(existing) = self.load_alias_target(alias)? {
            if &existing == new_id {
                return Ok(());
            }
            if self.load_record_by_id_unlocked(&existing)?.is_some() {
                return Err(RepositoryError::AliasConflict {
                    alias: alias.to_string(),
                    existing,
                    new_id: new_id.clone(),
                });
            }
            self.remove_file_if_exists(&alias_path)?;
        }
        Ok(())
    }

    fn write_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_json(&self.record_path(&record.id), record)
    }

    fn committed_record_unlocked(
        &self,
        metadata: &SnapshotPublishMetadata,
        committed: CommittedSnapshot,
        now_unix_ms: i64,
    ) -> RepositoryResult<SnapshotRecord> {
        let id = metadata.id.clone();
        let alias = metadata.alias.clone();
        let resources = metadata.resources;
        let source = metadata.source.clone();
        if let Some(mut record) = self.load_record_by_id_unlocked(&id)? {
            record.mark_committed(alias, resources, committed, source, now_unix_ms);
            return Ok(record);
        }

        let source = match source {
            SnapshotPublishSource::Template => SnapshotSource::Template {
                build: TemplateBuildInfo {
                    status: TemplateBuildStatus::Ready,
                    started_at_unix_ms: None,
                    finished_at_unix_ms: Some(now_unix_ms),
                    error_reason: None,
                },
            },
            SnapshotPublishSource::Sandbox { source_sandbox_id } => {
                SnapshotSource::Sandbox { source_sandbox_id }
            }
        };

        Ok(SnapshotRecord {
            id,
            alias,
            source,
            resources,
            created_at_unix_ms: now_unix_ms,
            updated_at_unix_ms: now_unix_ms,
            committed: Some(committed),
        })
    }

    fn write_committed_record_unlocked(&self, record: &SnapshotRecord) -> RepositoryResult<()> {
        self.write_record_unlocked(record)
    }

    fn matches_record_filter(record: &SnapshotRecord, filter: &SnapshotListFilter) -> bool {
        if let Some(alias_prefix) = filter.alias_prefix.as_deref() {
            match record.alias.as_ref() {
                Some(alias) if alias.to_string().starts_with(alias_prefix) => {}
                _ => return false,
            }
        }

        if let Some(ids) = filter.snapshot_ids.as_ref() {
            if !ids.iter().any(|id| id == &record.id) {
                return false;
            }
        }

        if let Some(id_or_alias) = filter.snapshot_id_or_alias.as_deref() {
            if record.id.to_string() != id_or_alias
                && record
                    .alias
                    .as_ref()
                    .is_none_or(|alias| alias.as_ref() != id_or_alias)
            {
                return false;
            }
        }

        if let Some(source_sandbox_id) = filter.source_sandbox_id.as_deref() {
            match &record.source {
                SnapshotSource::Sandbox {
                    source_sandbox_id: record_source_sandbox_id,
                } if record_source_sandbox_id == source_sandbox_id => {}
                _ => return false,
            }
        }

        if let Some(sources) = filter.sources.as_ref() {
            let source = match &record.source {
                SnapshotSource::Template { .. } => SnapshotSourceKind::Template,
                SnapshotSource::Sandbox { .. } => SnapshotSourceKind::Sandbox,
            };
            if !sources.contains(&source) {
                return false;
            }
        }

        if let Some(statuses) = filter.template_statuses.as_ref() {
            let SnapshotSource::Template { build } = &record.source else {
                return false;
            };
            if !statuses.contains(&build.status) {
                return false;
            };
        }

        true
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::layout::PosixFsSnapshotArtifactLayout;
    use super::{fs, thread, Duration};
    use super::{read_lock_token, PosixFileLockGuard, PosixFsCatalogStore, PosixFsLockStrategy};
    use crate::snapshot::{
        CommittedSnapshot, RepositoryError, SnapshotAlias, SnapshotId, SnapshotListFilter,
        SnapshotPublishMetadata, SnapshotPublishSource, SnapshotRecord, SnapshotSourceKind,
        TemplateBuildStatus,
    };

    #[test]
    fn begin_and_commit_make_snapshot_visible() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");

        store
            .commit_publish(
                &session,
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .expect("commit should work");

        assert!(store
            .get(&snapshot_id.to_string())
            .expect("get should work")
            .expect("snapshot should exist")
            .committed
            .is_some());
        assert!(
            PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id)
                .path(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER)
                .exists()
        );
    }

    /// Everything an acknowledged publish depends on has to be rename-durable:
    /// a snapshot the client can name but the catalog forgets after a power
    /// loss is worse than a publish that failed.
    #[test]
    fn commit_syncs_every_directory_it_renames_into() {
        use super::super::durable::probe::{self, Synced};

        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let snapshot_id = SnapshotId::generate();
        let alias = SnapshotAlias::parse("durable-alias").expect("alias should parse");
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");

        let _ = probe::take();
        store
            .commit_publish(
                &session,
                SnapshotPublishMetadata {
                    id: snapshot_id.clone(),
                    alias: Some(alias.clone()),
                    source: SnapshotPublishSource::Template,
                    ..SnapshotPublishMetadata::mock()
                },
                CommittedSnapshot::mock(),
            )
            .expect("commit should work");
        let synced = probe::take();

        let layout = PosixFsSnapshotArtifactLayout::new(tempdir.path(), &snapshot_id);
        let marker = layout.path(super::super::layout::POSIXFS_SNAPSHOT_COMMIT_MARKER);
        let record = PosixFsSnapshotArtifactLayout::record_path(tempdir.path(), &snapshot_id);
        let alias_path = PosixFsSnapshotArtifactLayout::alias_path(tempdir.path(), &alias);

        for path in [&marker, &record, &alias_path] {
            assert!(
                synced.contains(&Synced::File(path.clone())),
                "contents of '{}' were never synced: {synced:?}",
                path.display()
            );
            let parent = path.parent().expect("published files have a parent");
            assert!(
                synced.contains(&Synced::Dir(parent.to_path_buf())),
                "'{}' was renamed into an unsynced directory: {synced:?}",
                parent.display()
            );
        }
    }

    fn committed_metadata(
        id: SnapshotId,
        alias: &str,
        source: SnapshotPublishSource,
    ) -> SnapshotPublishMetadata {
        SnapshotPublishMetadata {
            id,
            alias: Some(SnapshotAlias::parse(alias).expect("alias should parse")),
            source,
            ..SnapshotPublishMetadata::mock()
        }
    }

    fn commit_record(store: &PosixFsCatalogStore, metadata: SnapshotPublishMetadata) -> SnapshotId {
        let snapshot_id = metadata.id.clone();
        let session = store
            .begin_publish(&snapshot_id)
            .expect("begin should work");
        store
            .commit_publish(&session, metadata, CommittedSnapshot::mock())
            .expect("commit should work");
        snapshot_id
    }

    fn listed_ids(store: &PosixFsCatalogStore, filter: SnapshotListFilter) -> Vec<SnapshotId> {
        store
            .list(filter)
            .expect("list should work")
            .into_iter()
            .map(|record| record.id)
            .collect()
    }

    #[test]
    fn list_applies_record_filters() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let template_alpha = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-alpha",
                SnapshotPublishSource::Template,
            ),
        );
        let template_beta = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "template-beta",
                SnapshotPublishSource::Template,
            ),
        );
        let sandbox_one = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-one",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-1".to_string(),
                },
            ),
        );
        let sandbox_two = commit_record(
            &store,
            committed_metadata(
                SnapshotId::generate(),
                "sandbox-two",
                SnapshotPublishSource::Sandbox {
                    source_sandbox_id: "sandbox-2".to_string(),
                },
            ),
        );
        let errored_template = SnapshotId::generate();
        store
            .create(SnapshotRecord::template_waiting(
                errored_template.clone(),
                Some(SnapshotAlias::parse("template-error").expect("alias should parse")),
                Default::default(),
            ))
            .expect("create template should work");
        store
            .mark_error(
                &errored_template,
                crate::snapshot::TemplateBuildErrorReason::new("boom"),
            )
            .expect("mark error should work");

        let ids = listed_ids(
            &store,
            SnapshotListFilter::by_ids([template_alpha.clone(), sandbox_one.clone()]),
        );
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("template-".to_string()),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));

        let ids = listed_ids(&store, SnapshotListFilter::templates());
        assert_eq!(ids.len(), 3);
        assert!(ids.contains(&template_alpha));
        assert!(ids.contains(&template_beta));
        assert!(ids.contains(&errored_template));
        assert!(!ids.contains(&sandbox_one));

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(Some("sandbox-1".to_string()), None),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some("team/sandbox-one:v1".to_string())),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(None, Some(format!("{}:v1", sandbox_one))),
        );
        assert_eq!(ids, vec![sandbox_one.clone()]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter::sandbox_snapshots(
                Some("sandbox-2".to_string()),
                Some("sandbox-one".to_string()),
            ),
        );
        assert!(ids.is_empty());

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                template_statuses: Some(vec![TemplateBuildStatus::Error]),
                ..SnapshotListFilter::templates()
            },
        );
        assert_eq!(ids, vec![errored_template]);

        let ids = listed_ids(
            &store,
            SnapshotListFilter {
                alias_prefix: Some("sandbox-".to_string()),
                sources: Some(vec![SnapshotSourceKind::Sandbox]),
                snapshot_ids: Some(vec![sandbox_two.clone(), template_alpha]),
                ..SnapshotListFilter::default()
            },
        );
        assert_eq!(ids, vec![sandbox_two]);
    }

    #[test]
    fn get_rejects_path_traversal_as_alias() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        // "../../etc/passwd" is not a valid alias (nor a UUID), so alias parsing
        // validation rejects it as InvalidRequest.
        let err = store
            .get("../../etc/passwd")
            .expect_err("path traversal should be rejected");
        assert!(
            matches!(err, crate::snapshot::RepositoryError::InvalidRequest { .. }),
            "expected InvalidRequest, got: {err:?}"
        );
    }

    #[test]
    fn get_returns_none_for_unknown_valid_uuid() {
        let tempdir = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::new(tempdir.path().to_path_buf());
        let unknown = SnapshotId::generate();
        let result = store
            .get(&unknown.to_string())
            .expect("valid UUID lookup should not error");
        assert!(result.is_none(), "non-existent snapshot should return None");
    }

    /// Two holders must never both believe they own the lock, however long the
    /// first one holds it.
    ///
    /// The previous implementation treated a lock file older than a fixed age
    /// as abandoned and deleted it, so a slow-but-live holder lost its lock to
    /// a contender. `flock` has no such window: the second acquire blocks until
    /// the first guard drops.
    #[test]
    fn file_lock_is_exclusive_while_held() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = PosixFsCatalogStore::new(root.path().to_path_buf());
        let alias = SnapshotAlias::parse("exclusive-alias").expect("alias");

        let held = store.acquire_alias_lock(&alias).expect("first acquire");

        let contended = store.acquire_alias_lock(&alias);
        assert!(
            contended.is_err(),
            "a second holder acquired a lock that was still held"
        );

        drop(held);
        store
            .acquire_alias_lock(&alias)
            .expect("lock should be acquirable once released");
    }

    /// Releasing one lock must not disturb another.
    ///
    /// The previous guard unlinked the lock file on drop with no ownership
    /// check, so a holder that had already lost its lock to a stale-steal went
    /// on to delete the new owner's file, admitting a third writer. Nothing is
    /// unlinked now, so this cannot recur.
    #[test]
    fn releasing_one_lock_does_not_release_another() {
        let root = tempfile::tempdir().expect("tempdir");
        let store = PosixFsCatalogStore::new(root.path().to_path_buf());
        let first = SnapshotAlias::parse("alias-one").expect("alias");
        let second = SnapshotAlias::parse("alias-two").expect("alias");

        let first_guard = store.acquire_alias_lock(&first).expect("first acquire");
        let second_guard = store.acquire_alias_lock(&second).expect("second acquire");

        drop(first_guard);

        assert!(
            store.acquire_alias_lock(&second).is_err(),
            "dropping an unrelated lock released a lock that was still held"
        );
        drop(second_guard);
    }

    /// Exactly one of many concurrent contenders may hold the lock at a time.
    #[test]
    fn concurrent_contenders_serialize_on_the_lock() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::thread;
        use std::time::Duration;

        let root = tempfile::tempdir().expect("tempdir");
        let store = Arc::new(PosixFsCatalogStore::new(root.path().to_path_buf()));
        let inside = Arc::new(AtomicUsize::new(0));
        let overlaps = Arc::new(AtomicUsize::new(0));
        let acquired = Arc::new(AtomicUsize::new(0));

        thread::scope(|scope| {
            for _ in 0..8 {
                let store = Arc::clone(&store);
                let inside = Arc::clone(&inside);
                let overlaps = Arc::clone(&overlaps);
                let acquired = Arc::clone(&acquired);
                scope.spawn(move || {
                    let alias = SnapshotAlias::parse("contended-alias").expect("alias");
                    let Ok(guard) = store.acquire_alias_lock(&alias) else {
                        return;
                    };
                    acquired.fetch_add(1, Ordering::SeqCst);
                    if inside.fetch_add(1, Ordering::SeqCst) != 0 {
                        overlaps.fetch_add(1, Ordering::SeqCst);
                    }
                    thread::sleep(Duration::from_millis(5));
                    inside.fetch_sub(1, Ordering::SeqCst);
                    drop(guard);
                });
            }
        });

        assert_eq!(
            overlaps.load(Ordering::SeqCst),
            0,
            "two contenders were inside the critical section at once"
        );
        assert!(
            acquired.load(Ordering::SeqCst) > 0,
            "no contender ever acquired the lock"
        );
    }

    /// Publishing an alias is a compare-and-set across two files, so it needs
    /// the lock to be a mutex and not a hint.
    ///
    /// Eight publishers, one alias: exactly one must win and the other seven
    /// must be told there is a conflict. Two publishers inside the critical
    /// section both read "no alias yet" and both write, and the loser's record
    /// then claims an alias pointing at the winner's snapshot.
    fn concurrent_alias_publish_yields_one_record(strategy: PosixFsLockStrategy) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let root = TempDir::new().expect("tempdir should exist");
        let store = Arc::new(PosixFsCatalogStore::with_lock_strategy(
            root.path().to_path_buf(),
            strategy,
        ));
        let alias = SnapshotAlias::parse("contended-publish").expect("alias");
        let published = Arc::new(AtomicUsize::new(0));
        let conflicted = Arc::new(AtomicUsize::new(0));
        let other = Arc::new(AtomicUsize::new(0));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let store = Arc::clone(&store);
                let alias = alias.clone();
                let published = Arc::clone(&published);
                let conflicted = Arc::clone(&conflicted);
                let other = Arc::clone(&other);
                scope.spawn(move || {
                    let snapshot_id = SnapshotId::generate();
                    let session = store
                        .begin_publish(&snapshot_id)
                        .expect("begin should work");
                    let result = store.commit_publish(
                        &session,
                        SnapshotPublishMetadata {
                            id: snapshot_id,
                            alias: Some(alias),
                            source: SnapshotPublishSource::Template,
                            ..SnapshotPublishMetadata::mock()
                        },
                        CommittedSnapshot::mock(),
                    );
                    match result {
                        Ok(_) => published.fetch_add(1, Ordering::SeqCst),
                        Err(RepositoryError::AliasConflict { .. }) => {
                            conflicted.fetch_add(1, Ordering::SeqCst)
                        }
                        Err(_) => other.fetch_add(1, Ordering::SeqCst),
                    };
                });
            }
        });

        assert_eq!(
            other.load(Ordering::SeqCst),
            0,
            "a publisher failed for a reason other than the alias conflict"
        );
        assert_eq!(
            published.load(Ordering::SeqCst),
            1,
            "{strategy}: exactly one publisher may take the alias"
        );
        assert_eq!(
            conflicted.load(Ordering::SeqCst),
            7,
            "{strategy}: every other publisher must be told the alias is taken"
        );

        let resolved = store
            .get(alias.as_ref())
            .expect("alias lookup should work")
            .expect("the alias should resolve");
        assert_eq!(
            resolved.alias.as_ref(),
            Some(&alias),
            "{strategy}: the alias must resolve to the record that claimed it"
        );
    }

    #[test]
    fn concurrent_alias_publish_yields_one_record_under_flock() {
        concurrent_alias_publish_yields_one_record(PosixFsLockStrategy::Flock);
    }

    #[test]
    fn concurrent_alias_publish_yields_one_record_under_create_new() {
        concurrent_alias_publish_yields_one_record(PosixFsLockStrategy::CreateNew);
    }

    /// Backdates a lock file past the age at which the fallback treats it as
    /// abandoned.
    fn age_lock_file(path: &std::path::Path) {
        let aged =
            std::time::SystemTime::now() - (super::CREATE_NEW_STALE_AGE + Duration::from_secs(1));
        let file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("lock file should be open-able");
        file.set_times(fs::FileTimes::new().set_modified(aged))
            .expect("lock file mtime should be settable");
    }

    /// The fallback's lock is a mutex even when the file it finds is stale.
    ///
    /// A stale lock is exactly where the previous implementation broke: every
    /// contender saw an abandoned file, every one deleted it, and every one
    /// created its own, so all of them proceeded together. Deleting and
    /// re-creating cannot be the steal; claiming the right to steal one
    /// observed token has to be.
    ///
    /// The contenders are released from a barrier, and the round is repeated.
    /// A version that only spawned threads in a loop was a lottery rather than
    /// a pin: thread creation costs more than the steal does, so the first
    /// thief had installed a fresh lock before the second contender looked at
    /// the file, the race never opened, and the naive delete-then-create steal
    /// passed it in 19 runs out of 25. With the barrier that steal loses rounds
    /// every time.
    #[test]
    fn a_stale_create_new_lock_admits_one_contender_at_a_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};

        /// Enough contenders that the window between one thief's re-read and
        /// its install is covered by somebody.
        const CONTENDERS: usize = 16;
        /// Rounds. One round is one abandoned lock and one stampede at it.
        const ROUNDS: usize = 40;

        let root = TempDir::new().expect("tempdir should exist");
        let store = Arc::new(
            PosixFsCatalogStore::with_lock_strategy(
                root.path().to_path_buf(),
                PosixFsLockStrategy::CreateNew,
            )
            // Short, so the contenders that lose a round give it up rather
            // than holding the test open for the full retry ladder.
            .with_lock_timeout(Duration::from_millis(80)),
        );

        let mut overlap_rounds = 0;
        for round in 0..ROUNDS {
            // A fresh alias per round: one abandoned lock, stolen once.
            let alias = SnapshotAlias::parse(&format!("stale-alias-{round}")).expect("alias");
            let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(root.path(), &alias);

            // A lock left behind by a process that is gone.
            std::mem::forget(store.acquire_alias_lock(&alias).expect("seed the lock"));
            age_lock_file(&lock_path);

            let start = Arc::new(Barrier::new(CONTENDERS));
            let inside = Arc::new(AtomicUsize::new(0));
            let overlaps = Arc::new(AtomicUsize::new(0));
            let acquired = Arc::new(AtomicUsize::new(0));

            std::thread::scope(|scope| {
                for _ in 0..CONTENDERS {
                    let store = Arc::clone(&store);
                    let alias = alias.clone();
                    let start = Arc::clone(&start);
                    let inside = Arc::clone(&inside);
                    let overlaps = Arc::clone(&overlaps);
                    let acquired = Arc::clone(&acquired);
                    scope.spawn(move || {
                        // Every contender looks at the same abandoned lock at
                        // the same moment. This is the whole point: the steal
                        // has to be exclusive under simultaneity, not under
                        // the accident of thread-spawn order.
                        start.wait();
                        let Ok(guard) = store.acquire_alias_lock(&alias) else {
                            return;
                        };
                        acquired.fetch_add(1, Ordering::SeqCst);
                        if inside.fetch_add(1, Ordering::SeqCst) != 0 {
                            overlaps.fetch_add(1, Ordering::SeqCst);
                        }
                        thread::sleep(Duration::from_millis(5));
                        inside.fetch_sub(1, Ordering::SeqCst);
                        drop(guard);
                    });
                }
            });

            if overlaps.load(Ordering::SeqCst) > 0 {
                overlap_rounds += 1;
            }
            assert!(
                acquired.load(Ordering::SeqCst) > 0,
                "round {round}: nobody managed to steal an abandoned lock"
            );
        }

        assert_eq!(
            overlap_rounds, 0,
            "two contenders held the same stolen lock in {overlap_rounds}/{ROUNDS} rounds"
        );
    }

    /// An abandoned lock has to be recoverable, or one crashed process wedges
    /// an alias for the life of the deployment.
    #[test]
    fn an_abandoned_create_new_lock_is_stolen() {
        let root = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::with_lock_strategy(
            root.path().to_path_buf(),
            PosixFsLockStrategy::CreateNew,
        )
        .with_lock_timeout(Duration::from_millis(200));
        let alias = SnapshotAlias::parse("abandoned-alias").expect("alias");
        let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(root.path(), &alias);

        // A lock whose holder is gone: the guard is leaked so nothing releases it.
        std::mem::forget(store.acquire_alias_lock(&alias).expect("seed the lock"));
        assert!(
            store.acquire_alias_lock(&alias).is_err(),
            "a lock that is not yet stale must not be stealable"
        );

        age_lock_file(&lock_path);
        store
            .acquire_alias_lock(&alias)
            .expect("an abandoned lock should be recoverable once it is stale");
    }

    /// Exactly one contender may take over one abandoned lock.
    ///
    /// The steal cannot be "delete it and create your own": two contenders that
    /// both saw the same abandoned file would both delete, both create, and
    /// both proceed — and the second delete removes the first's fresh lock.
    /// Claiming the right to steal one *observed token* is what makes it
    /// exclusive, and this is that claim.
    #[test]
    fn only_one_contender_may_claim_one_abandoned_lock() {
        let root = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::with_lock_strategy(
            root.path().to_path_buf(),
            PosixFsLockStrategy::CreateNew,
        );
        let alias = SnapshotAlias::parse("claimed-alias").expect("alias");
        let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(root.path(), &alias);

        std::mem::forget(store.acquire_alias_lock(&alias).expect("seed the lock"));
        age_lock_file(&lock_path);
        let stale = super::PosixFsCatalogStore::stale_lock_token(&lock_path)
            .expect("the aged lock should read as abandoned");

        assert!(
            PosixFsCatalogStore::win_steal_claim(&lock_path, &stale),
            "the first contender must be able to claim an abandoned lock"
        );
        assert!(
            !PosixFsCatalogStore::win_steal_claim(&lock_path, &stale),
            "a second contender claimed the same abandoned lock"
        );
    }

    /// A guard whose lock was stolen must not delete the thief's file.
    ///
    /// The previous guard unlinked unconditionally, so the original holder
    /// finishing its critical section removed the new owner's lock and let a
    /// third writer in behind it. Once that starts it does not settle.
    #[test]
    fn a_guard_does_not_remove_a_lock_it_no_longer_owns() {
        let root = TempDir::new().expect("tempdir should exist");
        let store = PosixFsCatalogStore::with_lock_strategy(
            root.path().to_path_buf(),
            PosixFsLockStrategy::CreateNew,
        )
        .with_lock_timeout(Duration::from_millis(200));
        let alias = SnapshotAlias::parse("stolen-alias").expect("alias");
        let lock_path = PosixFsSnapshotArtifactLayout::alias_lock_path(root.path(), &alias);

        let first = store.acquire_alias_lock(&alias).expect("first acquire");
        age_lock_file(&lock_path);

        let second = store
            .acquire_alias_lock(&alias)
            .expect("the stale lock is stealable");
        let PosixFileLockGuard::Token { token: thief, .. } = &second else {
            panic!("the create_new strategy must produce a token guard");
        };
        let thief = thief.clone();

        drop(first);

        assert!(
            lock_path.exists(),
            "the original holder deleted the thief's lock file"
        );
        assert_eq!(
            read_lock_token(&lock_path).as_ref(),
            Some(&thief),
            "the lock file no longer carries the current owner's token"
        );

        drop(second);
        assert!(
            !lock_path.exists(),
            "the owner's own drop should release the lock"
        );
    }
}
