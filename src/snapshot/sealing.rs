//! Encrypting snapshot artifacts before they leave the node.
//!
//! Snapshot fixed artifacts are published to peers over P2P under a key derived
//! from the snapshot id: `snapshot/v1/artifacts/{snapshot_id}/vm_state.bin`.
//! Anyone who can reach the mesh and name that key gets the bytes, and those
//! bytes are a guest's CPU and memory state — whatever the agent had in RAM,
//! including the credentials it was given. The only thing standing between a
//! mesh participant and another sandbox's memory is knowing a snapshot id.
//!
//! That is already thin, and mobility records make it thinner: their whole
//! purpose is to advertise where paused sandbox state lives, which turns
//! "you would have to guess the id" into "the index will tell you". So sealing
//! lands first.
//!
//! # What this buys
//!
//! Artifacts are encrypted with a key that never travels the P2P path. A peer
//! that fetches an artifact gets ciphertext; to read it, it must already hold
//! the cluster sealing secret, which is provisioned out of band exactly like
//! the sandbox access-token seed beside it. P2P becomes a way to move bytes
//! faster, not a way to obtain them — it confers no authority it did not have.
//!
//! # What this does not buy
//!
//! AgentENV has one trust domain: a single API key, no tenant model. This
//! cannot isolate tenants from each other because there are no tenants to
//! isolate, and claiming otherwise would be worse than saying nothing. What it
//! does do is derive a distinct key per snapshot and per artifact, so a future
//! tenant key hierarchy replaces the root of the derivation without changing
//! the on-disk format or re-sealing anything already written.
//!
//! # Format
//!
//! A 40-byte plaintext header followed by AES-256-GCM chunks:
//!
//! ```text
//! magic          8   b"AENVSEL1"
//! version        1   1
//! algorithm      1   1 = AES-256-GCM
//! reserved       2   0
//! chunk_size     4   big-endian plaintext bytes per chunk
//! salt          16   random, fresh per seal
//! plaintext_len  8   big-endian
//! ```
//!
//! Chunks are fixed-size plaintext except the last, so their boundaries are
//! computable from the header and need no length prefixes — a length prefix
//! would be attacker-controlled framing outside the authenticated envelope.
//!
//! Each chunk's associated data is the whole header, the chunk index, and
//! whether it is the last chunk. Binding the header means the declared length
//! and salt cannot be edited; binding the index means chunks cannot be
//! reordered or replayed between positions; binding the last-chunk flag plus
//! the authenticated length means truncation is detected rather than read as a
//! short file.
//!
//! # Nonces
//!
//! Nonces are a plain counter. That is safe here only because the encryption
//! key is itself derived from the per-seal random salt, so a key is never used
//! for two different streams and a counter cannot collide within one. The salt
//! is what carries the randomness; the nonce carries only position.

use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};
use hmac::{Hmac, Mac};
use rand::{rngs::SysRng, TryRng};
use sha2::Sha256;
use zeroize::Zeroizing;

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::Aes256Gcm;

type HmacSha256 = Hmac<Sha256>;

const MAGIC: &[u8; 8] = b"AENVSEL1";
const FORMAT_VERSION: u8 = 1;
const ALGORITHM_AES_256_GCM: u8 = 1;
const HEADER_LEN: usize = 40;
const SALT_LEN: usize = 16;
const TAG_LEN: usize = 16;
const KEY_LEN: usize = 32;

/// Plaintext bytes per chunk.
///
/// Large enough that a multi-gigabyte memory image costs a few thousand tags
/// rather than a few million, small enough that sealing streams in bounded
/// memory on a node already running VMs.
pub const DEFAULT_CHUNK_SIZE: u32 = 1 << 20;

/// Largest chunk size accepted when opening.
///
/// The header is attacker-supplied until its first tag verifies, and the reader
/// must allocate a chunk buffer before it can verify anything. This bounds that
/// allocation.
const MAX_CHUNK_SIZE: u32 = 64 << 20;

const KEY_DERIVATION_LABEL: &[u8] = b"agentenv/snapshot-artifact-seal/v1";

/// The cluster-wide secret snapshot artifact keys are derived from.
///
/// Every node that may resolve a snapshot must hold the same value; a node with
/// the wrong one fails to open rather than reading garbage, because the tag
/// check fails first.
#[derive(Clone)]
pub struct ArtifactSealingKey {
    secret: Zeroizing<Vec<u8>>,
}

impl ArtifactSealingKey {
    /// Builds a key from raw secret material.
    pub fn from_bytes(secret: impl Into<Vec<u8>>) -> Result<Self> {
        let secret = Zeroizing::new(secret.into());
        if secret.len() < KEY_LEN {
            bail!("snapshot artifact sealing secret must be at least {KEY_LEN} bytes");
        }
        Ok(Self { secret })
    }

    /// Builds a key from a lowercase hex-encoded secret, as stored on disk.
    pub fn from_hex(secret: &str) -> Result<Self> {
        let decoded = hex::decode(secret.trim())
            .context("snapshot artifact sealing secret must be hex-encoded")?;
        Self::from_bytes(decoded)
    }

    /// Generates fresh secret material, hex-encoded for storage.
    pub fn generate_hex() -> Result<Zeroizing<String>> {
        let mut secret = Zeroizing::new([0_u8; KEY_LEN]);
        SysRng
            .try_fill_bytes(secret.as_mut_slice())
            .context("generate snapshot artifact sealing secret")?;
        Ok(Zeroizing::new(hex::encode(secret.as_slice())))
    }

    /// Derives the AES key for one artifact of one snapshot under one salt.
    ///
    /// The salt is what makes each seal's key unique, which is what makes the
    /// counter nonces safe. Snapshot id and artifact name are length-prefixed
    /// so that no two distinct pairs produce the same derivation input.
    fn derive(&self, scope: &SealScope<'_>, salt: &[u8; SALT_LEN]) -> Zeroizing<[u8; KEY_LEN]> {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(&self.secret)
            .expect("HMAC accepts keys of any length");
        mac.update(KEY_DERIVATION_LABEL);
        mac.update(salt);
        mac.update(&(scope.snapshot_id.len() as u32).to_be_bytes());
        mac.update(scope.snapshot_id.as_bytes());
        mac.update(&(scope.artifact_name.len() as u32).to_be_bytes());
        mac.update(scope.artifact_name.as_bytes());

        let mut key = Zeroizing::new([0_u8; KEY_LEN]);
        key.copy_from_slice(&mac.finalize().into_bytes());
        key
    }
}

impl std::fmt::Debug for ArtifactSealingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ArtifactSealingKey(<redacted>)")
    }
}

/// What an artifact is sealed for.
///
/// Both halves are bound into the key, so a sealed `vm_state.bin` cannot be
/// opened as some other artifact, and one snapshot's artifact cannot be
/// substituted for another's.
#[derive(Clone, Copy, Debug)]
pub struct SealScope<'a> {
    pub snapshot_id: &'a str,
    pub artifact_name: &'a str,
}

impl<'a> SealScope<'a> {
    pub fn new(snapshot_id: &'a str, artifact_name: &'a str) -> Self {
        Self {
            snapshot_id,
            artifact_name,
        }
    }
}

/// Encrypts `plaintext` into `sealed`, returning the plaintext byte count.
///
/// `plaintext_len` must be the exact length; it is authenticated, so a wrong
/// value is a sealing error rather than a corrupt artifact discovered later.
pub fn seal(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    plaintext_len: u64,
    plaintext: &mut impl Read,
    sealed: &mut impl Write,
) -> Result<u64> {
    let mut salt = [0_u8; SALT_LEN];
    SysRng
        .try_fill_bytes(&mut salt)
        .context("generate snapshot artifact seal salt")?;

    let header = encode_header(DEFAULT_CHUNK_SIZE, &salt, plaintext_len);
    sealed
        .write_all(&header)
        .context("write sealed artifact header")?;

    let cipher = new_cipher(key, scope, &salt);
    let chunk_size = DEFAULT_CHUNK_SIZE as usize;
    let chunk_count = chunk_count(plaintext_len, DEFAULT_CHUNK_SIZE);

    let mut buffer = Zeroizing::new(vec![0_u8; chunk_size]);
    let mut written = 0_u64;
    for index in 0..chunk_count {
        let remaining = plaintext_len - written;
        let want = remaining.min(chunk_size as u64) as usize;
        plaintext
            .read_exact(&mut buffer[..want])
            .with_context(|| format!("read plaintext chunk {index}"))?;

        let last = index + 1 == chunk_count;
        let aad = chunk_aad(&header, index, last);
        let ciphertext = cipher
            .encrypt(
                GenericArray::from_slice(&chunk_nonce(index)),
                Payload {
                    msg: &buffer[..want],
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("seal snapshot artifact chunk {index}"))?;
        sealed
            .write_all(&ciphertext)
            .with_context(|| format!("write sealed chunk {index}"))?;
        written += want as u64;
    }

    // A short reader would otherwise produce an artifact whose authenticated
    // length disagrees with its contents, and the mismatch would only surface
    // on the node trying to resume from it.
    let mut trailing = [0_u8; 1];
    match plaintext.read(&mut trailing) {
        Ok(0) => {}
        Ok(_) => bail!("plaintext is longer than the declared {plaintext_len} bytes"),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {}
        Err(error) => return Err(error).context("check for trailing plaintext"),
    }

    sealed.flush().context("flush sealed artifact")?;
    Ok(written)
}

/// Decrypts `sealed` into `plaintext`, returning the plaintext byte count.
///
/// Every failure mode — wrong key, edited header, tampered chunk, reordered
/// chunks, truncated stream — surfaces here as an error, never as short or
/// wrong output.
pub fn open(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    sealed: &mut impl Read,
    plaintext: &mut impl Write,
) -> Result<u64> {
    let mut header = [0_u8; HEADER_LEN];
    sealed
        .read_exact(&mut header)
        .context("read sealed artifact header")?;
    let (chunk_size, salt, plaintext_len) = decode_header(&header)?;

    let cipher = new_cipher(key, scope, &salt);
    let chunk_count = chunk_count(plaintext_len, chunk_size);

    // Sized by what this artifact actually needs, not by what its chunk size
    // permits. The MAX_CHUNK_SIZE guard bounds the worst case, but a small
    // artifact declaring a large chunk size should not allocate for a chunk it
    // will never read — and the header is still unauthenticated here.
    let widest_chunk = plaintext_len.min(chunk_size as u64) as usize;
    let mut buffer = vec![0_u8; widest_chunk + TAG_LEN];
    let mut produced = 0_u64;
    for index in 0..chunk_count {
        let remaining = plaintext_len - produced;
        let want = remaining.min(chunk_size as u64) as usize + TAG_LEN;
        sealed
            .read_exact(&mut buffer[..want])
            .with_context(|| format!("read sealed chunk {index} (stream is truncated)"))?;

        let last = index + 1 == chunk_count;
        let aad = chunk_aad(&header, index, last);
        let chunk = cipher
            .decrypt(
                GenericArray::from_slice(&chunk_nonce(index)),
                Payload {
                    msg: &buffer[..want],
                    aad: &aad,
                },
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "sealed snapshot artifact chunk {index} failed authentication; \
                     the artifact was modified or the sealing secret is wrong"
                )
            })?;
        plaintext
            .write_all(&chunk)
            .with_context(|| format!("write plaintext chunk {index}"))?;
        produced += chunk.len() as u64;
    }

    // Trailing bytes are not covered by any tag, so they must be rejected
    // rather than ignored: silently accepting them lets an attacker append.
    let mut trailing = [0_u8; 1];
    match sealed.read(&mut trailing) {
        Ok(0) => {}
        Ok(_) => bail!("sealed snapshot artifact has trailing bytes after the final chunk"),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {}
        Err(error) => return Err(error).context("check for trailing sealed bytes"),
    }

    plaintext.flush().context("flush plaintext artifact")?;
    Ok(produced)
}

/// Size of the sealed form of a `plaintext_len`-byte artifact.
pub fn sealed_len(plaintext_len: u64) -> u64 {
    let chunks = chunk_count(plaintext_len, DEFAULT_CHUNK_SIZE);
    HEADER_LEN as u64 + plaintext_len + chunks * TAG_LEN as u64
}

/// Whether a stream begins with a sealed-artifact header.
///
/// Used to tell a sealed artifact from a legacy plaintext one during rollout,
/// not as a security check: the magic is public and unauthenticated.
pub fn has_sealed_magic(prefix: &[u8]) -> bool {
    prefix.starts_with(MAGIC)
}

fn new_cipher(key: &ArtifactSealingKey, scope: &SealScope<'_>, salt: &[u8; SALT_LEN]) -> Aes256Gcm {
    let derived = key.derive(scope, salt);
    <Aes256Gcm as KeyInit>::new_from_slice(derived.as_slice()).expect("derived key is 32 bytes")
}

/// Always at least one chunk, so the header is authenticated even when the
/// artifact is empty.
fn chunk_count(plaintext_len: u64, chunk_size: u32) -> u64 {
    plaintext_len.div_ceil(chunk_size as u64).max(1)
}

fn chunk_nonce(index: u64) -> [u8; 12] {
    let mut nonce = [0_u8; 12];
    nonce[4..].copy_from_slice(&index.to_be_bytes());
    nonce
}

fn chunk_aad(header: &[u8; HEADER_LEN], index: u64, last: bool) -> Vec<u8> {
    let mut aad = Vec::with_capacity(HEADER_LEN + 9);
    aad.extend_from_slice(header);
    aad.extend_from_slice(&index.to_be_bytes());
    aad.push(u8::from(last));
    aad
}

fn encode_header(chunk_size: u32, salt: &[u8; SALT_LEN], plaintext_len: u64) -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8] = FORMAT_VERSION;
    header[9] = ALGORITHM_AES_256_GCM;
    // header[10..12] stays zero: reserved.
    header[12..16].copy_from_slice(&chunk_size.to_be_bytes());
    header[16..32].copy_from_slice(salt);
    header[32..40].copy_from_slice(&plaintext_len.to_be_bytes());
    header
}

fn decode_header(header: &[u8; HEADER_LEN]) -> Result<(u32, [u8; SALT_LEN], u64)> {
    if &header[..8] != MAGIC {
        bail!("not a sealed snapshot artifact");
    }
    if header[8] != FORMAT_VERSION {
        bail!(
            "unsupported sealed artifact format version {}; this build understands {FORMAT_VERSION}",
            header[8]
        );
    }
    if header[9] != ALGORITHM_AES_256_GCM {
        bail!("unsupported sealed artifact algorithm {}", header[9]);
    }

    let chunk_size = u32::from_be_bytes(header[12..16].try_into().expect("4 bytes"));
    if chunk_size == 0 {
        bail!("sealed artifact declares a zero chunk size");
    }
    if chunk_size > MAX_CHUNK_SIZE {
        bail!("sealed artifact declares a {chunk_size}-byte chunk size, above the {MAX_CHUNK_SIZE}-byte limit");
    }

    let mut salt = [0_u8; SALT_LEN];
    salt.copy_from_slice(&header[16..32]);
    let plaintext_len = u64::from_be_bytes(header[32..40].try_into().expect("8 bytes"));
    Ok((chunk_size, salt, plaintext_len))
}

/// Seals `source` into `destination`.
pub fn seal_path(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    source: &Path,
    destination: &Path,
) -> Result<u64> {
    let mut plaintext = fs::File::open(source)
        .with_context(|| format!("open artifact {} for sealing", source.display()))?;
    let plaintext_len = plaintext
        .metadata()
        .with_context(|| format!("inspect artifact {}", source.display()))?
        .len();
    let mut sealed = fs::File::create(destination)
        .with_context(|| format!("create sealed artifact {}", destination.display()))?;
    seal(key, scope, plaintext_len, &mut plaintext, &mut sealed)
}

/// Opens `source` into `destination`.
pub fn open_path(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    source: &Path,
    destination: &Path,
) -> Result<u64> {
    let mut sealed = fs::File::open(source)
        .with_context(|| format!("open sealed artifact {}", source.display()))?;

    // Staged and renamed, never written in place. `open` streams verified
    // chunks as it goes, so writing straight to the destination would leave a
    // byte-exact prefix of the plaintext there on every failure — a truncated
    // memory image at the path an artifact cache treats as a hit, served to
    // every later reader as if it were whole.
    let directory = destination.parent().unwrap_or_else(|| Path::new("."));
    let staged = tempfile::NamedTempFile::new_in(directory)
        .with_context(|| format!("stage artifact in {}", directory.display()))?;

    let written = {
        let mut plaintext = staged.as_file();
        open(key, scope, &mut sealed, &mut plaintext)?
    };

    staged
        .persist(destination)
        .map_err(|error| error.error)
        .with_context(|| format!("publish artifact {}", destination.display()))?;
    Ok(written)
}

/// Seals a small in-memory artifact.
pub fn seal_slice(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    plaintext: &[u8],
) -> Result<Vec<u8>> {
    let mut sealed = Vec::with_capacity(sealed_len(plaintext.len() as u64) as usize);
    seal(
        key,
        scope,
        plaintext.len() as u64,
        &mut &plaintext[..],
        &mut sealed,
    )?;
    Ok(sealed)
}

/// Opens a small in-memory artifact.
pub fn open_slice(
    key: &ArtifactSealingKey,
    scope: &SealScope<'_>,
    sealed: &[u8],
) -> Result<Vec<u8>> {
    let mut plaintext = Vec::with_capacity(sealed.len());
    open(key, scope, &mut &sealed[..], &mut plaintext)?;
    Ok(plaintext)
}

/// Whether this node can seal snapshot artifacts, and with what.
///
/// Absent a secret this is `Disabled`, and the callers that would have
/// published guest state to peers do not. That is deliberately not a fallback
/// to plaintext publication: the whole point of the ordering is that nothing
/// leaves the node in the clear.
#[derive(Debug, Default)]
pub struct SnapshotSealing {
    key: Option<ArtifactSealingKey>,
}

impl SnapshotSealing {
    pub fn disabled() -> Self {
        Self { key: None }
    }

    pub fn with_key(key: ArtifactSealingKey) -> Self {
        Self { key: Some(key) }
    }

    /// Resolves the node's sealing state from configuration.
    ///
    /// A missing secret disables sealing rather than generating one. A
    /// node-local secret would be worse than none: every node would seal with
    /// a different key, every peer fetch would fail authentication and fall
    /// back to the repository, and the deployment would look protected while
    /// delivering nothing. The secret is cluster-wide by nature, so it has to
    /// be provisioned that way.
    pub fn from_config(config: &crate::cfg::AppConfig) -> Result<Self> {
        Self::from_secret(config.snapshot.artifact_sealing_secret.as_deref())
    }

    /// The decision `from_config` makes, without an `AppConfig` to assemble.
    ///
    /// Split out so the blank-and-absent handling can be tested directly: it
    /// decides whether a fleet publishes guest state at all, and it is exactly
    /// the kind of parsing that looks obviously right and silently is not.
    pub fn from_secret(secret: Option<&str>) -> Result<Self> {
        let Some(secret) = secret.map(str::trim).filter(|secret| !secret.is_empty()) else {
            return Ok(Self::disabled());
        };
        Ok(Self::with_key(
            ArtifactSealingKey::from_hex(secret).context(
                "parse [snapshot].artifact_sealing_secret (AENV_SNAPSHOT_ARTIFACT_SEALING_SECRET)",
            )?,
        ))
    }

    pub fn key(&self) -> Option<&ArtifactSealingKey> {
        self.key.as_ref()
    }

    pub fn is_enabled(&self) -> bool {
        self.key.is_some()
    }
}

static GLOBAL: std::sync::OnceLock<std::sync::Arc<SnapshotSealing>> = std::sync::OnceLock::new();

/// Installs the process-wide sealing state. Called once at startup; a later
/// call is ignored so a mis-ordered init cannot swap a live key.
pub fn set_global_snapshot_sealing(sealing: std::sync::Arc<SnapshotSealing>) {
    if GLOBAL.set(sealing).is_err() {
        tracing::warn!("snapshot artifact sealing was already installed; ignoring the later one");
    }
}

/// Returns the process-wide sealing state, disabled when none was installed.
pub fn global_snapshot_sealing() -> std::sync::Arc<SnapshotSealing> {
    std::sync::Arc::clone(GLOBAL.get_or_init(|| std::sync::Arc::new(SnapshotSealing::disabled())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ArtifactSealingKey {
        ArtifactSealingKey::from_bytes(vec![7_u8; KEY_LEN]).expect("key")
    }

    fn other_key() -> ArtifactSealingKey {
        ArtifactSealingKey::from_bytes(vec![9_u8; KEY_LEN]).expect("key")
    }

    fn scope() -> SealScope<'static> {
        SealScope::new("snap-1", "vm_state.bin")
    }

    fn seal_bytes(key: &ArtifactSealingKey, scope: &SealScope<'_>, plaintext: &[u8]) -> Vec<u8> {
        let mut sealed = Vec::new();
        let written = seal(
            key,
            scope,
            plaintext.len() as u64,
            &mut &plaintext[..],
            &mut sealed,
        )
        .expect("seal");
        assert_eq!(written, plaintext.len() as u64);
        sealed
    }

    fn open_bytes(
        key: &ArtifactSealingKey,
        scope: &SealScope<'_>,
        sealed: &[u8],
    ) -> Result<Vec<u8>> {
        let mut plaintext = Vec::new();
        open(key, scope, &mut &sealed[..], &mut plaintext)?;
        Ok(plaintext)
    }

    #[test]
    fn round_trips_across_chunk_boundaries() {
        let sizes = [
            0,
            1,
            DEFAULT_CHUNK_SIZE as usize - 1,
            DEFAULT_CHUNK_SIZE as usize,
            DEFAULT_CHUNK_SIZE as usize + 1,
            DEFAULT_CHUNK_SIZE as usize * 2 + 7,
        ];
        for size in sizes {
            let plaintext: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();
            let sealed = seal_bytes(&key(), &scope(), &plaintext);
            assert_eq!(
                sealed.len() as u64,
                sealed_len(size as u64),
                "sealed_len must predict the actual size for {size} bytes"
            );
            assert_eq!(
                open_bytes(&key(), &scope(), &sealed).expect("open"),
                plaintext,
                "round trip failed at {size} bytes"
            );
        }
    }

    /// The point of the whole exercise: a peer holding the bytes but not the
    /// secret learns nothing.
    #[test]
    fn plaintext_does_not_appear_in_the_sealed_form() {
        let plaintext = b"AWS_SECRET_ACCESS_KEY=not-in-the-ciphertext".repeat(64);
        let sealed = seal_bytes(&key(), &scope(), &plaintext);
        assert!(
            !sealed
                .windows(plaintext.len())
                .any(|window| window == plaintext.as_slice()),
            "sealed artifact must not contain the plaintext"
        );
    }

    #[test]
    fn a_wrong_secret_cannot_open() {
        let sealed = seal_bytes(&key(), &scope(), b"guest memory");
        let error = open_bytes(&other_key(), &scope(), &sealed).expect_err("wrong key");
        assert!(
            error.to_string().contains("failed authentication"),
            "unexpected error: {error}"
        );
    }

    /// Scope binding: an artifact sealed for one snapshot must not open as
    /// another's, which is what stops a peer from substituting state.
    #[test]
    fn scope_is_bound_into_the_key() {
        let sealed = seal_bytes(&key(), &scope(), b"guest memory");

        for wrong in [
            SealScope::new("snap-2", "vm_state.bin"),
            SealScope::new("snap-1", "firecracker-manifest.json"),
        ] {
            open_bytes(&key(), &wrong, &sealed)
                .expect_err("an artifact must not open outside its scope");
        }
    }

    #[test]
    fn tampering_with_a_chunk_is_detected() {
        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        let last = sealed.len() - 1;
        sealed[last] ^= 0x01;
        open_bytes(&key(), &scope(), &sealed).expect_err("modified tag");

        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[HEADER_LEN] ^= 0x01;
        open_bytes(&key(), &scope(), &sealed).expect_err("modified ciphertext");
    }

    /// The header is plaintext, so it must be authenticated by every chunk or
    /// an attacker could rewrite the declared length and salt at will.
    #[test]
    fn editing_the_header_is_detected() {
        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[16] ^= 0x01; // salt
        open_bytes(&key(), &scope(), &sealed).expect_err("edited salt");

        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[39] ^= 0x01; // declared plaintext length
        open_bytes(&key(), &scope(), &sealed).expect_err("edited length");
    }

    /// Swapping two chunks preserves every tag's key and every byte, so only
    /// binding the index into the associated data catches it.
    #[test]
    fn reordering_chunks_is_detected() {
        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let plaintext: Vec<u8> = (0..chunk * 2).map(|index| (index / chunk) as u8).collect();
        let mut sealed = seal_bytes(&key(), &scope(), &plaintext);

        let sealed_chunk = chunk + TAG_LEN;
        let (head, body) = sealed.split_at_mut(HEADER_LEN);
        let _ = head;
        let (first, second) = body.split_at_mut(sealed_chunk);
        first.swap_with_slice(second);

        open_bytes(&key(), &scope(), &sealed).expect_err("reordered chunks");
    }

    /// Dropping the tail must fail rather than yield a short artifact: a
    /// silently short memory image is a corrupt guest, discovered on resume.
    #[test]
    fn truncation_is_detected() {
        let plaintext = vec![3_u8; DEFAULT_CHUNK_SIZE as usize * 2];
        let sealed = seal_bytes(&key(), &scope(), &plaintext);
        let truncated = &sealed[..sealed.len() - (DEFAULT_CHUNK_SIZE as usize + TAG_LEN)];

        let error = open_bytes(&key(), &scope(), truncated).expect_err("truncated stream");
        assert!(
            error.to_string().contains("truncated"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn appending_after_the_final_chunk_is_detected() {
        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed.extend_from_slice(b"appended");
        let error = open_bytes(&key(), &scope(), &sealed).expect_err("trailing bytes");
        assert!(
            error.to_string().contains("trailing"),
            "unexpected error: {error}"
        );
    }

    /// A hostile chunk size must be refused by the bound rather than acted on.
    ///
    /// Deliberately not named for "no allocation before verification": the
    /// reader does allocate before any tag is checked, for every size the
    /// guard admits. What the guard provides is a ceiling on that allocation,
    /// which is a weaker and more honest claim.
    #[test]
    fn a_chunk_size_outside_the_accepted_range_is_refused() {
        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[12..16].copy_from_slice(&u32::MAX.to_be_bytes());
        let error = open_bytes(&key(), &scope(), &sealed).expect_err("absurd chunk size");
        assert!(error.to_string().contains("limit"), "unexpected: {error}");

        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[12..16].copy_from_slice(&0_u32.to_be_bytes());
        open_bytes(&key(), &scope(), &sealed).expect_err("zero chunk size");
    }

    #[test]
    fn a_plaintext_artifact_is_not_mistaken_for_a_sealed_one() {
        let error = open_bytes(&key(), &scope(), &[0_u8; HEADER_LEN + TAG_LEN])
            .expect_err("plaintext artifact");
        assert!(
            error.to_string().contains("not a sealed"),
            "unexpected: {error}"
        );
        assert!(!has_sealed_magic(&[0_u8; 8]));
        assert!(has_sealed_magic(&seal_bytes(&key(), &scope(), b"x")));
    }

    #[test]
    fn a_future_format_version_is_refused_rather_than_guessed() {
        let mut sealed = seal_bytes(&key(), &scope(), b"guest memory");
        sealed[8] = FORMAT_VERSION + 1;
        let error = open_bytes(&key(), &scope(), &sealed).expect_err("future version");
        assert!(
            error
                .to_string()
                .contains("unsupported sealed artifact format version"),
            "unexpected: {error}"
        );
    }

    /// Declaring a length the reader does not supply would produce an artifact
    /// whose authenticated length disagrees with its contents, discovered only
    /// on the node trying to resume.
    #[test]
    fn a_mismatched_declared_length_fails_at_seal_time() {
        let plaintext = [1_u8; 64];

        let mut sealed = Vec::new();
        seal(&key(), &scope(), 128, &mut &plaintext[..], &mut sealed)
            .expect_err("declared longer than supplied");

        let mut sealed = Vec::new();
        let error = seal(&key(), &scope(), 32, &mut &plaintext[..], &mut sealed)
            .expect_err("declared shorter than supplied");
        assert!(error.to_string().contains("longer than"), "got: {error}");
    }

    #[test]
    fn a_short_secret_is_refused() {
        ArtifactSealingKey::from_bytes(vec![1_u8; KEY_LEN - 1]).expect_err("short secret");
        ArtifactSealingKey::from_hex("not-hex").expect_err("non-hex secret");
        let generated = ArtifactSealingKey::generate_hex().expect("generate");
        assert_eq!(generated.len(), KEY_LEN * 2);
        ArtifactSealingKey::from_hex(&generated).expect("generated secret is usable");
    }

    /// Two seals of the same bytes must differ, or an observer learns that a
    /// snapshot was re-published unchanged.
    #[test]
    fn each_seal_uses_fresh_salt() {
        let first = seal_bytes(&key(), &scope(), b"guest memory");
        let second = seal_bytes(&key(), &scope(), b"guest memory");
        assert_ne!(first, second, "salt must be fresh per seal");
        assert_eq!(
            open_bytes(&key(), &scope(), &first).expect("open"),
            open_bytes(&key(), &scope(), &second).expect("open"),
        );
    }

    #[test]
    fn the_secret_is_not_printed() {
        let rendered = format!("{:?}", key());
        assert_eq!(rendered, "ArtifactSealingKey(<redacted>)");
    }
}

#[cfg(test)]
mod file_and_config_tests {
    use super::*;

    fn key() -> ArtifactSealingKey {
        ArtifactSealingKey::from_bytes(vec![7_u8; KEY_LEN]).expect("key")
    }

    fn scope() -> SealScope<'static> {
        SealScope::new("snap-1", "vm_state.bin")
    }

    /// `seal_path` was covered and `open_path` was not, which left the two
    /// halves of one production round trip asymmetrically tested: publication
    /// seals a file, resolution opens one, and only the first had ever run.
    #[test]
    fn a_file_round_trips_through_seal_and_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plain = dir.path().join("vm_state.bin");
        let sealed = dir.path().join("vm_state.sealed");
        let reopened = dir.path().join("vm_state.reopened");

        // Larger than one chunk, so the file path exercises the same chunk
        // loop the streaming API does rather than a single-chunk special case.
        let contents: Vec<u8> = (0..DEFAULT_CHUNK_SIZE as usize * 2 + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        std::fs::write(&plain, &contents).expect("write plaintext");

        let sealed_len = seal_path(&key(), &scope(), &plain, &sealed).expect("seal");
        assert_eq!(sealed_len, contents.len() as u64);

        let opened_len = open_path(&key(), &scope(), &sealed, &reopened).expect("open");
        assert_eq!(opened_len, contents.len() as u64);
        assert_eq!(
            std::fs::read(&reopened).expect("read reopened"),
            contents,
            "the file must come back byte for byte"
        );
    }

    /// A peer that hands over a file it cannot authenticate must fail the
    /// open, not write a partial or wrong plaintext that a guest then boots.
    #[test]
    fn a_tampered_file_fails_to_open() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plain = dir.path().join("vm_state.bin");
        let sealed = dir.path().join("vm_state.sealed");
        let reopened = dir.path().join("vm_state.reopened");
        std::fs::write(&plain, b"guest memory").expect("write");
        seal_path(&key(), &scope(), &plain, &sealed).expect("seal");

        let mut bytes = std::fs::read(&sealed).expect("read sealed");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&sealed, &bytes).expect("rewrite sealed");

        open_path(&key(), &scope(), &sealed, &reopened).expect_err("a modified file must not open");
    }

    #[test]
    fn opening_a_missing_file_reports_the_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = open_path(
            &key(),
            &scope(),
            &dir.path().join("absent.sealed"),
            &dir.path().join("out"),
        )
        .expect_err("a missing artifact must not open");
        assert!(
            error.to_string().contains("absent.sealed"),
            "the error should name the file: {error}"
        );
    }

    /// This decides whether a fleet publishes guest state at all. Absent and
    /// blank must both disable rather than produce a key from nothing, and a
    /// malformed secret must fail loudly at startup rather than quietly
    /// leaving sealing off.
    #[test]
    fn the_sealing_secret_is_parsed_the_way_startup_needs() {
        for secret in [None, Some(""), Some("   "), Some("\n")] {
            let sealing = SnapshotSealing::from_secret(secret).expect("blank disables");
            assert!(
                !sealing.is_enabled(),
                "secret {secret:?} must leave sealing off"
            );
        }

        let hex = hex::encode([3_u8; KEY_LEN]);
        let sealing = SnapshotSealing::from_secret(Some(&hex)).expect("valid secret");
        assert!(sealing.is_enabled());

        // Surrounding whitespace is a copy-paste artefact, not a different key.
        let padded = format!("  {hex}\n");
        assert!(SnapshotSealing::from_secret(Some(&padded))
            .expect("padded secret")
            .is_enabled());

        // A secret that is present but wrong must stop the node, because the
        // alternative is a fleet that believes it is sealing and is not.
        let error = SnapshotSealing::from_secret(Some("not-hex-at-all"))
            .expect_err("a malformed secret must fail startup");
        assert!(
            error.to_string().contains("artifact_sealing_secret"),
            "the error should name the setting: {error:#}"
        );
        SnapshotSealing::from_secret(Some(&hex::encode([1_u8; 16])))
            .expect_err("a too-short secret must fail startup");
    }
}

#[cfg(test)]
mod construction_tests {
    use super::*;

    fn key() -> ArtifactSealingKey {
        ArtifactSealingKey::from_bytes(vec![7_u8; KEY_LEN]).expect("key")
    }

    fn scope() -> SealScope<'static> {
        SealScope::new("snap-1", "vm_state.bin")
    }

    fn seal(plaintext: &[u8]) -> Vec<u8> {
        seal_slice(&key(), &scope(), plaintext).expect("seal")
    }

    /// Nonce reuse under one AES-GCM key is catastrophic — it leaks the XOR of
    /// the plaintexts and, worse, the authentication key itself. Every other
    /// sealing test passes with a constant nonce, because a consistent seal and
    /// open still round trip. This is the one that does not.
    ///
    /// Two identical plaintext chunks under identical keys produce identical
    /// ciphertext if and only if they share a nonce, so comparing the chunks
    /// of one artifact detects it directly.
    #[test]
    fn every_chunk_of_one_artifact_uses_a_distinct_nonce() {
        let chunk = DEFAULT_CHUNK_SIZE as usize;
        // Three identical chunks. Anything that makes the nonce a constant
        // makes these encrypt to the same bytes.
        let plaintext = vec![0xAB_u8; chunk * 3];
        let sealed = seal(&plaintext);

        // Compare the ciphertext only, NOT the trailing tag. The tag covers
        // the associated data, which already includes the chunk index, so
        // tags differ per chunk even under a reused nonce — comparing them
        // would make this test pass while proving nothing.
        let body = &sealed[HEADER_LEN..];
        let sealed_chunk = chunk + TAG_LEN;
        let ciphertext = |index: usize| &body[index * sealed_chunk..index * sealed_chunk + chunk];
        let first = ciphertext(0);
        let second = ciphertext(1);
        let third = ciphertext(2);

        assert_ne!(
            first, second,
            "identical plaintext chunks encrypted alike: the nonce is not varying"
        );
        assert_ne!(first, third, "chunks 0 and 2 share a nonce");
        assert_ne!(second, third, "chunks 1 and 2 share a nonce");
    }

    /// The header is plaintext and is only protected by being bound into every
    /// chunk's associated data. Without that binding, rewriting the declared
    /// length and truncating the file to match yields a *shorter artifact that
    /// still authenticates* — a forgery that hands a guest a truncated memory
    /// image with every tag valid.
    ///
    /// The other tampering tests do not cover this: editing the salt changes
    /// the derived key, and reordering changes the nonce, so both fail for
    /// reasons that have nothing to do with the associated data.
    #[test]
    fn a_truncation_forgery_that_rewrites_the_declared_length_is_rejected() {
        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let plaintext = vec![5_u8; chunk * 3];
        let sealed = seal(&plaintext);

        // Claim one chunk, and cut the file to exactly that. Every remaining
        // tag is genuine; only the header disagrees with what was sealed.
        let mut forged = sealed[..HEADER_LEN + chunk + TAG_LEN].to_vec();
        forged[32..40].copy_from_slice(&(chunk as u64).to_be_bytes());

        let error = open_slice(&key(), &scope(), &forged)
            .expect_err("a rewritten length with matching truncation must not authenticate");
        assert!(
            error.to_string().contains("failed authentication"),
            "the header must be authenticated by the chunk tags, got: {error}"
        );
    }

    /// The same forgery in the other direction: a header that declares more
    /// than was sealed must not be accepted as a longer artifact.
    #[test]
    fn a_lengthened_declared_length_is_rejected() {
        let plaintext = vec![9_u8; 64];
        let mut forged = seal(&plaintext);
        forged[32..40].copy_from_slice(&1024_u64.to_be_bytes());

        open_slice(&key(), &scope(), &forged)
            .expect_err("a header claiming more than was sealed must not authenticate");
    }

    /// The chunk-size field decides how much the reader allocates before any
    /// tag has verified, so it too must be covered by the tags rather than
    /// merely bounded.
    #[test]
    fn a_rewritten_chunk_size_is_rejected() {
        let plaintext = vec![3_u8; 128];
        let mut forged = seal(&plaintext);
        forged[12..16].copy_from_slice(&(DEFAULT_CHUNK_SIZE / 2).to_be_bytes());

        open_slice(&key(), &scope(), &forged)
            .expect_err("a rewritten chunk size must not authenticate");
    }

    /// The guard exists to bound an allocation made from an attacker-supplied
    /// header before anything has been verified, so its boundary is worth
    /// pinning rather than only its far side.
    #[test]
    fn the_chunk_size_guard_is_exclusive_at_its_limit() {
        let mut at_limit = seal(b"guest memory");
        at_limit[12..16].copy_from_slice(&MAX_CHUNK_SIZE.to_be_bytes());
        let error = open_slice(&key(), &scope(), &at_limit)
            .expect_err("the maximum is rejected by the tag, not by the bound");
        assert!(
            !error.to_string().contains("limit"),
            "exactly MAX_CHUNK_SIZE must pass the bound and fail authentication instead: {error}"
        );

        let mut past_limit = seal(b"guest memory");
        past_limit[12..16].copy_from_slice(&(MAX_CHUNK_SIZE + 1).to_be_bytes());
        let error = open_slice(&key(), &scope(), &past_limit).expect_err("past the limit");
        assert!(
            error.to_string().contains("limit"),
            "one byte past the maximum must be refused by the bound: {error}"
        );
    }
}

#[cfg(test)]
mod staging_tests {
    use super::*;

    fn key() -> ArtifactSealingKey {
        ArtifactSealingKey::from_bytes(vec![7_u8; KEY_LEN]).expect("key")
    }

    fn scope() -> SealScope<'static> {
        SealScope::new("snap-1", "vm_state.bin")
    }

    /// A failed open must leave nothing at the destination. `open` streams
    /// verified chunks as it goes, so writing in place left a byte-exact
    /// prefix of the plaintext on every failure — and the artifact cache
    /// treats any file at that path as a hit, so a truncated memory image
    /// would be served to every later reader as if it were whole.
    #[test]
    fn a_failed_open_leaves_no_file_behind() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plain = dir.path().join("vm_state.bin");
        let sealed = dir.path().join("vm_state.sealed");
        let destination = dir.path().join("vm_state.out");

        // Two chunks, so the first verifies and lands in the stream before the
        // second fails — the case that actually produced a partial file.
        let contents = vec![4_u8; DEFAULT_CHUNK_SIZE as usize * 2];
        std::fs::write(&plain, &contents).expect("write");
        seal_path(&key(), &scope(), &plain, &sealed).expect("seal");

        let mut bytes = std::fs::read(&sealed).expect("read");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        std::fs::write(&sealed, &bytes).expect("rewrite");

        open_path(&key(), &scope(), &sealed, &destination)
            .expect_err("a corrupted final chunk must fail the open");
        assert!(
            !destination.exists(),
            "a failed open must leave nothing at the destination, found {} bytes",
            std::fs::metadata(&destination)
                .map(|m| m.len())
                .unwrap_or(0)
        );

        // And the staging file must not be left lying beside it either.
        let strays: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with("vm_state."))
            .collect();
        assert!(strays.is_empty(), "staging left behind: {strays:?}");
    }

    /// A small artifact must not allocate for a chunk size it will never read,
    /// even though the header is still unauthenticated at that point.
    #[test]
    fn a_small_artifact_does_not_allocate_for_its_declared_chunk_size() {
        let plaintext = b"small";
        let sealed = seal_slice(&key(), &scope(), plaintext).expect("seal");
        // The header declares the full default chunk size; the artifact is
        // five bytes. Opening must succeed without depending on that size.
        assert_eq!(
            open_slice(&key(), &scope(), &sealed).expect("open"),
            plaintext
        );
    }
}
