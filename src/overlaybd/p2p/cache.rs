use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::p2p::metrics::{self as p2p_metrics, DescriptorCacheResult};
use crate::p2p::{P2pArtifactDescriptor, P2pArtifactKey};

use super::artifact::{CanonicalBlobIdentity, LayerMetadata};

/// A descriptor whose metadata has already been parsed and checked against the
/// identity the caller asked for.
///
/// The parse is a `serde_json::from_value` and it used to run on every range
/// request, on a cache hit as much as on a resolve. A layer is read at block
/// granularity, so that was once per block for the life of the device.
#[derive(Clone)]
pub(super) struct ResolvedLayer {
    pub(super) descriptor: P2pArtifactDescriptor,
    pub(super) metadata: LayerMetadata,
}

#[derive(Clone)]
pub(super) struct DescriptorCache {
    inner: Arc<DashMap<P2pArtifactKey, CachedDescriptor>>,
    hit_ttl: Duration,
    miss_ttl: Duration,
    max_entries: usize,
}

#[derive(Clone)]
struct CachedDescriptor {
    inserted_at: Instant,
    layer: Option<ResolvedLayer>,
}

impl DescriptorCache {
    pub(super) fn new(hit_ttl: Duration, miss_ttl: Duration, max_entries: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            hit_ttl,
            miss_ttl,
            max_entries,
        }
    }

    pub(super) fn get(&self, key: &P2pArtifactKey) -> Option<Option<ResolvedLayer>> {
        let Some(cached) = self.inner.get(key) else {
            p2p_metrics::record_descriptor_cache(DescriptorCacheResult::Absent);
            return None;
        };
        let ttl = if cached.layer.is_some() {
            self.hit_ttl
        } else {
            self.miss_ttl
        };
        if cached.inserted_at.elapsed() <= ttl {
            let layer = cached.layer.clone();
            p2p_metrics::record_descriptor_cache(if layer.is_some() {
                DescriptorCacheResult::Hit
            } else {
                DescriptorCacheResult::Miss
            });
            return Some(layer);
        }
        drop(cached);
        self.inner.remove(key);
        p2p_metrics::record_descriptor_cache(DescriptorCacheResult::Absent);
        None
    }

    pub(super) fn insert(&self, key: P2pArtifactKey, layer: Option<ResolvedLayer>) {
        p2p_metrics::record_descriptor_cache(DescriptorCacheResult::Insert);
        self.inner.insert(
            key,
            CachedDescriptor {
                inserted_at: Instant::now(),
                layer,
            },
        );
        self.prune_if_needed();
    }

    pub(super) fn remove(&self, key: &P2pArtifactKey) {
        self.inner.remove(key);
    }

    fn prune_if_needed(&self) {
        if self.max_entries == 0 {
            self.inner.clear();
            return;
        }
        if self.inner.len() <= self.max_entries {
            return;
        }

        let now = Instant::now();
        let expired: Vec<_> = self
            .inner
            .iter()
            .filter_map(|entry| {
                let ttl = if entry.layer.is_some() {
                    self.hit_ttl
                } else {
                    self.miss_ttl
                };
                (now.duration_since(entry.inserted_at) > ttl).then(|| entry.key().clone())
            })
            .collect();
        for key in expired {
            self.inner.remove(&key);
        }
        if self.inner.len() <= self.max_entries {
            return;
        }

        let mut entries: Vec<_> = self
            .inner
            .iter()
            .map(|entry| (entry.inserted_at, entry.key().clone()))
            .collect();
        entries.sort_by_key(|(inserted_at, _)| *inserted_at);
        let remove_count = self.inner.len().saturating_sub(self.max_entries);
        for (_, key) in entries.into_iter().take(remove_count) {
            self.inner.remove(&key);
        }
    }
}

/// The identity derived from an origin URL, remembered per URL path.
#[derive(Clone)]
pub(super) struct OriginIdentity {
    pub(super) canonical: CanonicalBlobIdentity,
    pub(super) key: P2pArtifactKey,
}

/// Memoises origin-URL parsing and key derivation.
///
/// Deriving an artifact key from an origin URL costs two `Url::parse` calls, a
/// digest scan and a `format!`, and the result is a pure function of the URL
/// path — the presigned query deliberately does not participate. Every range
/// request on a layer re-derives the same answer, so the derivation is
/// remembered rather than the request paying for it.
///
/// No TTL: the mapping cannot go stale, only crowded. Entries are dropped
/// oldest-first at the bound.
#[derive(Clone)]
pub(super) struct OriginKeyCache {
    inner: Arc<DashMap<String, (u64, OriginIdentity)>>,
    /// Insertion order, so eviction has a total order to work from rather than
    /// a clock whose resolution can tie under a burst.
    sequence: Arc<AtomicU64>,
    max_entries: usize,
}

impl OriginKeyCache {
    pub(super) fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
            sequence: Arc::new(AtomicU64::new(0)),
            max_entries,
        }
    }

    pub(super) fn get(&self, origin_path: &str) -> Option<OriginIdentity> {
        self.inner
            .get(origin_path)
            .map(|entry| entry.value().1.clone())
    }

    pub(super) fn insert(&self, origin_path: String, identity: OriginIdentity) {
        if self.max_entries == 0 {
            return;
        }
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        self.inner.insert(origin_path, (sequence, identity));
        if self.inner.len() <= self.max_entries {
            return;
        }
        let mut entries: Vec<_> = self
            .inner
            .iter()
            .map(|entry| (entry.value().0, entry.key().clone()))
            .collect();
        entries.sort_by_key(|(sequence, _)| *sequence);
        let remove_count = self.inner.len().saturating_sub(self.max_entries);
        for (_, key) in entries.into_iter().take(remove_count) {
            self.inner.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_cache_prunes_old_entries_at_max_size() {
        let cache = DescriptorCache::new(Duration::from_secs(60), Duration::from_secs(60), 2);
        cache.insert("old-a".to_string(), None);
        cache.insert("old-b".to_string(), None);
        cache.insert("new-c".to_string(), None);

        assert!(cache.get(&"old-a".to_string()).is_none());
        assert!(cache.get(&"old-b".to_string()).is_some());
        assert!(cache.get(&"new-c".to_string()).is_some());
    }

    #[test]
    fn origin_key_cache_prunes_oldest_entries_at_max_size() {
        let cache = OriginKeyCache::new(2);
        for name in ["a", "b", "c"] {
            cache.insert(
                name.to_string(),
                OriginIdentity {
                    canonical: CanonicalBlobIdentity::from_digest("sha256:aa"),
                    key: name.to_string(),
                },
            );
        }

        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
    }
}
