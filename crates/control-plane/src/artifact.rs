use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ArtifactKey {
    cluster_id: String,
    backend: String,
    key: String,
}

#[derive(Default)]
struct ArtifactState {
    entries: HashMap<ArtifactKey, HashSet<String>>,
    by_node: HashMap<String, HashSet<ArtifactKey>>,
}

/// Bounded peer-artifact index. New keys fail closed at the configured limit
/// rather than allowing an untrusted report stream to grow memory without bound.
pub struct ArtifactIndex {
    state: RwLock<ArtifactState>,
    max_keys: usize,
    max_nodes_per_artifact: usize,
}

impl ArtifactIndex {
    pub fn new(max_keys: usize, max_nodes_per_artifact: usize) -> Result<Self, &'static str> {
        if max_keys == 0 {
            return Err("max_keys must be greater than zero");
        }
        if max_nodes_per_artifact == 0 {
            return Err("max_nodes_per_artifact must be greater than zero");
        }
        Ok(Self {
            state: RwLock::new(ArtifactState::default()),
            max_keys,
            max_nodes_per_artifact,
        })
    }

    pub fn record(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str) -> bool {
        let artifact = ArtifactKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut state = self.state.write();
        if !state.entries.contains_key(&artifact) && state.entries.len() == self.max_keys {
            return false;
        }
        let nodes = state.entries.entry(artifact.clone()).or_default();
        if !nodes.contains(node_id) && nodes.len() == self.max_nodes_per_artifact {
            return false;
        }
        nodes.insert(node_id.to_string());
        state
            .by_node
            .entry(node_id.to_string())
            .or_default()
            .insert(artifact);
        true
    }

    pub fn forget(&self, cluster_id: &str, backend: &str, key: &str, node_id: &str) {
        let artifact = ArtifactKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut state = self.state.write();
        if let Some(nodes) = state.entries.get_mut(&artifact) {
            nodes.remove(node_id);
            if nodes.is_empty() {
                state.entries.remove(&artifact);
            }
        }
        if let Some(keys) = state.by_node.get_mut(node_id) {
            keys.remove(&artifact);
            if keys.is_empty() {
                state.by_node.remove(node_id);
            }
        }
    }

    pub fn lookup(&self, cluster_id: &str, backend: &str, key: &str) -> Vec<String> {
        let artifact = ArtifactKey {
            cluster_id: cluster_id.to_string(),
            backend: backend.to_string(),
            key: key.to_string(),
        };
        let mut nodes = self
            .state
            .read()
            .entries
            .get(&artifact)
            .map(|nodes| nodes.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        nodes.sort_unstable();
        nodes
    }

    pub fn forget_node(&self, node_id: &str) {
        let mut state = self.state.write();
        let Some(keys) = state.by_node.remove(node_id) else {
            return;
        };
        for key in keys {
            if let Some(nodes) = state.entries.get_mut(&key) {
                nodes.remove(node_id);
                if nodes.is_empty() {
                    state.entries.remove(&key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_is_bounded_and_reverse_cleanup_is_consistent() {
        let index = ArtifactIndex::new(1, 2).unwrap();
        assert!(index.record("cluster", "iroh", "digest", "node-a"));
        assert!(index.record("cluster", "iroh", "digest", "node-b"));
        assert!(!index.record("cluster", "iroh", "digest", "node-c"));
        assert!(!index.record("cluster", "iroh", "other", "node-a"));

        index.forget_node("node-a");
        assert_eq!(index.lookup("cluster", "iroh", "digest"), vec!["node-b"]);
        index.forget("cluster", "iroh", "digest", "node-b");
        assert!(index.lookup("cluster", "iroh", "digest").is_empty());
    }
}
