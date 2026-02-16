use std::collections::HashMap;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;

// Basic type aliases for clarity
pub type NodeId = Uuid;
type Key = String;
type Value = String;

/// Returns current Unix timestamp in milliseconds
pub fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Default timestamp for serde deserialization of legacy data
fn default_timestamp() -> i64 {
    0
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub label: String,
    pub metadata: HashMap<Key, Value>,
    pub out_rels: Vec<Uuid>,
    pub in_rels: Vec<Uuid>,
    /// Unix timestamp (milliseconds) when the node was created
    #[serde(default = "default_timestamp")]
    pub created_at: i64,
    /// Unix timestamp (milliseconds) when the node was last updated
    #[serde(default = "default_timestamp")]
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Relationship {
    pub id: Uuid,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub label: String,
    pub metadata: HashMap<Key, Value>,
    /// Unix timestamp (milliseconds) when the relationship was created
    #[serde(default = "default_timestamp")]
    pub created_at: i64,
    /// Unix timestamp (milliseconds) when the relationship was last updated
    #[serde(default = "default_timestamp")]
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphDatabase {
    pub nodes: HashMap<NodeId, Node>,
    pub relationships: HashMap<Uuid, Relationship>,
    #[serde(skip)]
    pub label_index: HashMap<String, Vec<NodeId>>,
}

impl GraphDatabase {
    // Instantiate a new, empty graph database
    pub fn new() -> Self {
        GraphDatabase {
            nodes: HashMap::new(),
            relationships: HashMap::new(),
            label_index: HashMap::new(),
        }
    }

    // Add a node and return its new ID
    pub fn add_node(&mut self, label: String, metadata: HashMap<Key, Value>) -> NodeId {
        let id = Uuid::now_v7();
        let now = current_timestamp_ms();
        let node = Node {
            id,
            label: label.clone(),
            metadata,
            out_rels: Vec::new(),
            in_rels: Vec::new(),
            created_at: now,
            updated_at: now,
        };
        self.nodes.insert(id, node);
        self.label_index.entry(label).or_default().push(id);
        id
    }

    // Add a relationship if both ends exist; returns the relationship ID
    pub fn add_relationship(
        &mut self,
        from_node: NodeId,
        to_node: NodeId,
        label: String,
        metadata: HashMap<Key, Value>,
    ) -> Option<Uuid> {
        if self.nodes.contains_key(&from_node) && self.nodes.contains_key(&to_node) {
            let id = Uuid::now_v7();
            let now = current_timestamp_ms();
            let relationship = Relationship { id, from_node, to_node, label, metadata, created_at: now, updated_at: now };
            self.relationships.insert(id, relationship);

            // Update adjacency lists
            if let Some(from) = self.nodes.get_mut(&from_node) {
                from.out_rels.push(id);
            }
            if let Some(to) = self.nodes.get_mut(&to_node) {
                to.in_rels.push(id);
            }

            Some(id)
        } else {
            None
        }
    }

    pub fn update_node_label(&mut self, id: NodeId, new_label: String) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            let old_label = node.label.clone();
            if old_label == new_label {
                return true;
            }

            // Remove from old label index
            if let Some(vec) = self.label_index.get_mut(&old_label) {
                vec.retain(|&x| x != id);
            }

            node.label = new_label.clone();
            self.label_index.entry(new_label).or_default().push(id);
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn set_node_metadata(&mut self, id: NodeId, new_metadata: HashMap<Key, Value>) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.metadata = new_metadata;
            true
        } else {
            false
        }
    }

    pub fn upsert_node_metadata(&mut self, id: NodeId, key: String, value: String) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.metadata.insert(key, value);
            true
        } else {
            false
        }
    }

    pub fn remove_node_metadata_key(&mut self, id: NodeId, key: &str) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.metadata.remove(key).is_some()
        } else {
            false
        }
    }

    pub fn update_relationship_label(&mut self, id: Uuid, new_label: String) -> bool {
        if let Some(rel) = self.relationships.get_mut(&id) {
            rel.label = new_label;
            true
        } else {
            false
        }
    }

    #[allow(dead_code)]
    pub fn set_relationship_metadata(&mut self, id: Uuid, new_metadata: HashMap<Key, Value>) -> bool {
        if let Some(rel) = self.relationships.get_mut(&id) {
            rel.metadata = new_metadata;
            true
        } else {
            false
        }
    }

    pub fn upsert_relationship_metadata(&mut self, id: Uuid, key: String, value: String) -> bool {
        if let Some(rel) = self.relationships.get_mut(&id) {
            rel.metadata.insert(key, value);
            true
        } else {
            false
        }
    }

    pub fn remove_relationship_metadata_key(&mut self, id: Uuid, key: &str) -> bool {
        if let Some(rel) = self.relationships.get_mut(&id) {
            rel.metadata.remove(key).is_some()
        } else {
            false
        }
    }

    // Delete operations
    pub fn remove_relationship(&mut self, id: Uuid) -> bool {
        if let Some(rel) = self.relationships.remove(&id) {
            // Remove from adjacency lists
            if let Some(from) = self.nodes.get_mut(&rel.from_node) {
                from.out_rels.retain(|&x| x != id);
            }
            if let Some(to) = self.nodes.get_mut(&rel.to_node) {
                to.in_rels.retain(|&x| x != id);
            }
            true
        } else {
            false
        }
    }

    pub fn remove_node(&mut self, id: NodeId) -> bool {
        if let Some(node) = self.nodes.remove(&id) {
            // Remove from label index
            if let Some(vec) = self.label_index.get_mut(&node.label) {
                vec.retain(|&x| x != id);
            }

            // Cascade delete relationships involving this node
            let to_remove: Vec<Uuid> = node.out_rels.iter().chain(node.in_rels.iter()).copied().collect();
            for rid in to_remove {
                self.remove_relationship(rid);
            }
            true
        } else {
            false
        }
    }

    pub fn get_node(&self, id: NodeId) -> Option<&Node> { self.nodes.get(&id) }
    pub fn get_relationship(&self, id: Uuid) -> Option<&Relationship> { self.relationships.get(&id) }
    #[allow(dead_code)]
    pub fn node_count(&self) -> usize { self.nodes.len() }
    #[allow(dead_code)]
    pub fn relationship_count(&self) -> usize { self.relationships.len() }

    /// Rebuilds the label index from the current nodes. Useful after deserialization.
    pub fn rebuild_indices(&mut self) {
        self.label_index.clear();
        for (id, node) in &self.nodes {
            self.label_index.entry(node.label.clone()).or_default().push(*id);
        }
    }

    // Fetch helpers:
    // Nodes
    pub fn find_node_ids_by_label(&self, label: &str) -> Vec<NodeId> {
        self.label_index.get(label).cloned().unwrap_or_default()
    }

    #[allow(dead_code)]
    pub fn find_node_ids_by_metadata_key(&self, key: &str) -> Vec<NodeId> {
        self
            .nodes
            .par_iter()
            .filter_map(|(&id, node)| if node.metadata.contains_key(key) { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_node_ids_by_metadata_kv(&self, key: &str, value: &str) -> Vec<NodeId> {
        self
            .nodes
            .par_iter()
            .filter_map(|(&id, node)| match node.metadata.get(key) {
                Some(v) if v == value => Some(id),
                _ => None,
            })
            .collect()
    }

    // Relationships
    pub fn find_relationship_ids_by_label(&self, label: &str) -> Vec<Uuid> {
        self
            .relationships
            .par_iter()
            .filter_map(|(&id, rel)| if rel.label == label { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_relationship_ids_by_metadata_key(&self, key: &str) -> Vec<Uuid> {
        self
            .relationships
            .par_iter()
            .filter_map(|(&id, rel)| if rel.metadata.contains_key(key) { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_relationship_ids_by_metadata_kv(&self, key: &str, value: &str) -> Vec<Uuid> {
        self
            .relationships
            .par_iter()
            .filter_map(|(&id, rel)| match rel.metadata.get(key) {
                Some(v) if v == value => Some(id),
                _ => None,
            })
            .collect()
    }
}
