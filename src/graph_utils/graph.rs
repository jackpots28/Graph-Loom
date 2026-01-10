use std::collections::HashMap;
use uuid::Uuid;
use serde::{Serialize, Deserialize};

// Basic type aliases for clarity
pub type NodeId = Uuid;
type Key = String;
type Value = String;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub label: String,
    pub metadata: HashMap<Key, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Relationship {
    pub id: Uuid,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub label: String,
    pub metadata: HashMap<Key, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphDatabase {
    pub nodes: HashMap<NodeId, Node>,
    pub relationships: HashMap<Uuid, Relationship>,
}

impl GraphDatabase {
    // Instantiate a new, empty graph database
    pub fn new() -> Self {
        GraphDatabase {
            nodes: HashMap::new(),
            relationships: HashMap::new(),
        }
    }

    // Add a node and return its new ID
    pub fn add_node(&mut self, label: String, metadata: HashMap<Key, Value>) -> NodeId {
        let id = Uuid::now_v7();
        let node = Node { id, label, metadata };
        self.nodes.insert(id, node);
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
            let relationship = Relationship { id, from_node, to_node, label, metadata };
            self.relationships.insert(id, relationship);
            Some(id)
        } else {
            None
        }
    }

    pub fn update_node_label(&mut self, id: NodeId, new_label: String) -> bool {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.label = new_label;
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
        self.relationships.remove(&id).is_some()
    }

    pub fn remove_node(&mut self, id: NodeId) -> bool {
        if self.nodes.remove(&id).is_some() {
            // Cascade delete relationships involving this node
            let to_remove: Vec<Uuid> = self
                .relationships
                .iter()
                .filter_map(|(rid, rel)| {
                    if rel.from_node == id || rel.to_node == id { Some(*rid) } else { None }
                })
                .collect();
            for rid in to_remove {
                self.relationships.remove(&rid);
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

    // Fetch helpers:
    // Nodes
    pub fn find_node_ids_by_label(&self, label: &str) -> Vec<NodeId> {
        self
            .nodes
            .iter()
            .filter_map(|(&id, node)| if node.label == label { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_node_ids_by_metadata_key(&self, key: &str) -> Vec<NodeId> {
        self
            .nodes
            .iter()
            .filter_map(|(&id, node)| if node.metadata.contains_key(key) { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_node_ids_by_metadata_kv(&self, key: &str, value: &str) -> Vec<NodeId> {
        self
            .nodes
            .iter()
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
            .iter()
            .filter_map(|(&id, rel)| if rel.label == label { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_relationship_ids_by_metadata_key(&self, key: &str) -> Vec<Uuid> {
        self
            .relationships
            .iter()
            .filter_map(|(&id, rel)| if rel.metadata.contains_key(key) { Some(id) } else { None })
            .collect()
    }

    #[allow(dead_code)]
    pub fn find_relationship_ids_by_metadata_kv(&self, key: &str, value: &str) -> Vec<Uuid> {
        self
            .relationships
            .iter()
            .filter_map(|(&id, rel)| match rel.metadata.get(key) {
                Some(v) if v == value => Some(id),
                _ => None,
            })
            .collect()
    }
}
