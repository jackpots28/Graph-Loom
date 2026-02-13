//! Temporal graph utilities for time-travel queries and timeline analysis.

use crate::graph_utils::graph::{GraphDatabase, Node, NodeId, Relationship};
use uuid::Uuid;

/// Event types for timeline tracking
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum EventType {
    NodeCreated,
    NodeUpdated,
    RelationshipCreated,
    RelationshipUpdated,
}

/// A single event in the graph's timeline
#[derive(Debug, Clone)]
pub struct TimelineEvent {
    pub timestamp: i64,
    pub event_type: EventType,
    pub entity_id: String,
    pub label: String,
}

/// Get a filtered view of the graph as it existed at a specific point in time.
/// Returns nodes and relationships that were created at or before the given timestamp.
pub fn graph_at_time(db: &GraphDatabase, timestamp: i64) -> (Vec<Node>, Vec<Relationship>) {
    let nodes: Vec<Node> = db.nodes.values()
        .filter(|n| n.created_at <= timestamp)
        .cloned()
        .collect();
    
    let node_ids: std::collections::HashSet<NodeId> = nodes.iter().map(|n| n.id).collect();
    
    let relationships: Vec<Relationship> = db.relationships.values()
        .filter(|r| {
            r.created_at <= timestamp 
                && node_ids.contains(&r.from_node) 
                && node_ids.contains(&r.to_node)
        })
        .cloned()
        .collect();
    
    (nodes, relationships)
}

/// Get nodes created within a specific time range (inclusive).
pub fn nodes_in_range(db: &GraphDatabase, from: i64, to: i64) -> Vec<NodeId> {
    db.nodes.values()
        .filter(|n| n.created_at >= from && n.created_at <= to)
        .map(|n| n.id)
        .collect()
}

/// Get relationships created within a specific time range (inclusive).
#[allow(dead_code)]
pub fn relationships_in_range(db: &GraphDatabase, from: i64, to: i64) -> Vec<Uuid> {
    db.relationships.values()
        .filter(|r| r.created_at >= from && r.created_at <= to)
        .map(|r| r.id)
        .collect()
}

/// Get a timeline of all creation events, sorted by timestamp (oldest first).
pub fn get_timeline(db: &GraphDatabase) -> Vec<TimelineEvent> {
    let mut events: Vec<TimelineEvent> = Vec::new();
    
    // Add node creation events
    for node in db.nodes.values() {
        events.push(TimelineEvent {
            timestamp: node.created_at,
            event_type: EventType::NodeCreated,
            entity_id: node.id.to_string(),
            label: node.label.clone(),
        });
    }
    
    // Add relationship creation events
    for rel in db.relationships.values() {
        events.push(TimelineEvent {
            timestamp: rel.created_at,
            event_type: EventType::RelationshipCreated,
            entity_id: rel.id.to_string(),
            label: rel.label.clone(),
        });
    }
    
    // Sort by timestamp (oldest first)
    events.sort_by_key(|e| e.timestamp);
    events
}

/// Get the timestamp range of the graph (min_created_at, max_created_at).
/// Returns None if the graph is empty.
pub fn get_timestamp_range(db: &GraphDatabase) -> Option<(i64, i64)> {
    let node_times: Vec<i64> = db.nodes.values().map(|n| n.created_at).collect();
    let rel_times: Vec<i64> = db.relationships.values().map(|r| r.created_at).collect();
    
    let all_times: Vec<i64> = node_times.into_iter().chain(rel_times).collect();
    
    if all_times.is_empty() {
        return None;
    }
    
    let min = *all_times.iter().min().unwrap();
    let max = *all_times.iter().max().unwrap();
    Some((min, max))
}

/// Count nodes and relationships at a given timestamp.
#[allow(dead_code)]
pub fn count_at_time(db: &GraphDatabase, timestamp: i64) -> (usize, usize) {
    let node_count = db.nodes.values().filter(|n| n.created_at <= timestamp).count();
    let rel_count = db.relationships.values().filter(|r| r.created_at <= timestamp).count();
    (node_count, rel_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_graph_at_time() {
        let mut db = GraphDatabase::new();
        
        // Create nodes with specific timestamps
        let id1 = db.add_node("Person".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id1) {
            n.created_at = 1000;
        }
        
        let id2 = db.add_node("Company".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id2) {
            n.created_at = 2000;
        }
        
        // At time 1500, only first node should exist
        let (nodes, _) = graph_at_time(&db, 1500);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].label, "Person");
        
        // At time 2500, both nodes should exist
        let (nodes, _) = graph_at_time(&db, 2500);
        assert_eq!(nodes.len(), 2);
    }

    #[test]
    fn test_nodes_in_range() {
        let mut db = GraphDatabase::new();
        
        let id1 = db.add_node("A".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id1) { n.created_at = 1000; }
        
        let id2 = db.add_node("B".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id2) { n.created_at = 2000; }
        
        let id3 = db.add_node("C".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id3) { n.created_at = 3000; }
        
        let in_range = nodes_in_range(&db, 1500, 2500);
        assert_eq!(in_range.len(), 1);
        assert_eq!(in_range[0], id2);
    }

    #[test]
    fn test_get_timeline() {
        let mut db = GraphDatabase::new();
        
        let id1 = db.add_node("First".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id1) { n.created_at = 2000; }
        
        let id2 = db.add_node("Second".to_string(), HashMap::new());
        if let Some(n) = db.nodes.get_mut(&id2) { n.created_at = 1000; }
        
        let timeline = get_timeline(&db);
        assert_eq!(timeline.len(), 2);
        // Should be sorted oldest first
        assert_eq!(timeline[0].label, "Second");
        assert_eq!(timeline[1].label, "First");
    }
}
