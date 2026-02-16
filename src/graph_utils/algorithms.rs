//! Graph algorithms module for enterprise analytics
//!
//! Implements PageRank, Betweenness Centrality, and Pathfinding algorithms.

use std::collections::{HashMap, HashSet, VecDeque};
use crate::graph_utils::graph::{GraphDatabase, NodeId};

/// PageRank algorithm for identifying important nodes.
///
/// Uses the power iteration method with configurable damping factor and iterations.
///
/// # Arguments
/// * `db` - The graph database to analyze
/// * `damping` - Damping factor, typically 0.85
/// * `iterations` - Number of iterations, typically 20-100
///
/// # Returns
/// HashMap mapping each NodeId to its PageRank score (normalized to sum to 1.0)
pub fn pagerank(
    db: &GraphDatabase,
    damping: f64,
    iterations: usize,
) -> HashMap<NodeId, f64> {
    let node_ids: Vec<NodeId> = db.nodes.keys().copied().collect();
    let n = node_ids.len();
    
    if n == 0 {
        return HashMap::new();
    }
    
    // Initialize scores uniformly
    let initial_score = 1.0 / n as f64;
    let mut scores: HashMap<NodeId, f64> = node_ids.iter().map(|&id| (id, initial_score)).collect();
    
    // Build outgoing edge count map
    let out_degree: HashMap<NodeId, usize> = node_ids
        .iter()
        .map(|&id| {
            let degree = db.nodes.get(&id).map(|n| n.out_rels.len()).unwrap_or(0);
            (id, degree)
        })
        .collect();
    
    // Build incoming edges map (which nodes point to each node)
    let mut incoming: HashMap<NodeId, Vec<NodeId>> = node_ids.iter().map(|&id| (id, Vec::new())).collect();
    for rel in db.relationships.values() {
        if let Some(vec) = incoming.get_mut(&rel.to_node) {
            vec.push(rel.from_node);
        }
    }
    
    let teleport = (1.0 - damping) / n as f64;
    
    for _ in 0..iterations {
        let mut new_scores: HashMap<NodeId, f64> = HashMap::with_capacity(n);
        
        // Calculate dangling node contribution (nodes with no outgoing edges)
        let dangling_sum: f64 = node_ids
            .iter()
            .filter(|id| out_degree.get(id).copied().unwrap_or(0) == 0)
            .map(|id| scores.get(id).copied().unwrap_or(0.0))
            .sum();
        let dangling_contrib = damping * dangling_sum / n as f64;
        
        for &node_id in &node_ids {
            let mut score = teleport + dangling_contrib;
            
            // Sum contributions from incoming edges
            if let Some(in_nodes) = incoming.get(&node_id) {
                for &from_id in in_nodes {
                    let from_score = scores.get(&from_id).copied().unwrap_or(0.0);
                    let from_out = out_degree.get(&from_id).copied().unwrap_or(1).max(1);
                    score += damping * from_score / from_out as f64;
                }
            }
            
            new_scores.insert(node_id, score);
        }
        
        scores = new_scores;
    }
    
    scores
}

/// Betweenness Centrality - identifies bridge nodes that connect different parts of the graph.
///
/// Uses Brandes' algorithm for efficient computation.
///
/// # Arguments
/// * `db` - The graph database to analyze
///
/// # Returns
/// HashMap mapping each NodeId to its betweenness centrality score
pub fn betweenness_centrality(db: &GraphDatabase) -> HashMap<NodeId, f64> {
    let node_ids: Vec<NodeId> = db.nodes.keys().copied().collect();
    let n = node_ids.len();
    
    if n == 0 {
        return HashMap::new();
    }
    
    // Initialize centrality scores
    let mut centrality: HashMap<NodeId, f64> = node_ids.iter().map(|&id| (id, 0.0)).collect();
    
    // Build adjacency list for faster traversal
    let mut adjacency: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for &id in &node_ids {
        adjacency.insert(id, Vec::new());
    }
    for rel in db.relationships.values() {
        if let Some(vec) = adjacency.get_mut(&rel.from_node) {
            vec.push(rel.to_node);
        }
    }
    
    // Brandes' algorithm
    for &source in &node_ids {
        // Single-source shortest paths
        let mut stack: Vec<NodeId> = Vec::new();
        let mut predecessors: HashMap<NodeId, Vec<NodeId>> = node_ids.iter().map(|&id| (id, Vec::new())).collect();
        let mut sigma: HashMap<NodeId, f64> = node_ids.iter().map(|&id| (id, 0.0)).collect();
        let mut dist: HashMap<NodeId, i64> = node_ids.iter().map(|&id| (id, -1)).collect();
        
        sigma.insert(source, 1.0);
        dist.insert(source, 0);
        
        let mut queue: VecDeque<NodeId> = VecDeque::new();
        queue.push_back(source);
        
        while let Some(v) = queue.pop_front() {
            stack.push(v);
            let v_dist = dist.get(&v).copied().unwrap_or(-1);
            
            if let Some(neighbors) = adjacency.get(&v) {
                for &w in neighbors {
                    let w_dist = dist.get(&w).copied().unwrap_or(-1);
                    
                    // First visit?
                    if w_dist < 0 {
                        queue.push_back(w);
                        dist.insert(w, v_dist + 1);
                    }
                    
                    // Shortest path to w via v?
                    if dist.get(&w).copied().unwrap_or(-1) == v_dist + 1 {
                        let v_sigma = sigma.get(&v).copied().unwrap_or(0.0);
                        *sigma.entry(w).or_insert(0.0) += v_sigma;
                        predecessors.entry(w).or_default().push(v);
                    }
                }
            }
        }
        
        // Accumulation
        let mut delta: HashMap<NodeId, f64> = node_ids.iter().map(|&id| (id, 0.0)).collect();
        
        while let Some(w) = stack.pop() {
            if let Some(preds) = predecessors.get(&w) {
                for &v in preds {
                    let v_sigma = sigma.get(&v).copied().unwrap_or(1.0);
                    let w_sigma = sigma.get(&w).copied().unwrap_or(1.0);
                    let w_delta = delta.get(&w).copied().unwrap_or(0.0);
                    
                    let contrib = (v_sigma / w_sigma) * (1.0 + w_delta);
                    *delta.entry(v).or_insert(0.0) += contrib;
                }
            }
            
            if w != source {
                let w_delta = delta.get(&w).copied().unwrap_or(0.0);
                *centrality.entry(w).or_insert(0.0) += w_delta;
            }
        }
    }
    
    // Normalize for undirected graphs (divide by 2) - skip for directed
    // For directed graphs, the scores are already correct
    
    centrality
}

/// Shortest path using BFS (unweighted edges).
///
/// # Arguments
/// * `db` - The graph database
/// * `from` - Starting node
/// * `to` - Target node
///
/// # Returns
/// Option containing the path as a vector of NodeIds, or None if no path exists
pub fn shortest_path(
    db: &GraphDatabase,
    from: NodeId,
    to: NodeId,
) -> Option<Vec<NodeId>> {
    if from == to {
        return Some(vec![from]);
    }
    
    if !db.nodes.contains_key(&from) || !db.nodes.contains_key(&to) {
        return None;
    }
    
    // Build adjacency list
    let mut adjacency: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for id in db.nodes.keys() {
        adjacency.insert(*id, Vec::new());
    }
    for rel in db.relationships.values() {
        if let Some(vec) = adjacency.get_mut(&rel.from_node) {
            vec.push(rel.to_node);
        }
    }
    
    // BFS
    let mut visited: HashSet<NodeId> = HashSet::new();
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
    let mut queue: VecDeque<NodeId> = VecDeque::new();
    
    visited.insert(from);
    queue.push_back(from);
    
    while let Some(current) = queue.pop_front() {
        if current == to {
            // Reconstruct path
            let mut path = vec![to];
            let mut node = to;
            while let Some(&p) = parent.get(&node) {
                path.push(p);
                node = p;
            }
            path.reverse();
            return Some(path);
        }
        
        if let Some(neighbors) = adjacency.get(&current) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    visited.insert(neighbor);
                    parent.insert(neighbor, current);
                    queue.push_back(neighbor);
                }
            }
        }
    }
    
    None
}

/// A* pathfinding with Euclidean distance heuristic using node positions.
///
/// # Arguments
/// * `db` - The graph database
/// * `positions` - HashMap of node positions (x, y coordinates)
/// * `from` - Starting node
/// * `to` - Target node
///
/// # Returns
/// Option containing the path as a vector of NodeIds, or None if no path exists
pub fn astar_path(
    db: &GraphDatabase,
    positions: &HashMap<NodeId, (f32, f32)>,
    from: NodeId,
    to: NodeId,
) -> Option<Vec<NodeId>> {
    if from == to {
        return Some(vec![from]);
    }
    
    if !db.nodes.contains_key(&from) || !db.nodes.contains_key(&to) {
        return None;
    }
    
    let goal_pos = positions.get(&to).copied().unwrap_or((0.0, 0.0));
    
    // Heuristic: Euclidean distance to goal
    let heuristic = |node: NodeId| -> f64 {
        let pos = positions.get(&node).copied().unwrap_or((0.0, 0.0));
        let dx = (goal_pos.0 - pos.0) as f64;
        let dy = (goal_pos.1 - pos.1) as f64;
        (dx * dx + dy * dy).sqrt()
    };
    
    // Build adjacency list
    let mut adjacency: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for id in db.nodes.keys() {
        adjacency.insert(*id, Vec::new());
    }
    for rel in db.relationships.values() {
        if let Some(vec) = adjacency.get_mut(&rel.from_node) {
            vec.push(rel.to_node);
        }
    }
    
    // A* using a simple priority queue (Vec sorted by f-score)
    let mut g_score: HashMap<NodeId, f64> = HashMap::new();
    let mut f_score: HashMap<NodeId, f64> = HashMap::new();
    let mut parent: HashMap<NodeId, NodeId> = HashMap::new();
    let mut open_set: Vec<NodeId> = vec![from];
    let mut closed_set: HashSet<NodeId> = HashSet::new();
    
    g_score.insert(from, 0.0);
    f_score.insert(from, heuristic(from));
    
    while !open_set.is_empty() {
        // Find node with lowest f_score
        open_set.sort_by(|a, b| {
            let fa = f_score.get(a).copied().unwrap_or(f64::MAX);
            let fb = f_score.get(b).copied().unwrap_or(f64::MAX);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        let current = open_set.remove(0);
        
        if current == to {
            // Reconstruct path
            let mut path = vec![to];
            let mut node = to;
            while let Some(&p) = parent.get(&node) {
                path.push(p);
                node = p;
            }
            path.reverse();
            return Some(path);
        }
        
        closed_set.insert(current);
        
        let current_g = g_score.get(&current).copied().unwrap_or(f64::MAX);
        
        if let Some(neighbors) = adjacency.get(&current) {
            for &neighbor in neighbors {
                if closed_set.contains(&neighbor) {
                    continue;
                }
                
                // Edge weight = 1 for unweighted graph
                let tentative_g = current_g + 1.0;
                
                let neighbor_g = g_score.get(&neighbor).copied().unwrap_or(f64::MAX);
                
                if tentative_g < neighbor_g {
                    parent.insert(neighbor, current);
                    g_score.insert(neighbor, tentative_g);
                    f_score.insert(neighbor, tentative_g + heuristic(neighbor));
                    
                    if !open_set.contains(&neighbor) {
                        open_set.push(neighbor);
                    }
                }
            }
        }
    }
    
    None
}

/// Find all paths between two nodes up to a maximum depth.
///
/// # Arguments
/// * `db` - The graph database
/// * `from` - Starting node
/// * `to` - Target node
/// * `max_depth` - Maximum path length to search
///
/// # Returns
/// Vector of all paths found, each path being a vector of NodeIds
pub fn all_paths(
    db: &GraphDatabase,
    from: NodeId,
    to: NodeId,
    max_depth: usize,
) -> Vec<Vec<NodeId>> {
    let mut results: Vec<Vec<NodeId>> = Vec::new();
    
    if !db.nodes.contains_key(&from) || !db.nodes.contains_key(&to) {
        return results;
    }
    
    if from == to {
        return vec![vec![from]];
    }
    
    // Build adjacency list
    let mut adjacency: HashMap<NodeId, Vec<NodeId>> = HashMap::new();
    for id in db.nodes.keys() {
        adjacency.insert(*id, Vec::new());
    }
    for rel in db.relationships.values() {
        if let Some(vec) = adjacency.get_mut(&rel.from_node) {
            vec.push(rel.to_node);
        }
    }
    
    // DFS with path tracking
    let mut stack: Vec<(NodeId, Vec<NodeId>, HashSet<NodeId>)> = Vec::new();
    let mut initial_visited = HashSet::new();
    initial_visited.insert(from);
    stack.push((from, vec![from], initial_visited));
    
    while let Some((current, path, visited)) = stack.pop() {
        if path.len() > max_depth {
            continue;
        }
        
        if let Some(neighbors) = adjacency.get(&current) {
            for &neighbor in neighbors {
                if neighbor == to {
                    let mut new_path = path.clone();
                    new_path.push(neighbor);
                    results.push(new_path);
                } else if !visited.contains(&neighbor) && path.len() < max_depth {
                    let mut new_path = path.clone();
                    new_path.push(neighbor);
                    let mut new_visited = visited.clone();
                    new_visited.insert(neighbor);
                    stack.push((neighbor, new_path, new_visited));
                }
            }
        }
    }
    
    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_graph() -> GraphDatabase {
        let mut db = GraphDatabase::new();
        
        // Create a simple graph:
        //   A -> B -> C
        //   |    |
        //   v    v
        //   D -> E
        
        let a = db.add_node("A".to_string(), HashMap::new());
        let b = db.add_node("B".to_string(), HashMap::new());
        let c = db.add_node("C".to_string(), HashMap::new());
        let d = db.add_node("D".to_string(), HashMap::new());
        let e = db.add_node("E".to_string(), HashMap::new());
        
        db.add_relationship(a, b, "CONNECTS".to_string(), HashMap::new());
        db.add_relationship(b, c, "CONNECTS".to_string(), HashMap::new());
        db.add_relationship(a, d, "CONNECTS".to_string(), HashMap::new());
        db.add_relationship(b, e, "CONNECTS".to_string(), HashMap::new());
        db.add_relationship(d, e, "CONNECTS".to_string(), HashMap::new());
        
        db
    }

    #[test]
    fn test_pagerank_basic() {
        let db = create_test_graph();
        let scores = pagerank(&db, 0.85, 20);
        
        // All nodes should have scores
        assert_eq!(scores.len(), 5);
        
        // Scores should sum to approximately 1.0
        let total: f64 = scores.values().sum();
        assert!((total - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_pagerank_empty_graph() {
        let db = GraphDatabase::new();
        let scores = pagerank(&db, 0.85, 20);
        assert!(scores.is_empty());
    }

    #[test]
    fn test_betweenness_centrality_basic() {
        let db = create_test_graph();
        let scores = betweenness_centrality(&db);
        
        // All nodes should have scores
        assert_eq!(scores.len(), 5);
        
        // B should have high centrality (it's a bridge node)
        let b_id = db.nodes.iter().find(|(_, n)| n.label == "B").map(|(id, _)| *id).unwrap();
        let b_score = scores.get(&b_id).copied().unwrap_or(0.0);
        
        // B connects A to C and A to E, so it should have positive centrality
        assert!(b_score > 0.0);
    }

    #[test]
    fn test_shortest_path_exists() {
        let db = create_test_graph();
        
        let a_id = db.nodes.iter().find(|(_, n)| n.label == "A").map(|(id, _)| *id).unwrap();
        let c_id = db.nodes.iter().find(|(_, n)| n.label == "C").map(|(id, _)| *id).unwrap();
        
        let path = shortest_path(&db, a_id, c_id);
        assert!(path.is_some());
        
        let path = path.unwrap();
        assert_eq!(path.len(), 3); // A -> B -> C
        assert_eq!(path[0], a_id);
        assert_eq!(path[2], c_id);
    }

    #[test]
    fn test_shortest_path_same_node() {
        let db = create_test_graph();
        let a_id = db.nodes.iter().find(|(_, n)| n.label == "A").map(|(id, _)| *id).unwrap();
        
        let path = shortest_path(&db, a_id, a_id);
        assert!(path.is_some());
        assert_eq!(path.unwrap(), vec![a_id]);
    }

    #[test]
    fn test_shortest_path_no_path() {
        let db = create_test_graph();
        
        // C has no outgoing edges, so there's no path from C to A
        let a_id = db.nodes.iter().find(|(_, n)| n.label == "A").map(|(id, _)| *id).unwrap();
        let c_id = db.nodes.iter().find(|(_, n)| n.label == "C").map(|(id, _)| *id).unwrap();
        
        let path = shortest_path(&db, c_id, a_id);
        assert!(path.is_none());
    }

    #[test]
    fn test_all_paths() {
        let db = create_test_graph();
        
        let a_id = db.nodes.iter().find(|(_, n)| n.label == "A").map(|(id, _)| *id).unwrap();
        let e_id = db.nodes.iter().find(|(_, n)| n.label == "E").map(|(id, _)| *id).unwrap();
        
        let paths = all_paths(&db, a_id, e_id, 5);
        
        // Should find two paths: A->B->E and A->D->E
        assert_eq!(paths.len(), 2);
        
        // All paths should start with A and end with E
        for path in &paths {
            assert_eq!(path[0], a_id);
            assert_eq!(path[path.len() - 1], e_id);
        }
    }

    #[test]
    fn test_astar_path() {
        let db = create_test_graph();
        
        let a_id = db.nodes.iter().find(|(_, n)| n.label == "A").map(|(id, _)| *id).unwrap();
        let c_id = db.nodes.iter().find(|(_, n)| n.label == "C").map(|(id, _)| *id).unwrap();
        
        // Create positions
        let mut positions: HashMap<NodeId, (f32, f32)> = HashMap::new();
        for (id, node) in &db.nodes {
            let x = match node.label.as_str() {
                "A" => 0.0,
                "B" => 1.0,
                "C" => 2.0,
                "D" => 0.0,
                "E" => 1.0,
                _ => 0.0,
            };
            let y = match node.label.as_str() {
                "A" => 0.0,
                "B" => 0.0,
                "C" => 0.0,
                "D" => 1.0,
                "E" => 1.0,
                _ => 0.0,
            };
            positions.insert(*id, (x, y));
        }
        
        let path = astar_path(&db, &positions, a_id, c_id);
        assert!(path.is_some());
        
        let path = path.unwrap();
        assert_eq!(path[0], a_id);
        assert_eq!(path[path.len() - 1], c_id);
    }
}
