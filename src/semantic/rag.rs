//! Graph RAG (Retrieval-Augmented Generation) implementation
//!
//! Enables natural language queries against the graph database.

use std::collections::HashMap;

use crate::graph_utils::graph::GraphDatabase;
use super::llm_client::{LlmClient, LlmError, ChatMessage};

/// System prompt for natural language to Cypher translation
const NL_TO_CYPHER_PROMPT: &str = r#"You are a Cypher query translator. Convert natural language questions into valid Cypher queries.

Available node labels in this graph: {labels}
Available relationship types: {rel_types}

Rules:
- Output ONLY the Cypher query, no explanations
- Use MATCH for reading data
- Use appropriate WHERE clauses for filtering
- Use RETURN to specify what to return
- For counting, use count()
- For finding paths, use variable-length patterns like -[:REL*1..3]->

Examples:
- "Find all people" -> MATCH (p:Person) RETURN p
- "Who works at Google?" -> MATCH (p:Person)-[:WORKS_AT]->(c:Company {name: "Google"}) RETURN p
- "How many companies are there?" -> MATCH (c:Company) RETURN count(c)
- "Find connections between Alice and Bob" -> MATCH path = (a:Person {name: "Alice"})-[*1..4]-(b:Person {name: "Bob"}) RETURN path"#;

/// System prompt for Graph RAG question answering
const RAG_SYSTEM_PROMPT: &str = r#"You are a helpful assistant that answers questions based on graph data.

You will be given:
1. A question from the user
2. Relevant data from a graph database

Answer the question based ONLY on the provided graph data. If the data doesn't contain enough information to answer, say so.
Be concise and factual. Reference specific entities and relationships from the data when possible."#;

/// Convert a natural language question to a Cypher query
///
/// # Arguments
/// * `question` - The natural language question
/// * `db` - The graph database (used to extract schema information)
/// * `client` - The LLM client
///
/// # Returns
/// A Cypher query string
#[allow(dead_code)]
pub fn natural_language_to_cypher(
    question: &str,
    db: &GraphDatabase,
    client: &LlmClient,
) -> Result<String, LlmError> {
    if !client.is_available() {
        return Err(LlmError::Disabled);
    }
    
    // Extract schema information from the database
    let labels = extract_node_labels(db);
    let rel_types = extract_relationship_types(db);
    
    // Build the system prompt with schema info
    let system_prompt = NL_TO_CYPHER_PROMPT
        .replace("{labels}", &labels.join(", "))
        .replace("{rel_types}", &rel_types.join(", "));
    
    let messages = vec![
        ChatMessage::system(system_prompt),
        ChatMessage::user(question),
    ];
    
    let response = client.complete(&messages)?;
    
    // Clean up the response (remove markdown code blocks if present)
    Ok(clean_cypher_response(&response))
}

/// Perform a Graph RAG query - answer a question using graph data
///
/// # Arguments
/// * `question` - The natural language question
/// * `db` - The graph database
/// * `client` - The LLM client
///
/// # Returns
/// A natural language answer based on the graph data
#[allow(dead_code)]
pub fn graph_rag_query(
    question: &str,
    db: &GraphDatabase,
    client: &LlmClient,
) -> Result<String, LlmError> {
    if !client.is_available() {
        return Err(LlmError::Disabled);
    }
    
    // Step 1: Convert question to Cypher
    let cypher = natural_language_to_cypher(question, db, client)?;
    
    // Step 2: Execute the query (simplified - in real implementation would use query_interface)
    let context = execute_and_format_results(db, &cypher);
    
    // Step 3: Generate answer using the context
    let messages = vec![
        ChatMessage::system(RAG_SYSTEM_PROMPT),
        ChatMessage::user(format!(
            "Question: {}\n\nGraph Data:\n{}\n\nPlease answer the question based on this data.",
            question, context
        )),
    ];
    
    client.complete(&messages)
}

/// Extract unique node labels from the database
fn extract_node_labels(db: &GraphDatabase) -> Vec<String> {
    let mut labels: Vec<String> = db.label_index.keys().cloned().collect();
    labels.sort();
    labels.dedup();
    labels
}

/// Extract unique relationship types from the database
fn extract_relationship_types(db: &GraphDatabase) -> Vec<String> {
    let mut types: Vec<String> = db.relationships.values()
        .map(|r| r.label.clone())
        .collect();
    types.sort();
    types.dedup();
    types
}

/// Clean up a Cypher response from the LLM
fn clean_cypher_response(response: &str) -> String {
    let mut result = response.trim().to_string();
    
    // Remove markdown code blocks
    if result.starts_with("```cypher") {
        result = result.strip_prefix("```cypher").unwrap_or(&result).to_string();
    } else if result.starts_with("```") {
        result = result.strip_prefix("```").unwrap_or(&result).to_string();
    }
    
    if result.ends_with("```") {
        result = result.strip_suffix("```").unwrap_or(&result).to_string();
    }
    
    result.trim().to_string()
}

/// Execute a Cypher query and format results as text context
/// 
/// This is a simplified implementation that provides basic context.
/// In a full implementation, this would use the query_interface module.
fn execute_and_format_results(db: &GraphDatabase, _cypher: &str) -> String {
    // For now, provide a summary of the graph as context
    // A full implementation would actually execute the Cypher query
    let mut context = String::new();
    
    // Summarize nodes by label
    context.push_str("Nodes in the graph:\n");
    for (label, node_ids) in &db.label_index {
        context.push_str(&format!("- {} ({} nodes)\n", label, node_ids.len()));
        
        // Show first few nodes of each type
        for (i, node_id) in node_ids.iter().take(5).enumerate() {
            if let Some(node) = db.nodes.get(node_id) {
                let meta_str = node.metadata.iter()
                    .take(3)
                    .map(|(k, v)| format!("{}: {}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                context.push_str(&format!("  {}. {} ({})\n", i + 1, node.label, meta_str));
            }
        }
    }
    
    // Summarize relationships
    context.push_str("\nRelationships:\n");
    let mut rel_counts: HashMap<String, usize> = HashMap::new();
    for rel in db.relationships.values() {
        *rel_counts.entry(rel.label.clone()).or_insert(0) += 1;
    }
    for (label, count) in rel_counts {
        context.push_str(&format!("- {} ({} relationships)\n", label, count));
    }
    
    context
}

/// Generate Cypher query suggestions based on the graph schema
#[allow(dead_code)]
pub fn suggest_queries(db: &GraphDatabase) -> Vec<String> {
    let mut suggestions = Vec::new();
    let labels = extract_node_labels(db);
    let rel_types = extract_relationship_types(db);
    
    // Basic node queries
    for label in &labels {
        suggestions.push(format!("MATCH (n:{}) RETURN n LIMIT 10", label));
        suggestions.push(format!("MATCH (n:{}) RETURN count(n)", label));
    }
    
    // Relationship queries
    for rel_type in &rel_types {
        suggestions.push(format!("MATCH ()-[r:{}]->() RETURN r LIMIT 10", rel_type));
    }
    
    // Path queries if we have multiple labels
    if labels.len() >= 2 {
        suggestions.push(format!(
            "MATCH (a:{})-[*1..3]-(b:{}) RETURN a, b LIMIT 10",
            labels[0], labels[1]
        ));
    }
    
    suggestions
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_labels() {
        let mut db = GraphDatabase::new();
        db.add_node("Person".to_string(), HashMap::new());
        db.add_node("Company".to_string(), HashMap::new());
        db.add_node("Person".to_string(), HashMap::new());
        
        let labels = extract_node_labels(&db);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"Company".to_string()));
    }
    
    #[test]
    fn test_clean_cypher_response() {
        let response = "```cypher\nMATCH (n) RETURN n\n```";
        let cleaned = clean_cypher_response(response);
        assert_eq!(cleaned, "MATCH (n) RETURN n");
        
        let plain = "MATCH (n) RETURN n";
        assert_eq!(clean_cypher_response(plain), plain);
    }
    
    #[test]
    fn test_suggest_queries() {
        let mut db = GraphDatabase::new();
        db.add_node("Person".to_string(), HashMap::new());
        db.add_node("Company".to_string(), HashMap::new());
        
        let suggestions = suggest_queries(&db);
        assert!(!suggestions.is_empty());
        assert!(suggestions.iter().any(|s| s.contains("Person")));
    }
}
