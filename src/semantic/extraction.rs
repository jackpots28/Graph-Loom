//! Entity and relationship extraction from text
//!
//! Uses LLM to parse unstructured text and extract graph entities.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::llm_client::{LlmClient, LlmError, ChatMessage};

/// An extracted entity (potential node)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedEntity {
    /// Suggested label for the node (e.g., "Person", "Company", "Event")
    pub label: String,
    /// Primary name or identifier
    pub name: String,
    /// Additional metadata extracted
    pub metadata: HashMap<String, String>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
}

/// An extracted relationship between entities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractedRelationship {
    /// Name of the source entity
    pub from_name: String,
    /// Name of the target entity
    pub to_name: String,
    /// Relationship type/label (e.g., "WORKS_AT", "FOUNDED", "KNOWS")
    pub label: String,
    /// Additional metadata for the relationship
    pub metadata: HashMap<String, String>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
}

/// Result of entity extraction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionResult {
    /// Extracted entities
    pub entities: Vec<ExtractedEntity>,
    /// Extracted relationships
    pub relationships: Vec<ExtractedRelationship>,
    /// Raw LLM response (for debugging)
    pub raw_response: Option<String>,
}

impl Default for ExtractionResult {
    fn default() -> Self {
        Self {
            entities: Vec::new(),
            relationships: Vec::new(),
            raw_response: None,
        }
    }
}

/// System prompt for entity extraction
const EXTRACTION_SYSTEM_PROMPT: &str = r#"You are an entity extraction assistant. Given text, extract entities and relationships in JSON format.

Output format:
{
  "entities": [
    {"label": "Person", "name": "John Smith", "metadata": {"role": "CEO"}, "confidence": 0.95}
  ],
  "relationships": [
    {"from_name": "John Smith", "to_name": "Acme Corp", "label": "WORKS_AT", "metadata": {"since": "2020"}, "confidence": 0.9}
  ]
}

Rules:
- Use PascalCase for entity labels (Person, Company, Event, Location, Product, etc.)
- Use SCREAMING_SNAKE_CASE for relationship labels (WORKS_AT, FOUNDED, KNOWS, LOCATED_IN, etc.)
- Extract all meaningful entities and their relationships
- Include confidence scores based on how explicit the information is in the text
- Only output valid JSON, no explanations"#;

/// Extract entities and relationships from text using an LLM
///
/// # Arguments
/// * `text` - The text to extract entities from
/// * `client` - The LLM client to use
///
/// # Returns
/// An `ExtractionResult` containing extracted entities and relationships
#[allow(dead_code)]
pub fn extract_entities(text: &str, client: &LlmClient) -> Result<ExtractionResult, LlmError> {
    if !client.is_available() {
        return Err(LlmError::Disabled);
    }
    
    let messages = vec![
        ChatMessage::system(EXTRACTION_SYSTEM_PROMPT),
        ChatMessage::user(format!("Extract entities and relationships from the following text:\n\n{}", text)),
    ];
    
    let response = client.complete(&messages)?;
    
    // Parse the JSON response
    parse_extraction_response(&response)
}

/// Parse the LLM's JSON response into an ExtractionResult
fn parse_extraction_response(response: &str) -> Result<ExtractionResult, LlmError> {
    // Try to find JSON in the response (LLM might include extra text)
    let json_str = extract_json_from_response(response);
    
    match serde_json::from_str::<ExtractionResult>(&json_str) {
        Ok(mut result) => {
            result.raw_response = Some(response.to_string());
            Ok(result)
        }
        Err(e) => Err(LlmError::InvalidResponse(format!("Failed to parse extraction result: {}", e))),
    }
}

/// Extract JSON object from a response that might contain extra text
fn extract_json_from_response(response: &str) -> String {
    // Find the first '{' and last '}'
    if let (Some(start), Some(end)) = (response.find('{'), response.rfind('}')) {
        if start < end {
            return response[start..=end].to_string();
        }
    }
    response.to_string()
}

/// Extract entities using simple heuristics (no LLM required)
/// 
/// This is a fallback method that uses basic NLP patterns to extract entities.
/// Less accurate than LLM-based extraction but works offline.
#[allow(dead_code)]
pub fn extract_entities_simple(text: &str) -> ExtractionResult {
    let mut entities = Vec::new();
    let mut seen_names = std::collections::HashSet::new();
    
    // Simple heuristic: look for capitalized words that might be names
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut i = 0;
    
    while i < words.len() {
        let word = words[i].trim_matches(|c: char| !c.is_alphanumeric());
        
        // Check if word starts with capital letter (potential proper noun)
        if !word.is_empty() && word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
            // Try to capture multi-word names (e.g., "John Smith")
            let mut name_parts = vec![word.to_string()];
            let mut j = i + 1;
            
            while j < words.len() {
                let next_word = words[j].trim_matches(|c: char| !c.is_alphanumeric());
                if !next_word.is_empty() && next_word.chars().next().map(|c| c.is_uppercase()).unwrap_or(false) {
                    name_parts.push(next_word.to_string());
                    j += 1;
                } else {
                    break;
                }
            }
            
            let name = name_parts.join(" ");
            
            // Skip common words and short names
            if name.len() > 2 && !is_common_word(&name) && !seen_names.contains(&name) {
                seen_names.insert(name.clone());
                
                // Guess the label based on context
                let label = guess_entity_label(&name, text);
                
                entities.push(ExtractedEntity {
                    label,
                    name,
                    metadata: HashMap::new(),
                    confidence: 0.5, // Lower confidence for heuristic extraction
                });
            }
            
            i = j;
        } else {
            i += 1;
        }
    }
    
    ExtractionResult {
        entities,
        relationships: Vec::new(), // Simple extraction doesn't detect relationships
        raw_response: None,
    }
}

/// Check if a word is a common word that shouldn't be extracted as an entity
fn is_common_word(word: &str) -> bool {
    const COMMON_WORDS: &[&str] = &[
        "The", "This", "That", "These", "Those", "It", "They", "We", "You", "I",
        "He", "She", "His", "Her", "Their", "Our", "Your", "My",
        "And", "But", "Or", "If", "When", "Where", "What", "Who", "How", "Why",
        "Is", "Are", "Was", "Were", "Be", "Been", "Being",
        "Have", "Has", "Had", "Do", "Does", "Did",
        "Will", "Would", "Could", "Should", "May", "Might", "Must",
        "In", "On", "At", "To", "For", "With", "By", "From", "About",
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December",
        "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    ];
    COMMON_WORDS.iter().any(|&w| w.eq_ignore_ascii_case(word))
}

/// Guess the entity label based on the name and surrounding context
fn guess_entity_label(name: &str, context: &str) -> String {
    let context_lower = context.to_lowercase();
    let name_lower = name.to_lowercase();
    
    // Check for company indicators
    if name.ends_with("Inc") || name.ends_with("Corp") || name.ends_with("LLC") 
        || name.ends_with("Ltd") || name.ends_with("Co") {
        return "Company".to_string();
    }
    
    // Check context for clues
    let company_words = ["company", "corporation", "business", "firm", "enterprise"];
    let person_words = ["mr.", "mrs.", "ms.", "dr.", "professor", "ceo", "founder", "employee"];
    let location_words = ["city", "country", "state", "located", "based in", "headquarters"];
    
    // Look for the name in context with surrounding words
    if let Some(pos) = context_lower.find(&name_lower) {
        let start = pos.saturating_sub(50);
        let end = (pos + name.len() + 50).min(context.len());
        let surrounding = &context_lower[start..end];
        
        for word in company_words {
            if surrounding.contains(word) {
                return "Company".to_string();
            }
        }
        
        for word in person_words {
            if surrounding.contains(word) {
                return "Person".to_string();
            }
        }
        
        for word in location_words {
            if surrounding.contains(word) {
                return "Location".to_string();
            }
        }
    }
    
    // Default to generic Entity
    "Entity".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_extraction() {
        let text = "John Smith is the CEO of Acme Corp. He founded the company in 2020.";
        let result = extract_entities_simple(text);
        
        assert!(!result.entities.is_empty());
        
        // Should find "John Smith" and "Acme Corp"
        let names: Vec<&str> = result.entities.iter().map(|e| e.name.as_str()).collect();
        assert!(names.iter().any(|n| n.contains("John")));
        assert!(names.iter().any(|n| n.contains("Acme")));
    }
    
    #[test]
    fn test_common_word_filter() {
        assert!(is_common_word("The"));
        assert!(is_common_word("January"));
        assert!(!is_common_word("Google"));
        assert!(!is_common_word("Microsoft"));
    }
    
    #[test]
    fn test_json_extraction() {
        let response = r#"Here is the result:
        {"entities": [], "relationships": []}
        That's all."#;
        
        let json = extract_json_from_response(response);
        assert!(json.starts_with('{'));
        assert!(json.ends_with('}'));
    }
}
