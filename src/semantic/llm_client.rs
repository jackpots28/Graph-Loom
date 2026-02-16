//! LLM client abstraction for multiple providers
//!
//! Supports OpenAI, Anthropic, and Ollama (local) backends.

#![allow(dead_code)]

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// LLM provider selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LlmProvider {
    #[default]
    OpenAI,
    Anthropic,
    Ollama,
}

/// Configuration for LLM integration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    /// Whether LLM features are enabled
    pub enabled: bool,
    /// Which LLM provider to use
    pub provider: LlmProvider,
    /// API key for cloud providers (OpenAI, Anthropic)
    pub api_key: Option<String>,
    /// Model name (e.g., "gpt-4", "claude-3-sonnet", "llama2")
    pub model: String,
    /// Custom endpoint URL (for Ollama or proxies)
    pub endpoint: Option<String>,
    /// Maximum tokens for response
    pub max_tokens: u32,
    /// Temperature for generation (0.0 - 1.0)
    pub temperature: f32,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: LlmProvider::default(),
            api_key: None,
            model: "gpt-4".to_string(),
            endpoint: None,
            max_tokens: 1024,
            temperature: 0.7,
        }
    }
}

impl LlmConfig {
    /// Get the API endpoint URL for the configured provider
    pub fn get_endpoint(&self) -> String {
        if let Some(custom) = &self.endpoint {
            return custom.clone();
        }
        match self.provider {
            LlmProvider::OpenAI => "https://api.openai.com/v1/chat/completions".to_string(),
            LlmProvider::Anthropic => "https://api.anthropic.com/v1/messages".to_string(),
            LlmProvider::Ollama => "http://localhost:11434/api/generate".to_string(),
        }
    }
}

/// Error type for LLM operations
#[derive(Debug, Clone)]
pub enum LlmError {
    /// LLM features are disabled
    Disabled,
    /// Missing API key
    MissingApiKey,
    /// Network or HTTP error
    NetworkError(String),
    /// Invalid response from LLM
    InvalidResponse(String),
    /// Rate limit exceeded
    RateLimited,
    /// Model not found or unavailable
    ModelUnavailable(String),
}

impl std::fmt::Display for LlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmError::Disabled => write!(f, "LLM features are disabled"),
            LlmError::MissingApiKey => write!(f, "API key is required for this provider"),
            LlmError::NetworkError(e) => write!(f, "Network error: {}", e),
            LlmError::InvalidResponse(e) => write!(f, "Invalid LLM response: {}", e),
            LlmError::RateLimited => write!(f, "Rate limit exceeded"),
            LlmError::ModelUnavailable(m) => write!(f, "Model unavailable: {}", m),
        }
    }
}

impl std::error::Error for LlmError {}

/// Message role for chat completions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

/// A single message in a chat conversation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: String,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self { role: MessageRole::System, content: content.into() }
    }
    
    pub fn user(content: impl Into<String>) -> Self {
        Self { role: MessageRole::User, content: content.into() }
    }
    
    pub fn assistant(content: impl Into<String>) -> Self {
        Self { role: MessageRole::Assistant, content: content.into() }
    }
}

/// LLM client for making API calls
#[derive(Debug, Clone)]
pub struct LlmClient {
    config: LlmConfig,
}

impl LlmClient {
    /// Create a new LLM client with the given configuration
    pub fn new(config: LlmConfig) -> Self {
        Self { config }
    }
    
    /// Check if the client is properly configured and enabled
    pub fn is_available(&self) -> bool {
        if !self.config.enabled {
            return false;
        }
        // Ollama doesn't require an API key
        if self.config.provider == LlmProvider::Ollama {
            return true;
        }
        self.config.api_key.is_some()
    }
    
    /// Generate a completion for the given messages
    /// 
    /// This is a synchronous blocking call. For async usage, use `complete_async`.
    #[allow(dead_code)]
    pub fn complete(&self, messages: &[ChatMessage]) -> Result<String, LlmError> {
        if !self.config.enabled {
            return Err(LlmError::Disabled);
        }
        
        // For now, return a placeholder since we don't have reqwest as a required dependency
        // The actual HTTP calls would be made here when the `semantic` feature is enabled
        let _ = messages;
        Err(LlmError::NetworkError("HTTP client not available. Enable 'semantic' feature.".to_string()))
    }
    
    /// Build the request body for OpenAI-compatible APIs
    #[allow(dead_code)]
    fn build_openai_request(&self, messages: &[ChatMessage]) -> HashMap<String, serde_json::Value> {
        let mut body = HashMap::new();
        body.insert("model".to_string(), serde_json::json!(self.config.model));
        body.insert("messages".to_string(), serde_json::json!(messages));
        body.insert("max_tokens".to_string(), serde_json::json!(self.config.max_tokens));
        body.insert("temperature".to_string(), serde_json::json!(self.config.temperature));
        body
    }
    
    /// Build the request body for Anthropic API
    #[allow(dead_code)]
    fn build_anthropic_request(&self, messages: &[ChatMessage]) -> HashMap<String, serde_json::Value> {
        let mut body = HashMap::new();
        body.insert("model".to_string(), serde_json::json!(self.config.model));
        body.insert("max_tokens".to_string(), serde_json::json!(self.config.max_tokens));
        
        // Anthropic uses a different message format
        let system_msg = messages.iter()
            .find(|m| matches!(m.role, MessageRole::System))
            .map(|m| m.content.clone());
        
        if let Some(sys) = system_msg {
            body.insert("system".to_string(), serde_json::json!(sys));
        }
        
        let non_system: Vec<_> = messages.iter()
            .filter(|m| !matches!(m.role, MessageRole::System))
            .map(|m| {
                serde_json::json!({
                    "role": match m.role {
                        MessageRole::User => "user",
                        MessageRole::Assistant => "assistant",
                        MessageRole::System => "user", // shouldn't happen
                    },
                    "content": m.content
                })
            })
            .collect();
        
        body.insert("messages".to_string(), serde_json::json!(non_system));
        body
    }
    
    /// Build the request body for Ollama API
    #[allow(dead_code)]
    fn build_ollama_request(&self, messages: &[ChatMessage]) -> HashMap<String, serde_json::Value> {
        let mut body = HashMap::new();
        body.insert("model".to_string(), serde_json::json!(self.config.model));
        body.insert("stream".to_string(), serde_json::json!(false));
        
        // Combine messages into a single prompt for Ollama's generate endpoint
        let prompt = messages.iter()
            .map(|m| {
                let prefix = match m.role {
                    MessageRole::System => "System: ",
                    MessageRole::User => "User: ",
                    MessageRole::Assistant => "Assistant: ",
                };
                format!("{}{}", prefix, m.content)
            })
            .collect::<Vec<_>>()
            .join("\n\n");
        
        body.insert("prompt".to_string(), serde_json::json!(prompt));
        
        // Options
        let options = serde_json::json!({
            "temperature": self.config.temperature,
            "num_predict": self.config.max_tokens
        });
        body.insert("options".to_string(), options);
        
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_config() {
        let config = LlmConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.provider, LlmProvider::OpenAI);
        assert_eq!(config.model, "gpt-4");
    }
    
    #[test]
    fn test_client_availability() {
        let mut config = LlmConfig::default();
        let client = LlmClient::new(config.clone());
        assert!(!client.is_available()); // disabled
        
        config.enabled = true;
        let client = LlmClient::new(config.clone());
        assert!(!client.is_available()); // no API key
        
        config.api_key = Some("test-key".to_string());
        let client = LlmClient::new(config.clone());
        assert!(client.is_available());
        
        // Ollama doesn't need API key
        config.provider = LlmProvider::Ollama;
        config.api_key = None;
        let client = LlmClient::new(config);
        assert!(client.is_available());
    }
    
    #[test]
    fn test_endpoint_urls() {
        let mut config = LlmConfig::default();
        
        config.provider = LlmProvider::OpenAI;
        assert!(config.get_endpoint().contains("openai.com"));
        
        config.provider = LlmProvider::Anthropic;
        assert!(config.get_endpoint().contains("anthropic.com"));
        
        config.provider = LlmProvider::Ollama;
        assert!(config.get_endpoint().contains("localhost:11434"));
        
        // Custom endpoint overrides
        config.endpoint = Some("http://custom.endpoint/api".to_string());
        assert_eq!(config.get_endpoint(), "http://custom.endpoint/api");
    }
}
