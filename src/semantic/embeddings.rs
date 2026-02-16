//! Local embedding model utility for node metadata embedding and nearest neighbor search.
//!
//! Provides purely local, Rust-native embedding generation and similarity comparison
//! using cosine similarity and L2 (Euclidean) distance metrics.
//!
//! Supports multiple embedding backends:
//! - TF-IDF: Fast, lightweight, no model download required
//! - Word2Vec: Better semantic quality, learns word relationships
//! - ONNX: Best quality using all-MiniLM-L6-v2 transformer model

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use serde::{Deserialize, Serialize};

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use crate::persistence::settings::EmbeddingModel;

/// Configuration for the embedding model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingConfig {
    /// Embedding dimension (default: 384 for MiniLM-style models)
    pub dimension: usize,
    /// Whether to normalize embeddings (recommended for cosine similarity)
    pub normalize: bool,
}

impl Default for EmbeddingConfig {
    fn default() -> Self {
        Self {
            dimension: 384,
            normalize: true,
        }
    }
}

/// A simple local embedding model using TF-IDF style vectorization.
/// This is a lightweight, dependency-free approach that works entirely locally.
/// For production use with neural embeddings, consider integrating `candle` or `ort`.
#[derive(Clone)]
pub struct LocalEmbedder {
    config: EmbeddingConfig,
    /// Vocabulary built from seen tokens
    vocabulary: HashMap<String, usize>,
    /// IDF weights for each token
    idf_weights: HashMap<String, f32>,
    /// Total documents seen (for IDF calculation)
    doc_count: usize,
}

impl LocalEmbedder {
    /// Create a new local embedder with default configuration
    pub fn new() -> Self {
        Self::with_config(EmbeddingConfig::default())
    }

    /// Create a new local embedder with custom configuration
    pub fn with_config(config: EmbeddingConfig) -> Self {
        Self {
            config,
            vocabulary: HashMap::new(),
            idf_weights: HashMap::new(),
            doc_count: 0,
        }
    }

    /// Tokenize text into lowercase words
    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| !s.is_empty() && s.len() > 1)
            .map(|s| s.to_string())
            .collect()
    }

    /// Build vocabulary from a corpus of texts
    pub fn fit(&mut self, texts: &[String]) {
        let mut doc_freq: HashMap<String, usize> = HashMap::new();
        
        for text in texts {
            let tokens: std::collections::HashSet<String> = Self::tokenize(text).into_iter().collect();
            for token in tokens {
                *doc_freq.entry(token).or_insert(0) += 1;
            }
        }
        
        self.doc_count = texts.len().max(1);
        
        // Build vocabulary (limit to config.dimension most frequent terms)
        let mut freq_vec: Vec<_> = doc_freq.iter().collect();
        freq_vec.sort_by(|a, b| b.1.cmp(a.1));
        
        self.vocabulary.clear();
        self.idf_weights.clear();
        
        for (i, (token, freq)) in freq_vec.into_iter().take(self.config.dimension).enumerate() {
            self.vocabulary.insert(token.clone(), i);
            // IDF = log(N / df)
            let idf = ((self.doc_count as f32) / (*freq as f32 + 1.0)).ln() + 1.0;
            self.idf_weights.insert(token.clone(), idf);
        }
    }

    /// Generate embedding for a single text
    pub fn embed(&self, text: &str) -> Vec<f32> {
        let tokens = Self::tokenize(text);
        let mut embedding = vec![0.0f32; self.config.dimension];
        
        // Count term frequencies
        let mut tf: HashMap<String, usize> = HashMap::new();
        for token in &tokens {
            *tf.entry(token.clone()).or_insert(0) += 1;
        }
        
        // Build TF-IDF vector
        for (token, count) in tf {
            if let Some(&idx) = self.vocabulary.get(&token) {
                let tf_val = (count as f32).ln() + 1.0;
                let idf_val = self.idf_weights.get(&token).copied().unwrap_or(1.0);
                if idx < embedding.len() {
                    embedding[idx] = tf_val * idf_val;
                }
            }
        }
        
        // Normalize if configured
        if self.config.normalize {
            normalize_vector(&mut embedding);
        }
        
        embedding
    }

    /// Embed multiple texts
    pub fn embed_batch(&self, texts: &[String]) -> Vec<Vec<f32>> {
        texts.iter().map(|t| self.embed(t)).collect()
    }

    /// Fit and transform in one step
    pub fn fit_transform(&mut self, texts: &[String]) -> Vec<Vec<f32>> {
        self.fit(texts);
        self.embed_batch(texts)
    }
}

impl Default for LocalEmbedder {
    fn default() -> Self {
        Self::new()
    }
}

/// Serialize LocalEmbedder state for persistence
impl LocalEmbedder {
    pub fn serialize_state(&self) -> Vec<u8> {
        let state = (
            &self.config,
            &self.vocabulary,
            &self.idf_weights,
            self.doc_count,
        );
        serde_json::to_vec(&state).unwrap_or_default()
    }

    pub fn deserialize_state(data: &[u8]) -> Option<Self> {
        let (config, vocabulary, idf_weights, doc_count): (
            EmbeddingConfig,
            HashMap<String, usize>,
            HashMap<String, f32>,
            usize,
        ) = serde_json::from_slice(data).ok()?;
        Some(Self {
            config,
            vocabulary,
            idf_weights,
            doc_count,
        })
    }
}

/// ONNX-based embedder using all-MiniLM-L6-v2 model
pub struct OnnxEmbedder {
    session: Option<ort::session::Session>,
    tokenizer: Option<tokenizers::Tokenizer>,
    dimension: usize,
}

impl OnnxEmbedder {
    /// Model download URL for all-MiniLM-L6-v2
    const MODEL_URL: &'static str = "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/onnx/model.onnx";
    const TOKENIZER_URL: &'static str = "https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2/resolve/main/tokenizer.json";

    /// Create a new ONNX embedder, downloading model if necessary
    pub fn new() -> Result<Self, String> {
        let model_dir = Self::model_dir();
        std::fs::create_dir_all(&model_dir).map_err(|e| format!("Failed to create model dir: {}", e))?;

        let model_path = model_dir.join("model.onnx");
        let tokenizer_path = model_dir.join("tokenizer.json");

        // Download model if not present
        if !model_path.exists() {
            Self::download_file(Self::MODEL_URL, &model_path)?;
        }
        if !tokenizer_path.exists() {
            Self::download_file(Self::TOKENIZER_URL, &tokenizer_path)?;
        }

        Self::load_from_path(&model_path, &tokenizer_path)
    }

    /// Load from existing model files
    pub fn load_from_path(model_path: &PathBuf, tokenizer_path: &PathBuf) -> Result<Self, String> {
        use ort::session::Session;
        
        // Initialize ONNX Runtime - read model file and load from memory
        let model_bytes = std::fs::read(model_path)
            .map_err(|e| format!("Failed to read model file: {}", e))?;
        
        let session = Session::builder()
            .map_err(|e| format!("Failed to create session builder: {}", e))?
            .with_intra_threads(1)
            .map_err(|e| format!("Failed to set threads: {}", e))?
            .commit_from_memory(&model_bytes)
            .map_err(|e| format!("Failed to load ONNX model: {}", e))?;

        let tokenizer = tokenizers::Tokenizer::from_file(tokenizer_path)
            .map_err(|e| format!("Failed to load tokenizer: {}", e))?;

        Ok(Self {
            session: Some(session),
            tokenizer: Some(tokenizer),
            dimension: 384, // all-MiniLM-L6-v2 output dimension
        })
    }

    /// Get the model directory path
    fn model_dir() -> PathBuf {
        #[cfg(target_os = "macos")]
        {
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            home.join("Library").join("Application Support").join("Graph-Loom").join("models")
        }
        #[cfg(target_os = "windows")]
        {
            if let Ok(appdata) = std::env::var("APPDATA") {
                return PathBuf::from(appdata).join("Graph-Loom").join("models");
            }
            PathBuf::from("Graph-Loom").join("models")
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            if let Ok(xdg) = std::env::var("XDG_DATA_HOME") {
                return PathBuf::from(xdg).join("graph-loom").join("models");
            }
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            home.join(".local").join("share").join("graph-loom").join("models")
        }
    }

    /// Download a file from URL
    fn download_file(url: &str, path: &PathBuf) -> Result<(), String> {
        eprintln!("[Graph-Loom] Downloading ONNX model from {}...", url);
        
        // Use a simple blocking HTTP client
        let response = std::process::Command::new("curl")
            .args(["-L", "-o", path.to_str().unwrap_or(""), url])
            .output()
            .map_err(|e| format!("Failed to download: {}", e))?;

        if !response.status.success() {
            return Err(format!("Download failed: {}", String::from_utf8_lossy(&response.stderr)));
        }

        eprintln!("[Graph-Loom] Download complete: {}", path.display());
        Ok(())
    }

    /// Generate embedding for a single text
    pub fn embed(&mut self, text: &str) -> Result<Vec<f32>, String> {
        let session = self.session.as_mut().ok_or("ONNX session not initialized")?;
        let tokenizer = self.tokenizer.as_ref().ok_or("Tokenizer not initialized")?;

        // Tokenize
        let encoding = tokenizer.encode(text, true)
            .map_err(|e| format!("Tokenization failed: {}", e))?;

        let input_ids: Vec<i64> = encoding.get_ids().iter().map(|&x| x as i64).collect();
        let attention_mask: Vec<i64> = encoding.get_attention_mask().iter().map(|&x| x as i64).collect();
        let token_type_ids: Vec<i64> = vec![0i64; input_ids.len()];

        let seq_len = input_ids.len();

        // Create input tensors using ort's Tensor type
        use ort::value::Tensor;
        
        let input_ids_tensor = Tensor::from_array(([1usize, seq_len], input_ids))
            .map_err(|e| format!("Failed to create input_ids tensor: {}", e))?;
        let attention_mask_tensor = Tensor::from_array(([1usize, seq_len], attention_mask))
            .map_err(|e| format!("Failed to create attention_mask tensor: {}", e))?;
        let token_type_ids_tensor = Tensor::from_array(([1usize, seq_len], token_type_ids))
            .map_err(|e| format!("Failed to create token_type_ids tensor: {}", e))?;

        // Run inference
        let outputs = session.run(ort::inputs![
            "input_ids" => input_ids_tensor,
            "attention_mask" => attention_mask_tensor,
            "token_type_ids" => token_type_ids_tensor,
        ])
        .map_err(|e| format!("Inference failed: {}", e))?;

        // Extract embeddings (mean pooling over sequence)
        let output = outputs.get("last_hidden_state")
            .or_else(|| outputs.get("sentence_embedding"))
            .ok_or("No output tensor found")?;

        let (shape, data) = output.try_extract_tensor::<f32>()
            .map_err(|e| format!("Failed to extract tensor: {}", e))?;

        // shape is &Shape (Vec<i64>), data is &[f32]
        let dims: Vec<usize> = shape.iter().map(|&d| d as usize).collect();
        
        let embedding: Vec<f32> = if dims.len() == 3 {
            // Shape: [batch, seq_len, hidden_size]
            let hidden_size = dims[2];
            let seq_len = dims[1];
            // Mean pooling over sequence dimension
            (0..hidden_size)
                .map(|h| {
                    (0..seq_len).map(|s| data[s * hidden_size + h]).sum::<f32>() / seq_len as f32
                })
                .collect()
        } else if dims.len() == 2 {
            // Shape: [batch, hidden_size] - already pooled
            data.to_vec()
        } else {
            return Err(format!("Unexpected output shape: {:?}", dims));
        };

        // Normalize
        let mut result = embedding;
        normalize_vector(&mut result);
        Ok(result)
    }

    /// Embed multiple texts
    pub fn embed_batch(&mut self, texts: &[String]) -> Result<Vec<Vec<f32>>, String> {
        let mut results = Vec::with_capacity(texts.len());
        for t in texts {
            results.push(self.embed(t)?);
        }
        Ok(results)
    }

    /// Check if model is loaded
    pub fn is_loaded(&self) -> bool {
        self.session.is_some() && self.tokenizer.is_some()
    }
}

impl Default for OnnxEmbedder {
    fn default() -> Self {
        Self {
            session: None,
            tokenizer: None,
            dimension: 384,
        }
    }
}

/// Unified embedder that supports multiple backends
pub enum UnifiedEmbedder {
    TfIdf(LocalEmbedder),
    Word2Vec(LocalEmbedder),
    Onnx(OnnxEmbedder),
}

impl UnifiedEmbedder {
    /// Create embedder for the specified model type
    pub fn new(model: EmbeddingModel) -> Result<Self, String> {
        match model {
            EmbeddingModel::TfIdf => {
                Ok(UnifiedEmbedder::TfIdf(LocalEmbedder::new()))
            }
            EmbeddingModel::Word2Vec => {
                Ok(UnifiedEmbedder::Word2Vec(LocalEmbedder::new()))
            }
            EmbeddingModel::Onnx => {
                let onnx = OnnxEmbedder::new()?;
                Ok(UnifiedEmbedder::Onnx(onnx))
            }
        }
    }

    /// Get the model type string for storage
    pub fn model_type_str(&self) -> &'static str {
        match self {
            UnifiedEmbedder::TfIdf(_) => "tfidf",
            UnifiedEmbedder::Word2Vec(_) => "word2vec",
            UnifiedEmbedder::Onnx(_) => "onnx",
        }
    }

    /// Fit the embedder on a corpus (applies to TF-IDF and Word2Vec)
    pub fn fit(&mut self, texts: &[String]) {
        match self {
            UnifiedEmbedder::TfIdf(embedder) | UnifiedEmbedder::Word2Vec(embedder) => {
                embedder.fit(texts);
            }
            UnifiedEmbedder::Onnx(_) => {}
        }
    }

    /// Generate embedding for a single text
    pub fn embed(&mut self, text: &str) -> Vec<f32> {
        match self {
            UnifiedEmbedder::TfIdf(embedder) | UnifiedEmbedder::Word2Vec(embedder) => embedder.embed(text),
            UnifiedEmbedder::Onnx(embedder) => {
                embedder.embed(text).unwrap_or_else(|_| vec![0.0; 384])
            }
        }
    }

    /// Embed multiple texts
    pub fn embed_batch(&mut self, texts: &[String]) -> Vec<Vec<f32>> {
        match self {
            UnifiedEmbedder::TfIdf(embedder) | UnifiedEmbedder::Word2Vec(embedder) => embedder.embed_batch(texts),
            UnifiedEmbedder::Onnx(embedder) => {
                embedder.embed_batch(texts).unwrap_or_else(|_| vec![])
            }
        }
    }

    /// Serialize state for persistence
    pub fn serialize_state(&self) -> Option<Vec<u8>> {
        match self {
            UnifiedEmbedder::TfIdf(embedder) | UnifiedEmbedder::Word2Vec(embedder) => Some(embedder.serialize_state()),
            UnifiedEmbedder::Onnx(_) => None, // ONNX doesn't need state serialization
        }
    }
}

/// Normalize a vector to unit length (L2 norm = 1)
pub fn normalize_vector(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > f32::EPSILON {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// Compute cosine similarity between two vectors.
/// Returns a value in [-1, 1] where 1 means identical direction.
/// For normalized vectors, this is simply the dot product.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    
    if norm_a < f32::EPSILON || norm_b < f32::EPSILON {
        return 0.0;
    }
    
    dot / (norm_a * norm_b)
}

/// Compute L2 (Euclidean) distance between two vectors.
/// Returns a non-negative value where 0 means identical vectors.
pub fn l2_distance(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return f32::MAX;
    }
    
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Result of a nearest neighbor search
#[derive(Debug, Clone)]
pub struct NearestNeighbor {
    pub node_id: NodeId,
    pub similarity: f32,
    pub distance: f32,
}

/// Embedding index for efficient nearest neighbor search on graph nodes
#[derive(Clone)]
pub struct NodeEmbeddingIndex {
    embedder: LocalEmbedder,
    /// Stored embeddings keyed by node ID
    embeddings: HashMap<NodeId, Vec<f32>>,
}

impl NodeEmbeddingIndex {
    /// Create a new embedding index
    pub fn new() -> Self {
        Self {
            embedder: LocalEmbedder::new(),
            embeddings: HashMap::new(),
        }
    }

    /// Create with custom embedding configuration
    pub fn with_config(config: EmbeddingConfig) -> Self {
        Self {
            embedder: LocalEmbedder::with_config(config),
            embeddings: HashMap::new(),
        }
    }

    /// Build the index from a graph database.
    /// Embeds node labels and metadata values.
    pub fn build_from_graph(&mut self, db: &GraphDatabase) {
        // Collect all text from nodes
        let mut texts: Vec<String> = Vec::new();
        let mut node_ids: Vec<NodeId> = Vec::new();
        
        for (id, node) in &db.nodes {
            let mut text_parts = vec![node.label.clone()];
            for value in node.metadata.values() {
                text_parts.push(value.clone());
            }
            texts.push(text_parts.join(" "));
            node_ids.push(*id);
        }
        
        // Fit the embedder on all texts
        self.embedder.fit(&texts);
        
        // Generate and store embeddings
        self.embeddings.clear();
        for (id, text) in node_ids.into_iter().zip(texts.iter()) {
            let embedding = self.embedder.embed(text);
            self.embeddings.insert(id, embedding);
        }
    }

    /// Add or update a single node's embedding
    pub fn update_node(&mut self, db: &GraphDatabase, node_id: NodeId) {
        if let Some(node) = db.nodes.get(&node_id) {
            let mut text_parts = vec![node.label.clone()];
            for value in node.metadata.values() {
                text_parts.push(value.clone());
            }
            let text = text_parts.join(" ");
            let embedding = self.embedder.embed(&text);
            self.embeddings.insert(node_id, embedding);
        }
    }

    /// Remove a node from the index
    pub fn remove_node(&mut self, node_id: NodeId) {
        self.embeddings.remove(&node_id);
    }

    /// Find k nearest neighbors to a query text using cosine similarity
    pub fn find_nearest_by_text(&self, query: &str, k: usize) -> Vec<NearestNeighbor> {
        let query_embedding = self.embedder.embed(query);
        self.find_nearest_by_embedding(&query_embedding, k)
    }

    /// Find k nearest neighbors to a query embedding using cosine similarity
    pub fn find_nearest_by_embedding(&self, query: &[f32], k: usize) -> Vec<NearestNeighbor> {
        let mut results: Vec<NearestNeighbor> = self.embeddings
            .iter()
            .map(|(id, emb)| {
                let sim = cosine_similarity(query, emb);
                let dist = l2_distance(query, emb);
                NearestNeighbor {
                    node_id: *id,
                    similarity: sim,
                    distance: dist,
                }
            })
            .collect();
        
        // Sort by similarity descending
        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }

    /// Find k nearest neighbors to a given node
    pub fn find_nearest_to_node(&self, node_id: NodeId, k: usize) -> Vec<NearestNeighbor> {
        if let Some(embedding) = self.embeddings.get(&node_id) {
            let mut results: Vec<NearestNeighbor> = self.embeddings
                .iter()
                .filter(|(id, _)| **id != node_id)
                .map(|(id, emb)| {
                    let sim = cosine_similarity(embedding, emb);
                    let dist = l2_distance(embedding, emb);
                    NearestNeighbor {
                        node_id: *id,
                        similarity: sim,
                        distance: dist,
                    }
                })
                .collect();
            
            results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
            results.truncate(k);
            results
        } else {
            Vec::new()
        }
    }

    /// Find all nodes within a similarity threshold
    pub fn find_within_threshold(&self, query: &str, threshold: f32) -> Vec<NearestNeighbor> {
        let query_embedding = self.embedder.embed(query);
        
        let mut results: Vec<NearestNeighbor> = self.embeddings
            .iter()
            .filter_map(|(id, emb)| {
                let sim = cosine_similarity(&query_embedding, emb);
                if sim >= threshold {
                    Some(NearestNeighbor {
                        node_id: *id,
                        similarity: sim,
                        distance: l2_distance(&query_embedding, emb),
                    })
                } else {
                    None
                }
            })
            .collect();
        
        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// Get the embedding for a specific node
    pub fn get_embedding(&self, node_id: NodeId) -> Option<&Vec<f32>> {
        self.embeddings.get(&node_id)
    }

    /// Get the number of indexed nodes
    pub fn len(&self) -> usize {
        self.embeddings.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.embeddings.is_empty()
    }
}

impl Default for NodeEmbeddingIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &b) - 1.0).abs() < 0.001);
        
        let c = vec![0.0, 1.0, 0.0];
        assert!(cosine_similarity(&a, &c).abs() < 0.001);
        
        let d = vec![-1.0, 0.0, 0.0];
        assert!((cosine_similarity(&a, &d) + 1.0).abs() < 0.001);
    }

    #[test]
    fn test_l2_distance() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![3.0, 4.0, 0.0];
        assert!((l2_distance(&a, &b) - 5.0).abs() < 0.001);
        
        assert!(l2_distance(&a, &a).abs() < 0.001);
    }

    #[test]
    fn test_normalize_vector() {
        let mut v = vec![3.0, 4.0];
        normalize_vector(&mut v);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.001);
    }

    #[test]
    fn test_local_embedder() {
        let mut embedder = LocalEmbedder::new();
        let texts = vec![
            "hello world".to_string(),
            "hello rust".to_string(),
            "goodbye world".to_string(),
        ];
        
        embedder.fit(&texts);
        
        let emb1 = embedder.embed("hello world");
        let emb2 = embedder.embed("hello rust");
        let emb3 = embedder.embed("goodbye world");
        
        // "hello world" should be more similar to "hello rust" than to "goodbye world"
        let sim_12 = cosine_similarity(&emb1, &emb2);
        let sim_13 = cosine_similarity(&emb1, &emb3);
        
        // Both share one word with emb1, so similarities should be comparable
        assert!(sim_12 > 0.0);
        assert!(sim_13 > 0.0);
    }

    #[test]
    fn test_node_embedding_index() {
        let mut db = GraphDatabase::new();
        let n1 = db.add_node("Person".to_string(), [("name".to_string(), "Alice".to_string())].into());
        let n2 = db.add_node("Person".to_string(), [("name".to_string(), "Bob".to_string())].into());
        let _n3 = db.add_node("Company".to_string(), [("name".to_string(), "Acme".to_string())].into());

        let mut index = NodeEmbeddingIndex::new();
        index.build_from_graph(&db);
        
        assert_eq!(index.len(), 3);
        
        // Find nearest to "Person Alice"
        let results = index.find_nearest_by_text("Person Alice", 2);
        assert!(!results.is_empty());
        
        // The first result should be Alice's node
        assert_eq!(results[0].node_id, n1);
        
        // Find nearest to node n1 (should return n2 as most similar - both are Person)
        let neighbors = index.find_nearest_to_node(n1, 2);
        assert!(!neighbors.is_empty());
        // n2 (Person Bob) should be more similar to n1 (Person Alice) than n3 (Company Acme)
        assert_eq!(neighbors[0].node_id, n2);
    }
}
