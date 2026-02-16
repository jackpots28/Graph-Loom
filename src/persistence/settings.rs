use std::fs;
use std::io::{Read, Write};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::api::auth::AuthConfig;

/// Storage backend selection for graph persistence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum StorageBackend {
    /// RON text format (legacy, human-readable)
    Ron,
    /// SQLite database (ACID-compliant, scalable)
    #[default]
    Sqlite,
}

/// LLM provider selection for semantic features
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LlmProvider {
    #[default]
    None,
    OpenAI,
    Anthropic,
    Ollama,
}

impl LlmProvider {
    pub fn as_str(&self) -> &'static str {
        match self {
            LlmProvider::None => "None",
            LlmProvider::OpenAI => "OpenAI",
            LlmProvider::Anthropic => "Anthropic",
            LlmProvider::Ollama => "Ollama",
        }
    }
}

/// Embedding model selection for semantic search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EmbeddingModel {
    /// TF-IDF based embeddings (fast, lightweight)
    TfIdf,
    /// Word2Vec style embeddings (learns from graph)
    Word2Vec,
    /// ONNX all-MiniLM-L6-v2 model (best quality)
    #[default]
    Onnx,
}

fn default_llm_model() -> String { "gpt-4o-mini".to_string() }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSettings {
    // If None, use OS default autosave directory
    pub autosave_override: Option<PathBuf>,
    // If None, use OS temporary directory for exports
    #[serde(default)]
    pub export_override: Option<PathBuf>,
    // If None, server traffic logs go to OS temp dir
    #[serde(default)]
    pub api_log_override: Option<PathBuf>,
    // API service configuration (actix)
    #[serde(default)]
    pub api_enabled: bool,
    #[serde(default = "AppSettings::default_bind_addr")]
    pub api_bind_addr: String,
    #[serde(default = "AppSettings::default_port")]
    pub api_port: u16,
    #[serde(default)]
    pub api_key: Option<String>,
    // gRPC service configuration
    #[serde(default)]
    pub grpc_enabled: bool,
    #[serde(default = "AppSettings::default_grpc_port")]
    pub grpc_port: u16,
    // Whether to continue running in background when GUI window is closed
    #[serde(default)]
    pub background_on_close: bool,
    // Default directory for saving notes
    #[serde(default)]
    pub notes_dir_override: Option<PathBuf>,
    // Storage backend selection (Ron or Sqlite)
    #[serde(default)]
    pub storage_backend: StorageBackend,
    // LLM integration settings
    #[serde(default)]
    pub llm_provider: LlmProvider,
    #[serde(default)]
    pub llm_api_key: Option<String>,
    #[serde(default = "default_llm_model")]
    pub llm_model: String,
    #[serde(default)]
    pub llm_endpoint: Option<String>,
    /// Authentication configuration for API access
    #[serde(default)]
    pub auth_config: AuthConfig,
    /// Embedding model for semantic search
    #[serde(default)]
    pub embedding_model: EmbeddingModel,
    /// Physics timeout in seconds (0 = indefinite movement)
    #[serde(default = "AppSettings::default_physics_timeout")]
    pub physics_timeout_secs: u64,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            autosave_override: None,
            export_override: None,
            api_log_override: None,
            api_enabled: false,
            api_bind_addr: Self::default_bind_addr(),
            api_port: Self::default_port(),
            api_key: None,
            grpc_enabled: false,
            grpc_port: Self::default_grpc_port(),
            background_on_close: false,
            notes_dir_override: None,
            storage_backend: StorageBackend::default(),
            llm_provider: LlmProvider::default(),
            llm_api_key: None,
            llm_model: Self::default_llm_model(),
            llm_endpoint: None,
            auth_config: AuthConfig::default(),
            embedding_model: EmbeddingModel::default(),
            physics_timeout_secs: Self::default_physics_timeout(),
        }
    }
}

impl AppSettings {
    fn config_dir() -> PathBuf {
        // Cross-platform user config dir
        #[cfg(target_os = "macos")]
        {
            // ~/Library/Application Support/Graph-Loom
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            return home.join("Library").join("Application Support").join("Graph-Loom");
        }
        #[cfg(target_os = "windows")]
        {
            // %APPDATA%\Graph-Loom
            if let Ok(appdata) = std::env::var("APPDATA") {
                return PathBuf::from(appdata).join("Graph-Loom");
            }
            return PathBuf::from("Graph-Loom");
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            // $XDG_CONFIG_HOME/Graph-Loom or ~/.config/Graph-Loom
            if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
                return PathBuf::from(xdg).join("Graph-Loom");
            }
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            return home.join(".config").join("Graph-Loom");
        }
    }

    fn autosave_default_dir() -> PathBuf {
        // Cross-platform user-writable autosave dir
        #[cfg(target_os = "macos")]
        {
            // Prefer system temp autosave like Sublime, else App Support
            let tmp = std::env::var_os("TMPDIR").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("/tmp"));
            return tmp.join("Graph-Loom");
        }
        #[cfg(target_os = "windows")]
        {
            // %LOCALAPPDATA%\Graph-Loom\Autosave else TEMP
            if let Ok(local) = std::env::var("LOCALAPPDATA") {
                return PathBuf::from(local).join("Graph-Loom").join("Autosave");
            }
            if let Ok(temp) = std::env::var("TEMP") {
                return PathBuf::from(temp).join("Graph-Loom");
            }
            return PathBuf::from("Graph-Loom");
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            // $XDG_STATE_HOME/graph-loom or ~/.local/state/graph-loom, else /tmp/Graph-Loom
            if let Ok(xdg) = std::env::var("XDG_STATE_HOME") {
                return PathBuf::from(xdg).join("graph-loom");
            }
            if let Ok(home) = std::env::var("HOME") {
                return PathBuf::from(home).join(".local").join("state").join("graph-loom");
            }
            return PathBuf::from("/tmp").join("Graph-Loom");
        }
    }

    pub fn load() -> anyhow::Result<Self> {
        // New JSON settings path
        let json_path = Self::config_dir().join("settings.json");
        if json_path.exists() {
            let mut f = std::fs::File::open(json_path)?;
            let mut s = String::new();
            f.read_to_string(&mut s)?;
            let v: Self = serde_json::from_str(&s)?;
            return Ok(v);
        }
        // Migrate from legacy RON if present
        let ron_path = Self::config_dir().join("settings.ron");
        if ron_path.exists() {
            let mut f = std::fs::File::open(&ron_path)?;
            let mut s = String::new();
            f.read_to_string(&mut s)?;
            let v: Self = ron::from_str(&s)?;
            // Save immediately to JSON for future reads, ignore errors silently
            let _ = v.save();
            return Ok(v);
        }
        Ok(Self::default())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let dir = Self::config_dir();
        fs::create_dir_all(&dir)?;
        let path = dir.join("settings.json");
        let s = serde_json::to_string_pretty(self)?;
        let mut f = std::fs::File::create(path)?;
        f.write_all(s.as_bytes())?;
        Ok(())
    }

    pub fn autosave_dir(&self) -> PathBuf {
        if let Some(p) = &self.autosave_override { return p.clone(); }
        Self::autosave_default_dir()
    }

    /// Return the directory where the settings file (settings.json) is stored.
    /// This is OS-specific and resolves to a per-user configuration directory.
    pub fn settings_dir() -> PathBuf {
        Self::config_dir()
    }

    /// Default export directory when no override is set: OS temporary directory.
    /// Example: {temp_dir}/Graph-Loom/exports
    pub fn export_default_dir() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("Graph-Loom");
        p.push("exports");
        p
    }

    /// Effective export directory honoring user override or falling back to OS temp.
    pub fn export_dir(&self) -> PathBuf {
        if let Some(p) = &self.export_override { return p.clone(); }
        Self::export_default_dir()
    }

    pub(crate) fn default_bind_addr() -> String { "127.0.0.1".to_string() }
    pub(crate) fn default_port() -> u16 { 8787 }
    pub(crate) fn default_grpc_port() -> u16 { 50051 }
    pub(crate) fn default_llm_model() -> String { "gpt-4o-mini".to_string() }
    /// Default physics timeout: 0 means indefinite (nodes move until settled)
    pub(crate) fn default_physics_timeout() -> u64 { 0 }

    pub fn api_endpoint(&self) -> String {
        format!("{}:{}", self.api_bind_addr, self.api_port)
    }

    /// Default API log directory when no override is set: OS temporary directory.
    /// Example: {temp_dir}/Graph-Loom/api-logs
    pub fn api_log_default_dir() -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push("Graph-Loom");
        p.push("api-logs");
        p
    }

    /// Effective API log directory honoring user override or falling back to OS temp.
    pub fn api_log_dir(&self) -> PathBuf {
        if let Some(p) = &self.api_log_override { return p.clone(); }
        Self::api_log_default_dir()
    }

    /// Default notes directory when no override is set: ~/Documents/Graph-Loom/notes
    pub fn notes_default_dir() -> PathBuf {
        #[cfg(target_os = "macos")]
        {
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            return home.join("Documents").join("Graph-Loom").join("notes");
        }
        #[cfg(target_os = "windows")]
        {
            if let Ok(userprofile) = std::env::var("USERPROFILE") {
                return PathBuf::from(userprofile).join("Documents").join("Graph-Loom").join("notes");
            }
            return PathBuf::from("Graph-Loom").join("notes");
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let home = std::env::var_os("HOME").map(PathBuf::from).unwrap_or_else(|| PathBuf::from("~"));
            return home.join("Documents").join("Graph-Loom").join("notes");
        }
    }

    /// Effective notes directory honoring user override or falling back to default.
    pub fn notes_dir(&self) -> PathBuf {
        if let Some(p) = &self.notes_dir_override { return p.clone(); }
        Self::notes_default_dir()
    }
}
