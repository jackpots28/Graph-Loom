use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use ron::ser::PrettyConfig;
use serde::{Deserialize, Serialize};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use super::settings::{AppSettings, StorageBackend};
use super::sqlite_backend::SqliteStorage;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppStateFile {
    pub db: GraphDatabase,
    // store positions as map entries of node id -> (x, y)
    pub node_positions: Vec<(NodeId, f32, f32)>,
    pub pan: (f32, f32),
    pub zoom: f32,
}

impl AppStateFile {
    pub fn from_runtime(db: &GraphDatabase, node_positions: &HashMap<NodeId, egui::Pos2>, pan: egui::Vec2, zoom: f32) -> Self {
        let node_positions = node_positions
            .iter()
            .map(|(id, pos)| (*id, pos.x, pos.y))
            .collect();
        Self {
            db: db.clone(),
            node_positions,
            pan: (pan.x, pan.y),
            zoom,
        }
    }

    /// Create from runtime components without cloning the database if possible.
    pub fn from_runtime_owned(db: GraphDatabase, node_positions: &HashMap<NodeId, egui::Pos2>, pan: egui::Vec2, zoom: f32) -> Self {
        let node_positions = node_positions
            .iter()
            .map(|(id, pos)| (*id, pos.x, pos.y))
            .collect();
        Self {
            db,
            node_positions,
            pan: (pan.x, pan.y),
            zoom,
        }
    }

    /// Convert a persisted AppStateFile into runtime structures.
    ///
    /// This intentionally consumes `self` to avoid cloning large buffers.
    /// Keeping the existing API preserves behavior; allow clippy's naming lint.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_runtime(mut self) -> (GraphDatabase, HashMap<NodeId, egui::Pos2>, egui::Vec2, f32) {
        let positions: HashMap<NodeId, egui::Pos2> = self
            .node_positions
            .into_iter()
            .map(|(id, x, y)| (id, egui::pos2(x, y)))
            .collect();
        let pan = egui::vec2(self.pan.0, self.pan.1);
        self.db.rebuild_indices();
        (self.db, positions, pan, self.zoom)
    }
}

use std::sync::OnceLock;

static SETTINGS_OVERRIDE: OnceLock<AppSettings> = OnceLock::new();

pub fn set_settings_override(settings: AppSettings) {
    let _ = SETTINGS_OVERRIDE.set(settings);
}

fn autosave_dir() -> PathBuf {
    // If an override is set (e.g. from main.rs), use it.
    if let Some(settings) = SETTINGS_OVERRIDE.get() {
        return settings.autosave_dir();
    }
    // Load settings if present; else use defaults
    let settings = AppSettings::load().unwrap_or_default();
    settings.autosave_dir()
}

pub fn active_state_path() -> PathBuf {
    autosave_dir().join("state.ron")
}

pub fn active_sqlite_path() -> PathBuf {
    autosave_dir().join("state.db")
}

fn get_storage_backend() -> StorageBackend {
    if let Some(settings) = SETTINGS_OVERRIDE.get() {
        return settings.storage_backend;
    }
    AppSettings::load().map(|s| s.storage_backend).unwrap_or_default()
}

/// Get access to the SQLite storage for direct operations (e.g., embedding management)
pub fn get_sqlite_storage() -> Option<SqliteStorage> {
    if get_storage_backend() != StorageBackend::Sqlite {
        return None;
    }
    let path = active_sqlite_path();
    SqliteStorage::open(&path).ok()
}

pub fn versioned_state_path_now() -> PathBuf {
    let now = OffsetDateTime::now_utc();
    let fmt = format_description!("[year][month][day]_[hour][minute][second]");
    let stamp = now.format(fmt).unwrap_or_else(|_| "unknown".to_string());
    autosave_dir().join(format!("state_{}.ron", stamp))
}

fn ensure_autosave_dir() -> std::io::Result<()> {
    fs::create_dir_all(autosave_dir())
}

fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp_path = path.with_extension("ron.tmp");
    {
        let mut f = File::create(&tmp_path)?;
        f.write_all(data)?;
        f.flush()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

pub fn save_active(state: &AppStateFile) -> anyhow::Result<PathBuf> {
    ensure_autosave_dir()?;
    
    match get_storage_backend() {
        StorageBackend::Sqlite => {
            let path = active_sqlite_path();
            let mut storage = SqliteStorage::open(&path)?;
            let positions: HashMap<NodeId, egui::Pos2> = state.node_positions
                .iter()
                .map(|(id, x, y)| (*id, egui::pos2(*x, *y)))
                .collect();
            let pan = egui::vec2(state.pan.0, state.pan.1);
            storage.save_graph(&state.db, &positions, pan, state.zoom)?;
            Ok(path)
        }
        StorageBackend::Ron => {
            let pretty = PrettyConfig::new()
                .separate_tuple_members(true)
                .enumerate_arrays(true);
            let s = ron::ser::to_string_pretty(state, pretty)?;
            let path = active_state_path();
            atomic_write(&path, s.as_bytes())?;
            Ok(path)
        }
    }
}

pub fn save_versioned(state: &AppStateFile) -> anyhow::Result<PathBuf> {
    ensure_autosave_dir()?;
    let pretty = PrettyConfig::new()
        .separate_tuple_members(true)
        .enumerate_arrays(true);
    let s = ron::ser::to_string_pretty(state, pretty)?;
    let path = versioned_state_path_now();
    atomic_write(&path, s.as_bytes())?;
    Ok(path)
}

pub fn load_active() -> anyhow::Result<Option<AppStateFile>> {
    match get_storage_backend() {
        StorageBackend::Sqlite => {
            let path = active_sqlite_path();
            if !path.exists() {
                // Try fallback to RON for migration
                let ron_path = active_state_path();
                if ron_path.exists() {
                    return load_from_path(&ron_path).map(Some);
                }
                return Ok(None);
            }
            let storage = SqliteStorage::open(&path)?;
            let (db, positions, pan, zoom) = storage.load_graph()?;
            let node_positions = positions.into_iter().map(|(id, p)| (id, p.x, p.y)).collect();
            Ok(Some(AppStateFile {
                db,
                node_positions,
                pan: (pan.x, pan.y),
                zoom,
            }))
        }
        StorageBackend::Ron => {
            let path = active_state_path();
            if !path.exists() {
                return Ok(None);
            }
            load_from_path(&path).map(Some)
        }
    }
}

pub fn load_from_path(path: &Path) -> anyhow::Result<AppStateFile> {
    let mut f = File::open(path)?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    let state: AppStateFile = ron::from_str(&buf)?;
    Ok(state)
}

pub fn list_versions() -> anyhow::Result<Vec<PathBuf>> {
    let dir = autosave_dir();
    let mut entries: Vec<PathBuf> = Vec::new();
    if dir.exists() {
        for e in fs::read_dir(dir)? {
            let p = e?.path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str())
                && name.starts_with("state_") && name.ends_with(".ron")
            {
                entries.push(p);
            }
        }
    }
    // sort descending by filename (timestamp)
    entries.sort();
    entries.reverse();
    Ok(entries)
}

/// Check if all per-model embedding tables are empty in SQLite storage
pub fn is_embeddings_empty() -> bool {
    let path = active_sqlite_path();
    if !path.exists() {
        return true;
    }
    match SqliteStorage::open(&path) {
        Ok(storage) => {
            match storage.get_stats() {
                Ok(stats) => stats.tfidf_embeddings == 0 && stats.word2vec_embeddings == 0 && stats.onnx_embeddings == 0,
                Err(_) => true,
            }
        }
        Err(_) => true,
    }
}

/// Get the currently saved embedding model type from SQLite
pub fn get_current_embedding_model() -> Option<crate::persistence::settings::EmbeddingModel> {
    let path = active_sqlite_path();
    if !path.exists() {
        return None;
    }
    match SqliteStorage::open(&path) {
        Ok(storage) => {
            match storage.load_current_embedding_model() {
                Ok(Some(model_str)) => {
                    match model_str.as_str() {
                        "tfidf" => Some(crate::persistence::settings::EmbeddingModel::TfIdf),
                        "word2vec" => Some(crate::persistence::settings::EmbeddingModel::Word2Vec),
                        "onnx" => Some(crate::persistence::settings::EmbeddingModel::Onnx),
                        _ => None,
                    }
                }
                _ => None,
            }
        }
        Err(_) => None,
    }
}

/// Save the current embedding model type to SQLite
pub fn save_current_embedding_model(model: crate::persistence::settings::EmbeddingModel) -> anyhow::Result<()> {
    let model_str = match model {
        crate::persistence::settings::EmbeddingModel::TfIdf => "tfidf",
        crate::persistence::settings::EmbeddingModel::Word2Vec => "word2vec",
        crate::persistence::settings::EmbeddingModel::Onnx => "onnx",
    };
    let path = active_sqlite_path();
    let storage = SqliteStorage::open(&path)?;
    storage.save_current_embedding_model(model_str)?;
    Ok(())
}