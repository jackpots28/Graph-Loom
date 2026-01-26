use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use ron::ser::PrettyConfig;
use serde::{Deserialize, Serialize};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use super::settings::AppSettings;

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

    /// Convert a persisted AppStateFile into runtime structures.
    ///
    /// This intentionally consumes `self` to avoid cloning large buffers.
    /// Keeping the existing API preserves behavior; allow clippy's naming lint.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_runtime(self) -> (GraphDatabase, HashMap<NodeId, egui::Pos2>, egui::Vec2, f32) {
        let positions: HashMap<NodeId, egui::Pos2> = self
            .node_positions
            .into_iter()
            .map(|(id, x, y)| (id, egui::pos2(x, y)))
            .collect();
        let pan = egui::vec2(self.pan.0, self.pan.1);
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
    let pretty = PrettyConfig::new()
        .separate_tuple_members(true)
        .enumerate_arrays(true);
    let s = ron::ser::to_string_pretty(state, pretty)?;
    let path = active_state_path();
    atomic_write(&path, s.as_bytes())?;
    Ok(path)
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
    let path = active_state_path();
    if !path.exists() {
        return Ok(None);
    }
    load_from_path(&path).map(Some)
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