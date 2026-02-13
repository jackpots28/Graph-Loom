//! SQLite storage backend for Graph-Loom
//! Provides ACID-compliant persistence with atomic transactions.

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::Path;

use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, Node, NodeId, Relationship};

/// SQLite-based storage for the graph database
pub struct SqliteStorage {
    conn: Connection,
}

impl SqliteStorage {
    /// Open or create a SQLite database at the given path
    pub fn open<P: AsRef<Path>>(path: P) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        // Enable WAL mode for better concurrent read/write performance
        conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000;
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 268435456;
            "#
        )?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Open an in-memory database (useful for testing)
    #[allow(dead_code)]
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        // In-memory doesn't need WAL but set other optimizations
        conn.execute_batch(
            r#"
            PRAGMA synchronous = OFF;
            PRAGMA cache_size = -64000;
            PRAGMA temp_store = MEMORY;
            "#
        )?;
        let storage = Self { conn };
        storage.init_schema()?;
        Ok(storage)
    }

    /// Initialize the database schema
    fn init_schema(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            r#"
            -- Enable foreign keys
            PRAGMA foreign_keys = ON;

            -- nodes table
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                label TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            -- node_metadata table (key-value pairs)
            CREATE TABLE IF NOT EXISTS node_metadata (
                node_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (node_id, key),
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            );

            -- relationships table
            CREATE TABLE IF NOT EXISTS relationships (
                id TEXT PRIMARY KEY,
                from_node TEXT NOT NULL,
                to_node TEXT NOT NULL,
                label TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY (from_node) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY (to_node) REFERENCES nodes(id) ON DELETE CASCADE
            );

            -- relationship_metadata table
            CREATE TABLE IF NOT EXISTS relationship_metadata (
                rel_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (rel_id, key),
                FOREIGN KEY (rel_id) REFERENCES relationships(id) ON DELETE CASCADE
            );

            -- graph_state table (for pan, zoom, etc.)
            CREATE TABLE IF NOT EXISTS graph_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            -- node_positions table
            CREATE TABLE IF NOT EXISTS node_positions (
                node_id TEXT PRIMARY KEY,
                x REAL NOT NULL,
                y REAL NOT NULL,
                FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
            );

            -- Create indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(label);
            CREATE INDEX IF NOT EXISTS idx_relationships_from ON relationships(from_node);
            CREATE INDEX IF NOT EXISTS idx_relationships_to ON relationships(to_node);
            CREATE INDEX IF NOT EXISTS idx_relationships_label ON relationships(label);

            -- FTS5 full-text search virtual table for nodes
            CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5(
                node_id,
                label,
                metadata_text,
                content='',
                contentless_delete=1
            );

            -- R-tree spatial index for node positions (viewport queries)
            -- Note: R-tree id must be INTEGER; we store node_id separately
            CREATE VIRTUAL TABLE IF NOT EXISTS node_positions_rtree USING rtree(
                rowid,
                min_x, max_x,
                min_y, max_y
            );

            -- Mapping table for R-tree rowid to node_id
            CREATE TABLE IF NOT EXISTS rtree_node_map (
                rowid INTEGER PRIMARY KEY,
                node_id TEXT NOT NULL UNIQUE
            );

            -- Embedding model state table (stores vocabulary, IDF weights, etc.)
            CREATE TABLE IF NOT EXISTS embedding_model_state (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            );

            -- Per-model embedding tables for persistent storage
            -- TF-IDF embeddings (no CASCADE - embeddings persist independently)
            CREATE TABLE IF NOT EXISTS embeddings_tfidf (
                node_id TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            );

            -- Word2Vec embeddings (no CASCADE - embeddings persist independently)
            CREATE TABLE IF NOT EXISTS embeddings_word2vec (
                node_id TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            );

            -- ONNX all-MiniLM-L6-v2 embeddings (no CASCADE - embeddings persist independently)
            CREATE TABLE IF NOT EXISTS embeddings_onnx (
                node_id TEXT PRIMARY KEY,
                embedding BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            );

            "#,
        )
    }

    /// Save the entire graph database in a single transaction
    pub fn save_graph(
        &mut self,
        db: &GraphDatabase,
        positions: &HashMap<NodeId, egui::Pos2>,
        pan: egui::Vec2,
        zoom: f32,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;

        // Clear existing data
        tx.execute("DELETE FROM node_metadata", [])?;
        tx.execute("DELETE FROM relationship_metadata", [])?;
        tx.execute("DELETE FROM node_positions", [])?;
        tx.execute("DELETE FROM node_positions_rtree", [])?;
        tx.execute("DELETE FROM rtree_node_map", [])?;
        tx.execute("DELETE FROM nodes_fts", [])?;
        tx.execute("DELETE FROM relationships", [])?;
        tx.execute("DELETE FROM nodes", [])?;
        tx.execute("DELETE FROM graph_state", [])?;

        // Insert nodes
        for node in db.nodes.values() {
            tx.execute(
                "INSERT INTO nodes (id, label, created_at, updated_at) VALUES (?1, ?2, ?3, ?4)",
                params![node.id.to_string(), node.label, node.created_at, node.updated_at],
            )?;

            // Insert node metadata
            for (key, value) in &node.metadata {
                tx.execute(
                    "INSERT INTO node_metadata (node_id, key, value) VALUES (?1, ?2, ?3)",
                    params![node.id.to_string(), key, value],
                )?;
            }
        }

        // Insert relationships
        for rel in db.relationships.values() {
            tx.execute(
                "INSERT INTO relationships (id, from_node, to_node, label, created_at, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    rel.id.to_string(),
                    rel.from_node.to_string(),
                    rel.to_node.to_string(),
                    rel.label,
                    rel.created_at,
                    rel.updated_at
                ],
            )?;

            // Insert relationship metadata
            for (key, value) in &rel.metadata {
                tx.execute(
                    "INSERT INTO relationship_metadata (rel_id, key, value) VALUES (?1, ?2, ?3)",
                    params![rel.id.to_string(), key, value],
                )?;
            }
        }

        // Insert positions and spatial index (only for nodes that exist in the graph)
        for (node_id, pos) in positions {
            // Skip positions for nodes that no longer exist (e.g., deleted nodes)
            if !db.nodes.contains_key(node_id) {
                continue;
            }
            tx.execute(
                "INSERT INTO node_positions (node_id, x, y) VALUES (?1, ?2, ?3)",
                params![node_id.to_string(), pos.x, pos.y],
            )?;
            // Insert mapping and R-tree entry
            tx.execute(
                "INSERT INTO rtree_node_map (node_id) VALUES (?1)",
                params![node_id.to_string()],
            )?;
            let rowid = tx.last_insert_rowid();
            tx.execute(
                "INSERT INTO node_positions_rtree (rowid, min_x, max_x, min_y, max_y) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![rowid, pos.x, pos.x, pos.y, pos.y],
            )?;
        }

        // Populate FTS5 index for nodes
        for node in db.nodes.values() {
            let metadata_text: String = node.metadata.values().cloned().collect::<Vec<_>>().join(" ");
            tx.execute(
                "INSERT INTO nodes_fts (node_id, label, metadata_text) VALUES (?1, ?2, ?3)",
                params![node.id.to_string(), node.label, metadata_text],
            )?;
        }

        // Insert graph state
        tx.execute(
            "INSERT INTO graph_state (key, value) VALUES ('pan_x', ?1)",
            params![pan.x.to_string()],
        )?;
        tx.execute(
            "INSERT INTO graph_state (key, value) VALUES ('pan_y', ?1)",
            params![pan.y.to_string()],
        )?;
        tx.execute(
            "INSERT INTO graph_state (key, value) VALUES ('zoom', ?1)",
            params![zoom.to_string()],
        )?;

        tx.commit()
    }

    /// Load the entire graph database
    pub fn load_graph(&self) -> rusqlite::Result<(GraphDatabase, HashMap<NodeId, egui::Pos2>, egui::Vec2, f32)> {
        let mut db = GraphDatabase::new();
        let mut positions: HashMap<NodeId, egui::Pos2> = HashMap::new();

        // Load nodes
        let mut stmt = self.conn.prepare("SELECT id, label, created_at, updated_at FROM nodes")?;
        let node_rows = stmt.query_map([], |row| {
            let id_str: String = row.get(0)?;
            let label: String = row.get(1)?;
            let created_at: i64 = row.get(2)?;
            let updated_at: i64 = row.get(3)?;
            Ok((id_str, label, created_at, updated_at))
        })?;

        for row in node_rows {
            let (id_str, label, created_at, updated_at) = row?;
            let id = Uuid::parse_str(&id_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
            
            let node = Node {
                id,
                label,
                metadata: HashMap::new(),
                out_rels: Vec::new(),
                in_rels: Vec::new(),
                created_at,
                updated_at,
            };
            db.nodes.insert(id, node);
        }

        // Load node metadata
        let mut stmt = self.conn.prepare("SELECT node_id, key, value FROM node_metadata")?;
        let meta_rows = stmt.query_map([], |row| {
            let node_id: String = row.get(0)?;
            let key: String = row.get(1)?;
            let value: String = row.get(2)?;
            Ok((node_id, key, value))
        })?;

        for row in meta_rows {
            let (node_id_str, key, value) = row?;
            if let Ok(node_id) = Uuid::parse_str(&node_id_str) {
                if let Some(node) = db.nodes.get_mut(&node_id) {
                    node.metadata.insert(key, value);
                }
            }
        }

        // Load relationships
        let mut stmt = self.conn.prepare("SELECT id, from_node, to_node, label, created_at, updated_at FROM relationships")?;
        let rel_rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let from_node: String = row.get(1)?;
            let to_node: String = row.get(2)?;
            let label: String = row.get(3)?;
            let created_at: i64 = row.get(4)?;
            let updated_at: i64 = row.get(5)?;
            Ok((id, from_node, to_node, label, created_at, updated_at))
        })?;

        for row in rel_rows {
            let (id_str, from_str, to_str, label, created_at, updated_at) = row?;
            let id = Uuid::parse_str(&id_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
            let from_node = Uuid::parse_str(&from_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;
            let to_node = Uuid::parse_str(&to_str).map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?;

            let rel = Relationship {
                id,
                from_node,
                to_node,
                label,
                metadata: HashMap::new(),
                created_at,
                updated_at,
            };

            // Update adjacency lists
            if let Some(from) = db.nodes.get_mut(&from_node) {
                from.out_rels.push(id);
            }
            if let Some(to) = db.nodes.get_mut(&to_node) {
                to.in_rels.push(id);
            }

            db.relationships.insert(id, rel);
        }

        // Load relationship metadata
        let mut stmt = self.conn.prepare("SELECT rel_id, key, value FROM relationship_metadata")?;
        let rel_meta_rows = stmt.query_map([], |row| {
            let rel_id: String = row.get(0)?;
            let key: String = row.get(1)?;
            let value: String = row.get(2)?;
            Ok((rel_id, key, value))
        })?;

        for row in rel_meta_rows {
            let (rel_id_str, key, value) = row?;
            if let Ok(rel_id) = Uuid::parse_str(&rel_id_str) {
                if let Some(rel) = db.relationships.get_mut(&rel_id) {
                    rel.metadata.insert(key, value);
                }
            }
        }

        // Load positions
        let mut stmt = self.conn.prepare("SELECT node_id, x, y FROM node_positions")?;
        let pos_rows = stmt.query_map([], |row| {
            let node_id: String = row.get(0)?;
            let x: f32 = row.get(1)?;
            let y: f32 = row.get(2)?;
            Ok((node_id, x, y))
        })?;

        for row in pos_rows {
            let (node_id_str, x, y) = row?;
            if let Ok(node_id) = Uuid::parse_str(&node_id_str) {
                positions.insert(node_id, egui::pos2(x, y));
            }
        }

        // Load graph state
        let mut pan = egui::Vec2::ZERO;
        let mut zoom = 1.0f32;

        let mut stmt = self.conn.prepare("SELECT key, value FROM graph_state")?;
        let state_rows = stmt.query_map([], |row| {
            let key: String = row.get(0)?;
            let value: String = row.get(1)?;
            Ok((key, value))
        })?;

        for row in state_rows {
            let (key, value) = row?;
            match key.as_str() {
                "pan_x" => pan.x = value.parse().unwrap_or(0.0),
                "pan_y" => pan.y = value.parse().unwrap_or(0.0),
                "zoom" => zoom = value.parse().unwrap_or(1.0),
                _ => {}
            }
        }

        // Rebuild label index
        db.rebuild_indices();

        Ok((db, positions, pan, zoom))
    }

    /// Full-text search for nodes matching a query string.
    /// Searches node labels and metadata values.
    pub fn search_nodes(&self, query: &str) -> rusqlite::Result<Vec<NodeId>> {
        let mut stmt = self.conn.prepare(
            "SELECT node_id FROM nodes_fts WHERE nodes_fts MATCH ?1"
        )?;
        let rows = stmt.query_map([query], |row| {
            let id_str: String = row.get(0)?;
            Ok(id_str)
        })?;

        let mut results = Vec::new();
        for row in rows {
            if let Ok(id_str) = row {
                if let Ok(id) = Uuid::parse_str(&id_str) {
                    results.push(id);
                }
            }
        }
        Ok(results)
    }

    /// Find nodes within a rectangular viewport (spatial query).
    /// Uses R-tree index for efficient lookup.
    pub fn find_nodes_in_rect(&self, min_x: f32, min_y: f32, max_x: f32, max_y: f32) -> rusqlite::Result<Vec<NodeId>> {
        let mut stmt = self.conn.prepare(
            "SELECT m.node_id FROM node_positions_rtree r 
             JOIN rtree_node_map m ON r.rowid = m.rowid 
             WHERE r.min_x >= ?1 AND r.max_x <= ?2 AND r.min_y >= ?3 AND r.max_y <= ?4"
        )?;
        let rows = stmt.query_map(params![min_x, max_x, min_y, max_y], |row| {
            let id_str: String = row.get(0)?;
            Ok(id_str)
        })?;

        let mut results = Vec::new();
        for row in rows {
            if let Ok(id_str) = row {
                if let Ok(id) = Uuid::parse_str(&id_str) {
                    results.push(id);
                }
            }
        }
        Ok(results)
    }

    /// Save embedding model state (vocabulary, IDF weights, etc.)
    pub fn save_embedding_model_state(&self, key: &str, data: &[u8]) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO embedding_model_state (key, value) VALUES (?1, ?2)",
            params![key, data],
        )?;
        Ok(())
    }

    /// Load embedding model state
    pub fn load_embedding_model_state(&self, key: &str) -> rusqlite::Result<Option<Vec<u8>>> {
        let mut stmt = self.conn.prepare("SELECT value FROM embedding_model_state WHERE key = ?1")?;
        let result = stmt.query_row([key], |row| row.get(0));
        match result {
            Ok(data) => Ok(Some(data)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }


    // ==================== Per-Model Embedding Methods ====================

    /// Get the table name for a specific model type
    fn embedding_table_for_model(model_type: &str) -> &'static str {
        match model_type {
            "tfidf" => "embeddings_tfidf",
            "word2vec" => "embeddings_word2vec",
            "onnx" => "embeddings_onnx",
            _ => "embeddings_tfidf", // fallback to tfidf table
        }
    }

    /// Save a node's embedding to the model-specific table
    pub fn save_model_embedding(&self, node_id: NodeId, model_type: &str, embedding: &[f32]) -> rusqlite::Result<()> {
        let embedding_bytes: Vec<u8> = embedding.iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!(
            "INSERT OR REPLACE INTO {} (node_id, embedding, updated_at) VALUES (?1, ?2, ?3)",
            table
        );
        self.conn.execute(&sql, params![node_id.to_string(), embedding_bytes, now])?;
        Ok(())
    }

    /// Load a node's embedding from the model-specific table
    pub fn load_model_embedding(&self, node_id: NodeId, model_type: &str) -> rusqlite::Result<Option<Vec<f32>>> {
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!("SELECT embedding FROM {} WHERE node_id = ?1", table);
        let mut stmt = self.conn.prepare(&sql)?;
        let result = stmt.query_row([node_id.to_string()], |row| {
            let embedding_bytes: Vec<u8> = row.get(0)?;
            Ok(embedding_bytes)
        });
        
        match result {
            Ok(bytes) => {
                let embedding: Vec<f32> = bytes.chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                Ok(Some(embedding))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Load all embeddings for a specific model type
    pub fn load_all_model_embeddings(&self, model_type: &str) -> rusqlite::Result<HashMap<NodeId, Vec<f32>>> {
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!("SELECT node_id, embedding FROM {}", table);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let node_id_str: String = row.get(0)?;
            let embedding_bytes: Vec<u8> = row.get(1)?;
            Ok((node_id_str, embedding_bytes))
        })?;

        let mut result = HashMap::new();
        for row in rows {
            let (node_id_str, bytes) = row?;
            if let Ok(node_id) = Uuid::parse_str(&node_id_str) {
                let embedding: Vec<f32> = bytes.chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                result.insert(node_id, embedding);
            }
        }
        Ok(result)
    }

    /// Clear embeddings for a specific model type only
    pub fn clear_model_embeddings(&self, model_type: &str) -> rusqlite::Result<()> {
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!("DELETE FROM {}", table);
        self.conn.execute(&sql, [])?;
        // Also clear model state for this model
        self.conn.execute(
            "DELETE FROM embedding_model_state WHERE key LIKE ?1",
            [format!("{}%", model_type)],
        )?;
        Ok(())
    }

    /// Get embedding count for a specific model
    pub fn count_model_embeddings(&self, model_type: &str) -> rusqlite::Result<usize> {
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!("SELECT COUNT(*) FROM {}", table);
        let count: i64 = self.conn.query_row(&sql, [], |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Check if embeddings exist for a specific model
    pub fn has_model_embeddings(&self, model_type: &str) -> rusqlite::Result<bool> {
        Ok(self.count_model_embeddings(model_type)? > 0)
    }

    /// Delete a specific node's embedding from model-specific table
    pub fn delete_model_embedding(&self, node_id: NodeId, model_type: &str) -> rusqlite::Result<()> {
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!("DELETE FROM {} WHERE node_id = ?1", table);
        self.conn.execute(&sql, [node_id.to_string()])?;
        Ok(())
    }

    /// Batch save embeddings for better performance
    pub fn save_model_embeddings_batch(&mut self, model_type: &str, embeddings: &[(NodeId, Vec<f32>)]) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        let table = Self::embedding_table_for_model(model_type);
        let sql = format!(
            "INSERT OR REPLACE INTO {} (node_id, embedding, updated_at) VALUES (?1, ?2, ?3)",
            table
        );
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        {
            let mut stmt = tx.prepare(&sql)?;
            for (node_id, embedding) in embeddings {
                let embedding_bytes: Vec<u8> = embedding.iter()
                    .flat_map(|f| f.to_le_bytes())
                    .collect();
                stmt.execute(params![node_id.to_string(), embedding_bytes, now])?;
            }
        }
        tx.commit()
    }

    /// Checkpoint WAL to main database file (for clean shutdown)
    pub fn checkpoint(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    /// Optimize database (vacuum and analyze)
    pub fn optimize(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA optimize;
            VACUUM;
            ANALYZE;
            "#
        )?;
        Ok(())
    }

    /// Get database statistics for monitoring
    pub fn get_stats(&self) -> rusqlite::Result<DbStats> {
        let node_count: i64 = self.conn.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
        let rel_count: i64 = self.conn.query_row("SELECT COUNT(*) FROM relationships", [], |row| row.get(0))?;
        let tfidf_count: i64 = self.conn.query_row("SELECT COUNT(*) FROM embeddings_tfidf", [], |row| row.get(0))?;
        let word2vec_count: i64 = self.conn.query_row("SELECT COUNT(*) FROM embeddings_word2vec", [], |row| row.get(0))?;
        let onnx_count: i64 = self.conn.query_row("SELECT COUNT(*) FROM embeddings_onnx", [], |row| row.get(0))?;
        
        Ok(DbStats {
            node_count: node_count as usize,
            relationship_count: rel_count as usize,
            tfidf_embeddings: tfidf_count as usize,
            word2vec_embeddings: word2vec_count as usize,
            onnx_embeddings: onnx_count as usize,
        })
    }
}

/// Database statistics for monitoring
#[derive(Debug, Clone)]
pub struct DbStats {
    pub node_count: usize,
    pub relationship_count: usize,
    pub tfidf_embeddings: usize,
    pub word2vec_embeddings: usize,
    pub onnx_embeddings: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_roundtrip() {
        let mut storage = SqliteStorage::open_in_memory().unwrap();
        
        let mut db = GraphDatabase::new();
        let node1 = db.add_node("Person".to_string(), [("name".to_string(), "Alice".to_string())].into());
        let node2 = db.add_node("Person".to_string(), [("name".to_string(), "Bob".to_string())].into());
        db.add_relationship(node1, node2, "KNOWS".to_string(), HashMap::new());

        let mut positions = HashMap::new();
        positions.insert(node1, egui::pos2(100.0, 200.0));
        positions.insert(node2, egui::pos2(300.0, 400.0));

        let pan = egui::vec2(50.0, 75.0);
        let zoom = 1.5;

        storage.save_graph(&db, &positions, pan, zoom).unwrap();

        let (loaded_db, loaded_pos, loaded_pan, loaded_zoom) = storage.load_graph().unwrap();

        assert_eq!(loaded_db.nodes.len(), 2);
        assert_eq!(loaded_db.relationships.len(), 1);
        assert_eq!(loaded_pos.len(), 2);
        assert!((loaded_pan.x - 50.0).abs() < 0.001);
        assert!((loaded_pan.y - 75.0).abs() < 0.001);
        assert!((loaded_zoom - 1.5).abs() < 0.001);

        // Check metadata was preserved
        let alice = loaded_db.nodes.values().find(|n| n.metadata.get("name") == Some(&"Alice".to_string()));
        assert!(alice.is_some());
    }
}
