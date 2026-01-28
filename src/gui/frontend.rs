#![allow(clippy::collapsible_if)]
#![allow(clippy::needless_return)]
#![allow(clippy::excessive_precision)]
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};

use eframe::egui::{self, Color32, Pos2, Rect, Sense, Stroke, Vec2};
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use crate::persistence::persist::{self, AppStateFile};
use crate::persistence::settings::AppSettings;
use crate::gql::query_interface::{self, QueryResultRow};
use crate::api::{self, ApiRequest};

// Export matched nodes
fn export_nodes_json(db: &GraphDatabase, ids: &[NodeId], path: &std::path::Path) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;
    #[derive(serde::Serialize)]
    struct NodeOut<'a> {
        id: &'a Uuid,
        label: &'a str,
        metadata: &'a HashMap<String, String>,
    }
    let mut out: Vec<NodeOut> = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(n) = db.get_node(*id) {
            out.push(NodeOut { id: &n.id, label: &n.label, metadata: &n.metadata });
        }
    }
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let f = File::create(path)?;
    serde_json::to_writer_pretty(f, &out).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    // ensure newline at end
    let mut f2 = std::fs::OpenOptions::new().append(true).open(path)?;
    let _ = f2.write_all(b"\n");
    Ok(())
}

fn export_nodes_csv(db: &GraphDatabase, ids: &[NodeId], path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let mut wtr = csv::Writer::from_path(path)?;
    // headers: id,label,metadata_json
    wtr.write_record(["id", "label", "metadata_json"]) ?;
    for id in ids {
        if let Some(n) = db.get_node(*id) {
            let meta_json = serde_json::to_string(&n.metadata).unwrap_or_else(|_| "{}".into());
            wtr.write_record(&[n.id.to_string(), n.label.clone(), meta_json])?;
        }
    }
    wtr.flush()?;
    Ok(())
}

// Helpers for exporting the entire graph
fn export_graph_json(db: &GraphDatabase, path: &std::path::Path) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::Write;
    #[derive(serde::Serialize)]
    struct RelRef<'a> {
        rel_id: &'a uuid::Uuid,
        label: &'a str,
        peer: &'a uuid::Uuid,
        direction: &'a str, // "out" or "in"
    }
    #[derive(serde::Serialize)]
    struct NodeOut<'a> {
        id: &'a uuid::Uuid,
        label: &'a str,
        metadata: &'a HashMap<String, String>,
        out_rels: Vec<RelRef<'a>>,
        in_rels: Vec<RelRef<'a>>,
    }
    #[derive(serde::Serialize)]
    struct RelOut<'a> {
        id: &'a uuid::Uuid,
        from: &'a uuid::Uuid,
        to: &'a uuid::Uuid,
        label: &'a str,
        metadata: &'a HashMap<String, String>,
    }
    #[derive(serde::Serialize)]
    struct GraphOut<'a> {
        nodes: Vec<NodeOut<'a>>,
        relationships: Vec<RelOut<'a>>,
    }

    let mut node_outs: Vec<NodeOut> = Vec::with_capacity(db.nodes.len());
    for (_id, node) in db.nodes.iter() {
        let mut out_rels: Vec<RelRef> = Vec::new();
        let mut in_rels: Vec<RelRef> = Vec::new();
        for rel in db.relationships.values() {
            if rel.from_node == node.id {
                out_rels.push(RelRef { rel_id: &rel.id, label: &rel.label, peer: &rel.to_node, direction: "out" });
            } else if rel.to_node == node.id {
                in_rels.push(RelRef { rel_id: &rel.id, label: &rel.label, peer: &rel.from_node, direction: "in" });
            }
        }
        node_outs.push(NodeOut { id: &node.id, label: &node.label, metadata: &node.metadata, out_rels, in_rels });
    }
    let mut rel_outs: Vec<RelOut> = Vec::with_capacity(db.relationships.len());
    for (_rid, rel) in db.relationships.iter() {
        rel_outs.push(RelOut { id: &rel.id, from: &rel.from_node, to: &rel.to_node, label: &rel.label, metadata: &rel.metadata });
    }
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let f = File::create(path)?;
    let g = GraphOut { nodes: node_outs, relationships: rel_outs };
    serde_json::to_writer_pretty(f, &g).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    // newline at end
    let mut f2 = std::fs::OpenOptions::new().append(true).open(path)?;
    let _ = f2.write_all(b"\n");
    Ok(())
}

fn export_graph_csv(db: &GraphDatabase, base_path: &std::path::Path) -> std::io::Result<(std::path::PathBuf, std::path::PathBuf)> {
    // Derive nodes/relationships file paths from base
    let parent = base_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent)?;
    let stem = base_path.file_stem().and_then(|s| s.to_str()).unwrap_or("graph");
    let nodes_path = parent.join(format!("{}_nodes.csv", stem));
    let rels_path = parent.join(format!("{}_relationships.csv", stem));
    // Write nodes CSV: id,label,metadata_json,out_rels_json,in_rels_json
    {
        let mut wtr = csv::Writer::from_path(&nodes_path)?;
        wtr.write_record(["id", "label", "metadata_json", "out_rels_json", "in_rels_json"])?;
        for (_id, n) in db.nodes.iter() {
            let meta_json = serde_json::to_string(&n.metadata).unwrap_or_else(|_| "{}".into());
            let mut out_refs: Vec<serde_json::Value> = Vec::new();
            let mut in_refs: Vec<serde_json::Value> = Vec::new();
            for rel in db.relationships.values() {
                if rel.from_node == n.id {
                    out_refs.push(serde_json::json!({"rel_id": rel.id, "label": rel.label, "to": rel.to_node}));
                } else if rel.to_node == n.id {
                    in_refs.push(serde_json::json!({"rel_id": rel.id, "label": rel.label, "from": rel.from_node}));
                }
            }
            let out_json = serde_json::to_string(&out_refs).unwrap_or_else(|_| "[]".into());
            let in_json = serde_json::to_string(&in_refs).unwrap_or_else(|_| "[]".into());
            wtr.write_record(&[n.id.to_string(), n.label.clone(), meta_json, out_json, in_json])?;
        }
        wtr.flush()?;
    }
    // Write relationships CSV: id,from,to,label,metadata_json
    {
        let mut wtr = csv::Writer::from_path(&rels_path)?;
        wtr.write_record(["id", "from", "to", "label", "metadata_json"])?;
        for (_rid, r) in db.relationships.iter() {
            let meta_json = serde_json::to_string(&r.metadata).unwrap_or_else(|_| "{}".into());
            wtr.write_record(&[r.id.to_string(), r.from_node.to_string(), r.to_node.to_string(), r.label.clone(), meta_json])?;
        }
        wtr.flush()?;
    }
    Ok((nodes_path, rels_path))
}

// Style for toast notifications
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
enum NoticeStyle {
    Subtle,
    Prominent,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SelectedItem {
    Node(NodeId),
    Rel(Uuid),
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PickTarget {
    From,
    To,
    // Used when creating a brand-new node and pre-linking it to an existing node
    NewNodeTarget,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum NewNodeRelDir {
    NewToExisting,
    ExistingToNew,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SidebarMode {
    Tooling,
    Query,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum PrefsTab {
    App,
    Api,
}

pub struct GraphApp {
    db: GraphDatabase,
    node_positions: HashMap<NodeId, Pos2>,
    // Per-node velocities (for smooth, damped motion)
    node_velocities: HashMap<NodeId, Vec2>,
    // When physics-based convergence started; stop after timeout
    converge_start: Option<Instant>,
    selected: Option<SelectedItem>,
    dragging: Option<NodeId>,
    pan: Vec2,
    zoom: f32,
    // persistence
    dirty: bool,
    last_change: Instant,
    last_save: Instant,
    save_error: Option<String>,
    last_save_info: Option<String>,
    // Timestamp for transient info banner (e.g., "Saved" toast)
    last_info_time: Option<Instant>,
    // Visual style for the transient info toast
    last_info_style: NoticeStyle,
    show_load_versions: bool,
    // Sidebar visibility
    sidebar_open: bool,
    sidebar_mode: SidebarMode,
    // Sidebar density
    sidebar_compact: bool,
    // Remember last canvas rect to place newly created nodes near the origin
    last_canvas_rect: Option<Rect>,
    // Track multiple open pop-out windows
    open_node_windows: BTreeSet<NodeId>,
    open_rel_windows: BTreeSet<Uuid>,
    // Creation forms state
    create_node_label: String,
    create_node_meta: Vec<(String, String)>,
    create_rel_label: String,
    create_rel_from: Option<NodeId>,
    create_rel_to: Option<NodeId>,
    create_rel_meta: Vec<(String, String)>,
    create_rel_display_key: String,
    pick_target: Option<PickTarget>,
    // Preemptive relationship when creating a new node
    create_node_rel_enabled: bool,
    create_node_rel_direction: NewNodeRelDir,
    create_node_rel_label: String,
    create_node_rel_target: Option<NodeId>,
    pending_new_node_for_link: Option<NodeId>,
    // Per-window edit helpers
    node_label_edits: HashMap<NodeId, String>,
    node_meta_new_kv: HashMap<NodeId, (String, String)>,
    rel_label_edits: HashMap<Uuid, String>,
    rel_meta_new_kv: HashMap<Uuid, (String, String)>,
    // Bulk edit / multi-select state
    multi_select_active: bool,
    multi_selected_nodes: HashSet<NodeId>,
    // Rectangle (rubber-band) selection while in multi-select mode
    rect_select_start: Option<Pos2>,
    rect_select_current: Option<Pos2>,
    bulk_add_key: String,
    bulk_add_value: String,
    bulk_delete_keys: String,
    bulk_status: Option<String>,
    // Confirm modals
    confirm_mass_delete: bool,
    // Query console state
    query_text: String,
    query_history: Vec<String>,
    query_output: Vec<String>,
    last_query_error: Option<String>,
    // Query matches highlighting
    query_selected_nodes: HashSet<NodeId>,
    query_selected_rels: HashSet<Uuid>,
    // Export options for query matches
    query_export_is_json: bool,
    query_export_path: String,
    query_export_status: Option<String>,
    // Export entire graph modal
    show_export_all_window: bool,
    export_all_is_json: bool,
    export_all_path: String,
    export_all_status: Option<String>,
    // Query suggestions
    query_suggest_visible: bool,
    query_suggest_items: Vec<String>,
    query_suggest_index: usize,
    query_suggest_hover_index: Option<usize>,
    // Layout control
    re_cluster_pending: bool,
    // Cluster convergence controls (helps separate large groups visually)
    _cluster_converge_enabled: bool,
    _cluster_converge_threshold: usize,
    _cluster_converge_strength: f32,
    gravity_enabled: bool,
    gravity_strength: f32,
    // Center-of-mass (COM) local gravity settings
    com_gravity_radius: f32,         // within this radius, prefer attraction to local COM
    com_gravity_min_neighbors: usize, // minimum nearby nodes to switch from global to local COM
    hub_repulsion_scale: f32,
    // Level-of-detail (LOD) rendering controls
    lod_enabled: bool,
    lod_label_min_zoom: f32,
    lod_hide_labels_node_threshold: usize,
    // Edge label readability controls
    _edge_labels_enabled: bool,
    _edge_labels_only_on_hover: bool,
    edge_label_min_zoom: f32,
    edge_label_count_threshold: usize,
    edge_label_bg_alpha: u8,
    // Focus/hover state for dimming/highlighting
    hover_node: Option<NodeId>,
    // Transient zoom HUD (show current zoom briefly when scrolling)
    zoom_hud_until: Option<Instant>,
    // App settings and Preferences UI state
    app_settings: AppSettings,
    show_prefs_window: bool,
    prefs_edit: AppSettings,
    prefs_status: Option<String>,
    prefs_autosave_override_str: String,
    // Preferences: export directory override editor buffer
    prefs_export_override_str: String,
    // Preferences: which tab is active in the Preferences window
    prefs_tab: PrefsTab,
    // Preferences: API log directory override editor buffer
    prefs_api_log_override_str: String,
    // API server runtime
    api_rx: Option<Receiver<ApiRequest>>,
    api_running: bool,
    // Prevention for immediate re-open loop
    last_background_time: Option<Instant>,
    first_focused_observed: Option<Instant>,
}

impl GraphApp {
    pub fn new(db: GraphDatabase) -> Self {
        let settings = AppSettings::load().unwrap_or_default();
        let mut s = Self {
            db,
            node_positions: HashMap::new(),
            node_velocities: HashMap::new(),
            converge_start: Some(Instant::now()),
            selected: None,
            dragging: None,
            pan: Vec2::ZERO,
            zoom: 1.0,
            dirty: false,
            last_change: Instant::now(),
            last_save: Instant::now(),
            save_error: None,
            last_save_info: None,
            last_info_time: None,
            last_info_style: NoticeStyle::Prominent,
            show_load_versions: false,
            sidebar_open: true,
            sidebar_mode: SidebarMode::Tooling,
            sidebar_compact: true,
            last_canvas_rect: None,
            open_node_windows: BTreeSet::new(),
            open_rel_windows: BTreeSet::new(),
            create_node_label: String::new(),
            create_node_meta: vec![],
            create_rel_label: String::new(),
            create_rel_from: None,
            create_rel_to: None,
            create_rel_meta: vec![],
            create_rel_display_key: String::new(),
            pick_target: None,
            create_node_rel_enabled: false,
            create_node_rel_direction: NewNodeRelDir::NewToExisting,
            create_node_rel_label: String::from("REL"),
            create_node_rel_target: None,
            pending_new_node_for_link: None,
            node_label_edits: HashMap::new(),
            node_meta_new_kv: HashMap::new(),
            rel_label_edits: HashMap::new(),
            rel_meta_new_kv: HashMap::new(),
            multi_select_active: false,
            multi_selected_nodes: HashSet::new(),
            rect_select_start: None,
            rect_select_current: None,
            bulk_add_key: String::new(),
            bulk_add_value: String::new(),
            bulk_delete_keys: String::new(),
            bulk_status: None,
            confirm_mass_delete: false,
            query_text: String::new(),
            query_history: Vec::new(),
            query_output: Vec::new(),
            last_query_error: None,
            query_selected_nodes: HashSet::new(),
            query_selected_rels: HashSet::new(),
            query_export_is_json: true,
            query_export_path: String::new(),
            query_export_status: None,
            show_export_all_window: false,
            export_all_is_json: true,
            export_all_path: String::new(),
            export_all_status: None,
            query_suggest_visible: false,
            query_suggest_items: Vec::new(),
            query_suggest_index: 0,
            query_suggest_hover_index: None,
            re_cluster_pending: true,
            _cluster_converge_enabled: false, // deprecated in favor of gravity/repulsion aids
            _cluster_converge_threshold: 30,
            _cluster_converge_strength: 3.0,
            gravity_enabled: false,
            gravity_strength: 6.0,
            com_gravity_radius: 150.0,
            com_gravity_min_neighbors: 2,
            hub_repulsion_scale: 1.0,
            lod_enabled: true,
            lod_label_min_zoom: 0.7,
            lod_hide_labels_node_threshold: 200,
            _edge_labels_enabled: true,
            _edge_labels_only_on_hover: false,
            edge_label_min_zoom: 0.8,
            edge_label_count_threshold: 500,
            edge_label_bg_alpha: 170,
            hover_node: None,
            zoom_hud_until: None,
            app_settings: settings.clone(),
            show_prefs_window: false,
            prefs_edit: AppSettings::default(),
            prefs_status: None,
            prefs_autosave_override_str: String::new(),
            prefs_export_override_str: String::new(),
            prefs_tab: PrefsTab::App,
            prefs_api_log_override_str: String::new(),
            api_rx: None,
            api_running: false,
            last_background_time: None,
            first_focused_observed: None,
        };
        // Apply settings to runtime toggles
        s.lod_enabled = s.app_settings.lod_enabled;
        s.lod_label_min_zoom = s.app_settings.lod_label_min_zoom;
        s.lod_hide_labels_node_threshold = s.app_settings.lod_hide_labels_node_threshold;
        // Initialize API broker and server based on settings
        let rx = api::init_broker();
        s.api_rx = Some(rx);
        if s.app_settings.api_enabled {
            let _ = api::server::start_server(&s.app_settings);
        }
        if s.app_settings.grpc_enabled {
            let _ = api::grpc::start_grpc_server(&s.app_settings);
        }
        if s.app_settings.api_enabled || s.app_settings.grpc_enabled {
            s.api_running = true;
        }
        s
    }

    fn ensure_layout(&mut self, rect: Rect) {
        if self.node_positions.len() == self.db.nodes.len() {
            return;
        }

        // Community-aware initial layout for nodes missing positions.
        // Existing positions (e.g., from manual drags or previous sessions) are preserved.
        let cluster_positions = self.compute_community_layout(rect);

        // Fill in only nodes that are currently missing a position.
        let mut missing: Vec<NodeId> = self
            .db
            .nodes
            .keys()
            .filter(|id| !self.node_positions.contains_key(id))
            .copied()
            .collect();
        missing.sort();

        if missing.is_empty() {
            return;
        }

        let center = rect.center();
        for id in missing {
            if let Some(p) = cluster_positions.get(&id).copied() {
                self.node_positions.insert(id, p);
            } else {
                // Fallback to golden-spiral if clustering somehow missed this node
                let k = self.node_positions.len() as u32;
                let pos = golden_spiral_position(center, k, rect);
                self.node_positions.insert(id, pos);
            }
        }

        // After assigning positions for missing nodes, resolve any overlaps
        self.resolve_overlaps(rect);
        // Restart convergence timer since positions changed
        self.converge_start = Some(Instant::now());
    }

    fn apply_cluster_layout_all(&mut self, rect: Rect) {
        let cluster_positions = self.compute_community_layout(rect);
        let center = rect.center();
        for id in self.db.nodes.keys().copied() {
            let p = cluster_positions.get(&id).copied().unwrap_or(center);
            self.node_positions.insert(id, p);
        }
        // Ensure nodes are not overlapping after layout
        self.resolve_overlaps(rect);
        self.re_cluster_pending = false;
        // Restart convergence timer for fresh layout
        self.converge_start = Some(Instant::now());
        self.mark_dirty();
    }

    // Compute a community-based layout for all nodes without overriding existing positions.
    // - Communities are detected via simple label propagation, with extra similarity from labels and metadata overlaps.
    // - Dense communities are placed closer to the border; sparse nodes are biased toward the center.
    fn compute_community_layout(&self, rect: Rect) -> HashMap<NodeId, Pos2> {
        use std::collections::{HashMap as Map, HashSet as Set};

        // Build adjacency and degree
        let mut neighbors: Map<NodeId, Vec<NodeId>> = Map::new();
        for id in self.db.nodes.keys() {
            neighbors.entry(*id).or_default();
        }
        for rel in self.db.relationships.values() {
            neighbors.entry(rel.from_node).or_default().push(rel.to_node);
            neighbors.entry(rel.to_node).or_default().push(rel.from_node);
        }

        // Precompute label/meta for similarity
        let mut node_label: Map<NodeId, String> = Map::new();
        let mut node_meta: Map<NodeId, Map<String, String>> = Map::new();
        for (id, n) in &self.db.nodes {
            node_label.insert(*id, n.label.clone());
            node_meta.insert(*id, n.metadata.clone());
        }

        // Initialize labels (each node in its own community)
        let mut community: Map<NodeId, NodeId> = Map::new();
        for id in self.db.nodes.keys() {
            community.insert(*id, *id);
        }

        // Helper: compute similarity weight between two nodes
        let mut sim_cache: Map<(NodeId, NodeId), f32> = Map::new();
        let similarity = |a: NodeId, b: NodeId, sim_cache: &mut Map<(NodeId, NodeId), f32>| -> f32 {
            if let Some(v) = sim_cache.get(&(a, b)) { return *v; }
            let la = node_label.get(&a).map(|s| s.as_str()).unwrap_or("");
            let lb = node_label.get(&b).map(|s| s.as_str()).unwrap_or("");
            let label_bonus = if la == lb && !la.is_empty() { 1.0 } else { 0.0 };
            let ma = node_meta.get(&a);
            let mb = node_meta.get(&b);
            let mut meta_overlap = 0.0f32;
            if let (Some(ma), Some(mb)) = (ma, mb) {
                // simple key/value overlap count
                let mut count = 0usize;
                let total = ma.len().max(1);
                for (k, va) in ma {
                    if let Some(vb) = mb.get(k) {
                        if vb == va { count += 1; }
                    }
                }
                // normalize by max meta size to bound in [0,1]
                meta_overlap = (count as f32) / (total as f32);
            }
            // base weight for an edge is 1.0, plus label/meta bonuses when neighbors are similar
            let w = 1.0 + 0.75 * label_bonus + 0.5 * meta_overlap;
            sim_cache.insert((a, b), w);
            w
        };

        // Label propagation iterations
        let mut order: Vec<NodeId> = self.db.nodes.keys().copied().collect();
        order.sort();
        for _iter in 0..8 { // few iterations for stability
            let mut changed = false;
            for &u in &order {
                let mut scores: Map<NodeId, f32> = Map::new();
                for &v in neighbors.get(&u).unwrap_or(&Vec::new()) {
                    let c = *community.get(&v).unwrap_or(&v);
                    let w = similarity(u, v, &mut sim_cache);
                    *scores.entry(c).or_insert(0.0) += w;
                }
                if let Some((&best_comm, _)) = scores
                    .iter()
                    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                {
                    let cur = community.get(&u).copied().unwrap_or(u);
                    if best_comm != cur {
                        community.insert(u, best_comm);
                        changed = true;
                    }
                }
            }
            if !changed { break; }
        }

        // Group nodes by community
        let mut groups: Map<NodeId, Vec<NodeId>> = Map::new();
        for (n, c) in &community {
            groups.entry(*c).or_default().push(*n);
        }

        // Compute internal degree per node and per community density
        let mut degree: Map<NodeId, usize> = Map::new();
        for (u, nbrs) in &neighbors {
            degree.insert(*u, nbrs.len());
        }

        let mut comm_density: Map<NodeId, f32> = Map::new();
        for (c, nodes) in &groups {
            let s: Set<NodeId> = nodes.iter().copied().collect();
            let mut internal_edges = 0usize;
            let mut possible_edges = nodes.len().saturating_sub(1) * nodes.len() / 2; // undirected approximation
            if possible_edges == 0 { possible_edges = 1; }
            for &u in nodes {
                for &v in neighbors.get(&u).unwrap_or(&Vec::new()) {
                    if s.contains(&v) { internal_edges += 1; }
                }
            }
            // undirected correction
            let internal_undirected = internal_edges as f32 / 2.0;
            comm_density.insert(*c, (internal_undirected) / (possible_edges as f32));
        }

        // Place community centroids around a circle; radius based on density
        let center = rect.center();
        let min_dim = rect.width().min(rect.height());
        let max_radius = 0.46 * min_dim; // near border
        let min_radius = 0.12 * min_dim; // closer to center for sparse ones

        // Sort communities for stable placement
        let mut comm_ids: Vec<NodeId> = groups.keys().copied().collect();
        comm_ids.sort();
        let comm_count = comm_ids.len().max(1) as f32;

        let mut comm_centroids: Map<NodeId, Pos2> = Map::new();
        for (idx, cid) in comm_ids.iter().enumerate() {
            let density = *comm_density.get(cid).unwrap_or(&0.0);
            let r = min_radius + (max_radius - min_radius) * density.clamp(0.0, 1.0);
            let angle = (idx as f32) * (std::f32::consts::TAU / comm_count);
            let pos = Pos2::new(center.x + r * angle.cos(), center.y + r * angle.sin());
            comm_centroids.insert(*cid, pos);
        }

        // Within each community, spread nodes around its centroid
        let mut out: Map<NodeId, Pos2> = Map::new();
        for (cid, nodes) in &groups {
            let centroid = *comm_centroids
                .get(cid)
                .unwrap_or(&center); // fallback to center if missing (shouldn't happen)
            let n = nodes.len().max(1) as f32;
            // local radius scales with community size while also being capped
            let local_r_base = (min_dim * 0.08).min(30.0 + 6.0 * n.sqrt());
            let mut local_nodes = nodes.clone();
            local_nodes.sort();
            for (i, node) in local_nodes.iter().enumerate() {
                let deg = *degree.get(node).unwrap_or(&0) as f32;
                // Sparse nodes closer to center: lerp toward global center based on low degree
                let deg_factor = (deg / 6.0).clamp(0.0, 1.0); // >6 neighbors => strong
                let toward_center = 1.0 - deg_factor; // low degree -> higher pull

                let angle = (i as f32) * (std::f32::consts::TAU / n);
                let local_r = local_r_base * (0.6 + 0.6 * deg_factor); // higher degree slightly farther within cluster
                let p_cluster = Pos2::new(centroid.x + local_r * angle.cos(), centroid.y + local_r * angle.sin());
                let p = Pos2::new(
                    p_cluster.x * (1.0 - toward_center) + center.x * toward_center,
                    p_cluster.y * (1.0 - toward_center) + center.y * toward_center,
                );
                out.insert(*node, p);
            }
        }

        out
    }

    // Label-centric target layout: place one centroid per distinct node label around a ring,
    // then distribute nodes of that label in a small local spiral around the centroid.
    // Returns a target position per node id.
    #[allow(dead_code)]
    fn compute_label_layout(&self, rect: Rect) -> HashMap<NodeId, Pos2> {
        use std::collections::HashMap as Map;
        let mut by_label: Map<String, Vec<NodeId>> = Map::new();
        for (id, n) in &self.db.nodes {
            by_label.entry(n.label.clone()).or_default().push(*id);
        }
        let labels: Vec<String> = by_label.keys().cloned().collect();
        let k = labels.len().max(1) as f32;
        let center = rect.center();
        let ring_r = 0.35 * rect.width().min(rect.height());
        let mut centroid_for_label: Map<String, Pos2> = Map::new();
        for (i, lab) in labels.iter().enumerate() {
            let angle = (i as f32) * (std::f32::consts::TAU / k);
            let cx = center.x + ring_r * angle.cos();
            let cy = center.y + ring_r * angle.sin();
            centroid_for_label.insert(lab.clone(), Pos2::new(cx, cy));
        }
        // Distribute per-label nodes around its centroid
        let mut targets: Map<NodeId, Pos2> = Map::new();
        for (lab, ids) in by_label {
            if let Some(c) = centroid_for_label.get(&lab).copied() {
                // local radius scaled by group size
                let group_n = ids.len().max(1) as f32;
                let local_radius = 30.0_f32 + 3.0 * group_n.sqrt();
                for (j, nid) in ids.iter().enumerate() {
                    let jj = j as f32;
                    // golden angle spiral for even spread
                    let ang = jj * 2.39996323; // ~137.5°
                    let r = (jj + 1.0).sqrt() * (local_radius / group_n.sqrt().max(1.0));
                    let p = Pos2::new(c.x + r * ang.cos(), c.y + r * ang.sin());
                    targets.insert(*nid, p);
                }
            }
        }
        targets
    }

    // Stable color per label, chosen from a small distinct palette via hashing.
    fn color_for_label(label: &str) -> Color32 {
        const PALETTE: [Color32; 12] = [
            Color32::from_rgb(0x7b, 0xa3, 0xff), // blue
            Color32::from_rgb(0xff, 0xa3, 0x7b), // orange
            Color32::from_rgb(0x7b, 0xff, 0xa3), // green
            Color32::from_rgb(0xff, 0x7b, 0xa3), // pink
            Color32::from_rgb(0xa3, 0x7b, 0xff), // violet
            Color32::from_rgb(0xff, 0xe0, 0x7b), // yellow
            Color32::from_rgb(0x7b, 0xff, 0xe0), // teal
            Color32::from_rgb(0xe0, 0x7b, 0xff), // purple
            Color32::from_rgb(0x7b, 0xe0, 0xff), // cyan
            Color32::from_rgb(0xff, 0x7b, 0xe0), // magenta
            Color32::from_rgb(0x9a, 0xcd, 0x32), // yellowgreen
            Color32::from_rgb(0xcd, 0x32, 0x9a), // fuchsia
        ];
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        label.hash(&mut hasher);
        let h = hasher.finish() as usize;
        PALETTE[h % PALETTE.len()]
    }

    // Post-process to ensure nodes are not overlapping. Operates in world space.
    // Uses a simple spatial hash grid and a few iterations of repulsive separation.
    fn resolve_overlaps(&mut self, rect: Rect) {
        use std::collections::HashMap as Map;

        // In world space, a node visual radius is ~10 units (since draw uses 10.0 * zoom for screen radius)
        // We add a small padding to keep labels from colliding too closely.
        let min_dist: f32 = 24.0; // diameter ~20 + padding
        let min_dist_sq = min_dist * min_dist;
        let cell = min_dist; // grid cell size

        // Run a few iterations to settle
        for _step in 0..4 {
            // Build spatial grid: key by (ix, iy)
            let mut grid: Map<(i32, i32), Vec<NodeId>> = Map::new();
            for (&id, &pos) in &self.node_positions {
                let ix = (pos.x / cell).floor() as i32;
                let iy = (pos.y / cell).floor() as i32;
                grid.entry((ix, iy)).or_default().push(id);
            }

            // For each cell, check pairs in this and neighbor cells
            let offsets = [
                (-1, -1), (0, -1), (1, -1),
                (-1,  0), (0,  0), (1,  0),
                (-1,  1), (0,  1), (1,  1),
            ];

            // Collect keys to avoid cloning the whole grid for iteration
            let keys: Vec<(i32, i32)> = grid.keys().cloned().collect();

            for (ix, iy) in keys {
                if let Some(ids) = grid.get(&(ix, iy)) {
                    for (dx, dy) in offsets {
                        let key = (ix + dx, iy + dy);
                        if let Some(neigh_ids) = grid.get(&key) {
                            for &a in ids {
                                for &b in neigh_ids {
                                    if a >= b { continue; } // avoid double-processing and self
                                    
                                    // Use a single borrow check if possible
                                    let (pa, pb) = match (self.node_positions.get(&a), self.node_positions.get(&b)) {
                                        (Some(pa), Some(pb)) => (*pa, *pb),
                                        _ => continue,
                                    };
                                    
                                    let dx = pb.x - pa.x;
                                    let dy = pb.y - pa.y;
                                    let d2 = dx*dx + dy*dy;
                                    if d2 < min_dist_sq && d2 > 1e-6 {
                                        let d = d2.sqrt();
                                        let overlap = (min_dist - d) * 0.5; // split push
                                        let nx = dx / d;
                                        let ny = dy / d;
                                        if let Some(p) = self.node_positions.get_mut(&a) {
                                            p.x -= nx * overlap;
                                            p.y -= ny * overlap;
                                        }
                                        if let Some(p) = self.node_positions.get_mut(&b) {
                                            p.x += nx * overlap;
                                            p.y += ny * overlap;
                                        }
                                    } else if d2 <= 1e-6 {
                                        // Same position: nudge apart deterministically
                                        if let Some(pa_mut) = self.node_positions.get_mut(&a) {
                                            pa_mut.x -= 0.5 * min_dist;
                                            pa_mut.y -= 0.3 * min_dist;
                                        }
                                        if let Some(pb_mut) = self.node_positions.get_mut(&b) {
                                            pb_mut.x += 0.5 * min_dist;
                                            pb_mut.y += 0.3 * min_dist;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Clamp into rect to avoid drifting out of view
            for p in self.node_positions.values_mut() {
                p.x = p.x.clamp(rect.left() + 8.0, rect.right() - 8.0);
                p.y = p.y.clamp(rect.top() + 8.0, rect.bottom() - 8.0);
            }
        }
    }

    pub fn from_state(state: AppStateFile) -> Self {
        let (db, positions, pan, zoom) = state.to_runtime();
        let settings = AppSettings::load().unwrap_or_default();
        let mut s = Self {
            db,
            node_positions: positions,
            node_velocities: HashMap::new(),
            converge_start: Some(Instant::now()),
            selected: None,
            dragging: None,
            pan,
            zoom,
            dirty: false,
            last_change: Instant::now(),
            last_save: Instant::now(),
            save_error: None,
            last_save_info: None,
            last_info_time: None,
            last_info_style: NoticeStyle::Prominent,
            show_load_versions: false,
            sidebar_open: true,
            sidebar_mode: SidebarMode::Tooling,
            sidebar_compact: true,
            last_canvas_rect: None,
            open_node_windows: BTreeSet::new(),
            open_rel_windows: BTreeSet::new(),
            create_node_label: String::new(),
            create_node_meta: vec![],
            create_rel_label: String::new(),
            create_rel_from: None,
            create_rel_to: None,
            create_rel_meta: vec![],
            create_rel_display_key: String::new(),
            pick_target: None,
            create_node_rel_enabled: false,
            create_node_rel_direction: NewNodeRelDir::NewToExisting,
            create_node_rel_label: String::from("REL"),
            create_node_rel_target: None,
            pending_new_node_for_link: None,
            node_label_edits: HashMap::new(),
            node_meta_new_kv: HashMap::new(),
            rel_label_edits: HashMap::new(),
            rel_meta_new_kv: HashMap::new(),
            multi_select_active: false,
            multi_selected_nodes: HashSet::new(),
            rect_select_start: None,
            rect_select_current: None,
            bulk_add_key: String::new(),
            bulk_add_value: String::new(),
            bulk_delete_keys: String::new(),
            bulk_status: None,
            confirm_mass_delete: false,
            query_text: String::new(),
            query_history: Vec::new(),
            query_output: Vec::new(),
            last_query_error: None,
            query_selected_nodes: HashSet::new(),
            query_selected_rels: HashSet::new(),
            query_export_is_json: true,
            query_export_path: String::new(),
            query_export_status: None,
            show_export_all_window: false,
            export_all_is_json: true,
            export_all_path: String::new(),
            export_all_status: None,
            query_suggest_visible: false,
            query_suggest_items: Vec::new(),
            query_suggest_index: 0,
            query_suggest_hover_index: None,
            re_cluster_pending: true,
            _cluster_converge_enabled: false,
            _cluster_converge_threshold: 30,
            _cluster_converge_strength: 3.0,
            gravity_enabled: false,
            gravity_strength: 6.0,
            com_gravity_radius: 150.0,
            com_gravity_min_neighbors: 2,
            hub_repulsion_scale: 1.0,
            lod_enabled: true,
            lod_label_min_zoom: 0.7,
            lod_hide_labels_node_threshold: 200,
            _edge_labels_enabled: true,
            _edge_labels_only_on_hover: false,
            edge_label_min_zoom: 0.8,
            edge_label_count_threshold: 500,
            edge_label_bg_alpha: 170,
            hover_node: None,
            zoom_hud_until: None,
            app_settings: settings.clone(),
            show_prefs_window: false,
            prefs_edit: AppSettings::default(),
            prefs_status: None,
            prefs_autosave_override_str: String::new(),
            prefs_export_override_str: String::new(),
            prefs_tab: PrefsTab::App,
            prefs_api_log_override_str: String::new(),
            api_rx: None,
            api_running: false,
            last_background_time: None,
            first_focused_observed: None,
        };
        // Apply settings to runtime toggles
        s.lod_enabled = s.app_settings.lod_enabled;
        s.lod_label_min_zoom = s.app_settings.lod_label_min_zoom;
        s.lod_hide_labels_node_threshold = s.app_settings.lod_hide_labels_node_threshold;
        // Initialize API broker and server based on settings
        let rx = api::init_broker();
        s.api_rx = Some(rx);
        if s.app_settings.api_enabled {
            let _ = api::server::start_server(&s.app_settings);
        }
        if s.app_settings.grpc_enabled {
            let _ = api::grpc::start_grpc_server(&s.app_settings);
        }
        if s.app_settings.api_enabled || s.app_settings.grpc_enabled {
            s.api_running = true;
        }
        s
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
        self.last_change = Instant::now();
    }

    fn save_now_with(&mut self, style: NoticeStyle) {
        let state = AppStateFile::from_runtime(&self.db, &self.node_positions, self.pan, self.zoom);
        match persist::save_active(&state) {
            Ok(path) => {
                self.dirty = false;
                self.last_save = Instant::now();
                self.save_error = None;
                self.last_save_info = Some(format!("Saved to {}", path.display()));
                self.last_info_time = Some(Instant::now());
                self.last_info_style = style;
            }
            Err(e) => {
                self.save_error = Some(format!("Save failed: {}", e));
            }
        }
    }

    fn save_now(&mut self) { self.save_now_with(NoticeStyle::Prominent); }

    fn save_versioned_now(&mut self) {
        let state = AppStateFile::from_runtime(&self.db, &self.node_positions, self.pan, self.zoom);
        match persist::save_versioned(&state) {
            Ok(path) => {
                self.last_save = Instant::now();
                self.save_error = None;
                self.last_save_info = Some(format!("Saved version {}", path.display()));
                self.last_info_time = Some(Instant::now());
                self.last_info_style = NoticeStyle::Prominent;
            }
            Err(e) => self.save_error = Some(format!("Save version failed: {}", e)),
        }
    }

    /// Clear all selections and related transient UI state
    fn deselect_all(&mut self) {
        self.selected = None;
        self.dragging = None;
        self.hover_node = None;
        self.multi_selected_nodes.clear();
        self.query_selected_nodes.clear();
        self.query_selected_rels.clear();
        self.pick_target = None;
        self.create_rel_from = None;
        self.create_rel_to = None;
        self.pending_new_node_for_link = None;
        self.mark_dirty();
    }

    // Get a node position if present; otherwise, initialize a reasonable default
    // position (golden spiral around canvas center) and return it. This prevents
    // panics when newly created nodes have not yet been laid out by ensure_layout.
    fn get_or_init_position(&mut self, id: NodeId, rect: Rect) -> Pos2 {
        if let Some(p) = self.node_positions.get(&id) {
            return *p;
        }
        let center = rect.center();
        let k = self.node_positions.len() as u32;
        let pos = golden_spiral_position(center, k, rect);
        self.node_positions.insert(id, pos);
        pos
    }

    // Public helpers callable from native (OS) menu integrations
    pub fn menu_save(&mut self) { self.save_now(); }

    pub fn menu_save_version(&mut self) { self.save_versioned_now(); }

    pub fn menu_load_latest(&mut self) {
        match persist::load_active() {
            Ok(Some(state)) => {
                let (db, pos, pan, zoom) = state.to_runtime();
                self.db = db; self.node_positions = pos; self.pan = pan; self.zoom = zoom;
                self.selected = None; self.open_node_windows.clear(); self.open_rel_windows.clear();
                self.dirty = false; self.last_change = Instant::now();
                self.last_save_info = Some("Loaded latest state".into());
                self.last_info_time = Some(Instant::now());
                self.last_info_style = NoticeStyle::Prominent;
                self.save_error = None;
            }
            Ok(None) => { self.save_error = Some("No active state file found".into()); }
            Err(e) => { self.save_error = Some(format!("Load failed: {}", e)); }
        }
    }

    pub fn menu_new_graph(&mut self) {
        // Back up existing graph if it's non-empty
        let had_content = !self.db.nodes.is_empty() || !self.db.relationships.is_empty();
        if had_content { self.save_versioned_now(); }

        // Reset runtime to a fresh, empty graph
        self.db = GraphDatabase::new();
        self.node_positions.clear();
        self.node_velocities.clear();
        self.selected = None;
        self.dragging = None;
        self.open_node_windows.clear();
        self.open_rel_windows.clear();
        self.multi_selected_nodes.clear();
        self.pick_target = None;
        self.create_rel_from = None;
        self.create_rel_to = None;
        self.pending_new_node_for_link = None;
        self.pan = Vec2::ZERO;
        self.zoom = 1.0;
        self.re_cluster_pending = true;
        self.converge_start = Some(Instant::now());
        self.dirty = true;
        self.last_change = Instant::now();
        self.save_error = None;
        self.last_info_time = Some(Instant::now());
        self.last_info_style = NoticeStyle::Prominent;
        self.last_save_info = Some(
            if had_content { "Created new empty graph (backup saved)" } else { "Created new empty graph" }
                .to_string(),
        );
    }

    pub fn menu_reset_view(&mut self) {
        self.pan = Vec2::ZERO;
        self.zoom = 1.0;
        self.mark_dirty();
    }

    pub fn menu_open_prefs(&mut self) {
        // Prepare editable copy and open the window
        self.prefs_edit = self.app_settings.clone();
        self.prefs_autosave_override_str = match &self.prefs_edit.autosave_override {
            Some(p) => p.display().to_string(),
            None => String::new(),
        };
        self.prefs_export_override_str = match &self.prefs_edit.export_override {
            Some(p) => p.display().to_string(),
            None => String::new(),
        };
        self.prefs_api_log_override_str = match &self.prefs_edit.api_log_override {
            Some(p) => p.display().to_string(),
            None => String::new(),
        };
        self.prefs_tab = PrefsTab::App;
        self.prefs_status = None;
        self.show_prefs_window = true;
    }

}

impl eframe::App for GraphApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Detect if the window was shown externally (e.g. by another instance using Win32 API)
        if !crate::gui::app_state::SHOW_WINDOW.load(std::sync::atomic::Ordering::SeqCst) {
            let cooldown_passed = self.last_background_time
                .map(|t| t.elapsed() > Duration::from_secs(2))
                .unwrap_or(true);

            if cooldown_passed && ctx.input(|i| i.viewport().focused == Some(true)) {
                // Double check focus over 100ms to avoid transient reports during backgrounding
                match self.first_focused_observed {
                    Some(t) if t.elapsed() >= Duration::from_millis(100) => {
                        crate::gui::app_state::SHOW_WINDOW.store(true, std::sync::atomic::Ordering::SeqCst);
                        self.first_focused_observed = None;
                    }
                    Some(_) => {
                        // Still waiting for 100ms to pass
                        ctx.request_repaint(); // Keep checking
                    }
                    None => {
                        self.first_focused_observed = Some(Instant::now());
                        ctx.request_repaint();
                    }
                }
            } else {
                self.first_focused_observed = None;
            }
        } else {
            self.first_focused_observed = None;
        }

        // Handle window close event for backgrounding
        if ctx.input(|i| i.viewport().close_requested()) {
            if self.app_settings.background_on_close && (self.app_settings.api_enabled || self.app_settings.grpc_enabled) {
                // Use the static from gui::app_state
                crate::gui::app_state::SHOW_WINDOW.store(false, std::sync::atomic::Ordering::SeqCst);
                self.last_background_time = Some(Instant::now());
                ctx.send_viewport_cmd(egui::ViewportCommand::CancelClose);
            }
        }

        // Handle window visibility and background mode
        let show_window = crate::gui::app_state::SHOW_WINDOW.load(std::sync::atomic::Ordering::SeqCst);
        static LAST_SHOW_WINDOW: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
        if show_window != LAST_SHOW_WINDOW.load(std::sync::atomic::Ordering::SeqCst) {
            if show_window {
                // RESTORING from background
                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                ctx.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                // Also request attention when showing from internal state change
                ctx.send_viewport_cmd(egui::ViewportCommand::RequestUserAttention(egui::UserAttentionType::Critical));
                // Briefly set AlwaysOnTop here too to be safe
                ctx.send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::AlwaysOnTop));

                // Use Win32 API to force foreground on Windows
                crate::gui::win_utils::force_foreground_window();

                let ctx_clone = ctx.clone();
                std::thread::spawn(move || {
                    for i in 1..=5 {
                        std::thread::sleep(std::time::Duration::from_millis(500));
                        
                        // If the user has hidden the window again during this loop, stop immediately
                        if !crate::gui::app_state::SHOW_WINDOW.load(std::sync::atomic::Ordering::SeqCst) {
                            ctx_clone.send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::Normal));
                            break;
                        }

                        ctx_clone.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                        ctx_clone.send_viewport_cmd(egui::ViewportCommand::Minimized(false));
                        
                        // Use Win32 API to force foreground on Windows
                        #[cfg(target_os = "windows")]
                        unsafe {
                            let _ = windows::Win32::UI::WindowsAndMessaging::AllowSetForegroundWindow(windows::Win32::UI::WindowsAndMessaging::ASFW_ANY);
                        }
                        crate::gui::win_utils::force_foreground_window();

                        ctx_clone.send_viewport_cmd(egui::ViewportCommand::Focus);

                        // Double check after commands
                        if !crate::gui::app_state::SHOW_WINDOW.load(std::sync::atomic::Ordering::SeqCst) {
                            ctx_clone.send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::Normal));
                            break;
                        }

                        if i % 2 == 0 {
                            ctx_clone.send_viewport_cmd(egui::ViewportCommand::RequestUserAttention(egui::UserAttentionType::Critical));
                            ctx_clone.send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::AlwaysOnTop));
                        }
                        if i == 4 {
                            ctx_clone.send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::Normal));
                        }
                        ctx_clone.request_repaint();
                    }
                });
            } else {
                // GOING to background
                // On Windows, if we want the app icon to STAY in the taskbar but the window to be hidden,
                // Minimized(true) is often better than Visible(false).
                // However, the user said "The app icon on the taskbar also does not return as it should",
                // implying it DOES leave the taskbar (which is what we want for "background mode").
                // If we use Visible(false), it leaves the taskbar. 
                // To make it come back, we MUST use Visible(true).
                ctx.send_viewport_cmd(egui::ViewportCommand::Visible(false));
            }
            LAST_SHOW_WINDOW.store(show_window, std::sync::atomic::Ordering::SeqCst);
        }

        if !show_window {
            // When hidden, we don't need to update the UI at all.
            // But we might still need to process API requests.
            if let Some(rx) = &self.api_rx {
                if let Ok(req) = rx.recv_timeout(Duration::from_millis(500)) {
                    // Execute query on GUI thread
                    let res = match &req.params {
                        Some(p) => query_interface::execute_query_with_params(&mut self.db, &req.query, p),
                        None => query_interface::execute_and_log(&mut self.db, &req.query),
                    };
                    let _ = req.respond_to.send(res.map_err(|e| e.to_string()));
                    
                    // If we mutated the DB, we might want to save eventually.
                    // But we don't need to repaint the UI.
                }
            } else {
                // No API, just sleep
                std::thread::sleep(Duration::from_millis(500));
            }
            // Ask egui to wake us up later, or when there is input (though there shouldn't be when hidden)
            ctx.request_repaint_after(Duration::from_millis(500));
            return;
        }

    // Process pending API requests (execute queries on the GUI thread safely)
    if let Some(rx) = &self.api_rx {
        // Limit processing per frame to avoid freezing the GUI
        let mut count = 0;
        while let Ok(req) = rx.try_recv() {
            let t0 = std::time::Instant::now();
            // Execute query on GUI thread
            let res = match &req.params {
                Some(p) => query_interface::execute_query_with_params(&mut self.db, &req.query, p),
                None => query_interface::execute_and_log(&mut self.db, &req.query),
            };
            let dt = t0.elapsed();
            // Debug print for visibility in console during development
            eprintln!(
                "[API GUI] RID={} done mutated={} dt_ms={}",
                req.request_id,
                res.as_ref().map(|o| o.mutated).unwrap_or(false),
                dt.as_millis()
            );
            // Best effort respond; ignore send errors if client disconnected
            let _ = req.respond_to.send(res.map_err(|e| e.to_string()));
            
            count += 1;
            if count >= 5 { break; } // Process at most 5 requests per frame
        }
    }
        // Native menu command handling removed; in-window menus cover these actions

        // Preferences window
        if self.show_prefs_window {
            let mut open = true;
            egui::Window::new("Preferences")
                .open(&mut open)
                .resizable(true)
                .collapsible(false)
                .show(ctx, |ui| {
                    // Tabs: App vs API
                    ui.horizontal(|ui| {
                        let app_sel = self.prefs_tab == PrefsTab::App;
                        if ui.selectable_label(app_sel, "App Settings").clicked() { self.prefs_tab = PrefsTab::App; }
                        let api_sel = self.prefs_tab == PrefsTab::Api;
                        if ui.selectable_label(api_sel, "API Settings").clicked() { self.prefs_tab = PrefsTab::Api; }
                    });
                    ui.separator();

                    match self.prefs_tab {
                        PrefsTab::App => {
                            ui.heading("General");
                            ui.separator();

                            // Autosave directory override
                            ui.label("Autosave directory (leave empty for OS default):");
                            let resp = ui.text_edit_singleline(&mut self.prefs_autosave_override_str);
                            if resp.lost_focus() {
                                // no-op; parse on Save
                            }
                            if ui.button("Clear to default (OS temp)").clicked() {
                                self.prefs_autosave_override_str.clear();
                            }

                            ui.add_space(8.0);
                            // Export directory override
                            ui.label("Export directory (leave empty for OS temp):");
                            let resp2 = ui.text_edit_singleline(&mut self.prefs_export_override_str);
                            if resp2.lost_focus() {
                                // no-op; parse on Save
                            }
                            if ui.button("Clear to default (OS temp)").clicked() {
                                self.prefs_export_override_str.clear();
                            }

                            ui.add_space(8.0);
                            // Show where the settings file is stored on this system (read-only info)
                            let settings_dir = AppSettings::settings_dir();
                            ui.label("Settings save directory:");
                            ui.monospace(settings_dir.display().to_string());

                            ui.add_space(4.0);
                            // Show effective export directory that will be used when path is not specified
                            let eff_export = if self.prefs_export_override_str.trim().is_empty() {
                                AppSettings::export_default_dir()
                            } else {
                                std::path::PathBuf::from(self.prefs_export_override_str.trim())
                            };
                            ui.label("Effective export default directory:");
                            ui.monospace(eff_export.display().to_string());

                            ui.separator();
                            ui.heading("Rendering / LOD");
                            ui.checkbox(&mut self.prefs_edit.lod_enabled, "Enable level-of-detail (LOD)");
                            ui.add(egui::Slider::new(&mut self.prefs_edit.lod_label_min_zoom, 0.1..=3.0).text("Label min zoom"));
                            ui.add(egui::Slider::new(&mut self.prefs_edit.lod_hide_labels_node_threshold, 0..=5000).text("Hide labels above N nodes"));

                            ui.separator();
                            ui.heading("Background Mode");
                            ui.checkbox(&mut self.prefs_edit.background_on_close, "Continue running in background when window is closed")
                                .on_hover_text("If enabled, closing the window will not stop the API server. You can restore the window from the system tray icon.");
                        }
                        PrefsTab::Api => {
                            ui.heading("API Service");
                            ui.horizontal(|ui| {
                                ui.checkbox(&mut self.prefs_edit.api_enabled, "Enable HTTP/WS API Server");
                            });
                            ui.horizontal(|ui| {
                                ui.checkbox(&mut self.prefs_edit.grpc_enabled, "Enable gRPC Server");
                            });
                            ui.horizontal(|ui| {
                                ui.label("Bind address");
                                ui.text_edit_singleline(&mut self.prefs_edit.api_bind_addr);
                            });
                            ui.horizontal(|ui| {
                                ui.label("HTTP Port");
                                let mut port = self.prefs_edit.api_port as i32;
                                if ui.add(egui::DragValue::new(&mut port).range(1..=65535)).changed() {
                                    self.prefs_edit.api_port = port as u16;
                                }
                                ui.label(format!("Endpoint: {}", self.prefs_edit.api_endpoint()));
                            });
                            ui.horizontal(|ui| {
                                ui.label("gRPC Port");
                                let mut gport = self.prefs_edit.grpc_port as i32;
                                if ui.add(egui::DragValue::new(&mut gport).range(1..=65535)).changed() {
                                    self.prefs_edit.grpc_port = gport as u16;
                                }
                                ui.label(format!("Endpoint: {}:{}", self.prefs_edit.api_bind_addr, self.prefs_edit.grpc_port));
                            });
                            ui.horizontal(|ui| {
                                ui.label("API Key (optional)");
                                let mut key = self.prefs_edit.api_key.clone().unwrap_or_default();
                                if ui.text_edit_singleline(&mut key).changed() {
                                    if key.trim().is_empty() { self.prefs_edit.api_key = None; } else { self.prefs_edit.api_key = Some(key.clone()); }
                                }
                                if ui.button("Clear").clicked() { self.prefs_edit.api_key = None; }
                            });

                            ui.add_space(6.0);
                            ui.label("API log directory (leave empty for OS temp):");
                            let _ = ui.text_edit_singleline(&mut self.prefs_api_log_override_str);
                            if ui.button("Clear to default (OS temp)").clicked() {
                                self.prefs_api_log_override_str.clear();
                            }
                            let eff_api_log = if self.prefs_api_log_override_str.trim().is_empty() {
                                AppSettings::api_log_default_dir()
                            } else {
                                std::path::PathBuf::from(self.prefs_api_log_override_str.trim())
                            };
                            ui.small(format!("Effective API log dir: {}", eff_api_log.display()));
                        }
                    }

                    if let Some(msg) = &self.prefs_status {
                        ui.separator();
                        ui.label(msg);
                    }

                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button("Save").clicked() {
                            // Apply autosave path
                            self.prefs_edit.autosave_override = if self.prefs_autosave_override_str.trim().is_empty() {
                                None
                            } else {
                                Some(std::path::PathBuf::from(self.prefs_autosave_override_str.trim()))
                            };
                            // Apply export path
                            self.prefs_edit.export_override = if self.prefs_export_override_str.trim().is_empty() {
                                None
                            } else {
                                Some(std::path::PathBuf::from(self.prefs_export_override_str.trim()))
                            };
                            // Apply API log path
                            self.prefs_edit.api_log_override = if self.prefs_api_log_override_str.trim().is_empty() {
                                None
                            } else {
                                Some(std::path::PathBuf::from(self.prefs_api_log_override_str.trim()))
                            };
                            // Persist
                            match self.prefs_edit.save() {
                                Ok(()) => {
                                    // Determine if API server config changed
                                    let old_api = (self.app_settings.api_enabled.clone(), self.app_settings.api_bind_addr.clone(), self.app_settings.api_port, self.app_settings.api_key.clone());
                                    let old_grpc = (self.app_settings.grpc_enabled.clone(), self.app_settings.grpc_port, self.app_settings.api_bind_addr.clone(), self.app_settings.api_key.clone());
                                    // Detect export dir change to refresh default export paths in views
                                    let old_export_dir = self.app_settings.export_dir();
                                    self.app_settings = self.prefs_edit.clone();
                                    // Apply to runtime
                                    self.lod_enabled = self.app_settings.lod_enabled;
                                    self.lod_label_min_zoom = self.app_settings.lod_label_min_zoom;
                                    self.lod_hide_labels_node_threshold = self.app_settings.lod_hide_labels_node_threshold;
                                    let new_api = (self.app_settings.api_enabled.clone(), self.app_settings.api_bind_addr.clone(), self.app_settings.api_port, self.app_settings.api_key.clone());
                                    let new_grpc = (self.app_settings.grpc_enabled.clone(), self.app_settings.grpc_port, self.app_settings.api_bind_addr.clone(), self.app_settings.api_key.clone());
                                    
                                    if old_api != new_api {
                                        // Restart server
                                        api::server::stop_server();
                                        if self.app_settings.api_enabled {
                                            let _ = api::server::start_server(&self.app_settings);
                                        }
                                    }

                                    if old_grpc != new_grpc {
                                        api::grpc::stop_grpc_server();
                                        if self.app_settings.grpc_enabled {
                                            let _ = api::grpc::start_grpc_server(&self.app_settings);
                                        }
                                    }

                                    self.api_running = self.app_settings.api_enabled || self.app_settings.grpc_enabled;

                                    let new_export_dir = self.app_settings.export_dir();
                                    if old_export_dir != new_export_dir {
                                        // If export_all_path is empty or under old dir, regenerate under new dir
                                        let refresh_export_all = self.export_all_path.is_empty() || {
                                            let p = std::path::Path::new(&self.export_all_path);
                                            p.starts_with(&old_export_dir)
                                        };
                                        if refresh_export_all {
                                            let now = time::OffsetDateTime::now_utc();
                                            let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                                            let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                                            let ext = if self.export_all_is_json { "json" } else { "csv" };
                                            let mut base = new_export_dir.clone();
                                            base.push(format!("graph_export_{}.{}", stamp, ext));
                                            self.export_all_path = base.display().to_string();
                                        }
                                        // If query_export_path is empty or under old dir, regenerate under new dir
                                        let refresh_query = self.query_export_path.is_empty() || {
                                            let p = std::path::Path::new(&self.query_export_path);
                                            p.starts_with(&old_export_dir)
                                        };
                                        if refresh_query {
                                            let now = time::OffsetDateTime::now_utc();
                                            let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                                            let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                                            let ext = if self.query_export_is_json { "json" } else { "csv" };
                                            let mut base = new_export_dir;
                                            base.push(format!("query_export_{}.{}", stamp, ext));
                                            self.query_export_path = base.display().to_string();
                                        }
                                    }
                                    self.last_save_info = Some("Preferences saved".into());
                                    self.last_info_time = Some(Instant::now());
                                    self.last_info_style = NoticeStyle::Prominent;
                                    self.show_prefs_window = false;
                                }
                                Err(e) => {
                                    self.prefs_status = Some(format!("Failed to save preferences: {}", e));
                                }
                            }
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_prefs_window = false;
                        }
                    });
                });
            if !open { self.show_prefs_window = false; }
        }

        // Export Entire Graph modal
        if self.show_export_all_window {
            let mut open = true;
            egui::Window::new("Export Graph")
                .open(&mut open)
                .collapsible(false)
                .resizable(true)
                .show(ctx, |ui| {
                    ui.label("Choose export format and destination path.");
                    ui.separator();
                    ui.horizontal(|ui| {
                        ui.label("Format:");
                        let mut changed = false;
                        if ui.selectable_label(self.export_all_is_json, "JSON").clicked() {
                            if !self.export_all_is_json { self.export_all_is_json = true; changed = true; }
                        }
                        if ui.selectable_label(!self.export_all_is_json, "CSV").clicked() {
                            if self.export_all_is_json { self.export_all_is_json = false; changed = true; }
                        }
                        if changed {
                            // Update extension hint
                            let desired_ext = if self.export_all_is_json { ".json" } else { ".csv" };
                            if self.export_all_path.is_empty() {
                                let now = time::OffsetDateTime::now_utc();
                                let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                                let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                                let mut base = self.app_settings.export_dir();
                                base.push(format!("graph_export_{}{}", stamp, desired_ext));
                                self.export_all_path = base.display().to_string();
                            } else {
                                // Swap extension if present
                                if let Some(p) = std::path::Path::new(&self.export_all_path).file_stem() {
                                    let parent = std::path::Path::new(&self.export_all_path).parent().map(|p| p.to_path_buf()).unwrap_or_default();
                                    let stem = p.to_string_lossy();
                                    self.export_all_path = parent.join(format!("{}{}", stem, desired_ext)).display().to_string();
                                }
                            }
                        }
                    });
                    if self.export_all_path.is_empty() {
                        let now = time::OffsetDateTime::now_utc();
                        let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                        let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                        let ext = if self.export_all_is_json { "json" } else { "csv" };
                        let mut base = self.app_settings.export_dir();
                        base.push(format!("graph_export_{}.{}", stamp, ext));
                        self.export_all_path = base.display().to_string();
                    }
                    ui.label("Save to:");
                    ui.text_edit_singleline(&mut self.export_all_path);
                    ui.add_space(6.0);
                    ui.horizontal(|ui| {
                        if ui.button("Export").clicked() {
                            let path = std::path::PathBuf::from(self.export_all_path.clone());
                            let parent = path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::from("."));
                            let res_msg = if let Err(e) = std::fs::create_dir_all(&parent) {
                                Err(format!("Failed to create directory: {}", e))
                            } else if self.export_all_is_json {
                                match export_graph_json(&self.db, &path) {
                                    Ok(()) => Ok(format!("Exported JSON to {}", path.display())),
                                    Err(e) => Err(format!("Export failed: {}", e)),
                                }
                            } else {
                                match export_graph_csv(&self.db, &path) {
                                    Ok((np, rp)) => Ok(format!("Exported CSV files: {} and {}", np.display(), rp.display())),
                                    Err(e) => Err(format!("Export failed: {}", e)),
                                }
                            };
                            self.export_all_status = Some(res_msg.unwrap_or_else(|e| e));
                        }
                        if ui.button("Cancel").clicked() { self.show_export_all_window = false; }
                    });
                    if let Some(msg) = &self.export_all_status { ui.separator(); ui.small(msg.clone()); }
                });
            if !open { self.show_export_all_window = false; }
        }
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            // Check for keyboard shortcuts
            if ctx.input_mut(|i| i.consume_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::S))) {
                self.menu_save();
            }
            if ctx.input_mut(|i| i.consume_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND | egui::Modifiers::SHIFT, egui::Key::S))) {
                self.menu_save_version();
            }
            if ctx.input_mut(|i| i.consume_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::N))) {
                self.menu_new_graph();
            }
            if ctx.input_mut(|i| i.consume_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::O))) {
                self.menu_load_latest();
            }

            // Use compact menus so options remain accessible regardless of width
            ui.horizontal(|ui| {
                ui.label("Graph-Loom");

                // File menu:
                ui.menu_button("File", |ui| {
                    if ui.add(egui::Button::new("Save").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::S)))).clicked() {
                        self.menu_save();
                        ui.close();
                    }
                    if ui.add(egui::Button::new("Save As").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND | egui::Modifiers::SHIFT, egui::Key::S)))).clicked() {
                        self.menu_save_version();
                        ui.close();
                    }
                    if ui.button("Export Graph…").clicked() {
                        // Open modal to export the entire graph
                        self.show_export_all_window = true;
                        // Initialize default path if empty
                        if self.export_all_path.is_empty() {
                            let now = time::OffsetDateTime::now_utc();
                            let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                            let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                            let ext = if self.export_all_is_json { "json" } else { "csv" };
                            let mut base = self.app_settings.export_dir();
                            base.push(format!("graph_export_{}.{}", stamp, ext));
                            self.export_all_path = base.display().to_string();
                        }
                        ui.close();
                    }
                    if ui.add(egui::Button::new("Load Latest").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::O)))).clicked() {
                        self.menu_load_latest();
                        ui.close();
                    }
                    if ui.button("Load Version…").clicked() {
                        self.show_load_versions = true;
                        ui.close();
                    }
                    ui.separator();
                    if ui.add(egui::Button::new("New Graph").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::N)))).clicked() {
                        self.menu_new_graph();
                        ui.close();
                    }
                    ui.separator();
                    if ui.add(egui::Button::new("Quit").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::Q)))).clicked() {
                        ctx.send_viewport_cmd(egui::ViewportCommand::Close);
                        ui.close();
                    }
                });

                ui.menu_button("View", |ui| {
                    if ui.add(egui::Button::new("Reset View").shortcut_text(ctx.format_shortcut(&egui::KeyboardShortcut::new(egui::Modifiers::COMMAND, egui::Key::Num0)))).clicked() {
                        self.menu_reset_view();
                        ui.close();
                    }
                    ui.separator();
                    ui.label("Zoom");
                    ui.add(egui::Slider::new(&mut self.zoom, 0.25..=2.0).clamping(egui::SliderClamping::Always));
                });


                ui.menu_button("Window", |ui| {
                    let toggle_sidebar = if self.sidebar_open { "Hide Sidebar" } else { "Show Sidebar" };
                    if ui.button(toggle_sidebar).clicked() {
                        // Leaving/entering a view: clear all selections for consistency
                        self.deselect_all();
                        // If hiding the sidebar, end bulk-select mode
                        if self.sidebar_open {
                            self.multi_select_active = false;
                        }
                        self.sidebar_open = !self.sidebar_open;
                        ui.close();
                    }
                    ui.separator();
                    ui.label(format!(
                        "Open pop-outs: nodes {} | rels {}",
                        self.open_node_windows.len(),
                        self.open_rel_windows.len()
                    ));
                    if ui.button("Deselect All").clicked() {
                        self.deselect_all();
                    }
                    if ui.button("Close All Pop-outs").clicked() {
                        self.open_node_windows.clear();
                        self.open_rel_windows.clear();
                    }
                });

                // Settings/Preferences
                ui.menu_button("Settings", |ui| {
                    if ui.button("Preferences…").clicked() {
                        self.menu_open_prefs();
                        ui.close();
                    }
                });

                // Keep a tiny status label; avoid long texts to prevent hiding on small widths
                ui.small(format!("N:{} R:{}", self.db.nodes.len(), self.db.relationships.len()));
                if let Some(err) = &self.save_error { ui.separator(); ui.colored_label(Color32::RED, err); }
            });
        });

        // Sidebar switchable between Tooling and Query console
        if self.sidebar_open {
            let panel_id = match self.sidebar_mode {
                SidebarMode::Tooling => "tooling_sidebar",
                SidebarMode::Query => "query_sidebar",
            };
            egui::SidePanel::left(panel_id)
                .resizable(true)
                .default_width(match self.sidebar_mode {
                    SidebarMode::Tooling => 260.0,
                    SidebarMode::Query => 300.0,
                })
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        let tooling_sel = self.sidebar_mode == SidebarMode::Tooling;
                        if ui.selectable_label(tooling_sel, "Tooling").clicked() {
                            self.deselect_all();
                            self.sidebar_mode = SidebarMode::Tooling;
                        }
                        let query_sel = self.sidebar_mode == SidebarMode::Query;
                        if ui.selectable_label(query_sel, "Query").clicked() {
                            self.deselect_all();
                            self.multi_select_active = false;
                            self.sidebar_mode = SidebarMode::Query;
                        }
                    });
                    ui.separator();

                    match self.sidebar_mode {
                        SidebarMode::Tooling => {
                            ui.heading("Tooling");
                            ui.add_space(4.0);
                            // Make tooling usable on very small windows via scrolling
                            egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
                                egui::CollapsingHeader::new("Layout")
                                    .default_open(false)
                                    .show(ui, |ui| {
                        if ui.button("Auto-cluster layout").on_hover_text("Detect communities and arrange nodes").clicked() {
                            if let Some(r) = self.last_canvas_rect {
                                self.apply_cluster_layout_all(r);
                            } else {
                                self.re_cluster_pending = true;
                            }
                        }
                        ui.small("Clusters by relationships, labels, and metadata. Dense clusters toward border; sparse toward center.");

                        ui.separator();
                        ui.label("Layout aids for large graphs");
                        ui.horizontal(|ui| {
                            ui.checkbox(&mut self.gravity_enabled, "Enable gravity to center");
                            ui.add(egui::Slider::new(&mut self.gravity_strength, 0.5..=20.0)
                                .logarithmic(true)
                                .clamping(egui::SliderClamping::Always)
                                .text("gravity"));
                        });
                        ui.horizontal(|ui| {
                            ui.label("Local COM radius");
                            ui.add(egui::Slider::new(&mut self.com_gravity_radius, 60.0..=800.0)
                                .logarithmic(true)
                                .clamping(egui::SliderClamping::Always)
                                .suffix(" px"))
                                .on_hover_text("Within this radius, nodes are attracted to the center of mass of nearby nodes instead of the window center");
                        });
                        ui.horizontal(|ui| {
                            ui.label("Min neighbors for COM");
                            let mut min_n = self.com_gravity_min_neighbors as i32;
                            if ui.add(egui::Slider::new(&mut min_n, 1..=10).clamping(egui::SliderClamping::Always)).changed() {
                                self.com_gravity_min_neighbors = min_n as usize;
                            }
                        });
                        ui.horizontal(|ui| {
                            ui.label("Hub repulsion scale");
                            ui.add(egui::Slider::new(&mut self.hub_repulsion_scale, 0.0..=3.0)
                                .clamping(egui::SliderClamping::Always)
                                .text("hubs spread"));
                        });
                        ui.separator();
                        ui.label("Level of detail (LOD)");
                        ui.checkbox(&mut self.lod_enabled, "Enable LOD").on_hover_text("Hide most labels when zoomed out or when the graph is very large; always show for hovered/selected/query-matched nodes");
                        ui.horizontal(|ui| {
                            ui.label("Hide labels when nodes ≥");
                            ui.add(egui::DragValue::new(&mut self.lod_hide_labels_node_threshold).range(50..=2000));
                        });
                        ui.horizontal(|ui| {
                            ui.label("Min zoom for labels");
                            ui.add(egui::Slider::new(&mut self.lod_label_min_zoom, 0.3..=1.5).clamping(egui::SliderClamping::Always));
                        });

                        ui.separator();
                        ui.label("Relationship label readability");
                        ui.horizontal(|ui| {
                            ui.label("Min zoom for edge labels");
                            ui.add(egui::Slider::new(&mut self.edge_label_min_zoom, 0.3..=2.0).clamping(egui::SliderClamping::Always));
                        });
                        ui.horizontal(|ui| {
                            ui.label("Hide when edges ≥");
                            ui.add(egui::DragValue::new(&mut self.edge_label_count_threshold).range(100..=5000));
                        });
                        ui.horizontal(|ui| {
                            ui.label("Label background opacity");
                            let mut alpha_f: f32 = self.edge_label_bg_alpha as f32;
                            if ui.add(egui::Slider::new(&mut alpha_f, 30.0..=255.0)).changed() {
                                self.edge_label_bg_alpha = alpha_f as u8;
                            }
                        });
                        });

                    egui::CollapsingHeader::new("Create Node")
                        .default_open(false)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                ui.label("Label");
                                ui.text_edit_singleline(&mut self.create_node_label);
                            });
                            egui::CollapsingHeader::new("Optional: Pre-link a relationship")
                                .default_open(false)
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        ui.checkbox(&mut self.create_node_rel_enabled, "Also create relationship");
                                        ui.label("Label:");
                                        ui.text_edit_singleline(&mut self.create_node_rel_label);
                                    });
                                    ui.horizontal(|ui| {
                                        ui.label("Direction:");
                                        let mut dir = self.create_node_rel_direction;
                                        if ui.radio(dir == NewNodeRelDir::NewToExisting, "new → existing").clicked() {
                                            dir = NewNodeRelDir::NewToExisting;
                                        }
                                        if ui.radio(dir == NewNodeRelDir::ExistingToNew, "existing → new").clicked() {
                                            dir = NewNodeRelDir::ExistingToNew;
                                        }
                                        self.create_node_rel_direction = dir;
                                    });
                                    ui.horizontal(|ui| {
                                        ui.label("Target:");
                                        let tgt_text = self.create_node_rel_target
                                            .and_then(|id| self.db.nodes.get(&id).map(|_| format_short_node(&self.db, id)))
                                            .unwrap_or_else(|| "<none>".into());
                                        ui.monospace(tgt_text);
                                    });
                                    ui.horizontal(|ui| {
                                        let picking = matches!(self.pick_target, Some(PickTarget::NewNodeTarget));
                                        let txt = if picking { "Cancel Pick Target" } else { "Pick Target on Canvas" };
                                        if ui.button(txt).clicked() {
                                            self.pick_target = if picking { None } else { Some(PickTarget::NewNodeTarget) };
                                        }
                                        if ui.button("Clear Target").clicked() { self.create_node_rel_target = None; }
                                    });
                                    if matches!(self.pick_target, Some(PickTarget::NewNodeTarget)) {
                                        ui.colored_label(Color32::YELLOW, "Picking: click a node to set as target (Esc to cancel)");
                                    }
                                });
                            ui.label("Metadata (key/value rows)");
                            let mut to_remove_node: Option<usize> = None;
                            for (i, (k, v)) in self.create_node_meta.iter_mut().enumerate() {
                                ui.horizontal(|ui| {
                                    ui.text_edit_singleline(k);
                                    ui.label(":");
                                    ui.text_edit_singleline(v);
                                    if ui.button("-").on_hover_text("Remove row").clicked() { to_remove_node = Some(i); }
                                });
                            }
                            if let Some(i) = to_remove_node { self.create_node_meta.remove(i); }
                            if ui.button("+ Add row").clicked() { self.create_node_meta.push((String::new(), String::new())); }
                            let mut error_node: Option<String> = None;
                            if ui.button("Create Node").clicked() {
                                let label = self.create_node_label.trim().to_string();
                                if label.is_empty() {
                                    error_node = Some("Label cannot be empty".into());
                                } else {
                                    let mut md = HashMap::new();
                                    for (k, v) in &self.create_node_meta {
                                        let kk = k.trim();
                                        if !kk.is_empty() { md.insert(kk.to_string(), v.trim().to_string()); }
                                    }
                                    let id = self.db.add_node(label, md);
                                    self.re_cluster_pending = true;
                                    // Place the new node on the golden spiral around the current origin
                                    if let Some(r) = self.last_canvas_rect {
                                        let idx = self.node_positions.len();
                                        let pos = golden_spiral_position(r.center(), idx as u32, r);
                                        self.node_positions.insert(id, pos);
                                    }
                                    self.selected = Some(SelectedItem::Node(id));
                                    // Optionally create a relationship involving the new node
                                    if self.create_node_rel_enabled {
                                        let rel_label = if self.create_node_rel_label.trim().is_empty() { "REL".to_string() } else { self.create_node_rel_label.trim().to_string() };
                                        if let Some(other) = self.create_node_rel_target {
                                            if other != id {
                                                match self.create_node_rel_direction {
                                                    NewNodeRelDir::NewToExisting => { let _ = self.db.add_relationship(id, other, rel_label.clone(), HashMap::new()); self.re_cluster_pending = true; }
                                                    NewNodeRelDir::ExistingToNew => { let _ = self.db.add_relationship(other, id, rel_label.clone(), HashMap::new()); self.re_cluster_pending = true; }
                                                }
                                            }
                                        } else {
                                            // No target chosen yet: enter pick mode and remember the new node
                                            self.pending_new_node_for_link = Some(id);
                                            self.pick_target = Some(PickTarget::NewNodeTarget);
                                        }
                                    }
                                    self.create_node_label.clear();
                                    self.create_node_meta.clear();
                                    self.mark_dirty();
                                }
                            }
                            if let Some(e) = error_node { ui.colored_label(Color32::RED, e); }
                        });

                    egui::CollapsingHeader::new("Create Relationship")
                        .default_open(false)
                        .show(ui, |ui| {
                            // From/To via pick (no dropdowns)
                            ui.horizontal(|ui| {
                                ui.label("From:");
                                let key = self.create_rel_display_key.trim();
                                let from_text = self.create_rel_from.map(|id| {
                                    let base = format_short_node(&self.db, id);
                                    if !key.is_empty() {
                                        if let Some(n) = self.db.nodes.get(&id) {
                                            if let Some(val) = n.metadata.get(key) {
                                                return format!("{} — {}={}", base, key, val);
                                            }
                                        }
                                    }
                                    base
                                }).unwrap_or_else(|| "<none>".into());
                                ui.monospace(from_text);
                            });
                            ui.horizontal(|ui| {
                                let pick_from_active = matches!(self.pick_target, Some(PickTarget::From));
                                let pick_from_text = if pick_from_active { "Cancel Pick From" } else { "Pick From on Canvas" };
                                if ui.button(pick_from_text).clicked() {
                                    self.pick_target = if pick_from_active { None } else { Some(PickTarget::From) };
                                }
                                if ui.button("Clear From").clicked() { self.create_rel_from = None; }
                            });
                            ui.horizontal(|ui| {
                                ui.label("To:");
                                let key = self.create_rel_display_key.trim();
                                let to_text = self.create_rel_to.map(|id| {
                                    let base = format_short_node(&self.db, id);
                                    if !key.is_empty() {
                                        if let Some(n) = self.db.nodes.get(&id) {
                                            if let Some(val) = n.metadata.get(key) {
                                                return format!("{} — {}={}", base, key, val);
                                            }
                                        }
                                    }
                                    base
                                }).unwrap_or_else(|| "<none>".into());
                                ui.monospace(to_text);
                            });
                            ui.horizontal(|ui| {
                                let pick_to_active = matches!(self.pick_target, Some(PickTarget::To));
                                let pick_to_text = if pick_to_active { "Cancel Pick To" } else { "Pick To on Canvas" };
                                if ui.button(pick_to_text).clicked() {
                                    self.pick_target = if pick_to_active { None } else { Some(PickTarget::To) };
                                }
                                if ui.button("Clear To").clicked() { self.create_rel_to = None; }
                            });
                            if self.pick_target.is_some() {
                                ui.colored_label(Color32::YELLOW, "Picking on canvas: click a node to assign (Esc to cancel)");
                            }
                            ui.horizontal(|ui| {
                                ui.label("Display key");
                                ui.add(egui::TextEdit::singleline(&mut self.create_rel_display_key).hint_text("e.g. name"));
                            });
                            ui.horizontal(|ui| {
                                ui.label("Label");
                                ui.text_edit_singleline(&mut self.create_rel_label);
                            });
                            ui.label("Metadata (key/value rows)");
                            let mut to_remove_rel: Option<usize> = None;
                            for (i, (k, v)) in self.create_rel_meta.iter_mut().enumerate() {
                                ui.horizontal(|ui| {
                                    ui.text_edit_singleline(k);
                                    ui.label(":");
                                    ui.text_edit_singleline(v);
                                    if ui.button("-").on_hover_text("Remove row").clicked() { to_remove_rel = Some(i); }
                                });
                            }
                            if let Some(i) = to_remove_rel { self.create_rel_meta.remove(i); }
                            if ui.button("+ Add row").clicked() { self.create_rel_meta.push((String::new(), String::new())); }
                            let mut error_rel: Option<String> = None;
                            if ui.button("Create Relationship").clicked() {
                                let label = self.create_rel_label.trim().to_string();
                                let (from, to) = (self.create_rel_from, self.create_rel_to);
                                if label.is_empty() { error_rel = Some("Label cannot be empty".into()); }
                                else if from.is_none() || to.is_none() { error_rel = Some("Select both From and To nodes".into()); }
                                else if from == to { error_rel = Some("From and To must be different".into()); }
                                else {
                                    let mut md = HashMap::new();
                                    for (k, v) in &self.create_rel_meta {
                                        let kk = k.trim();
                                        if !kk.is_empty() { md.insert(kk.to_string(), v.trim().to_string()); }
                                    }
                                    if let (Some(from_id), Some(to_id)) = (from, to) {
                                        if let Some(rid) = self.db.add_relationship(from_id, to_id, label, md) {
                                            self.selected = Some(SelectedItem::Rel(rid));
                                            self.re_cluster_pending = true;
                                            self.create_rel_label.clear();
                                            self.create_rel_from = None;
                                            self.create_rel_to = None;
                                            self.create_rel_meta.clear();
                                            self.mark_dirty();
                                        } else {
                                            error_rel = Some("Failed to create relationship (nodes may not exist)".into());
                                        }
                                    } else {
                                        error_rel = Some("Select both From and To nodes".into());
                                    }
                                }
                            }
                            if let Some(e) = error_rel { ui.colored_label(Color32::RED, e); }
                        });

                    let bulk_resp = egui::CollapsingHeader::new("Bulk Edit Nodes")
                        .default_open(false)
                        .show(ui, |ui| {
                            ui.horizontal(|ui| {
                                let toggle_txt = if self.multi_select_active { "Stop Selecting" } else { "Start Selecting" };
                                if ui.button(toggle_txt).clicked() {
                                    self.multi_select_active = !self.multi_select_active;
                                }
                                if ui.button("Clear Selection").clicked() { self.multi_selected_nodes.clear(); }
                            });
                            ui.small(format!("Selected: {} nodes", self.multi_selected_nodes.len()));

                            ui.separator();
                            ui.label("Add/Update Metadata on selected nodes");
                            ui.label("Key");
                            ui.text_edit_singleline(&mut self.bulk_add_key);
                            ui.label("Value");
                            ui.text_edit_singleline(&mut self.bulk_add_value);
                            let disabled = self.multi_selected_nodes.is_empty() || self.bulk_add_key.trim().is_empty();
                            let btn = ui.add_enabled(!disabled, egui::Button::new("Apply"));
                            if btn.clicked() {
                                let key = self.bulk_add_key.trim().to_string();
                                let val = self.bulk_add_value.clone();
                                let mut count = 0usize;
                                for id in self.multi_selected_nodes.clone() {
                                    if self.db.upsert_node_metadata(id, key.clone(), val.clone()) { count += 1; }
                                }
                                if count > 0 { self.re_cluster_pending = true; self.mark_dirty(); }
                                self.bulk_status = Some(format!("Upserted '{}' for {} node(s)", key, count));
                            }

                            ui.separator();
                            ui.label("Delete Metadata key(s) on selected nodes");
                            ui.label("Keys (comma or space separated)");
                            ui.text_edit_singleline(&mut self.bulk_delete_keys);
                            let disabled = self.multi_selected_nodes.is_empty() || self.bulk_delete_keys.trim().is_empty();
                            let btn = ui.add_enabled(!disabled, egui::Button::new("Delete Keys"));
                            if btn.clicked() {
                                let keys: Vec<String> = self.bulk_delete_keys
                                    .split(|c: char| c == ',' || c.is_whitespace())
                                    .filter_map(|s| { let t = s.trim(); if t.is_empty() { None } else { Some(t.to_string()) } })
                                    .collect();
                                let mut affected = 0usize;
                                for id in self.multi_selected_nodes.clone() {
                                    let mut any = false;
                                    for k in &keys {
                                        if self.db.remove_node_metadata_key(id, k) { any = true; }
                                    }
                                    if any { affected += 1; }
                                }
                                if affected > 0 { self.re_cluster_pending = true; self.mark_dirty(); }
                                self.bulk_status = Some(format!("Deleted keys [{}] on {} node(s)", keys.join(", "), affected));
                            }
                            ui.separator();
                            // Mass delete selected nodes
                            let del_disabled = self.multi_selected_nodes.is_empty();
                            if ui.add_enabled(!del_disabled, egui::Button::new("Delete Selected Nodes")).clicked() {
                                self.confirm_mass_delete = true;
                            }
                            if let Some(msg) = &self.bulk_status { ui.small(msg.clone()); }
                        });
                    // If the Bulk Edit section is collapsed, automatically stop selecting mode
                    if !bulk_resp.fully_open() && self.multi_select_active {
                        self.multi_select_active = false;
                    }
                    });
                }
                SidebarMode::Query => {
                            ui.heading("Query Console");
                            ui.add_space(4.0);
                            let was_compact = self.sidebar_compact;
                            // Use compact styling if enabled
                            ui.scope(|ui| {
                                if was_compact {
                                    let mut style: egui::Style = (*ui.style()).as_ref().clone();
                                    style.spacing.item_spacing = egui::vec2(4.0, 4.0);
                                    style.spacing.button_padding = egui::vec2(6.0, 4.0);
                                    style.spacing.indent = 6.0;
                                    style.spacing.interact_size.y = 18.0;
                                    style.text_styles.insert(egui::TextStyle::Button, egui::FontId::proportional(12.0));
                                    style.text_styles.insert(egui::TextStyle::Body, egui::FontId::proportional(12.0));
                                    style.text_styles.insert(egui::TextStyle::Small, egui::FontId::proportional(11.0));
                                    ui.set_style(style);
                                }
                                egui::ScrollArea::vertical().show(ui, |ui| {
                            ui.label("Enter query (Cmd/Ctrl+Enter to run):");
                            let edit = egui::TextEdit::multiline(&mut self.query_text)
                                .desired_rows(8)
                                .lock_focus(true)
                                .desired_width(f32::INFINITY)
                                // Assign a persistent id so we can programmatically move the caret
                                .id_source("query_text_edit");
                            let te_resp = ui.add(edit);

                            // Suggestion logic: compute prefix token at end-of-text
                            // Global early cancel: ESC should always close the suggestions popup
                            // regardless of current focus nuances. Consume the key so egui doesn't
                            // also clear focus in a way that reopens or interferes with our state.
                            if ui.input(|i| i.key_pressed(egui::Key::Escape)) && self.query_suggest_visible {
                                self.query_suggest_visible = false;
                                self.query_suggest_hover_index = None;
                                ui.input_mut(|i| {
                                    i.consume_key(egui::Modifiers::NONE, egui::Key::Escape);
                                });
                            }

                            let want_popup_all = ui.input(|i| {
                                let pressed = i.key_pressed(egui::Key::Space);
                                let mod_ok = if cfg!(target_os = "macos") { i.modifiers.command } else { i.modifiers.ctrl };
                                pressed && mod_ok
                            });

                            // Detect acceptance keys early to avoid recomputing suggestions before using selection
                            let accept_enter_early = ui.input(|i| i.key_pressed(egui::Key::Enter) && !i.modifiers.command && !i.modifiers.ctrl && !i.modifiers.shift && !i.modifiers.alt);
                            let accept_tab_early = ui.input(|i| i.key_pressed(egui::Key::Tab));

                            let consider_recompute = (te_resp.changed() && !(accept_enter_early || accept_tab_early)) || want_popup_all;
                            // Only show suggestions when the text edit has focus
                            if !te_resp.has_focus() { self.query_suggest_visible = false; }

                            if consider_recompute && te_resp.has_focus() {
                                // Try to preserve the currently selected item across recomputes
                                let prev_selected_idx = self.query_suggest_hover_index.unwrap_or(self.query_suggest_index);
                                let prev_selected_item = self
                                    .query_suggest_items
                                    .get(prev_selected_idx)
                                    .cloned();
                                // Determine the active token prefix (only if cursor at end or assume end)
                                let text = self.query_text.as_str();
                                // New rule: if the character immediately before the cursor is a space,
                                // do not supply suggestions unless explicitly forced with Cmd/Ctrl+Space.
                                // We assume caret at end (common case for console typing).
                                let last_char_is_space = text.chars().last().map(|c| c.is_whitespace()).unwrap_or(false);
                                if last_char_is_space && !want_popup_all {
                                    // Hide suggestions and skip recompute
                                    self.query_suggest_visible = false;
                                    self.query_suggest_items.clear();
                                    self.query_suggest_hover_index = None;
                                    // Do not proceed with computing prefix/pool in this frame
                                } else {
                                let caret_at_end = true; // simplified: egui API for exact caret is elaborate; assume common case
                                let (prefix, _start_idx) = if caret_at_end {
                                    // Trim trailing whitespace (e.g., Enter inserted a newline) before detecting token
                                    let mut end = text.len();
                                    while end > 0 {
                                        let c = text.as_bytes()[end - 1] as char;
                                        if c.is_whitespace() { end -= 1; } else { break; }
                                    }
                                    // Walk back to find token start: letters, digits, underscore, colon, dot
                                    let bytes = text.as_bytes();
                                    let mut i = end;
                                    while i > 0 {
                                        let c = bytes[i-1] as char;
                                        if c.is_ascii_alphanumeric() || c == '_' || c == ':' || c == '.' { i -= 1; } else { break; }
                                    }
                                    (text[i..end].to_string(), i)
                                } else { (String::new(), text.len()) };

                                // Build suggestion universe (cached)
                                let mut pool: Vec<String> = Vec::new();
                                const KEYWORDS: &[&str] = &[
                                    "MATCH","OPTIONAL","OPTIONAL MATCH","WHERE","RETURN","ORDER BY","SKIP","LIMIT",
                                    "CREATE","MERGE","SET","REMOVE","DELETE","DETACH DELETE",
                                    "DISTINCT","ASC","DESC",
                                ];
                                pool.extend(KEYWORDS.iter().map(|s| s.to_string()));
                                
                                // Only add dynamic items if DB is small enough or if we really need to
                                // For performance, we could cache this, but let's at least limit it
                                if self.db.nodes.len() < 1000 {
                                    let mut labels: BTreeSet<String> = BTreeSet::new();
                                    let mut rels: BTreeSet<String> = BTreeSet::new();
                                    let mut props: BTreeSet<String> = BTreeSet::new();
                                    for n in self.db.nodes.values() {
                                        if !n.label.is_empty() { labels.insert(n.label.clone()); }
                                        for k in n.metadata.keys() { props.insert(k.clone()); }
                                    }
                                    for r in self.db.relationships.values() {
                                        if !r.label.is_empty() { rels.insert(r.label.clone()); }
                                        for k in r.metadata.keys() { props.insert(k.clone()); }
                                    }
                                    pool.extend(labels.into_iter().map(|l| format!(":{}", l)));
                                    pool.extend(rels.into_iter().map(|t| format!(":{}", t)));
                                    pool.extend(props.into_iter().map(|p| format!("{}.{}", "n", p)));
                                }

                                // Filter by prefix (case-insensitive)
                                let pfx_up = prefix.to_uppercase();
                                // Only show suggestions if there is a non-empty prefix,
                                // unless the user explicitly requested with Cmd/Ctrl+Space
                                let mut items: Vec<String> = if want_popup_all {
                                    pool
                                } else if !prefix.is_empty() {
                                    pool.into_iter().filter(|s| s.to_uppercase().starts_with(&pfx_up)).collect()
                                } else {
                                    Vec::new()
                                };
                                items.sort();
                                items.dedup();
                                if !items.is_empty() {
                                    self.query_suggest_items = items.into_iter().take(30).collect();
                                    self.query_suggest_visible = true;
                                    // Preserve previous selection when possible; otherwise clamp to 0
                                    if let Some(prev_item) = prev_selected_item {
                                        if let Some(pos) = self.query_suggest_items.iter().position(|s| s == &prev_item) {
                                            self.query_suggest_index = pos;
                                        } else {
                                            self.query_suggest_index = 0;
                                        }
                                    } else {
                                        self.query_suggest_index = 0;
                                    }
                                    self.query_suggest_hover_index = None;
                                } else {
                                    self.query_suggest_visible = false;
                                }
                                // Note: start_idx currently unused in this simplified approach
                                }
                            }

                            // Handle navigation/acceptance keys for suggestions
                            if self.query_suggest_visible && te_resp.has_focus() {
                                let move_up = ui.input(|i| i.key_pressed(egui::Key::ArrowUp));
                                let move_down = ui.input(|i| i.key_pressed(egui::Key::ArrowDown));
                                // Reuse early-detected acceptance to ensure consistent behavior
                                let accept_enter = accept_enter_early;
                                let accept_tab = accept_tab_early;
                                let cancel = ui.input(|i| i.key_pressed(egui::Key::Escape));
                                if cancel { self.query_suggest_visible = false; }
                                if move_up && !self.query_suggest_items.is_empty() {
                                    if self.query_suggest_index == 0 { self.query_suggest_index = self.query_suggest_items.len()-1; } else { self.query_suggest_index -= 1; }
                                    // keyboard navigation takes precedence; clear hover
                                    self.query_suggest_hover_index = None;
                                }
                                if move_down && !self.query_suggest_items.is_empty() {
                                    self.query_suggest_index = (self.query_suggest_index + 1) % self.query_suggest_items.len();
                                    self.query_suggest_hover_index = None;
                                }
                                if (accept_enter || accept_tab) && !self.query_suggest_items.is_empty() {
                                    let chosen_idx = self.query_suggest_hover_index.unwrap_or(self.query_suggest_index);
                                    let chosen = self.query_suggest_items[chosen_idx].clone();
                                    // Replace last token with chosen
                                    let text = self.query_text.clone();
                                    let mut end = text.len();
                                    // Skip trailing whitespace (e.g., newline inserted by Enter) to find the real token end
                                    while end > 0 {
                                        let c = text.as_bytes()[end - 1] as char;
                                        if c.is_whitespace() { end -= 1; } else { break; }
                                    }
                                    let bytes = text.as_bytes();
                                    let mut i = end;
                                    while i > 0 {
                                        let c = bytes[i-1] as char;
                                        if c.is_ascii_alphanumeric() || c == '_' || c == ':' || c == '.' { i -= 1; } else { break; }
                                    }
                                    // If there is no token (i == end), do not accept; allow Enter to insert newline
                                    if i == end { 
                                        // Hide suggestions on acceptance attempt without token
                                        self.query_suggest_visible = false; 
                                        self.query_suggest_hover_index = None; 
                                        // Do not modify text here; TextEdit will handle newline for Enter
                                        // and Tab will do nothing visible
                                        
                                    } else {
                                        let mut new_text = String::from(&text[..i]);
                                        // Tab-complete style: do not insert a leading space; replace token in-place
                                        new_text.push_str(&chosen);
                                        // For Enter acceptance, add a trailing space for convenience; Tab adds none
                                        if accept_enter { new_text.push(' '); }
                                        self.query_text = new_text;
                                        self.query_suggest_visible = false;
                                        self.query_suggest_hover_index = None;
                                        // Consume the Enter/Tab key so TextEdit doesn't also handle it (which could move the caret)
                                        ui.input_mut(|i| {
                                            if accept_enter {
                                                i.consume_key(egui::Modifiers::NONE, egui::Key::Enter);
                                            }
                                            if accept_tab {
                                                i.consume_key(egui::Modifiers::NONE, egui::Key::Tab);
                                            }
                                        });
                                        // Explicitly move caret to the end of the inserted suggestion (before any trailing space)
                                        // Compute char index at insertion start + chosen length
                                        let insertion_start_chars = text[..i].chars().count();
                                        let chosen_len_chars = chosen.chars().count();
                                        let target_char_index = insertion_start_chars + chosen_len_chars; // before the added space
                                        let id = egui::Id::new("query_text_edit");
                                        if let Some(mut state) = egui::text_edit::TextEditState::load(ui.ctx(), id) {
                                            let cursor = egui::text::CCursor::new(target_char_index);
                                            state.cursor.set_char_range(Some(egui::text::CCursorRange::one(cursor)));
                                            state.store(ui.ctx(), id);
                                        }
                                        // Do not force focus change here; requesting focus on a widget
                                        // that egui doesn't consider alive in this frame can cause a panic.
                                        // The editor typically retains focus after keyboard acceptance.
                                    }
                                }
                            }

                            // Render suggestions list under the editor
                            if self.query_suggest_visible && !self.query_suggest_items.is_empty() {
                                ui.add_space(4.0);
                                egui::Frame::popup(ui.style()).show(ui, |ui| {
                                    ui.set_width(ui.available_width());
                                    egui::ScrollArea::vertical().max_height(140.0).show(ui, |ui| {
                                        // reset hover before drawing
                                        self.query_suggest_hover_index = None;
                                        for (idx, it) in self.query_suggest_items.clone().into_iter().enumerate() {
                                            let is_selected = match self.query_suggest_hover_index {
                                                Some(h) => idx == h,
                                                None => idx == self.query_suggest_index,
                                            };
                                            let resp = ui.selectable_label(is_selected, it.clone());
                                            if resp.hovered() {
                                                self.query_suggest_hover_index = Some(idx);
                                            }
                                            if resp.clicked() {
                                                self.query_suggest_index = idx;
                                                // mimic acceptance
                                                let chosen = self.query_suggest_items[idx].clone();
                                                let text = self.query_text.clone();
                                                let mut end = text.len();
                                                // Skip trailing whitespace to find token end
                                                while end > 0 {
                                                    let c = text.as_bytes()[end - 1] as char;
                                                    if c.is_whitespace() { end -= 1; } else { break; }
                                                }
                                                let bytes = text.as_bytes();
                                                let mut i = end;
                                                while i > 0 {
                                                    let c = bytes[i-1] as char;
                                                    if c.is_ascii_alphanumeric() || c == '_' || c == ':' || c == '.' { i -= 1; } else { break; }
                                                }
                                                if i != end {
                                                    let mut new_text = String::from(&text[..i]);
                                                    // Mouse accept: replace token in-place, then add trailing space (common UX)
                                                    new_text.push_str(&chosen);
                                                    new_text.push(' ');
                                                    self.query_text = new_text;
                                                    self.query_suggest_visible = false;
                                                    self.query_suggest_hover_index = None;
                                                    // Explicitly move caret to the end of the inserted suggestion (before the trailing space)
                                                    let insertion_start_chars = text[..i].chars().count();
                                                    let chosen_len_chars = chosen.chars().count();
                                                    let target_char_index = insertion_start_chars + chosen_len_chars;
                                                    let id = egui::Id::new("query_text_edit");
                                                    if let Some(mut state) = egui::text_edit::TextEditState::load(ui.ctx(), id) {
                                                        let cursor = egui::text::CCursor::new(target_char_index);
                                                        state.cursor.set_char_range(Some(egui::text::CCursorRange::one(cursor)));
                                                        state.store(ui.ctx(), id);
                                                    }
                                                    // Avoid forcing focus to prevent potential egui panic when the
                                                    // focused id is not in the node list for the current frame.
                                                } else {
                                                    // No token: just close suggestions
                                                    self.query_suggest_visible = false;
                                                    self.query_suggest_hover_index = None;
                                                }
                                            }
                                        }
                                    });
                                });
                            }
                            let mut run_now = false;
                            if ui.button("Run").clicked() {
                                run_now = true;
                            }
                            // Keyboard shortcut
                            let run_shortcut = if cfg!(target_os = "macos") {
                                ui.input(|i| i.modifiers.command && i.key_pressed(egui::Key::Enter))
                            } else {
                                ui.input(|i| i.modifiers.ctrl && i.key_pressed(egui::Key::Enter))
                            };
                            if run_shortcut { run_now = true; }

                            if run_now {
                                let q = self.query_text.trim().to_string();
                                if !q.is_empty() {
                                    match query_interface::execute_and_log(&mut self.db, &q) {
                                        Ok(outcome) => {
                                            self.last_query_error = None;
                                            // record history
                                            if self.query_history.last().map(|h| h != &q).unwrap_or(true) {
                                                self.query_history.push(q.clone());
                                            }
                                            // display rows succinctly and capture matches
                                            self.query_selected_nodes.clear();
                                            self.query_selected_rels.clear();
                                            self.query_output.clear();
                                            for row in outcome.rows {
                                                match row {
                                                    QueryResultRow::Node { id, label, metadata } => {
                                                        self.query_output.push(format!("NODE {} {} {:?}", id, label, metadata));
                                                        self.query_selected_nodes.insert(id);
                                                    }
                                                    QueryResultRow::Relationship { id, from, to, label, metadata } => {
                                                        self.query_output.push(format!("REL {} {} {} {} {:?}", id, from, to, label, metadata));
                                                        self.query_selected_rels.insert(id);
                                                        // ensure endpoints are positioned if new
                                                        if let Some(pa) = self.node_positions.get(&from) { let _ = pa; } else { if let Some(rect) = self.last_canvas_rect { let pos = golden_spiral_position(rect.center(), self.node_positions.len() as u32, rect); self.node_positions.insert(from, pos); } }
                                                        if let Some(pb) = self.node_positions.get(&to) { let _ = pb; } else { if let Some(rect) = self.last_canvas_rect { let pos = golden_spiral_position(rect.center(), self.node_positions.len() as u32 + 1, rect); self.node_positions.insert(to, pos); } }
                                                    }
                                                    QueryResultRow::Info(s) => self.query_output.push(s),
                                                }
                                            }
                                            self.query_output.push(format!("Affected: nodes={} rels={}", outcome.affected_nodes, outcome.affected_relationships));
                                            if outcome.mutated { self.mark_dirty(); }
                                        }
                                        Err(err) => {
                                            self.last_query_error = Some(err.to_string());
                                        }
                                    }
                                }
                            }
                            ui.separator();
                            // Controls for selection and export
                            ui.horizontal(|ui| {
                                let deselect_disabled = self.query_selected_nodes.is_empty() && self.query_selected_rels.is_empty();
                                if ui.add_enabled(!deselect_disabled, egui::Button::new("Deselect Matches")).clicked() {
                                    self.query_selected_nodes.clear();
                                    self.query_selected_rels.clear();
                                }
                                ui.small(format!("Matched: {} node(s), {} rel(s)", self.query_selected_nodes.len(), self.query_selected_rels.len()));
                            });
                            ui.collapsing("Export Matches", |ui| {
                                ui.horizontal(|ui| {
                                    ui.label("Format:");
                                    ui.selectable_value(&mut self.query_export_is_json, true, "JSON");
                                    ui.selectable_value(&mut self.query_export_is_json, false, "CSV");
                                });
                                if self.query_export_path.is_empty() {
                                    let now = time::OffsetDateTime::now_utc();
                                    let fmt = time::macros::format_description!("[year][month][day]_[hour][minute][second]");
                                    let stamp = now.format(&fmt).unwrap_or_else(|_| "now".into());
                                    let ext = if self.query_export_is_json { "json" } else { "csv" };
                                    let mut base = self.app_settings.export_dir();
                                    base.push(format!("query_export_{}.{}", stamp, ext));
                                    self.query_export_path = base.display().to_string();
                                }
                                ui.label("Save as:");
                                ui.text_edit_singleline(&mut self.query_export_path);
                                let can_export = !self.query_selected_nodes.is_empty();
                                if ui.add_enabled(can_export, egui::Button::new("Export Selected Nodes")).clicked() {
                                    let path = std::path::PathBuf::from(self.query_export_path.clone());
                                    let parent = path.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| std::path::PathBuf::from("."));
                                    if let Err(e) = std::fs::create_dir_all(&parent) { self.query_export_status = Some(format!("Failed to create dir: {}", e)); }
                                    else {
                                        let ids: Vec<NodeId> = self.query_selected_nodes.iter().copied().collect();
                                        let res = if self.query_export_is_json { export_nodes_json(&self.db, &ids, &path) } else { export_nodes_csv(&self.db, &ids, &path) };
                                        match res {
                                            Ok(()) => self.query_export_status = Some(format!("Exported {} node(s) to {}", ids.len(), path.display())),
                                            Err(e) => self.query_export_status = Some(format!("Export failed: {}", e)),
                                        }
                                    }
                                }
                                if let Some(msg) = &self.query_export_status { ui.small(msg.clone()); }
                            });
                            if let Some(err) = &self.last_query_error {
                                ui.colored_label(Color32::RED, format!("Error: {}", err));
                            }
                            ui.label("Output:");
                            for line in &self.query_output {
                                ui.monospace(line);
                            }
                            ui.separator();
                            ui.horizontal(|ui| {
                                ui.label("History:");
                                let can_clear = !self.query_history.is_empty();
                                if ui.add_enabled(can_clear, egui::Button::new("Clear History")).on_hover_text("Remove all saved queries from this session").clicked() {
                                    self.query_history.clear();
                                }
                            });
                            for (idx, h) in self.query_history.iter().enumerate().rev().take(20) {
                                if ui.small_button(format!("{}: {}", idx+1, h)).clicked() {
                                    self.query_text = h.clone();
                                }
                            }
                        }); // close Query ScrollArea
                    }); // close Query scope
                } // close SidebarMode::Query
            } // close match self.sidebar_mode
        }); // close SidePanel::show
    } // close if self.sidebar_open

        // Confirmation modal for mass delete
        if self.confirm_mass_delete {
            egui::Window::new("Confirm Delete Selected Nodes")
                .collapsible(false)
                .resizable(false)
                .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
                .show(ctx, |ui| {
                    let count = self.multi_selected_nodes.len();
                    ui.label(format!("This will permanently delete {} selected node(s) and any relationships connected to them.", count));
                    ui.label("This action cannot be undone.");
                    ui.separator();
                    ui.horizontal(|ui| {
                        if ui.button(egui::RichText::new("Delete").color(Color32::RED)).clicked() {
                            let ids: Vec<NodeId> = self.multi_selected_nodes.iter().copied().collect();
                            let mut deleted = 0usize;
                            for id in ids {
                                if self.db.remove_node(id) {
                                    self.node_positions.remove(&id);
                                    self.open_node_windows.remove(&id);
                                    deleted += 1;
                                }
                            }
                            // prune any relationship popouts that no longer exist
                            self.open_rel_windows.retain(|rid| self.db.relationships.contains_key(rid));
                            // clear selection and multi-select
                            self.selected = None;
                            self.multi_selected_nodes.clear();
                            if deleted > 0 { self.mark_dirty(); }
                            self.bulk_status = Some(format!("Deleted {} node(s) and their relationships", deleted));
                            self.confirm_mass_delete = false;
                        }
                        if ui.button("Cancel").clicked() {
                            self.confirm_mass_delete = false;
                        }
                    });
                });
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            // Detect canvas size/position changes and adjust pan to keep view stable
            let prev_rect = self.last_canvas_rect;
            let available = ui.available_rect_before_wrap();
            if let Some(prev) = prev_rect {
                if prev != available {
                    let dc = available.center() - prev.center();
                    // Keep screen positions stable across resize: adjust pan by dc * (zoom - 1)
                    self.pan += dc * (self.zoom - 1.0);
                }
            }
            // remember canvas rect for new-node placement and future resize detection
            self.last_canvas_rect = Some(available);
            // If auto re-cluster requested, apply before drawing
            if self.re_cluster_pending {
                self.apply_cluster_layout_all(available);
            }
            self.ensure_layout(available);

            // Background allocation for panning/clicking, restricted when something is likely being dragged or interacted with.
            // We give nodes first priority for drag; bg_resp gets what's left.
            let bg_sense = Sense::click_and_drag();
            let bg_resp = ui.allocate_rect(available, bg_sense);

            // cancel pick with Esc
            if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                self.pick_target = None;
            }

            // Helpers to transform between world and screen space
            let center = available.center();
            let zoom = self.zoom;
            let pan = self.pan;
            let to_screen = move |p: Pos2| -> Pos2 {
                Pos2::new(
                    (p.x - center.x) * zoom + center.x + pan.x,
                    (p.y - center.y) * zoom + center.y + pan.y,
                )
            };
            let from_screen = move |p: Pos2| -> Pos2 {
                Pos2::new(
                    ((p.x - pan.x) - center.x) / zoom + center.x,
                    ((p.y - pan.y) - center.y) / zoom + center.y,
                )
            };

            // Rectangle (rubber-band) multi-select handling
            if self.multi_select_active {
                // Begin rectangle on left-button drag start over background
                if bg_resp.drag_started() {
                    if let Some(pos) = ui.input(|i| i.pointer.press_origin()) {
                        self.rect_select_start = Some(pos);
                        self.rect_select_current = Some(pos);
                    }
                }
                // Update current corner while dragging
                if let Some(cur) = ui.input(|i| i.pointer.latest_pos()) {
                    if self.rect_select_start.is_some() && bg_resp.dragged() {
                        self.rect_select_current = Some(cur);
                    }
                }
                // On release (primary button up), compute world-rect and add all nodes inside to multi selection
                if self.rect_select_start.is_some() && !ui.input(|i| i.pointer.primary_down()) {
                    if let (Some(a), Some(b)) = (self.rect_select_start.take(), self.rect_select_current.take()) {
                        let aw = from_screen(a);
                        let bw = from_screen(b);
                        let sel_rect = Rect::from_two_pos(aw, bw);
                        for (id, pos_w) in self.node_positions.iter() {
                            if sel_rect.contains(*pos_w) {
                                self.multi_selected_nodes.insert(*id);
                            }
                        }
                    }
                }
            } else {
                // Ensure rectangle state is cleared when not in multi-select mode
                self.rect_select_start = None;
                self.rect_select_current = None;
            }

            // Zoom with scroll only when pointer is over the canvas area
            if bg_resp.hovered() {
                let scroll = ui.input(|i| i.raw_scroll_delta.y);
                if scroll != 0.0 {
                    let factor = (1.0 + scroll * 0.001).clamp(0.9, 1.1);
                    self.zoom = (self.zoom * factor).clamp(0.25, 2.0);
                    // Show transient zoom HUD
                    self.zoom_hud_until = Some(Instant::now() + Duration::from_millis(1000));
                    ui.ctx().request_repaint_after(Duration::from_millis(16));
                }
            }

            // Panning: update pan based on background drag delta, if not in multi-select mode
            // and no node is being dragged.

            let painter = ui.painter_at(available);

            // Draw transient zoom HUD if active
            if let Some(until) = self.zoom_hud_until {
                let now = Instant::now();
                if now < until {
                    let text = format!("{:.2}x", self.zoom);
                    let font = egui::FontId::proportional(14.0);
                    let galley = ui.painter().layout_no_wrap(text, font, Color32::WHITE);
                    let pad = Vec2::new(8.0, 4.0);
                    let size = galley.size() + pad * 2.0;
                    let pos = Pos2::new(available.center().x - size.x * 0.5, available.top() + 12.0);
                    let rect = Rect::from_min_size(pos, size);
                    let bg = Color32::from_rgba_premultiplied(20, 20, 20, 200);
                    painter.rect_filled(rect, 8.0, bg);
                    painter.galley(pos + pad, galley, Color32::WHITE);
                    ui.ctx().request_repaint_after(Duration::from_millis(16));
                } else {
                    self.zoom_hud_until = None;
                }
            }

            // Determine hover before drawing for highlighting/dimming
            // Compute hover over nearest node within radius in screen space
            let mut hover_node: Option<NodeId> = None;
            if let Some(mouse_pos) = ui.ctx().pointer_hover_pos() {
                let node_radius = 10.0 * self.zoom;
                let mut best_d2 = f32::INFINITY;
                for id in self.db.nodes.keys() {
                    if let Some(pw) = self.node_positions.get(id) {
                        let ps = to_screen(*pw);
                        let dx = ps.x - mouse_pos.x; let dy = ps.y - mouse_pos.y;
                        let d2 = dx*dx + dy*dy;
                        if d2 <= (node_radius*node_radius) && d2 < best_d2 {
                            best_d2 = d2; hover_node = Some(*id);
                        }
                    }
                }
            }
            self.hover_node = hover_node;

            // Draw edges (with slight curvature and adaptive opacity)
            let edge_count = self.db.relationships.len();
            let base_alpha: u8 = if self.zoom < 0.7 || edge_count > 600 { 120 } else if self.zoom < 0.9 || edge_count > 300 { 160 } else { 200 };
            let base_color = Color32::from_rgba_premultiplied(200, 200, 200, base_alpha);
            let edge_stroke = Stroke { width: 1.5, color: base_color };
            for rel in self.db.relationships.values() {
                if let (Some(pa), Some(pb)) = (
                    self.node_positions.get(&rel.from_node),
                    self.node_positions.get(&rel.to_node),
                ) {
                    let a = to_screen(*pa);
                    let b = to_screen(*pb);
                    let incident_hover = self.hover_node.map(|h| h == rel.from_node || h == rel.to_node).unwrap_or(false);
            // Highlight if selected AND the popout for this relationship is open
            let is_sel = matches!(self.selected, Some(SelectedItem::Rel(id)) if id == rel.id)
                && self.open_rel_windows.contains(&rel.id);
            let is_qsel = self.query_selected_rels.contains(&rel.id);
            let mut stroke = if is_sel {
                Stroke { width: 3.0, color: Color32::from_rgb(255, 200, 80) }
            } else if is_qsel || incident_hover {
                Stroke { width: 2.5, color: Color32::from_rgb(120, 220, 255) }
            } else {
                edge_stroke
            };
            // Dim edges when hovering another node
            if self.hover_node.is_some() && !incident_hover && !is_sel && !is_qsel {
                let c = stroke.color; stroke.color = Color32::from_rgba_premultiplied(c.r(), c.g(), c.b(), (c.a() as f32 * 0.4) as u8);
            }

            // Curvature: offset midpoint along perpendicular; stable by hashing endpoints
            let dir = Vec2::new(b.x - a.x, b.y - a.y);
            let len = (dir.x * dir.x + dir.y * dir.y).sqrt();
            if len > 1.0 {
                let mid = Pos2::new((a.x + b.x) * 0.5, (a.y + b.y) * 0.5);
                let n = Vec2::new(-dir.y / len, dir.x / len);
                let mut seed = rel.from_node.as_u128() ^ rel.to_node.as_u128();
                seed ^= seed >> 33;
                let sign = if (seed & 1) == 0 { 1.0 } else { -1.0 };
                let mag = (8.0 * self.zoom).clamp(2.0, 16.0);
                let ctrl = mid + n * (mag * sign as f32);
                painter.line_segment([a, ctrl], stroke);
                painter.line_segment([ctrl, b], stroke);
            } else {
                painter.line_segment([a, b], stroke);
            }

                    // Relationship label at midpoint with improved LOD visibility and pill background
                    let mid = Pos2::new((a.x + b.x) * 0.5, (a.y + b.y) * 0.5);
                    let dir = Vec2::new(b.x - a.x, b.y - a.y);
                    let len = (dir.x * dir.x + dir.y * dir.y).sqrt();

                    // Visibility: only show relationship label text when hovering over a connected node
                    let show_label = incident_hover;

                    if show_label && len > f32::EPSILON {
                        // Perpendicular and tangential offsets, alternating per edge for separation
                        let n = Vec2::new(-dir.y / len, dir.x / len);
                        let t = Vec2::new(dir.x / len, dir.y / len);
                        let mut seed = rel.from_node.as_u128() ^ (rel.to_node.as_u128().rotate_left(17)) ^ rel.id.as_u128();
                        seed ^= seed >> 33;
                        let side = if (seed & 1) == 0 { 1.0 } else { -1.0 };
                        let lane = ((seed >> 1) & 3) as i32 - 1; // -1,0,1,2 → center-ish shift
                        let perp_mag = (8.0 * self.zoom).clamp(4.0, 16.0);
                        let tan_mag = (lane as f32) * 4.0 * self.zoom;
                        let offset = n * (perp_mag * side as f32) + t * tan_mag;

                        // Text styling
                        let font = egui::FontId::proportional((12.0 * self.zoom).clamp(8.0, 16.0));
                        let txt_color = if is_sel { Color32::from_rgb(30, 30, 30) } else { Color32::from_rgb(20, 20, 20) };
                        let pill_fill = if is_sel {
                            Color32::from_rgba_premultiplied(255, 220, 120, 220)
                        } else if is_qsel || incident_hover {
                            Color32::from_rgba_premultiplied(180, 235, 255, self.edge_label_bg_alpha)
                        } else {
                            Color32::from_rgba_premultiplied(245, 245, 245, self.edge_label_bg_alpha)
                        };
                        let _outline = Color32::from_rgba_premultiplied(0, 0, 0, 120);

                        // Layout the text to size the pill
                        let galley = ui.painter().layout_no_wrap(rel.label.clone(), font.clone(), txt_color);
                        let pad = Vec2::new(6.0 * self.zoom, 3.0 * self.zoom);
                        let pill_size = galley.size() + pad * 2.0;
                        let center = mid + offset;
                        let rect = Rect::from_center_size(center, pill_size);
                        // Halo: draw a slightly larger translucent rect behind
                        let halo_rect = Rect::from_center_size(center, pill_size + Vec2::new(4.0, 2.0));
                        let rounding = 6.0 * self.zoom;
                        painter.rect_filled(halo_rect, rounding, Color32::from_rgba_premultiplied(0, 0, 0, 25));
                        // Pill background (optionally could add outline if API supports it)
                        painter.rect_filled(rect, rounding, pill_fill);
                        // Draw text centered
                        painter.galley(center - galley.size() * 0.5, galley, txt_color);
                    }
                }
            }

            // Draw and interact with nodes
            let node_radius_draw = 10.0 * self.zoom; // scale with zoom for easier hit testing
            let mut clicked_node: Option<NodeId> = None;
            let mut any_node_dragged = false;
            let was_dragging = self.dragging.is_some();

            // Iterate over a snapshot of ids to avoid borrowing conflicts when we
            // lazily initialize positions.
            let node_ids: Vec<NodeId> = self.db.nodes.keys().copied().collect();
            for id in node_ids {
                // Be resilient if a node is missing a precomputed position
                let pos_world = self.get_or_init_position(id, available);
                // Safe to immutably read the node after the mutable borrow in get_or_init_position ends
                let node = match self.db.nodes.get(&id) { Some(n) => n, None => continue };
                let pos_screen = to_screen(pos_world);
                let rect = Rect::from_center_size(pos_screen, Vec2::splat(node_radius_draw * 2.0));
                let resp = ui.allocate_rect(rect, Sense::click_and_drag());

                // Soft dragging: we don't directly set position here; we mark dragging and add a spring-to-mouse force later.
                if resp.dragged() {
                    if self.dragging.is_none() {
                        // Drag start
                        self.converge_start = Some(Instant::now());
                        self.dragging = Some(id);
                    }
                    any_node_dragged = true;
                }

                if resp.clicked() {
                    clicked_node = Some(id);
                }

                // Hover tooltip: show readable details without cluttering the canvas
                resp.on_hover_ui(|ui| {
                    ui.label(egui::RichText::new(
                        format_short_node(&self.db, id)
                    ).strong());
                    ui.monospace(format!("UUID: {}", id));
                    // Show degree (incident edges) and up to 5 properties
                    let degree = self
                        .db
                        .relationships
                        .values()
                        .filter(|r| r.from_node == id || r.to_node == id)
                        .count();
                    ui.small(format!("degree: {}", degree));
                    if let Some(n) = self.db.nodes.get(&id) {
                        let mut shown = 0usize;
                        for (k, v) in n.metadata.iter() {
                            if shown >= 5 { break; }
                            ui.small(format!("{}: {}", k, v));
                            shown += 1;
                        }
                        if n.metadata.len() > 5 { ui.small(format!("(+{} more)", n.metadata.len() - 5)); }
                    }
                });

                // Visuals
                // A node is visually selected only if its details window is open
                let is_selected = matches!(self.selected, Some(SelectedItem::Node(nid)) if nid == id)
                    && self.open_node_windows.contains(&id);
                let fill = if is_selected { Color32::from_rgb(80, 120, 255) } else { Color32::from_rgb(60, 60, 60) };
                // Highlight From/To selections
                let mut stroke = if is_selected { Stroke::new(2.0, Color32::WHITE) } else { Stroke::new(1.5, Color32::DARK_GRAY) };
                if self.create_rel_from == Some(id) { stroke = Stroke::new(2.5, Color32::from_rgb(80, 220, 120)); }
                if self.create_rel_to == Some(id) { stroke = Stroke::new(2.5, Color32::from_rgb(255, 170, 60)); }
                painter.circle_filled(pos_screen, node_radius_draw, fill);
                painter.circle_stroke(pos_screen, node_radius_draw, stroke);

                // Bulk select halo indicator (independent from popout selection)
                if self.multi_selected_nodes.contains(&id) {
                    let halo_r = node_radius_draw + (3.0 * self.zoom).clamp(2.0, 8.0);
                    painter.circle_stroke(
                        pos_screen,
                        halo_r,
                        Stroke::new(1.5, Color32::from_rgb(120, 200, 255)),
                    );
                }

                // Label (no UUID) with label-based color coding and LOD rules
                let show_label = if !self.lod_enabled { true } else {
                    let many = self.db.nodes.len() >= self.lod_hide_labels_node_threshold;
                    let zoom_ok = self.zoom >= self.lod_label_min_zoom;
                    let is_hover = self.hover_node == Some(id);
                    let is_query = self.query_selected_nodes.contains(&id);
                    let is_sel = matches!(self.selected, Some(SelectedItem::Node(nid)) if nid == id);
                    (!many && zoom_ok) || is_hover || is_query || is_sel
                };
                if show_label {
                    let text = format_short_node(&self.db, id);
                    let label_color = GraphApp::color_for_label(&node.label);
                    let pos_text = pos_screen + Vec2::new(0.0, -node_radius_draw - 4.0);
                    // multi-direction halo for readability
                    painter.text(
                        pos_text + Vec2::new(0.0, 1.0),
                        egui::Align2::CENTER_BOTTOM,
                        &text,
                        egui::FontId::proportional((14.0 * self.zoom).clamp(10.0, 22.0)),
                        Color32::BLACK,
                    );
                    painter.text(
                        pos_text + Vec2::new(1.0, 0.0),
                        egui::Align2::CENTER_BOTTOM,
                        &text,
                        egui::FontId::proportional((14.0 * self.zoom).clamp(10.0, 22.0)),
                        Color32::BLACK,
                    );
                    painter.text(
                        pos_text,
                        egui::Align2::CENTER_BOTTOM,
                        text,
                        egui::FontId::proportional((14.0 * self.zoom).clamp(10.0, 22.0)),
                        label_color,
                    );
                }

                // Query-match halo indicator
                if self.query_selected_nodes.contains(&id) {
                    let halo_r = node_radius_draw + (5.0 * self.zoom).clamp(2.0, 10.0);
                    painter.circle_stroke(
                        pos_screen,
                        halo_r,
                        Stroke::new(2.0, Color32::from_rgb(120, 220, 255)),
                    );
                }
            }

            if let Some(id) = clicked_node {
                if let Some(target) = self.pick_target {
                    match target {
                        PickTarget::From => { self.create_rel_from = Some(id); self.pick_target = None; }
                        PickTarget::To => { self.create_rel_to = Some(id); self.pick_target = None; }
                        PickTarget::NewNodeTarget => {
                            // Set the target for pre-linking a new node
                            self.create_node_rel_target = Some(id);
                            if let Some(new_id) = self.pending_new_node_for_link {
                                if new_id != id {
                                    let rel_label = if self.create_node_rel_label.trim().is_empty() { "REL".to_string() } else { self.create_node_rel_label.trim().to_string() };
                                    let rid_opt = match self.create_node_rel_direction {
                                        NewNodeRelDir::NewToExisting => self.db.add_relationship(new_id, id, rel_label, HashMap::new()),
                                        NewNodeRelDir::ExistingToNew => self.db.add_relationship(id, new_id, rel_label, HashMap::new()),
                                    };
                                    if let Some(rid) = rid_opt {
                                        self.selected = Some(SelectedItem::Rel(rid));
                                    }
                                    self.mark_dirty();
                                }
                                // Clear pending regardless to end the flow
                                self.pending_new_node_for_link = None;
                            }
                            self.pick_target = None;
                        }
                    }
                } else if self.multi_select_active {
                    // Toggle membership in bulk selection; do not open popouts
                    if self.multi_selected_nodes.contains(&id) {
                        self.multi_selected_nodes.remove(&id);
                    } else {
                        self.multi_selected_nodes.insert(id);
                    }
                } else {
                    // Toggle behavior: if re-clicking the same node and its window is open, close it
                    if matches!(self.selected, Some(SelectedItem::Node(nid)) if nid == id)
                        && self.open_node_windows.contains(&id)
                    {
                        self.open_node_windows.remove(&id);
                        self.selected = None;
                    } else {
                        self.selected = Some(SelectedItem::Node(id));
                        // Open (or keep) a separate window for this node
                        self.open_node_windows.insert(id);
                    }
                }
            }

            if !any_node_dragged {
                // If a drag just ended, allow a brief settle period by restarting convergence
                if was_dragging && self.dragging.is_some() {
                    self.converge_start = Some(Instant::now());
                }
                self.dragging = None;

                // Background Panning: update pan based on background drag delta,
                // if not in multi-select mode and no node was dragged this frame.
                if !self.multi_select_active {
                    let delta = bg_resp.drag_delta();
                    if delta != Vec2::ZERO {
                        self.pan += delta;
                        self.mark_dirty();
                    }
                }
            }
            if any_node_dragged { self.mark_dirty(); }

            // Edge hit testing and selection when background is clicked and not dragging nodes
            if !self.multi_select_active && clicked_node.is_none() && !any_node_dragged && bg_resp.clicked() {
                if let Some(pointer_pos) = ui.input(|i| i.pointer.latest_pos()) {
                    // Helper: compute the same curved polyline used for drawing
                    let compute_edge_points = |a: Pos2, b: Pos2, _rel_id: Uuid, from_id: NodeId, to_id: NodeId| -> (Pos2, Pos2, Pos2) {
                        let dir = Vec2::new(b.x - a.x, b.y - a.y);
                        let len = (dir.x * dir.x + dir.y * dir.y).sqrt();
                        if len > 1.0 {
                            let mid = Pos2::new((a.x + b.x) * 0.5, (a.y + b.y) * 0.5);
                            let n = Vec2::new(-dir.y / len, dir.x / len);
                            let mut seed = from_id.as_u128() ^ to_id.as_u128();
                            seed ^= seed >> 33;
                            let sign = if (seed & 1) == 0 { 1.0 } else { -1.0 };
                            let mag = (8.0 * self.zoom).clamp(2.0, 16.0);
                            let ctrl = mid + n * (mag * sign as f32);
                            (a, ctrl, b)
                        } else {
                            // very short edge: treat as straight
                            (a, a.lerp(b, 0.5), b)
                        }
                    };

                    // Find nearest edge under cursor against the two drawn segments (a->ctrl, ctrl->b)
                    let mut best: Option<(Uuid, f32)> = None; // (rel_id, distance)
                    let tolerance_px = 8.0_f32; // selection slop in screen pixels
                    for rel in self.db.relationships.values() {
                        if let (Some(pa), Some(pb)) = (
                            self.node_positions.get(&rel.from_node),
                            self.node_positions.get(&rel.to_node),
                        ) {
                            let a = to_screen(*pa);
                            let b = to_screen(*pb);
                            // Quick AABB reject expanded by tolerance
                            let minx = a.x.min(b.x) - tolerance_px;
                            let maxx = a.x.max(b.x) + tolerance_px;
                            let miny = a.y.min(b.y) - tolerance_px;
                            let maxy = a.y.max(b.y) + tolerance_px;
                            if pointer_pos.x < minx || pointer_pos.x > maxx || pointer_pos.y < miny || pointer_pos.y > maxy {
                                // still continue because curved ctrl could extend beyond, but this is a good early out.
                            }
                            let (pa_s, pc_s, pb_s) = compute_edge_points(a, b, rel.id, rel.from_node, rel.to_node);
                            let d1 = point_segment_distance(pointer_pos, pa_s, pc_s);
                            let d2 = point_segment_distance(pointer_pos, pc_s, pb_s);
                            let d = d1.min(d2);
                            if d <= tolerance_px {
                                match best {
                                    None => best = Some((rel.id, d)),
                                    Some((_, bd)) if d < bd => best = Some((rel.id, d)),
                                    _ => {}
                                }
                            }
                        }
                    }
                    if let Some((rid, _)) = best {
                        // Toggle behavior: if re-clicking the same relationship and its window is open, close it
                        if matches!(self.selected, Some(SelectedItem::Rel(sel_rid)) if sel_rid == rid)
                            && self.open_rel_windows.contains(&rid)
                        {
                            self.open_rel_windows.remove(&rid);
                            self.selected = None;
                        } else {
                            self.selected = Some(SelectedItem::Rel(rid));
                            // Open (or keep) a separate window for this relationship
                            self.open_rel_windows.insert(rid);
                        }
                    }
                }
            }

            // Draw rectangle overlay last so it appears above nodes/edges
            if let (Some(a), Some(b)) = (self.rect_select_start, self.rect_select_current) {
                let rect = Rect::from_two_pos(a, b);
                let fill = Color32::from_rgba_premultiplied(100, 150, 255, 40);
                let stroke = Stroke::new(1.0, Color32::from_rgba_premultiplied(100, 150, 255, 160));
                painter.rect_filled(rect, 0.0, fill);
                painter.rect_stroke(rect, 0.0, stroke, egui::StrokeKind::Inside);
            }

            // Smooth convergence using a simple spring-damper integration.
            // Neo4j-style aids for large graphs: center gravity and degree-aware repulsion.
            let active = match self.converge_start { Some(t0) => t0.elapsed() < Duration::from_secs(5), None => false };
            if active || any_node_dragged || self.dragging.is_some() {
                // Nodes connected by relationships experience a spring force toward a target length.
                // Nearby nodes experience a soft repulsive force to maintain spacing.
                // We integrate per-node velocities with damping for fluid motion.
                let dt = ctx.input(|i| i.stable_dt).clamp(0.001, 0.033);
                let target_dist = 120.0_f32; // preferred edge length in world space
                let spring_k = 4.0_f32;      // edge spring stiffness (units/s^2)
                let damping = 6.0_f32;       // velocity damping (units/s)
                let min_sep = 90.0_f32;      // minimum comfortable spacing
                let repulse_k = 10.0_f32;    // repulsion strength
                let max_speed = 600.0_f32;   // clamp velocity magnitude (units/s)
                let max_step = 5.0_f32;      // clamp displacement per frame (units)
                let mouse_k = 20.0_f32;      // drag-to-mouse spring stiffness

                // Ensure velocity entries exist for all positioned nodes
                for id in self.db.nodes.keys().copied() {
                    self.node_positions.entry(id).or_insert_with(|| Pos2::new(0.0, 0.0));
                    self.node_velocities.entry(id).or_insert(Vec2::ZERO);
                }

                // Pre-calculate dragged unit if we are in a multiselect drag
                let mut dragged_unit: HashSet<NodeId> = HashSet::new();
                if let Some(drag_id) = self.dragging {
                    if self.multi_selected_nodes.contains(&drag_id) && !self.multi_selected_nodes.is_empty() {
                        dragged_unit.extend(self.multi_selected_nodes.iter().copied());
                        let mut stack: Vec<NodeId> = self.multi_selected_nodes.iter().copied().collect();
                        while let Some(curr) = stack.pop() {
                            for rel in self.db.relationships.values() {
                                if rel.from_node == curr {
                                    if dragged_unit.insert(rel.to_node) {
                                        stack.push(rel.to_node);
                                    }
                                } else if rel.to_node == curr {
                                    if dragged_unit.insert(rel.from_node) {
                                        stack.push(rel.from_node);
                                    }
                                }
                            }
                        }
                    } else {
                        dragged_unit.insert(drag_id);
                    }
                }

                // Accumulate forces
                let mut forces: HashMap<NodeId, Vec2> = HashMap::new();
                // Relationship springs (bidirectional: attract if stretched, repel if compressed)
                for rel in self.db.relationships.values() {
                    let (a_id, b_id) = (rel.from_node, rel.to_node);
                    
                    // If we are dragging a multi-selection, and either node is part of the unit,
                    // we "lock out" the physics for these nodes to prevent them from being pulled back.
                    if !dragged_unit.is_empty() && self.dragging.is_some() && !self.multi_selected_nodes.is_empty() {
                        if dragged_unit.contains(&a_id) || dragged_unit.contains(&b_id) {
                            continue;
                        }
                    }

                    let (pa_opt, pb_opt) = (self.node_positions.get(&a_id).copied(), self.node_positions.get(&b_id).copied());
                    if let (Some(pa), Some(pb)) = (pa_opt, pb_opt) {
                        let dx = pb.x - pa.x;
                        let dy = pb.y - pa.y;
                        let dist2 = dx * dx + dy * dy;
                        if dist2 > 1e-6 {
                            let dist = dist2.sqrt();
                            let dir = Vec2::new(dx / dist, dy / dist);
                            let stretch = dist - target_dist;
                            let f = dir * (spring_k * stretch);
                            *forces.entry(a_id).or_insert(Vec2::ZERO) += f;
                            *forces.entry(b_id).or_insert(Vec2::ZERO) -= f;
                        }
                    }
                }

                // Gravity: prefer local center-of-mass (COM) attraction when nodes cluster off-center; otherwise pull to window center.
                if self.gravity_enabled {
                    let center_world = from_screen(available.center());
                    let k_g = self.gravity_strength;
                    let r2 = self.com_gravity_radius * self.com_gravity_radius;
                    // Iterate over a snapshot to avoid borrow conflicts
                    let snapshot: Vec<(NodeId, Pos2)> = self.node_positions.iter().map(|(k,v)| (*k, *v)).collect();
                    for (id, pos) in snapshot.iter() {
                        // If we are dragging a multi-selection, and this node is part of the unit,
                        // we lock out gravity.
                        if !dragged_unit.is_empty() && self.dragging.is_some() && !self.multi_selected_nodes.is_empty() {
                            if dragged_unit.contains(id) {
                                continue;
                            }
                        }

                        // Compute local COM of neighbors within radius (excluding self)
                        let mut sum_x = 0.0f32;
                        let mut sum_y = 0.0f32;
                        let mut count = 0usize;
                        for (oid, opos) in snapshot.iter() {
                            if oid == id { continue; }
                            let dx = opos.x - pos.x;
                            let dy = opos.y - pos.y;
                            if dx*dx + dy*dy <= r2 {
                                sum_x += opos.x;
                                sum_y += opos.y;
                                count += 1;
                            }
                        }
                        let target = if count >= self.com_gravity_min_neighbors {
                            Pos2 { x: sum_x / (count as f32), y: sum_y / (count as f32) }
                        } else {
                            center_world
                        };
                        let dir = Vec2::new(target.x - pos.x, target.y - pos.y);
                        *forces.entry(*id).or_insert(Vec2::ZERO) += dir * k_g;
                    }
                }

                // Degree-aware repulsive separation for close pairs (O(N^2) but small/med graphs are fine)
                let mut deg: HashMap<NodeId, usize> = HashMap::new();
                for rel in self.db.relationships.values() {
                    *deg.entry(rel.from_node).or_insert(0) += 1;
                    *deg.entry(rel.to_node).or_insert(0) += 1;
                }
                let ids: Vec<NodeId> = self.db.nodes.keys().copied().collect();
                for i in 0..ids.len() {
                    for j in (i + 1)..ids.len() {
                        let a = ids[i];
                        let b = ids[j];

                        // If we are dragging a multi-selection, and either node is part of the unit,
                        // we lock out repulsion for these nodes.
                        if !dragged_unit.is_empty() && self.dragging.is_some() && !self.multi_selected_nodes.is_empty() {
                            if dragged_unit.contains(&a) || dragged_unit.contains(&b) {
                                continue;
                            }
                        }

                        let (pa_opt, pb_opt) = (self.node_positions.get(&a).copied(), self.node_positions.get(&b).copied());
                        let (pa, pb) = match (pa_opt, pb_opt) { (Some(pa), Some(pb)) => (pa, pb), _ => continue };
                        let dx = pb.x - pa.x;
                        let dy = pb.y - pa.y;
                        let dist2 = dx * dx + dy * dy;
                        if dist2 < 1e-6 { continue; }
                        let dist = dist2.sqrt();
                        if dist < min_sep {
                            let dir = Vec2::new(dx / dist, dy / dist);
                            let overlap = (min_sep - dist).max(0.0);
                            // Scale by node degrees to spread hubs a bit more
                            let da = *deg.get(&a).unwrap_or(&0) as f32;
                            let db = *deg.get(&b).unwrap_or(&0) as f32;
                            let scale_a = 1.0 + self.hub_repulsion_scale * (da + 1.0).ln();
                            let scale_b = 1.0 + self.hub_repulsion_scale * (db + 1.0).ln();
                            let f = dir * (repulse_k * overlap);
                            // push opposite directions
                            *forces.entry(a).or_insert(Vec2::ZERO) -= f * scale_a;
                            *forces.entry(b).or_insert(Vec2::ZERO) += f * scale_b;
                        }
                    }
                }

                // Soft drag: apply a spring pulling the dragged node towards the mouse in world space
                // If multiple nodes are selected, dragging one drags them all together by applying
                // the same translation force vector to each selected node.
                if let Some(drag_id) = self.dragging {
                    if let Some(mouse_pos_screen) = ui.input(|i| i.pointer.latest_pos()) {
                        let mouse_world = from_screen(mouse_pos_screen);
                        if let Some(p_drag) = self.node_positions.get(&drag_id).copied() {
                            let dir = Vec2::new(mouse_world.x - p_drag.x, mouse_world.y - p_drag.y);
                            // Apply force to all nodes in the unit
                            for nid in &dragged_unit {
                                *forces.entry(*nid).or_insert(Vec2::ZERO) += dir * mouse_k;
                            }
                        }
                    }
                }

                // Integrate velocities and positions
                let mut any_move = false;
                for (id, _pos) in self.node_positions.clone() {
                    let mut v = *self.node_velocities.entry(id).or_insert(Vec2::ZERO);
                    let f = *forces.get(&id).unwrap_or(&Vec2::ZERO);
                    // a = f - c*v (unit mass)
                    let a = f - v * damping;
                    v += a * dt;
                    // Clamp velocity
                    let speed = v.length();
                    if speed > max_speed { v *= max_speed / speed; }
                    // Displacement this frame
                    let mut step = v * dt;
                    let step_len = step.length();
                    if step_len > max_step { step *= max_step / step_len; }
                    if step != Vec2::ZERO {
                        if let Some(p) = self.node_positions.get_mut(&id) {
                            p.x += step.x;
                            p.y += step.y;
                            any_move = true;
                        }
                    }
                    self.node_velocities.insert(id, v);
                }
                if any_move { self.mark_dirty(); }
            } else {
                // Timeout reached: stop convergence by zeroing velocities
                for v in self.node_velocities.values_mut() { *v = Vec2::ZERO; }
            }
        });

        // Render all open Node windows
        let mut nodes_to_close: Vec<NodeId> = Vec::new();
        let open_node_ids: Vec<NodeId> = self.open_node_windows.iter().copied().collect();
        for id in open_node_ids {
            // Snapshot node and editable state
            let node_snapshot = self.db.nodes.get(&id).cloned();
            if let Some(node_snapshot) = node_snapshot {
                let mut open = true;
                // Prepare editable buffers
                let mut label_text = self
                    .node_label_edits
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| node_snapshot.label.clone());
                let mut new_meta_kv = self
                    .node_meta_new_kv
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| (String::new(), String::new()));
                // Actions to apply post-UI
                let mut do_save_label = false;
                let mut to_remove_keys: Vec<String> = Vec::new();
                let mut upsert_kv: Option<(String, String)> = None;
                let mut delete_node = false;

                egui::Window::new(format!("Node {} Details", id))
                    .id(egui::Id::new(("node_details", id)))
                    .open(&mut open)
                    .resizable(true)
                    .show(ctx, |ui| {
                        ui.label(format!("ID: {}", id));
                        // Label editing
                        ui.horizontal(|ui| {
                            ui.label("Label:");
                            ui.text_edit_singleline(&mut label_text);
                            if ui.button("Save").clicked() {
                                do_save_label = true;
                            }
                        });
                        ui.separator();
                        ui.heading("Metadata");
                        if node_snapshot.metadata.is_empty() {
                            ui.label("<no metadata>");
                        } else {
                            // Present metadata with remove buttons
                            let keys: Vec<String> = node_snapshot.metadata.keys().cloned().collect();
                            for k in keys {
                                let v = node_snapshot.metadata.get(&k).cloned().unwrap_or_default();
                                ui.horizontal(|ui| {
                                    ui.label(&k);
                                    ui.label(":");
                                    ui.monospace(&v);
                                    if ui.button("Remove").clicked() { to_remove_keys.push(k.clone()); }
                                });
                            }
                        }
                        // Add new metadata kv
                        ui.separator();
                        ui.label("Add/Update Metadata");
                        ui.horizontal(|ui| {
                            ui.add(egui::TextEdit::singleline(&mut new_meta_kv.0).hint_text("key"));
                            ui.label(":");
                            ui.add(egui::TextEdit::singleline(&mut new_meta_kv.1).hint_text("value"));
                            if ui.button("Upsert").clicked() {
                                if !new_meta_kv.0.trim().is_empty() {
                                    upsert_kv = Some((new_meta_kv.0.trim().to_string(), new_meta_kv.1.trim().to_string()));
                                    new_meta_kv.0.clear(); new_meta_kv.1.clear();
                                }
                            }
                        });
                        ui.separator();
                        if ui.button(egui::RichText::new("Delete Node").color(Color32::RED)).clicked() {
                            delete_node = true;
                        }
                    });
                // Apply actions
                if do_save_label {
                    if self.db.update_node_label(id, label_text.clone()) { self.re_cluster_pending = true; self.mark_dirty(); }
                }
                if !to_remove_keys.is_empty() {
                    for k in to_remove_keys { if self.db.remove_node_metadata_key(id, &k) { self.re_cluster_pending = true; self.mark_dirty(); } }
                }
                if let Some((k, v)) = upsert_kv { if self.db.upsert_node_metadata(id, k, v) { self.re_cluster_pending = true; self.mark_dirty(); } }
                // persist editors
                self.node_label_edits.insert(id, label_text);
                self.node_meta_new_kv.insert(id, new_meta_kv);
                if delete_node {
                    if self.db.remove_node(id) {
                        self.node_positions.remove(&id);
                        if self.selected == Some(SelectedItem::Node(id)) { self.selected = None; }
                        self.re_cluster_pending = true; self.mark_dirty();
                    }
                }
                if !open { nodes_to_close.push(id); }
            } else {
                nodes_to_close.push(id);
            }
        }
        for id in nodes_to_close {
            self.open_node_windows.remove(&id);
            if matches!(self.selected, Some(SelectedItem::Node(nid)) if nid == id) {
                self.selected = None;
            }
        }

        // Render all open Relationship windows
        let mut rels_to_close: Vec<Uuid> = Vec::new();
        let open_rel_ids: Vec<Uuid> = self.open_rel_windows.iter().copied().collect();
        for rid in open_rel_ids {
            let rel_snapshot = self.db.relationships.get(&rid).cloned();
            if let Some(rel_snapshot) = rel_snapshot {
                let mut open = true;
                let mut label_text = self
                    .rel_label_edits
                    .get(&rid)
                    .cloned()
                    .unwrap_or_else(|| rel_snapshot.label.clone());
                let mut new_meta_kv = self
                    .rel_meta_new_kv
                    .get(&rid)
                    .cloned()
                    .unwrap_or_else(|| (String::new(), String::new()));
                let mut save_label = false;
                let mut remove_keys: Vec<String> = Vec::new();
                let mut upsert_rel_kv: Option<(String, String)> = None;
                let mut delete_rel = false;

                egui::Window::new(format!("Relationship {} Details", rid))
                    .id(egui::Id::new(("rel_details", rid)))
                    .open(&mut open)
                    .resizable(true)
                    .show(ctx, |ui| {
                        ui.label(format!("ID: {}", rid));
                        ui.horizontal(|ui| {
                            ui.label("Label:");
                            ui.text_edit_singleline(&mut label_text);
                            if ui.button("Save").clicked() { save_label = true; }
                        });
                        ui.separator();
                        ui.heading("Endpoints");
                        ui.label(format!("from: {}", rel_snapshot.from_node));
                        ui.label(format!("to:   {}", rel_snapshot.to_node));
                        if let (Some(a), Some(b)) = (
                            self.db.nodes.get(&rel_snapshot.from_node),
                            self.db.nodes.get(&rel_snapshot.to_node),
                        ) {
                            ui.label(format!("from label: {}", a.label));
                            ui.label(format!("to label:   {}", b.label));
                        }
                        ui.separator();
                        ui.heading("Metadata");
                        if rel_snapshot.metadata.is_empty() {
                            ui.label("<no metadata>");
                        } else {
                            let keys: Vec<String> = rel_snapshot.metadata.keys().cloned().collect();
                            for k in keys {
                                let v = rel_snapshot.metadata.get(&k).cloned().unwrap_or_default();
                                ui.horizontal(|ui| {
                                    ui.label(&k);
                                    ui.label(":");
                                    ui.monospace(&v);
                                    if ui.button("Remove").clicked() { remove_keys.push(k.clone()); }
                                });
                            }
                        }
                        // Add/Upsert metadata
                        ui.separator();
                        ui.label("Add/Update Metadata");
                        ui.horizontal(|ui| {
                            ui.add(egui::TextEdit::singleline(&mut new_meta_kv.0).hint_text("key"));
                            ui.label(":");
                            ui.add(egui::TextEdit::singleline(&mut new_meta_kv.1).hint_text("value"));
                            if ui.button("Upsert").clicked() {
                                if !new_meta_kv.0.trim().is_empty() {
                                    upsert_rel_kv = Some((new_meta_kv.0.trim().to_string(), new_meta_kv.1.trim().to_string()));
                                    new_meta_kv.0.clear(); new_meta_kv.1.clear();
                                }
                            }
                        });
                        ui.separator();
                        if ui.button(egui::RichText::new("Delete Relationship").color(Color32::RED)).clicked() { delete_rel = true; }
                    });
                if save_label { if self.db.update_relationship_label(rid, label_text.clone()) { self.re_cluster_pending = true; self.mark_dirty(); } }
                for k in remove_keys { if self.db.remove_relationship_metadata_key(rid, &k) { self.re_cluster_pending = true; self.mark_dirty(); } }
                if let Some((k, v)) = upsert_rel_kv { if self.db.upsert_relationship_metadata(rid, k, v) { self.re_cluster_pending = true; self.mark_dirty(); } }
                self.rel_label_edits.insert(rid, label_text);
                self.rel_meta_new_kv.insert(rid, new_meta_kv);
                if delete_rel {
                    if self.db.remove_relationship(rid) {
                        if self.selected == Some(SelectedItem::Rel(rid)) { self.selected = None; }
                        self.re_cluster_pending = true; self.mark_dirty();
                    }
                }
                if !open { rels_to_close.push(rid); }
            } else {
                rels_to_close.push(rid);
            }
        }
        for rid in rels_to_close {
            self.open_rel_windows.remove(&rid);
            if matches!(self.selected, Some(SelectedItem::Rel(sel)) if sel == rid) {
                self.selected = None;
            }
        }

        // Final guard: if selected item has no corresponding open window, clear selection
        match self.selected {
            Some(SelectedItem::Node(nid)) => {
                if !self.open_node_windows.contains(&nid) {
                    self.selected = None;
                }
            }
            Some(SelectedItem::Rel(rid)) => {
                if !self.open_rel_windows.contains(&rid) {
                    self.selected = None;
                }
            }
            None => {}
        }

        // Autosave logic: only after edits (5 seconds after the last change, prominent)
        let now = Instant::now();
        if self.dirty && now.duration_since(self.last_change) >= Duration::from_secs(5) {
            self.save_now_with(NoticeStyle::Prominent);
        }

        // Load Versions modal
        if self.show_load_versions {
            let mut open = true;
            let mut to_load: Option<std::path::PathBuf> = None;
            let mut loaded_label: Option<String> = None;
            egui::Window::new("Load Version")
                .collapsible(false)
                .resizable(true)
                .open(&mut open)
                .show(ctx, |ui| {
                    match persist::list_versions() {
                        Ok(list) => {
                            if list.is_empty() { ui.label("No versioned files found in assets/"); }
                            for p in list.iter() {
                                let label = p.file_name().and_then(|s| s.to_str()).unwrap_or("<unknown>");
                                if ui.button(label).clicked() {
                                    to_load = Some(p.clone());
                                    loaded_label = Some(label.to_string());
                                }
                            }
                        }
                        Err(e) => { ui.colored_label(Color32::RED, format!("List failed: {}", e)); }
                    }
                });
            if let Some(p) = to_load {
                match persist::load_from_path(&p) {
                    Ok(state) => {
                        let (db, pos, pan, zoom) = state.to_runtime();
                        self.db = db; self.node_positions = pos; self.pan = pan; self.zoom = zoom;
                        self.selected = None; self.open_node_windows.clear(); self.open_rel_windows.clear();
                        self.dirty = false; self.last_change = Instant::now();
                        if let Some(lbl) = loaded_label { 
                            self.last_save_info = Some(format!("Loaded {}", lbl));
                            self.last_info_time = Some(Instant::now());
                            self.last_info_style = NoticeStyle::Prominent;
                        }
                        self.save_error = None;
                        open = false;
                    }
                    Err(e) => { self.save_error = Some(format!("Failed to load {}: {}", p.display(), e)); }
                }
            }
            self.show_load_versions = open;
        }

        // Bottom-right transient "saved"/info toast (visible for 3 seconds)
        if let (Some(msg), Some(when)) = (&self.last_save_info, self.last_info_time) {
            if Instant::now().duration_since(when) <= Duration::from_secs(3) {
                let margin = egui::vec2(12.0, 12.0);
                egui::Area::new("bottom_right_toast".into())
                    .anchor(egui::Align2::RIGHT_BOTTOM, egui::vec2(-margin.x, -margin.y))
                    .interactable(false)
                    .show(ctx, |ui| {
                        let (fill, stroke_col, stroke_w, text_col, inner_margin) = match self.last_info_style {
                            NoticeStyle::Subtle => (
                                Color32::from_rgba_premultiplied(20, 20, 20, 170),
                                Color32::from_gray(60),
                                0.5,
                                Color32::from_gray(200),
                                egui::Margin::symmetric(8, 6),
                            ),
                            NoticeStyle::Prominent => (
                                Color32::from_rgba_premultiplied(30, 30, 30, 230),
                                Color32::from_gray(100),
                                1.5,
                                Color32::LIGHT_GREEN,
                                egui::Margin::symmetric(12, 8),
                            ),
                        };
                        egui::Frame::popup(ui.style())
                            .corner_radius(egui::CornerRadius::same(8))
                            .stroke(Stroke { width: stroke_w, color: stroke_col })
                            .fill(fill)
                            .inner_margin(inner_margin)
                            .show(ui, |ui| {
                                match self.last_info_style {
                                    NoticeStyle::Subtle => { ui.small(egui::RichText::new(msg).color(text_col)); }
                                    NoticeStyle::Prominent => { ui.colored_label(text_col, msg); }
                                }
                            });
                    });
            }
        }
    }
    fn on_exit(&mut self, _gl: Option<&eframe::glow::Context>) {
        if self.app_settings.background_on_close && (self.app_settings.api_enabled || self.app_settings.grpc_enabled) {
            eprintln!("[Graph-Loom] background_on_close is enabled. The API server will continue to run if the process persists.");
            // Note: In standard eframe, on_exit is the last chance to do something before the process exits.
            // If we want to truly background, we would need to have started as a background-capable process.
            // For now, this serves as a hint/hook for future implementation of a persistent service.
        }
    }
}

// Geometry helper: distance from point P to segment AB in screen space
fn point_segment_distance(p: Pos2, a: Pos2, b: Pos2) -> f32 {
    let ap = Vec2::new(p.x - a.x, p.y - a.y);
    let ab = Vec2::new(b.x - a.x, b.y - a.y);
    let ab_len2 = ab.x * ab.x + ab.y * ab.y;
    if ab_len2 <= f32::EPSILON {
        return ((p.x - a.x).powi(2) + (p.y - a.y).powi(2)).sqrt();
    }
    let t = ((ap.x * ab.x + ap.y * ab.y) / ab_len2).clamp(0.0, 1.0);
    let proj = Pos2::new(a.x + ab.x * t, a.y + ab.y * t);
    ((p.x - proj.x).powi(2) + (p.y - proj.y).powi(2)).sqrt()
}

// UI helpers
fn _short_uuid(id: Uuid) -> String {
    let s = id.as_simple().to_string();
    s.chars().rev().take(8).collect::<Vec<char>>().into_iter().rev().collect()
}

fn format_short_node(db: &GraphDatabase, id: NodeId) -> String {
    // Render-friendly node caption without UUID to improve readability on canvas.
    // Prefer a human-friendly metadata field if present.
    if let Some(n) = db.nodes.get(&id) {
        // Prefer commonly used human-readable keys
        if let Some(name) = n.metadata.get("name").filter(|s| !s.is_empty()) {
            return name.clone();
        }
        if let Some(title) = n.metadata.get("title").filter(|s| !s.is_empty()) {
            return title.clone();
        }
        if let Some(keyword) = n.metadata.get("keyword").filter(|s| !s.is_empty()) {
            return keyword.clone();
        }
        // Requirement: Use one of the values from a node's metadata as the rendered name
        // If no preferred key exists but metadata has entries, use a deterministic choice:
        // pick the first non-empty value by alphabetical key order.
        if !n.metadata.is_empty() {
            let mut keys: Vec<&String> = n.metadata.keys().collect();
            keys.sort();
            for k in keys {
                if let Some(val) = n.metadata.get(k).filter(|s| !s.is_empty()) {
                    return val.clone();
                }
            }
        }
        // Fallback to label only (no short uuid)
        return n.label.clone();
    }
    // Unknown node fallback
    "<unknown>".to_string()
}

// Golden-angle spiral placement around the provided center.
// k is the 0-based index along the spiral.
fn golden_spiral_position(center: Pos2, k: u32, rect: Rect) -> Pos2 {
    // Golden angle in radians
    let golden_angle = std::f32::consts::TAU * (1.0 - 1.0 / 1.618_033_9);
    let t = k as f32;
    // Use sqrt growth to keep points from flying out too fast
    let base = (rect.size().min_elem() * 0.12).max(20.0);
    let r = base * t.sqrt();
    let theta = t * golden_angle;
    let x = center.x + r * theta.cos();
    let y = center.y + r * theta.sin();
    Pos2::new(x, y)
}
