use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use eframe::egui::{self, Color32, Pos2, Rect, Sense, Stroke, Vec2};
use uuid::Uuid;

use crate::graph_utils::graph::{GraphDatabase, NodeId};
use crate::persistence::persist::{self, AppStateFile};
use crate::gql::query_interface::{self, QueryResultRow};

// Helpers for exporting matched nodes
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
    // Layout control
    re_cluster_pending: bool,
}

impl GraphApp {
    pub fn new(db: GraphDatabase) -> Self {
        Self {
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
            re_cluster_pending: true,
        }
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
                if let Some((&best_comm, _)) = scores.iter().max_by(|a, b| a.1.partial_cmp(b.1).unwrap()) {
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
            let centroid = *comm_centroids.get(cid).unwrap();
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

            for (&(ix, iy), ids) in grid.clone().iter() {
                for (dx, dy) in offsets {
                    let key = (ix + dx, iy + dy);
                    if let Some(neigh_ids) = grid.get(&key) {
                        for &a in ids {
                            for &b in neigh_ids {
                                if a >= b { continue; } // avoid double-processing and self
                                let pa = *self.node_positions.get(&a).unwrap();
                                let pb = *self.node_positions.get(&b).unwrap();
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

            // Clamp into rect to avoid drifting out of view
            for p in self.node_positions.values_mut() {
                p.x = p.x.clamp(rect.left() + 8.0, rect.right() - 8.0);
                p.y = p.y.clamp(rect.top() + 8.0, rect.bottom() - 8.0);
            }
        }
    }

    pub fn from_state(state: AppStateFile) -> Self {
        let (db, positions, pan, zoom) = state.to_runtime();
        Self {
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
            re_cluster_pending: true,
        }
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
}

impl eframe::App for GraphApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        egui::TopBottomPanel::top("top_bar").show(ctx, |ui| {
            // Use compact menus so options remain accessible regardless of width
            ui.horizontal(|ui| {
                ui.label("Graph-Loom");
                ui.menu_button("File", |ui| {
                    if ui.button("Save").clicked() {
                        self.save_now();
                    }
                    if ui.button("New Graph").clicked() {
                        // Back up existing graph if it's non-empty
                        let had_content = !self.db.nodes.is_empty() || !self.db.relationships.is_empty();
                        if had_content {
                            self.save_versioned_now();
                        }

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
                        // menu will auto-close on click in recent egui versions
                    }
                    if ui.button("Save Versioned Copy").clicked() {
                        self.save_versioned_now();
                    }
                    if ui.button("Load Latest").clicked() {
                        match persist::load_active() {
                            Ok(Some(state)) => {
                                // replace runtime
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
                        // menu will auto-close on click in recent egui versions
                    }
                    if ui.button("Load Version…").clicked() { self.show_load_versions = true; }
                });

                ui.menu_button("View", |ui| {
                    if ui.button("Reset View").clicked() {
                        self.pan = Vec2::ZERO;
                        self.zoom = 1.0;
                        self.mark_dirty();
                    }
                    ui.separator();
                    ui.label("Zoom");
                    ui.add(egui::Slider::new(&mut self.zoom, 0.25..=2.0));
                });

                ui.menu_button("Sidebar", |ui| {
                    let toggle = if self.sidebar_open { "Hide Sidebar" } else { "Show Sidebar" };
                    if ui.button(toggle).clicked() { self.sidebar_open = !self.sidebar_open; }
                    ui.separator();
                    ui.label("Mode");
                    if ui.selectable_label(self.sidebar_mode == SidebarMode::Tooling, "Tooling").clicked() { self.sidebar_mode = SidebarMode::Tooling; }
                    if ui.selectable_label(self.sidebar_mode == SidebarMode::Query, "Query").clicked() { self.sidebar_mode = SidebarMode::Query; }
                });

                ui.menu_button("Windows", |ui| {
                    ui.label(format!(
                        "Open pop-outs: nodes {} | rels {}",
                        self.open_node_windows.len(),
                        self.open_rel_windows.len()
                    ));
                    if ui.button("Close All Pop-outs").clicked() {
                        self.open_node_windows.clear();
                        self.open_rel_windows.clear();
                    }
                });

                // Keep a tiny status label; avoid long texts to prevent hiding on small widths
                ui.small(format!("N:{} R:{}", self.db.nodes.len(), self.db.relationships.len()));
                if let Some(err) = &self.save_error { ui.separator(); ui.colored_label(Color32::RED, err); }
            });
        });

        // Sidebar switchable between Tooling and Query console
        if self.sidebar_open && self.sidebar_mode == SidebarMode::Tooling {
            egui::SidePanel::left("tooling_sidebar")
                .resizable(true)
                .default_width(260.0)
                .show(ctx, |ui| {
                    ui.heading("Tooling");
                    ui.separator();
                    // Make tooling usable on very small windows via scrolling
                    egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {

                    // Layout tools
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
                                    if let Some(rid) = self.db.add_relationship(from.unwrap(), to.unwrap(), label, md) {
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
                                }
                            }
                            if let Some(e) = error_rel { ui.colored_label(Color32::RED, e); }
                        });

                    egui::CollapsingHeader::new("Bulk Edit Nodes")
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
                    });
                });
        }
        if self.sidebar_open && self.sidebar_mode == SidebarMode::Query {
            egui::SidePanel::left("query_sidebar")
                .resizable(true)
                .default_width(300.0)
                .show(ctx, |ui| {
                    ui.heading("Query Console");
                    ui.separator();
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
                                .desired_width(f32::INFINITY);
                            ui.add(edit);
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
                                    self.query_export_path = format!("assets/exports/query_export_{}.{}", stamp, ext);
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
                            ui.label("History:");
                            for (idx, h) in self.query_history.iter().enumerate().rev().take(20) {
                                if ui.small_button(format!("{}: {}", idx+1, h)).clicked() {
                                    self.query_text = h.clone();
                                }
                            }
                        });
                    });
                });
        }

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

            // Panning with middle mouse or dragging background
            let bg_resp = ui.allocate_rect(available, Sense::click_and_drag());
            if bg_resp.dragged_by(egui::PointerButton::Middle)
                || (bg_resp.dragged() && self.dragging.is_none())
            {
                self.pan += bg_resp.drag_delta();
            }
            // cancel pick with Esc
            if ui.input(|i| i.key_pressed(egui::Key::Escape)) {
                self.pick_target = None;
            }
            // Zoom with scroll only when pointer is over the canvas area
            if bg_resp.hovered() {
                let scroll = ui.input(|i| i.raw_scroll_delta.y);
                if scroll != 0.0 {
                    let factor = (1.0 + scroll * 0.001).clamp(0.9, 1.1);
                    self.zoom = (self.zoom * factor).clamp(0.25, 2.0);
                }
            }

            let painter = ui.painter_at(available);

            // Helper to transform world->screen using captured pan/zoom to avoid borrowing self
            let center = available.center();
            let zoom = self.zoom;
            let pan = self.pan;
            let to_screen = move |p: Pos2| -> Pos2 {
                Pos2::new(
                    (p.x - center.x) * zoom + center.x + pan.x,
                    (p.y - center.y) * zoom + center.y + pan.y,
                )
            };
            // Inverse: screen -> world
            let from_screen = move |p: Pos2| -> Pos2 {
                Pos2::new(
                    ((p.x - pan.x) - center.x) / zoom + center.x,
                    ((p.y - pan.y) - center.y) / zoom + center.y,
                )
            };

            // Draw edges
            let edge_stroke = Stroke { width: 1.5, color: Color32::LIGHT_GRAY };
            for rel in self.db.relationships.values() {
                if let (Some(pa), Some(pb)) = (
                    self.node_positions.get(&rel.from_node),
                    self.node_positions.get(&rel.to_node),
                ) {
                    let a = to_screen(*pa);
                    let b = to_screen(*pb);
                    // Highlight if selected AND the popout for this relationship is open
                    let is_sel = matches!(self.selected, Some(SelectedItem::Rel(id)) if id == rel.id)
                        && self.open_rel_windows.contains(&rel.id);
                    let is_qsel = self.query_selected_rels.contains(&rel.id);
                    let stroke = if is_sel {
                        Stroke { width: 3.0, color: Color32::from_rgb(255, 200, 80) }
                    } else if is_qsel {
                        Stroke { width: 2.5, color: Color32::from_rgb(120, 220, 255) }
                    } else {
                        edge_stroke
                    };
                    painter.line_segment([a, b], stroke);

                    // Relationship label at midpoint with a small perpendicular offset
                    let mid = Pos2::new((a.x + b.x) * 0.5, (a.y + b.y) * 0.5);
                    let dir = Vec2::new(b.x - a.x, b.y - a.y);
                    let len = (dir.x * dir.x + dir.y * dir.y).sqrt();
                    let mut offset = Vec2::ZERO;
                    if len > f32::EPSILON {
                        // Perpendicular to the edge, scaled by zoom to keep readable
                        let n = Vec2::new(-dir.y / len, dir.x / len);
                        offset = n * (8.0f32 * self.zoom);
                    }
                    let rel_text_color = if is_sel { Color32::from_rgb(255, 230, 120) } else if is_qsel { Color32::from_rgb(180, 235, 255) } else { Color32::WHITE };
                    painter.text(
                        mid + offset,
                        egui::Align2::CENTER_CENTER,
                        rel.label.as_str(),
                        egui::FontId::proportional(12.0),
                        rel_text_color,
                    );
                }
            }

            // Draw and interact with nodes
            let node_radius = 10.0 * self.zoom; // scale with zoom for easier hit testing
            let mut clicked_node: Option<NodeId> = None;
            let mut any_node_dragged = false;
            // Track drag state transition to restart convergence timer
            let was_dragging = self.dragging.is_some();

            for (id, _node) in &self.db.nodes {
                let pos_world = self.node_positions[id];
                let pos_screen = to_screen(pos_world);
                let rect = Rect::from_center_size(pos_screen, Vec2::splat(node_radius * 2.0));
                let resp = ui.allocate_rect(rect, Sense::click_and_drag());

                // Soft dragging: we don't directly set position here; we mark dragging and add a spring-to-mouse force later.
                if resp.dragged() {
                    if self.dragging.is_none() {
                        // Drag start
                        self.converge_start = Some(Instant::now());
                        self.dragging = Some(*id);
                    }
                    any_node_dragged = true;
                }

                if resp.clicked() {
                    clicked_node = Some(*id);
                }

                // Visuals
                // A node is visually selected only if its details window is open
                let is_selected = matches!(self.selected, Some(SelectedItem::Node(nid)) if nid == *id)
                    && self.open_node_windows.contains(id);
                let fill = if is_selected { Color32::from_rgb(80, 120, 255) } else { Color32::from_rgb(60, 60, 60) };
                // Highlight From/To selections
                let mut stroke = if is_selected { Stroke::new(2.0, Color32::WHITE) } else { Stroke::new(1.5, Color32::DARK_GRAY) };
                if self.create_rel_from == Some(*id) { stroke = Stroke::new(2.5, Color32::from_rgb(80, 220, 120)); }
                if self.create_rel_to == Some(*id) { stroke = Stroke::new(2.5, Color32::from_rgb(255, 170, 60)); }
                painter.circle_filled(pos_screen, node_radius, fill);
                painter.circle_stroke(pos_screen, node_radius, stroke);

                // Bulk select halo indicator (independent from popout selection)
                if self.multi_selected_nodes.contains(id) {
                    let halo_r = node_radius + (3.0 * self.zoom).clamp(2.0, 8.0);
                    painter.circle_stroke(
                        pos_screen,
                        halo_r,
                        Stroke::new(1.5, Color32::from_rgb(120, 200, 255)),
                    );
                }

                // Label (append short uuid)
                let text = format_short_node(&self.db, *id);
                painter.text(
                    pos_screen + Vec2::new(0.0, -node_radius - 4.0),
                    egui::Align2::CENTER_BOTTOM,
                    text,
                    egui::FontId::proportional(14.0),
                    Color32::WHITE,
                );

                // Query-match halo indicator
                if self.query_selected_nodes.contains(id) {
                    let halo_r = node_radius + (5.0 * self.zoom).clamp(2.0, 10.0);
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
            }
            if any_node_dragged { self.mark_dirty(); }

            // Edge hit testing and selection when background is clicked and not dragging nodes
            if !self.multi_select_active && clicked_node.is_none() && !any_node_dragged && bg_resp.clicked() {
                if let Some(pointer_pos) = ui.input(|i| i.pointer.latest_pos()) {
                    // Find nearest edge under cursor
                    let mut best: Option<(Uuid, f32)> = None; // (rel_id, distance)
                    let threshold = 6.0_f32; // pixels
                    for rel in self.db.relationships.values() {
                        if let (Some(pa), Some(pb)) = (
                            self.node_positions.get(&rel.from_node),
                            self.node_positions.get(&rel.to_node),
                        ) {
                            let a = to_screen(*pa);
                            let b = to_screen(*pb);
                            let d = point_segment_distance(pointer_pos, a, b);
                            if d <= threshold {
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

            // Smooth convergence using a simple spring-damper integration, with a 3s timeout.
            let active = match self.converge_start {
                Some(t0) => t0.elapsed() < Duration::from_secs(3),
                None => false,
            };
            if active {
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

                // Accumulate forces
                let mut forces: HashMap<NodeId, Vec2> = HashMap::new();
                // Relationship springs (bidirectional: attract if stretched, repel if compressed)
                for rel in self.db.relationships.values() {
                    let (a_id, b_id) = (rel.from_node, rel.to_node);
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

                // Repulsive separation for close pairs (O(N^2) but small graphs are fine)
                let ids: Vec<NodeId> = self.db.nodes.keys().copied().collect();
                for i in 0..ids.len() {
                    for j in (i + 1)..ids.len() {
                        let a = ids[i];
                        let b = ids[j];
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
                            let f = dir * (repulse_k * overlap);
                            // push opposite directions
                            *forces.entry(a).or_insert(Vec2::ZERO) -= f;
                            *forces.entry(b).or_insert(Vec2::ZERO) += f;
                        }
                    }
                }

                // Soft drag: apply a spring pulling the dragged node towards the mouse in world space
                if let Some(drag_id) = self.dragging {
                    if let Some(mouse_pos_screen) = ui.input(|i| i.pointer.latest_pos()) {
                        let mouse_world = from_screen(mouse_pos_screen);
                        if let Some(p) = self.node_positions.get(&drag_id).copied() {
                            let dir = Vec2::new(mouse_world.x - p.x, mouse_world.y - p.y);
                            *forces.entry(drag_id).or_insert(Vec2::ZERO) += dir * mouse_k;
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
fn short_uuid(id: Uuid) -> String {
    let s = id.as_simple().to_string();
    s.chars().rev().take(8).collect::<Vec<char>>().into_iter().rev().collect()
}

fn format_short_node(db: &GraphDatabase, id: NodeId) -> String {
    if let Some(n) = db.nodes.get(&id) {
        format!("{} ({})", n.label, short_uuid(id))
    } else {
        format!("<unknown> ({})", short_uuid(id))
    }
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
