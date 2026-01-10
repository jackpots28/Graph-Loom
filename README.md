# GraphDB Note App (Rust): "Graph-Loom"

## Overview

Graph-Loom is a lightweight, local-first graph notebook and visualizer built with Rust + egui. It lets you create nodes and relationships, view them on a canvas, and interactively arrange them with a smooth, physics-assisted layout. A minimal GQL-like query console supports creating, matching, and deleting graph elements.

---

## Key Features

- Auto-cluster layout
  - Community detection clusters nodes by: relationships, label similarity, and metadata overlap.
  - Well-formed clusters are placed toward the border; sparsely connected/outlier nodes gravitate toward the center for readability.

- No-overlap guarantee
  - A fast post-layout resolver separates nodes to a minimum spacing so they never render on top of each other.

- Smooth motion with a 3s settle window
  - Spring–damper physics for edges and soft repulsion for nearby nodes yield natural movement.
  - “Soft dragging” pulls a node toward the cursor with a spring (neighbors flow out of the way) instead of hard snapping.
  - All motion automatically halts 3 seconds after the last layout/drag change.

- Responsive window resizing & robust top bar
  - The UI and canvas adapt to smaller or larger window sizes.
  - Tool sidebars are scrollable on small windows; the graph view recenters to keep content stable when resizing.
  - Top bar uses compact drop-down menus so actions remain accessible even on very narrow windows.

- Practical persistence
  - State is saved to assets/state.ron and loaded on startup. If no state exists, a blank graph is created.
  - File → New Graph creates an empty graph and saves a versioned backup of the current graph first (if non-empty).

- Clean UI grouping
  - The left sidebar groups tools into collapsible sections (collapsed by default on startup): Layout, Create Node, Create Relationship, Bulk Edit Nodes.

- Query logging
  - Executed queries are logged to assets/logs/queries_YYYYMMDD.log with basic status info.

---

## Getting Started

Prerequisites

- Rust toolchain (stable): https://www.rust-lang.org/tools/install

---

## Build and Run

- Debug: cargo run
- Release: cargo run --release

On first launch, if assets/state.ron is missing, you’ll start with an empty graph. Subsequent runs will load the saved state automatically.

---

## Using the App

- Panning/zooming & resizing
  - Zoom only applies when your cursor is over the graph canvas. Scrolling in the left sidebar or other UI panels will not change the graph zoom.
  - Drag the canvas background (or use middle mouse) to pan.
  - The window is fully resizable; when you resize, the canvas adjusts and attempts to keep the view stable. If you end up off-screen on very small windows, use "Reset View" in the top bar or pan back into view.

- Creating nodes
  - Sidebar → Create Node: choose a label and optional metadata key/value pairs, then Create. New nodes are rendered immediately (no auto pop-out).
  - Optionally, you can pre-link a relationship while creating a node when the UI offers that nested option.

- Creating relationships
  - Sidebar → Create Relationship: select a source and destination node and label; optionally add metadata. The new edge renders immediately.

- Arranging the graph
  - Sidebar → Layout → Auto-cluster layout re-runs the community layout on all nodes.
  - Drag nodes directly; neighbors will adjust smoothly. Motion stops after 3s unless you drag again or re-layout.

- Bulk edit
  - Sidebar → Bulk Edit Nodes for batch label/metadata operations, where available.

- New graph
  - File → New Graph creates a blank graph. If the current graph isn’t empty, a timestamped backup is written before clearing.


### Data & Files

- Graph state: assets/state.ron
- Query logs: assets/logs/queries_YYYYMMDD.log
- Backups on “New Graph”: saved with timestamp in assets (same format as state; see in-app message when created)


### GQL Tooling (Built-in Query Console)

This app ships a small, pragmatic subset of Cypher-like commands. Multiple statements can be separated by semicolons. Queries are logged automatically.

Supported commands:

- CREATE NODE Label {k:"v", ...}
- CREATE REL from=<uuid> to=<uuid> label=Label {k:"v", ...}
- MATCH NODE Label {k:"v", ...} [WHERE cond [AND cond ...]]
- MATCH REL Label {k:"v", ...} [WHERE cond [AND cond ...]]
- DELETE NODE <uuid>
- DELETE REL <uuid>

Notes

- Label is required for CREATE statements. The { ... } properties block is optional.
- MATCH returns rows (nodes or relationships) that match label and all provided key/value properties. Optional WHERE clauses can further filter results.
- UUIDs are shown in the UI; copy/paste them for REL creation or DELETE operations.
- Keys and values are strings; matching is exact. WHERE currently supports only simple predicates (see below) — no ranges, regex, partial/contains, arithmetic, or parentheses.
- Commands are case-insensitive for the verb (CREATE/MATCH/DELETE/REL/NODE), but labels/keys/values are case-sensitive.

Example queries

1) Create a few nodes

CREATE NODE Person {name:"Ada", role:"Engineer"};
CREATE NODE Person {name:"Bob", role:"Designer"};
CREATE NODE Company {name:"Acme"};

2) Find all Person nodes

MATCH NODE Person;

3) Match by property

MATCH NODE Person {role:"Engineer"};

3b) MATCH with WHERE filters

- By id:
  MATCH NODE Person WHERE id=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee;
- By metadata equality/inequality and existence:
  MATCH NODE Person WHERE name="Ada" AND role!="Manager" AND HAS(nickname);
- Combine with property block and label:
  MATCH NODE Person {role:"Engineer"} WHERE team="Platform";

4) Create a relationship (use real UUIDs from your nodes)

-- Suppose Ada has id aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee
-- and Acme has id 11111111-2222-3333-4444-555555555555
CREATE REL from=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee to=11111111-2222-3333-4444-555555555555 label=WORKS_AT {since:"2021"};

5) Match relationships by label (and optional properties)

MATCH REL WORKS_AT;
MATCH REL WORKS_AT {since:"2021"};
-- WHERE filters for relationships
MATCH REL WORKS_AT WHERE from=aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee AND to=11111111-2222-3333-4444-555555555555;
MATCH REL WORKS_AT WHERE id=01234567-89ab-cdef-0123-456789abcdef;
MATCH REL WORKS_AT WHERE HAS(since) AND since="2021";

6) Delete by id

DELETE REL 01234567-89ab-cdef-0123-456789abcdef;
DELETE NODE fedcba98-7654-3210-fedc-ba9876543210;


Troubleshooting & Known Limitations

- Motion stops “too soon”
  - By design, physics/convergence halts 3 seconds after the last activity (drag or layout). Begin a new drag or run Auto-cluster to resume.

- Relationships look crowded
  - Use Auto-cluster layout again or give the graph a nudge by dragging a few nodes. The overlap resolver prevents nodes from stacking, but edges can still cross in dense areas.

- Performance on very large graphs
  - The layout and physics are tuned for small-to-medium graphs. Extremely large graphs may feel sluggish.

- Limited query language
  - The GQL subset supports CREATE/MATCH/DELETE for NODE/REL with exact-match properties and basic WHERE clauses (id, label, has(key), key="v", key!="v", and for relationships from=<uuid>/to=<uuid>). No updates/SET, no pattern matching, multi-hop, variables, OR/NOT, or grouping/parentheses.

- UUIDs required for some operations
  - Relationship creation and deletes require UUIDs. Use MATCH first (or the UI) to identify ids.

- File system permissions
  - The app writes to assets/state.ron and assets/logs. Ensure the process has permission to create and write to these paths.

- Backups on New Graph
  - A backup is only created if the current graph is non-empty. Check the console/info message for the backup path when triggered.

---

## Contributing

Issues and PRs are welcome. Please run cargo fmt and cargo clippy before submitting changes, and ensure cargo build completes without warnings.

---

## License
unspecified currently - potential to change
