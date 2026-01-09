#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release
#![expect(rustdoc::missing_crate_level_docs)]
mod graph_utils;
mod gui;
mod persistence;
mod gql;

use fake::{faker::name::en::FirstName, Fake};
use graph_utils::graph::{GraphDatabase, NodeId};
use gui::egui_frontend::GraphApp;
use persistence::persist;
use std::collections::HashMap;

use eframe::egui;

fn main() -> eframe::Result {
    let icon = eframe::icon_data::from_png_bytes(
        include_bytes!("../assets/icon.jpg") // Path to your icon
    ).expect("Failed to load icon");
    // Try to load persisted state first
    let loaded_state = persist::load_active().ok().flatten();
    // If no state, prepare demo DB
    let demo_db: Option<GraphDatabase> = if loaded_state.is_none() {
        let mut db = GraphDatabase::new();
        let count = 10usize;
        let mut node_ids: Vec<NodeId> = Vec::with_capacity(count);
        let names: Vec<String> = (0..count).map(|_| FirstName().fake()).collect();
        let ages: Vec<u8> = (0..count).map(|_| rand::random()).collect();
        for (name, age) in names.iter().zip(ages.iter()) {
            let id = db.add_node(
                String::from("Person"),
                HashMap::from([
                    (String::from("name"), name.clone()),
                    (String::from("age"), age.to_string()),
                ]),
            );
            node_ids.push(id);
        }
        let _ = db.add_relationship(
            node_ids[0],
            node_ids[1],
            String::from("FRIENDS"),
            HashMap::new(),
        );
        let _ = db.add_relationship(
            node_ids[1],
            node_ids[2],
            String::from("COLLEAGUES"),
            HashMap::new(),
        );
        println!("Seeded demo graph: nodes={}, relationships={}", db.node_count(), db.relationship_count());
        Some(db)
    } else { None };

    env_logger::init(); // Log to stderr (if you run with `RUST_LOG=debug`).
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1300.0, 710.0])
            .with_icon(icon),
        ..Default::default()
    };
    eframe::run_native(
        "GraphDB Viewer",
        options,
        Box::new(move |_cc| {
            if let Some(state) = loaded_state {
                Ok(Box::new(GraphApp::from_state(state)) as Box<dyn eframe::App>)
            } else {
                let db = demo_db.expect("demo db to exist when no loaded state");
                Ok(Box::new(GraphApp::new(db)) as Box<dyn eframe::App>)
            }
        }),
    )
}
