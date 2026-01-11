mod graph_utils;
mod gui;
mod persistence;
mod gql;

use graph_utils::graph::GraphDatabase;
use gui::frontend::GraphApp;
use persistence::persist;

use eframe::egui;

fn main() -> eframe::Result {
    let icon = eframe::icon_data::from_png_bytes(
        // Really need a different icon...
        include_bytes!("../assets/icon.jpg")
    ).expect("Failed to load icon");
    let loaded_state = persist::load_active().ok().flatten();

    env_logger::init();
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1300.0, 710.0])
            // Provide sensible bounds so the UI stays usable on small screens
            .with_min_inner_size([700.0, 420.0])
            .with_resizable(true)
            .with_icon(icon),
        ..Default::default()
    };
    eframe::run_native(
        "Graph-Loom",
        options,
        Box::new(move |_cc| {
            if let Some(state) = loaded_state {
                Ok(Box::new(GraphApp::from_state(state)) as Box<dyn eframe::App>)
            } else {
                // No prior state: start with an empty graph
                Ok(Box::new(GraphApp::new(GraphDatabase::new())) as Box<dyn eframe::App>)
            }
        }),
    )
}
