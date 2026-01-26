mod gql;
mod graph_utils;
mod gui;
mod persistence;
mod api;

use graph_utils::graph::GraphDatabase;
use gui::frontend::GraphApp;
use persistence::persist;

use eframe::egui;
// All menus are now implemented within the egui window; no platform-specific menu code.

fn main() -> eframe::Result {
    // Optional: enable/run embedded API server from CLI flags (when compiled with `api` feature)
    // Usage examples:
    //   Graph-Loom --api-enable
    //   Graph-Loom --api-enable --api-bind 0.0.0.0 --api-port 8787 --api-key secret
    #[cfg(feature = "api")]
    {
        use std::env;
        let args = env::args().skip(1).collect::<Vec<String>>();
        if args.iter().any(|a| a == "--api-enable") {
            let mut settings = persistence::settings::AppSettings::load().unwrap_or_default();
            settings.api_enabled = true;
            // parse simple flags
            let mut i = 0usize;
            while i < args.len() {
                match args[i].as_str() {
                    "--api-bind" => {
                        if i + 1 < args.len() { settings.api_bind_addr = args[i+1].clone(); i += 1; }
                    }
                    "--api-port" => {
                        if i + 1 < args.len() { if let Ok(p) = args[i+1].parse::<u16>() { settings.api_port = p; } i += 1; }
                    }
                    "--api-key" => {
                        if i + 1 < args.len() { let v = args[i+1].clone(); settings.api_key = if v.is_empty() { None } else { Some(v) }; i += 1; }
                    }
                    _ => {}
                }
                i += 1;
            }
            let _ = settings.save();
            persistence::persist::set_settings_override(settings.clone());
            eprintln!("[Graph-Loom] API enabled on {} (configured in Preferences)", settings.api_endpoint());
        }
    }

    let settings = persistence::settings::AppSettings::load().unwrap_or_default();
    persistence::persist::set_settings_override(settings);

    let icon = eframe::icon_data::from_png_bytes(
        // Really need a different icon...
        include_bytes!("../assets/AppSet.iconset/icon_512x512.png"),
    )
    .expect("Failed to load icon");
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
                let app = GraphApp::from_state(state);
                Ok(Box::new(app) as Box<dyn eframe::App>)
            } else {
                // No prior state: start with an empty graph
                let app = GraphApp::new(GraphDatabase::new());
                Ok(Box::new(app) as Box<dyn eframe::App>)
            }
        }),
    )
}
