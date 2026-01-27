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
    let mut background_mode = false;

    #[cfg(feature = "api")]
    {
        use std::env;
        let args = env::args().skip(1).collect::<Vec<String>>();
        if args.iter().any(|a| a == "--api-enable") || args.iter().any(|a| a == "--background") || args.iter().any(|a| a == "-b") {
            let mut settings = persistence::settings::AppSettings::load().unwrap_or_default();
            if args.iter().any(|a| a == "--api-enable") {
                settings.api_enabled = true;
            }
            if args.iter().any(|a| a == "--background") || args.iter().any(|a| a == "-b") {
                background_mode = true;
            }
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
                    "--grpc-enable" => {
                        settings.grpc_enabled = true;
                    }
                    "--grpc-port" => {
                        if i + 1 < args.len() { if let Ok(p) = args[i+1].parse::<u16>() { settings.grpc_port = p; } i += 1; }
                    }
                    _ => {}
                }
                i += 1;
            }
            let _ = settings.save();
            persistence::persist::set_settings_override(settings.clone());
            eprintln!("[Graph-Loom] API enabled on {} (configured in Preferences)", settings.api_endpoint());
            if settings.grpc_enabled {
                eprintln!("[Graph-Loom] gRPC enabled on {}:{}", settings.api_bind_addr, settings.grpc_port);
            }
        }
    }

    let settings = persistence::settings::AppSettings::load().unwrap_or_default();
    persistence::persist::set_settings_override(settings.clone());

    #[cfg(feature = "api")]
    if background_mode {
        return run_background(settings);
    }

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
        Box::new(move |cc| {
            if let Some(state) = loaded_state {
                let app = GraphApp::from_state(state);
                #[cfg(feature = "api")]
                if let Some(storage) = cc.storage {
                    if storage.get_string("background_on_close").as_deref() == Some("true") {
                        // Logic to handle background on close could be added here
                    }
                }
                Ok(Box::new(app) as Box<dyn eframe::App>)
            } else {
                // No prior state: start with an empty graph
                let app = GraphApp::new(GraphDatabase::new());
                Ok(Box::new(app) as Box<dyn eframe::App>)
            }
        }),
    )
}

#[cfg(feature = "api")]
fn run_background(settings: persistence::settings::AppSettings) -> eframe::Result {
    use std::time::{Duration, Instant};
    use crate::api;
    use crate::gql::query_interface;

    eprintln!("[Graph-Loom] Running in BACKGROUND mode. No GUI will be shown.");
    eprintln!("[Graph-Loom] Press Ctrl+C to stop.");

    let mut db = if let Ok(Some(state)) = persist::load_active() {
        eprintln!("[Graph-Loom] Loaded existing state.");
        state.db
    } else {
        eprintln!("[Graph-Loom] Starting with empty database.");
        GraphDatabase::new()
    };

    let rx = api::init_broker();
    
    // Start servers
    if settings.api_enabled {
        if let Err(e) = api::server::start_server(&settings) {
            eprintln!("[Graph-Loom] Failed to start API server: {}", e);
        }
    }
    if settings.grpc_enabled {
        if let Err(e) = api::grpc::start_grpc_server(&settings) {
            eprintln!("[Graph-Loom] Failed to start gRPC server: {}", e);
        }
    }

    let mut last_save = Instant::now();
    let mut dirty = false;

    loop {
        // Process API requests
        while let Ok(req) = rx.try_recv() {
            let t0 = Instant::now();
            let res = match &req.params {
                Some(p) => query_interface::execute_query_with_params(&mut db, &req.query, p),
                None => query_interface::execute_and_log(&mut db, &req.query),
            };
            let dt = t0.elapsed();
            
            let mutated = res.as_ref().map(|o| o.mutated).unwrap_or(false);
            if mutated {
                dirty = true;
            }

            eprintln!(
                "[API Background] RID={} done mutated={} dt_ms={}",
                req.request_id,
                mutated,
                dt.as_millis()
            );
            let _ = req.respond_to.send(res.map_err(|e| e.to_string()));
        }

        // Periodic save
        if dirty && last_save.elapsed() > Duration::from_secs(5) {
            let state = persist::AppStateFile {
                db: db.clone(),
                node_positions: vec![], // positions not easily available/needed in background?
                pan: (0.0, 0.0),
                zoom: 1.0,
            };
            if let Err(e) = persist::save_active(&state) {
                eprintln!("[Graph-Loom] Background save failed: {}", e);
            } else {
                eprintln!("[Graph-Loom] Background state autosaved.");
                dirty = false;
                last_save = Instant::now();
            }
        }

        std::thread::sleep(Duration::from_millis(100));
    }
}
