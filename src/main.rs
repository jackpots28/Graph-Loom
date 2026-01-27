#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]
mod gql;
mod graph_utils;
mod gui;
mod persistence;
mod api;

use std::collections::HashMap;
use graph_utils::graph::GraphDatabase;
use gui::frontend::GraphApp;
use persistence::persist;

use eframe::egui;
// All menus are now implemented within the egui window; no platform-specific menu code.

use tray_icon::{
    menu::{Menu, MenuEvent, MenuItem},
    TrayIconBuilder,
};
use std::sync::atomic::Ordering;

fn main() -> eframe::Result {
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
            // parse flags
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
            eprintln!("[Graph-Loom] API enabled on {}", settings.api_endpoint());
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

    // If background_on_close is enabled and we have a server that could run,
    // start hidden by default on consecutive runs.
    #[cfg(feature = "api")]
    {
        if settings.background_on_close && (settings.api_enabled || settings.grpc_enabled) {
            crate::gui::app_state::SHOW_WINDOW.store(false, Ordering::SeqCst);
        } else {
            crate::gui::app_state::SHOW_WINDOW.store(true, Ordering::SeqCst);
        }
    }

    #[cfg(not(feature = "api"))]
    crate::gui::app_state::SHOW_WINDOW.store(true, Ordering::SeqCst);

    // Ensure LAST_SHOW_WINDOW matches initial state
    // We can't easily access LAST_SHOW_WINDOW from here as it is inside GraphApp::update,
    // but its default is true, so it will trigger if we start false.

    let icon_bytes = include_bytes!("../assets/AppSet.iconset/icon_512x512.png");
    let icon = match eframe::icon_data::from_png_bytes(icon_bytes) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("[Graph-Loom] Failed to load window icon: {}", e);
            // Fallback: we could return an error, but eframe::run_native needs eframe::Result which is specific.
            // Let's just panic here with a clear message or use a simpler error.
            panic!("Icon load failed: {}", e);
        }
    };

    // Initialize Tray Icon
    let tray_menu = Menu::new();
    let show_item = MenuItem::new("Show Graph-Loom", true, None);
    let quit_item = MenuItem::new("Quit", true, None);
    let _ = tray_menu.append(&show_item);
    let _ = tray_menu.append(&quit_item);

    let tray_icon_data = match tray_icon::Icon::from_rgba(icon.rgba.clone(), icon.width, icon.height) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("[Graph-Loom] Failed to create tray icon: {}", e);
            panic!("Tray icon creation failed: {}", e);
        }
    };

    let mut _tray_icon = match TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu))
        .with_tooltip("Graph-Loom")
        .with_icon(tray_icon_data)
        .build() {
            Ok(i) => Some(i),
            Err(e) => {
                eprintln!("[Graph-Loom] Failed to build tray icon: {}", e);
                None // Non-fatal if we can't show tray? Actually user might want it.
            }
        };

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

    let show_item_id = show_item.id().clone();
    let quit_item_id = quit_item.id().clone();

    eframe::run_native(
        "Graph-Loom",
        options,
        Box::new(move |cc| {
            // Setup tray event listener
            let ctx = cc.egui_ctx.clone();
            std::thread::spawn(move || {
                let menu_channel = MenuEvent::receiver();
                loop {
                    if let Ok(event) = menu_channel.try_recv() {
                        if event.id == show_item_id {
                            crate::gui::app_state::SHOW_WINDOW.store(true, Ordering::SeqCst);
                            ctx.send_viewport_cmd(egui::ViewportCommand::Visible(true));
                            ctx.send_viewport_cmd(egui::ViewportCommand::Focus);
                            // Also request attention to really bring it to the foreground on Windows
                            ctx.send_viewport_cmd(egui::ViewportCommand::RequestUserAttention(egui::UserAttentionType::Critical));
                            ctx.request_repaint();
                        } else if event.id == quit_item_id {
                            std::process::exit(0);
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            });

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
            // Note: in background mode, db is local so we can use it to create owned state
            let state = persist::AppStateFile::from_runtime_owned(
                db.clone(),
                &HashMap::new(), // positions not easily available/needed in background?
                egui::Vec2::ZERO,
                1.0,
            );
            if let Err(e) = persist::save_active(&state) {
                eprintln!("[Graph-Loom] Background save failed: {}", e);
            } else {
                eprintln!("[Graph-Loom] Background state autosaved.");
                dirty = false;
                last_save = Instant::now();
            }
        }

        // Use recv_timeout to wait for requests instead of busy-sleep
        if let Ok(req) = rx.recv_timeout(Duration::from_millis(500)) {
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
    }
}
