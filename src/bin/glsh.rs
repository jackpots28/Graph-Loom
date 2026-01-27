// glsh: Graph-Loom Shell (optional CLI client)
// Build with: cargo build --features cli --bin glsh

use clap::{Arg, ArgAction, Command};
use rustyline::history::DefaultHistory;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::time::{Duration, Instant};
use tungstenite::{client::IntoClientRequest, connect, protocol::Message, Error as WsError, WebSocket};
use url::Url;

fn settings_dir() -> std::path::PathBuf {
    // Reuse the app's settings directory for history storage
    graph_loom::persistence::settings::AppSettings::settings_dir()
}

fn is_banner_msg(s: &str) -> bool {
    s.trim_start().starts_with("Graph-Loom REPL ready.")
}

fn is_interrupted(e: &WsError) -> bool {
    match e {
        WsError::Io(ioe) => ioe.kind() == std::io::ErrorKind::Interrupted,
        _ => false,
    }
}

fn recv_message_with_retry<S: std::io::Read + std::io::Write>(sock: &mut WebSocket<S>, overall_timeout: Duration) -> Result<Message, WsError> {
    let start = Instant::now();
    loop {
        match sock.read() {
            Ok(m) => return Ok(m),
            Err(e) if is_interrupted(&e) => {
                // Retry on EINTR
                if start.elapsed() > overall_timeout { return Err(e); }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn send_text_with_retry<S: std::io::Read + std::io::Write>(sock: &mut WebSocket<S>, text: String, overall_timeout: Duration) -> Result<(), WsError> {
    let start = Instant::now();
    loop {
        match sock.send(Message::Text(text.clone())) {
            Ok(_) => return Ok(()),
            Err(e) if is_interrupted(&e) => {
                if start.elapsed() > overall_timeout { return Err(e); }
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

fn main() {
    let matches = Command::new("glsh")
        .about("Graph-Loom Shell — connect to a running Graph-Loom API REPL and run queries")
        .arg(Arg::new("host").long("host").default_value("127.0.0.1").help("Server host"))
        .arg(Arg::new("port").long("port").default_value("8787").help("Server port"))
        .arg(Arg::new("api_key").long("api-key").value_name("KEY").help("API key to send as X-API-Key header"))
        .arg(Arg::new("eval").short('e').long("eval").value_name("QUERY").help("Run a single query and exit"))
        .arg(Arg::new("quiet").short('q').long("quiet").action(ArgAction::SetTrue).help("Suppress banner/help text"))
        .get_matches();

    let host = matches.get_one::<String>("host").unwrap().to_string();
    let port = matches.get_one::<String>("port").unwrap().to_string();
    let api_key = matches.get_one::<String>("api_key").cloned();
    let eval = matches.get_one::<String>("eval").cloned();
    let quiet = matches.get_flag("quiet");

    let endpoint = format!("ws://{}:{}/api/repl", host, port);
    let url = match Url::parse(&endpoint) {
        Ok(u) => u,
        Err(e) => {
            eprintln!("invalid URL '{}': {}", endpoint, e);
            std::process::exit(1);
        }
    };
    let mut req = match url.into_client_request() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to create client request: {}", e);
            std::process::exit(1);
        }
    };
    if let Some(key) = api_key {
        let val = match http::HeaderValue::from_str(&key) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("invalid api key header value: {}", e);
                std::process::exit(1);
            }
        };
        req.headers_mut().insert("X-API-Key", val);
    }

    let (mut socket, _resp) = match connect(req) {
        Ok(ok) => ok,
        Err(e) => {
            eprintln!(
                "Failed to connect: {}\nHint: Ensure Graph-Loom is running and API is enabled in Preferences (default 127.0.0.1:8787).",
                e
            );
            std::process::exit(2);
        }
    };

    // The server sends a banner line upon WS connect; consume and ignore it so that
    // the first query's response isn't mistaken for the banner.
    if let Ok(msg) = recv_message_with_retry(&mut socket, Duration::from_secs(2)) {
        if let Message::Text(txt) = msg {
            if !is_banner_msg(&txt) {
                // Not a banner; ignore.
            }
        }
    }

    // One-off eval mode
    if let Some(query) = eval {
        if let Err(e) = send_text_with_retry(&mut socket, query, Duration::from_secs(5)) {
            eprintln!("send error: {}", e);
            std::process::exit(3);
        }
        // Read frames until we get a non-banner text/binary response
        loop {
            match recv_message_with_retry(&mut socket, Duration::from_secs(60)) {
                Ok(msg) => match msg {
                    Message::Text(txt) => {
                        if is_banner_msg(&txt) { continue; }
                        print_response(&txt);
                        break;
                    }
                    Message::Binary(b) => { print_response(&String::from_utf8_lossy(&b)); break; }
                    _ => { /* ignore pings/others */ }
                },
                Err(e) => {
                    eprintln!("Read error: {}", e);
                    std::process::exit(3);
                }
            }
        }
        return;
    }

    // Interactive mode with history
    let mut rl: Editor<(), DefaultHistory> = match Editor::new() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("failed to initialize editor: {}", e);
            std::process::exit(1);
        }
    };
    let mut hist_path = settings_dir();
    hist_path.push("glsh_history.txt");
    // Load history if present
    let _ = std::fs::create_dir_all(hist_path.parent().unwrap_or_else(|| std::path::Path::new(".")));
    let _ = rl.load_history(&hist_path);

    if !quiet {
        eprintln!(
            "Connected to {}.\nType queries and press Enter. Commands: :help, quit / exit. History saved at {}.\n",
            endpoint,
            hist_path.display()
        );
    }

    loop {
        let prompt = "glsh> ";
        match rl.readline(prompt) {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() { continue; }
                if input == ":quit" || input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("exit") { break; }
                if input == ":help" || input == "?" {
                    println!(
                        "Commands:\n  :help or ?    Show this help\n  :quit         Exit glsh\nNotes:\n  - Use Up/Down to navigate history.\n  - Send one query per line; multiline is not yet supported."
                    );
                    continue;
                }
                rl.add_history_entry(input).ok();

                if let Err(e) = send_text_with_retry(&mut socket, input.to_string(), Duration::from_secs(5)) {
                    eprintln!("send error: {}", e);
                    break;
                }
                // Read frames until non-banner response
                loop {
                    match recv_message_with_retry(&mut socket, Duration::from_secs(60)) {
                        Ok(msg) => match msg {
                            Message::Text(txt) => { if is_banner_msg(&txt) { continue; } print_response(&txt); break; }
                            Message::Binary(b) => { print_response(&String::from_utf8_lossy(&b)); break; }
                            _ => { /* ignore */ }
                        },
                        Err(e) => {
                            eprintln!("read error: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => { // Ctrl-C
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => { // Ctrl-D
                break;
            }
            Err(e) => {
                eprintln!("readline error: {}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&hist_path);
}

fn print_response(s: &str) {
    // Try to pretty-print JSON; otherwise print raw
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
        match serde_json::to_string_pretty(&v) {
            Ok(p) => println!("{}", p),
            Err(_) => println!("{}", s),
        }
    } else {
        println!("{}", s);
    }
}
