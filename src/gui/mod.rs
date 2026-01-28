pub mod frontend;
pub mod win_utils;
pub mod app_state {
    use std::sync::atomic::AtomicBool;
    pub static SHOW_WINDOW: AtomicBool = AtomicBool::new(true);
}