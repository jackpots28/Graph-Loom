//! This module serves as the main entry point for the application components.
//! It defines several submodules and a shared application state variable.
//!
//! Modules:
//! - `frontend`: The `frontend` module is used for managing the graphical user interface or other front-end related functionalities.
//! - `win_utils`: The `win_utils` module provides utility functions specific to window operations or management.
//!
//! Shared State:
//! - `app_state`:
//!   - Includes utility for global application state management.
//!   - Defines `SHOW_WINDOW`: A `static` variable of type `AtomicBool` used to determine the visibility of the window.
//!     The default initial state is set to `true`.
//!
pub mod frontend;
pub mod win_utils;
pub mod app_state {
    use std::sync::atomic::AtomicBool;
    pub static SHOW_WINDOW: AtomicBool = AtomicBool::new(true);
}