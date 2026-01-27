#[cfg(target_os = "windows")]
use windows::Win32::Foundation::{HWND, LPARAM, BOOL};
#[cfg(target_os = "windows")]
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, IsWindowVisible, ShowWindow, 
    SetForegroundWindow, SW_RESTORE, SW_SHOW, BringWindowToTop,
};
#[cfg(target_os = "windows")]
use windows::Win32::System::Threading::GetCurrentProcessId;

#[cfg(target_os = "windows")]
struct EnumData {
    process_id: u32,
    window_handle: Option<HWND>,
}

#[cfg(target_os = "windows")]
unsafe extern "system" fn enum_window_callback(hwnd: HWND, lparam: LPARAM) -> BOOL {
    let data = &mut *(lparam.0 as *mut EnumData);
    let mut pid: u32 = 0;
    GetWindowThreadProcessId(hwnd, Some(&mut pid));

    if pid == data.process_id && IsWindowVisible(hwnd).as_bool() {
        data.window_handle = Some(hwnd);
        return false.into(); // Stop enumeration
    }
    true.into()
}

pub fn force_foreground_window() {
    #[cfg(target_os = "windows")]
    unsafe {
        let process_id = GetCurrentProcessId();
        let mut data = EnumData {
            process_id,
            window_handle: None,
        };

        let _ = EnumWindows(Some(enum_window_callback), LPARAM(&mut data as *mut EnumData as isize));

        if let Some(hwnd) = data.window_handle {
            // Restore if minimized, then show and bring to top
            ShowWindow(hwnd, SW_RESTORE);
            ShowWindow(hwnd, SW_SHOW);
            BringWindowToTop(hwnd);
            SetForegroundWindow(hwnd);
        }
    }
}
