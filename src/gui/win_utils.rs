#[cfg(target_os = "windows")]
use windows::Win32::Foundation::{HWND, LPARAM, BOOL};
#[cfg(target_os = "windows")]
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, ShowWindow, 
    SetForegroundWindow, SW_RESTORE, SW_SHOW, BringWindowToTop,
    AllowSetForegroundWindow, ASFW_ANY, IsWindowVisible,
    GetWindowLongW, GWL_STYLE, WS_VISIBLE, SetWindowPos,
    HWND_TOPMOST, HWND_NOTOPMOST, SWP_NOMOVE, SWP_NOSIZE,
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

    if pid == data.process_id {
        // Prefer windows that are intended to be visible or have certain styles
        // even if they are currently hidden by the app.
        let style = GetWindowLongW(hwnd, GWL_STYLE) as u32;
        
        // If we haven't found any window yet, take this one.
        // If we have, but this one has WS_VISIBLE (or was intended to be), prefer it.
        if data.window_handle.is_none() || (style & WS_VISIBLE.0 != 0) {
            data.window_handle = Some(hwnd);
        }
        
        // We continue enumeration to find the "best" window handle if multiple exist
        return true.into();
    }
    true.into()
}

pub fn force_foreground_window() {
    #[cfg(target_os = "windows")]
    unsafe {
        let _ = AllowSetForegroundWindow(ASFW_ANY);
        
        let process_id = GetCurrentProcessId();
        let mut data = EnumData {
            process_id,
            window_handle: None,
        };

        let _ = EnumWindows(Some(enum_window_callback), LPARAM(&mut data as *mut EnumData as isize));

        if let Some(hwnd) = data.window_handle {
            // Force the window to be shown and restored
            // Using SW_SHOW explicitly before SW_RESTORE can sometimes help
            ShowWindow(hwnd, SW_SHOW);
            ShowWindow(hwnd, SW_RESTORE);
            
            // Re-order the window to the top
            let _ = SetWindowPos(hwnd, HWND_TOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            let _ = SetWindowPos(hwnd, HWND_NOTOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            
            BringWindowToTop(hwnd);
            SetForegroundWindow(hwnd);
        }
    }
}
