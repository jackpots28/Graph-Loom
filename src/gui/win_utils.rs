#[cfg(target_os = "windows")]
use windows::Win32::Foundation::{HWND, LPARAM, BOOL};
#[cfg(target_os = "windows")]
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, ShowWindow, 
    SetForegroundWindow, SW_RESTORE, SW_SHOW, BringWindowToTop,
    AllowSetForegroundWindow, ASFW_ANY, IsWindowVisible,
    GetWindowLongW, GWL_STYLE, WS_VISIBLE, SetWindowPos,
    HWND_TOPMOST, HWND_NOTOPMOST, SWP_NOMOVE, SWP_NOSIZE,
    IsIconic, PostMessageW, WM_SYSCOMMAND, SC_RESTORE,
};
#[cfg(target_os = "windows")]
use windows::Win32::Graphics::Gdi::RedrawWindow;
#[cfg(target_os = "windows")]
use windows::Win32::System::Threading::{GetCurrentProcessId, OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};
#[cfg(target_os = "windows")]
use windows::Win32::System::ProcessStatus::K32GetModuleFileNameExW;

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
        let process_id = GetCurrentProcessId();
        force_foreground_process(process_id);
    }
}

#[cfg(target_os = "windows")]
pub fn force_foreground_process(process_id: u32) {
    unsafe {
        let _ = AllowSetForegroundWindow(ASFW_ANY);
        
        let mut data = EnumData {
            process_id,
            window_handle: None,
        };

        let _ = EnumWindows(Some(enum_window_callback), LPARAM(&mut data as *mut EnumData as isize));

        if let Some(hwnd) = data.window_handle {
            // Force the window to be shown and restored
            // Using SW_SHOW explicitly before SW_RESTORE can sometimes help
            ShowWindow(hwnd, SW_SHOW);
            
            if IsIconic(hwnd).as_bool() {
                let _ = PostMessageW(hwnd, WM_SYSCOMMAND, windows::Win32::Foundation::WPARAM(SC_RESTORE as usize), windows::Win32::Foundation::LPARAM(0));
                ShowWindow(hwnd, SW_RESTORE);
            }
            
            // Re-order the window to the top
            let _ = SetWindowPos(hwnd, HWND_TOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            let _ = SetWindowPos(hwnd, HWND_NOTOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            
            BringWindowToTop(hwnd);
            SetForegroundWindow(hwnd);

            // Force a redraw to ensure the taskbar and window are updated
            RedrawWindow(hwnd, None, None, windows::Win32::Graphics::Gdi::RDW_INVALIDATE | windows::Win32::Graphics::Gdi::RDW_UPDATENOW | windows::Win32::Graphics::Gdi::RDW_FRAME);
        }
    }
}

#[cfg(target_os = "windows")]
pub fn find_running_instance() -> Option<u32> {
    unsafe {
        use windows::Win32::System::Threading::{EnumProcesses, OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};
        
        let mut pids = [0u32; 1024];
        let mut cb_needed = 0u32;
        if EnumProcesses(pids.as_mut_ptr(), (pids.len() * 4) as u32, &mut cb_needed).is_err() {
            return None;
        }

        let current_pid = GetCurrentProcessId();
        let count = cb_needed / 4;
        
        let mut current_path = [0u16; 1024];
        let h_process = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, false, current_pid);
        if let Ok(h) = h_process {
            let len = K32GetModuleFileNameExW(h, None, &mut current_path);
            let _ = windows::Win32::Foundation::CloseHandle(h);
            if len == 0 { return None; }
            let current_path_str = String::from_utf16_lossy(&current_path[..len as usize]);

            for i in 0..count {
                let pid = pids[i as usize];
                if pid == 0 || pid == current_pid { continue; }

                let h_other = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, false, pid);
                if let Ok(ho) = h_other {
                    let mut other_path = [0u16; 1024];
                    let other_len = K32GetModuleFileNameExW(ho, None, &mut other_path);
                    let _ = windows::Win32::Foundation::CloseHandle(ho);
                    
                    if other_len > 0 {
                        let other_path_str = String::from_utf16_lossy(&other_path[..other_len as usize]);
                        if other_path_str == current_path_str {
                            return Some(pid);
                        }
                    }
                }
            }
        }
    }
    None
}

#[cfg(not(target_os = "windows"))]
pub fn force_foreground_process(_process_id: u32) {}

#[cfg(not(target_os = "windows"))]
pub fn find_running_instance() -> Option<u32> { None }
