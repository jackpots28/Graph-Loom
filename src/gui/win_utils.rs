#[cfg(target_os = "windows")]
use windows::Win32::Foundation::{HWND, LPARAM, BOOL};
#[cfg(target_os = "windows")]
use windows::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetWindowThreadProcessId, ShowWindow, 
    SetForegroundWindow, SW_RESTORE, SW_SHOW, BringWindowToTop,
    AllowSetForegroundWindow, ASFW_ANY,
    GetWindowLongW, GWL_STYLE, SetWindowPos,
    HWND_TOPMOST, HWND_NOTOPMOST, SWP_NOMOVE, SWP_NOSIZE,
    IsIconic, PostMessageW, WM_SYSCOMMAND, SC_RESTORE,
    GetForegroundWindow,
};
#[cfg(target_os = "windows")]
use windows::Win32::Graphics::Gdi::RedrawWindow;
#[cfg(target_os = "windows")]
use windows::Win32::System::Threading::{GetCurrentProcessId, AttachThreadInput};
#[cfg(target_os = "windows")]
use windows::Win32::System::ProcessStatus::K32GetModuleFileNameExW;

#[cfg(target_os = "windows")]
struct EnumData {
    process_id: u32,
    window_handle: Option<HWND>,
}

#[cfg(target_os = "windows")]
unsafe extern "system" fn enum_window_callback(hwnd: HWND, lparam: LPARAM) -> BOOL {
    unsafe {
        let data = &mut *(lparam.0 as *mut EnumData);
        let mut pid: u32 = 0;
        let _thread_id = GetWindowThreadProcessId(hwnd, Some(&mut pid));

        if pid == data.process_id {
            let style = GetWindowLongW(hwnd, GWL_STYLE) as u32;
            let is_child = (style & windows::Win32::UI::WindowsAndMessaging::WS_CHILD.0) != 0;
            
            if !is_child {
                // Check if it's the main window by seeing if it has a title
                let mut title = [0u16; 1024];
                let len = windows::Win32::UI::WindowsAndMessaging::GetWindowTextW(hwnd, &mut title);
                
                // If it has no title, it might be a helper window (e.g. from a library)
                // eframe windows usually have a title.
                if len > 0 {
                    let title_str = String::from_utf16_lossy(&title[..len as usize]);
                    if title_str == "Graph-Loom" {
                        data.window_handle = Some(hwnd);
                        return false.into();
                    }
                }
                
                // Fallback: if we haven't found any window yet, take this one
                if data.window_handle.is_none() {
                    data.window_handle = Some(hwnd);
                }
            }
        }
        true.into()
    }
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
            // 1. Ensure the window is shown (this adds it back to taskbar if it was hidden)
            let _ = ShowWindow(hwnd, SW_SHOW);
            let _ = windows::Win32::UI::WindowsAndMessaging::SetWindowLongW(hwnd, GWL_STYLE, (GetWindowLongW(hwnd, GWL_STYLE) as u32 | windows::Win32::UI::WindowsAndMessaging::WS_VISIBLE.0) as i32);
            let _ = windows::Win32::UI::WindowsAndMessaging::SetWindowPos(hwnd, HWND(std::ptr::null_mut()), 0, 0, 0, 0, windows::Win32::UI::WindowsAndMessaging::SWP_SHOWWINDOW | windows::Win32::UI::WindowsAndMessaging::SWP_NOMOVE | windows::Win32::UI::WindowsAndMessaging::SWP_NOSIZE | windows::Win32::UI::WindowsAndMessaging::SWP_NOZORDER);
            
            // 2. Restore if minimized
            if IsIconic(hwnd).as_bool() {
                let _ = PostMessageW(hwnd, WM_SYSCOMMAND, windows::Win32::Foundation::WPARAM(SC_RESTORE as usize), windows::Win32::Foundation::LPARAM(0));
                let _ = ShowWindow(hwnd, SW_RESTORE);
            }
            
            // 3. The "Classic" Foreground Trick: AttachThreadInput
            let foreground_hwnd = GetForegroundWindow();
            if !foreground_hwnd.0.is_null() && foreground_hwnd != hwnd {
                let foreground_thread_id = GetWindowThreadProcessId(foreground_hwnd, None);
                let current_thread_id = windows::Win32::System::Threading::GetCurrentThreadId();
                
                if foreground_thread_id != current_thread_id {
                    let _ = AttachThreadInput(current_thread_id, foreground_thread_id, true);
                    let _ = SetForegroundWindow(hwnd);
                    let _ = BringWindowToTop(hwnd);
                    let _ = AttachThreadInput(current_thread_id, foreground_thread_id, false);
                }
            }

            // 4. Fallback/Standard foregrounding
            let _ = SetForegroundWindow(hwnd);
            let _ = BringWindowToTop(hwnd);

            // 5. Force to top using SetWindowPos
            let _ = SetWindowPos(hwnd, HWND_TOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            let _ = SetWindowPos(hwnd, HWND_NOTOPMOST, 0, 0, 0, 0, SWP_NOMOVE | SWP_NOSIZE);
            
            // 6. Force a redraw
            let _ = RedrawWindow(hwnd, None, None, windows::Win32::Graphics::Gdi::RDW_INVALIDATE | windows::Win32::Graphics::Gdi::RDW_UPDATENOW | windows::Win32::Graphics::Gdi::RDW_FRAME);
        }
    }
}

#[cfg(target_os = "windows")]
pub fn find_running_instance() -> Option<u32> {
    unsafe {
        use windows::Win32::System::Threading::{OpenProcess, PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};
        use windows::Win32::System::ProcessStatus::EnumProcesses;
        
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
