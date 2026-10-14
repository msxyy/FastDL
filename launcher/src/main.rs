//! FastDL Native Messaging Host
//! 职责：收到扩展请求后，检查 FastDL 引擎是否在跑，不在就启动它。
//! 协议：Chrome Native Messaging（4 字节 LE 长度 + JSON）

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

const FASTDL_PORT: u16 = 6800;
const PORT_CHECK_TIMEOUT_MS: u64 = 300;
const STARTUP_WAIT_MS: u64 = 5000;

#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

fn main() {
    // 从 stdin 读一条 native message
    let msg = match read_message() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("[launcher] 读取消息失败: {}", e);
            std::process::exit(1);
        }
    };

    eprintln!(
        "[launcher] 收到消息 {} 字节: {}",
        msg.len(),
        String::from_utf8_lossy(&msg)
    );

    // 处理并生成响应
    let response = handle();

    // 写响应
    if let Err(e) = write_message(response.as_bytes()) {
        eprintln!("[launcher] 写响应失败: {}", e);
        std::process::exit(1);
    }
}

/// 核心逻辑
fn handle() -> String {
    if is_port_open(FASTDL_PORT, PORT_CHECK_TIMEOUT_MS) {
        eprintln!("[launcher] FastDL 已在运行");
        return r#"{"status":"already_running"}"#.to_string();
    }

    let exe = match find_fastdl_exe() {
        Some(p) => p,
        None => {
            eprintln!("[launcher] 找不到 fastdl.exe");
            return r#"{"status":"error","message":"fastdl.exe not found"}"#.to_string();
        }
    };

    eprintln!("[launcher] 启动 {}", exe.display());
    if let Err(e) = spawn_detached(&exe) {
        return format!(r#"{{"status":"error","message":"spawn failed: {}"}}"#, e);
    }

    // 等端口起来
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(STARTUP_WAIT_MS) {
        if is_port_open(FASTDL_PORT, 100) {
            let ms = start.elapsed().as_millis();
            eprintln!("[launcher] 启动成功，耗时 {} ms", ms);
            return format!(r#"{{"status":"started","ms":{}}}"#, ms);
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    eprintln!("[launcher] 启动超时");
    r#"{"status":"timeout","message":"fastdl did not open port in time"}"#.to_string()
}

fn is_port_open(port: u16, timeout_ms: u64) -> bool {
    let addr: SocketAddr = match format!("127.0.0.1:{}", port).parse() {
        Ok(a) => a,
        Err(_) => return false,
    };
    TcpStream::connect_timeout(&addr, Duration::from_millis(timeout_ms)).is_ok()
}

fn find_fastdl_exe() -> Option<PathBuf> {
    // 优先同目录
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let p = dir.join("fastdl.exe");
            if p.exists() {
                return Some(p);
            }
        }
    }
    // 常见安装位置
    let candidates = [
        std::env::var("LOCALAPPDATA")
            .ok()
            .map(|p| PathBuf::from(p).join("FastDL").join("fastdl.exe")),
        Some(PathBuf::from(r"C:\Program Files\FastDL\fastdl.exe")),
    ];
    for c in candidates.iter().flatten() {
        if c.exists() {
            return Some(c.clone());
        }
    }
    None
}

fn spawn_detached(exe: &PathBuf) -> io::Result<()> {
    let mut cmd = Command::new(exe);
    #[cfg(windows)]
    {
        // detached + no window + new group 三重保险，防止 Chrome 关闭时杀进程
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
    }
    cmd.spawn()?;
    Ok(())
}

/// Native Messaging：读一条消息
fn read_message() -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    io::stdin().read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "消息过大 (max 1MB)",
        ));
    }
    let mut buf = vec![0u8; len];
    io::stdin().read_exact(&mut buf)?;
    Ok(buf)
}

/// Native Messaging：写一条消息
fn write_message(data: &[u8]) -> io::Result<()> {
    let len = data.len() as u32;
    io::stdout().write_all(&len.to_le_bytes())?;
    io::stdout().write_all(data)?;
    io::stdout().flush()?;
    Ok(())
}