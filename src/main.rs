#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod downloader;
mod magnet;
mod meta;
mod queue;
mod server;

use anyhow::Result;
use clap::Parser;
use downloader::Downloader;
use queue::RateLimiter;
use tao::{
    event::{Event, StartCause},
    event_loop::{ControlFlow, EventLoopBuilder},
};
use tray_icon::{
    menu::{Menu, MenuEvent, MenuItem, PredefinedMenuItem},
    TrayIconBuilder,
};

enum UserEvent {
    Menu(MenuEvent),
}

#[derive(Parser, Debug)]
#[command(name = "fastdl", version, about = "多线程 + BT 下载引擎")]
struct Args {
    /// 下载目录
    #[arg(short, long, default_value = "D:\\Downloads")]
    output: String,

    /// 每个任务的并发连接数
    #[arg(short = 'n', long, default_value_t = 32)]
    connections: usize,

    /// HTTP 服务端口
    #[arg(short, long, default_value_t = 6800)]
    port: u16,

    /// 服务模式最大并发任务数
    #[arg(long, default_value_t = 3)]
    max_concurrent: usize,

    /// 无托盘模式：只跑 HTTP 服务，Ctrl+C 退出
    #[arg(long)]
    headless: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // ============================================================
    // 单例检测：端口已被占，说明已有实例在跑
    // ============================================================
    if let Ok(addr) = format!("127.0.0.1:{}", args.port).parse() {
        if std::net::TcpStream::connect_timeout(
            &addr,
            std::time::Duration::from_millis(300),
        )
        .is_ok()
        {
            eprintln!("FastDL 已在运行 (端口 {})", args.port);
            return Ok(());
        }
    }

    let _log_guard = setup_logging(args.headless)?;

    tracing::info!("FastDL 启动 | 端口 {} | 下载目录 {}", args.port, args.output);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    let output_dir = args.output.clone();
    let port = args.port;
    let connections = args.connections;
    let max_concurrent = args.max_concurrent;
    let rate_limiter = RateLimiter::new();

    let rl = rate_limiter.clone();
    rt.spawn(async move {
        match Downloader::new(connections, rl.clone()) {
            Ok(dl) => {
                if let Err(e) =
                    server::run_server(port, dl, output_dir, max_concurrent, rl).await
                {
                    tracing::error!("服务器退出: {:#}", e);
                }
            }
            Err(e) => tracing::error!("初始化失败: {:#}", e),
        }
    });

    if args.headless {
        tracing::info!("无托盘模式：Ctrl+C 退出");
        rt.block_on(async {
            tokio::signal::ctrl_c().await.ok();
        });
        tracing::info!("退出");
        return Ok(());
    }

    run_tray(args.output, port)
}

fn setup_logging(headless: bool) -> Result<Option<tracing_appender::non_blocking::WorkerGuard>> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "info".into());

    if headless {
        tracing_subscriber::fmt().with_env_filter(filter).init();
        Ok(None)
    } else {
        std::fs::create_dir_all("logs").ok();
        let file_appender = tracing_appender::rolling::daily("logs", "fastdl.log");
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(non_blocking)
            .with_ansi(false)
            .init();
        Ok(Some(guard))
    }
}

fn run_tray(output_dir: String, port: u16) -> Result<()> {
    let event_loop = EventLoopBuilder::<UserEvent>::with_user_event().build();

    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
        let _ = proxy.send_event(UserEvent::Menu(event));
    }));

    let icon = make_icon()?;

    let open_dir_item = MenuItem::new("打开下载目录", true, None);
    let open_dir_id = open_dir_item.id().clone();

    let open_log_item = MenuItem::new("查看日志", true, None);
    let open_log_id = open_log_item.id().clone();

    let quit_item = MenuItem::new("退出 FastDL", true, None);
    let quit_id = quit_item.id().clone();

    let menu = Menu::new();
    menu.append_items(&[
        &open_dir_item,
        &open_log_item,
        &PredefinedMenuItem::separator(),
        &quit_item,
    ])?;

    let _tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip(format!("FastDL · 监听 127.0.0.1:{}", port))
        .with_icon(icon)
        .build()?;

    let output_dir_for_loop = output_dir.clone();

    event_loop.run(move |event, _, control_flow| {
        *control_flow = ControlFlow::Wait;

        match event {
            Event::NewEvents(StartCause::Init) => {
                tracing::info!("托盘已启动，下载目录: {}", output_dir_for_loop);
            }
            Event::UserEvent(UserEvent::Menu(e)) => {
                if e.id == quit_id {
                    tracing::info!("用户点击退出");
                    // 直接退出，不等 tao/tokio 的清理流程
                    std::process::exit(0);
                } else if e.id == open_dir_id {
                    open_path(&output_dir_for_loop);
                } else if e.id == open_log_id {
                    open_path("logs");
                }
            }
            _ => {}
        }
    });
}

fn make_icon() -> Result<tray_icon::Icon> {
    let size: u32 = 32;
    let c = size as f32 / 2.0;
    let r_max = c - 1.5;
    let mut rgba = Vec::with_capacity((size * size * 4) as usize);

    for y in 0..size {
        for x in 0..size {
            let dx = x as f32 - c + 0.5;
            let dy = y as f32 - c + 0.5;
            let dist = (dx * dx + dy * dy).sqrt();

            if dist <= r_max {
                rgba.extend_from_slice(&[59, 130, 246, 255]);
            } else if dist <= r_max + 1.0 {
                let alpha = ((r_max + 1.0 - dist) * 255.0) as u8;
                rgba.extend_from_slice(&[59, 130, 246, alpha]);
            } else {
                rgba.extend_from_slice(&[0, 0, 0, 0]);
            }
        }
    }

    tray_icon::Icon::from_rgba(rgba, size, size)
        .map_err(|e| anyhow::anyhow!("生成图标失败: {}", e))
}

#[cfg(target_os = "windows")]
fn open_path(path: &str) {
    let _ = std::process::Command::new("explorer").arg(path).spawn();
}

#[cfg(target_os = "macos")]
fn open_path(path: &str) {
    let _ = std::process::Command::new("open").arg(path).spawn();
}

#[cfg(target_os = "linux")]
fn open_path(path: &str) {
    let _ = std::process::Command::new("xdg-open").arg(path).spawn();
}