# Changelog

本项目的所有重要变更都会记录在这里。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

## [0.5.0] - 2026-10-14

### Added

- 多线程 HTTP 分片下载（默认 32 连接，可调）
- 断点续传（`.fastdl.meta` + 偏移写模式）
- GitHub 多镜像并发探测 + 实测速度选源
- 慢速域名自动降并发（百度网盘、腾讯 CDN 等）
- 磁力链接 / `.torrent` 文件 / 远程 `.torrent` 支持
- 浏览器扩展自动拦截下载（Chrome / Edge）
- 侧边栏 UI + 系统通知 + 图标 badge
- Native Messaging 引擎按需唤醒
- 任务队列（并发上限、暂停、恢复、取消）
- 任务持久化（`tasks.json`，重启后恢复）
- SHA-256 校验
- 全局限速开关（令牌桶）
- 系统托盘常驻（tao + tray-icon）
- aria2 JSON-RPC 兼容端点（`/jsonrpc`）
- SSE 实时进度推送（`/events`）
- Inno Setup 打包脚本

### Fixed

- 取消任务后残留临时文件自动清理
- 暂停状态下取消响应慢（23s → 1s）
- PowerShell 生成 manifest 时中文乱码（BOM 问题）
- 托盘退出慢（改为 `process::exit`）
- 目标文件被占用时降级重命名
- BT 任务 total=0 时不再误发进度事件

### Security

- `.gitignore` 排除私钥、任务持久化数据、日志、编译产物

## [0.1.0] - 2026-10-13

### Added

- 项目初始化
- 基础 HTTP 下载引擎
- 浏览器扩展骨架