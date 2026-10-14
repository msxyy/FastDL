# FastDL

一个自研的本地下载器，类似 IDM / 迅雷：
多线程分片 + 断点续传 + BT/磁力 + GitHub 镜像加速 + 浏览器扩展自动接管。

## 功能

- 多线程分片下载（默认 32 连接）
- 断点续传（`meta.json` + 稀疏文件偏移写）
- GitHub 多镜像并发探测 + 实测速度选源
- BT / 磁力 / `.torrent`
- SHA-256 校验
- 全局限速
- 任务持久化
- 浏览器扩展：侧边栏 + 通知 + 暂停/取消/恢复
- Native Messaging：浏览器启动时自动唤醒引擎
- 系统托盘

## 安装（从源码）

1. 装 Rust：https://rustup.rs
2. `cargo build --release --workspace`
3. 加载扩展：`chrome://extensions` → 开发者模式 → 加载已解压的扩展程序 → 选 `extension/`
4. 跑 `installer/install.ps1` 配置扩展 ID

## 打包安装器（可选）

用 Inno Setup 打开 `setup/FastDL.iss` → 编译。

## License

MIT（或你选）