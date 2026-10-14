$ErrorActionPreference = 'Stop'

$manifestPath = "D:\Rust\project\fastdl\extension\manifest.json"

# 用 Unicode 码点拼中文，避免脚本自身的编码问题
$desc = [char]0x628A + [char]0x6D4F + [char]0x89C8 + [char]0x5668 + [char]0x4E0B + [char]0x8F7D + [char]0x4EA4 + [char]0x7ED9 + [char]0x672C + [char]0x5730 + " FastDL " + [char]0x5F15 + [char]0x64CE
# 上面拼出来的是：把浏览器下载交给本地 FastDL 引擎

$title = "FastDL " + [char]0x4EFB + [char]0x52A1 + [char]0x9762 + [char]0x677F
# 上面拼出来的是：FastDL 任务面板

$json = [System.IO.File]::ReadAllText($manifestPath, [System.Text.Encoding]::UTF8)
$manifest = $json | ConvertFrom-Json

$manifest.description = $desc
$manifest.action.default_title = $title

# 写回 UTF-8 无 BOM
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
$out = $manifest | ConvertTo-Json -Depth 20
[System.IO.File]::WriteAllText($manifestPath, $out, $utf8NoBom)

Write-Host "Fixed: $manifestPath" -ForegroundColor Green

# 同步到安装目录（如果存在）
$installed = "D:\FastDL\extension\manifest.json"
if (Test-Path (Split-Path $installed -Parent)) {
    Copy-Item $manifestPath $installed -Force
    Write-Host "Synced to: $installed" -ForegroundColor Green
}

Write-Host ""
Write-Host "Next:" -ForegroundColor Yellow
Write-Host "  1. Reload extension at chrome://extensions" -ForegroundColor White
Write-Host "  2. Description should now show Chinese" -ForegroundColor White