# ============================================================
# FastDL - Configure Native Messaging Host
# Works in two modes:
#   A) Project mode:  run from D:\Rust\project\fastdl\installer\
#   B) Installed mode: run from the install directory (e.g. D:\FastDL)
# ============================================================

$ErrorActionPreference = 'Stop'

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host " FastDL Setup" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# ---- Detect mode ----
$projectRoot = Split-Path -Parent $PSScriptRoot
$isProjectMode = Test-Path (Join-Path $projectRoot "target\release\fastdl.exe")

if ($isProjectMode) {
    Write-Host "Mode: PROJECT (running from project source)" -ForegroundColor Yellow
    $srcFastdl   = Join-Path $projectRoot "target\release\fastdl.exe"
    $srcLauncher = Join-Path $projectRoot "target\release\fastdl-launcher.exe"
    $srcExt      = Join-Path $projectRoot "extension"
    $installDir  = Join-Path $env:LOCALAPPDATA "FastDL"
} else {
    Write-Host "Mode: INSTALLED (running from install directory)" -ForegroundColor Yellow
    $installDir  = $PSScriptRoot
    $srcFastdl   = Join-Path $installDir "fastdl.exe"
    $srcLauncher = Join-Path $installDir "fastdl-launcher.exe"
    $srcExt      = Join-Path $installDir "extension"
}

Write-Host "Install dir: $installDir" -ForegroundColor Gray
Write-Host ""

# ---- Verify files ----
foreach ($f in @($srcFastdl, $srcLauncher)) {
    if (-not (Test-Path $f)) {
        Write-Host "ERROR: missing file: $f" -ForegroundColor Red
        Write-Host ""
        Write-Host "Press Enter to exit..."
        Read-Host
        exit 1
    }
}

# ---- Project mode: copy files to install dir ----
if ($isProjectMode) {
    New-Item -ItemType Directory -Path $installDir -Force | Out-Null
    Copy-Item $srcFastdl   (Join-Path $installDir "fastdl.exe")          -Force
    Copy-Item $srcLauncher (Join-Path $installDir "fastdl-launcher.exe") -Force
    Write-Host "Copied exe files" -ForegroundColor Green

    if (Test-Path $srcExt) {
        $extDest = Join-Path $installDir "extension"
        if (Test-Path $extDest) { Remove-Item $extDest -Recurse -Force }
        Copy-Item $srcExt $extDest -Recurse -Force
        Write-Host "Copied extension files" -ForegroundColor Green
    }
}

# ---- Ask for extension ID ----
Write-Host ""
Write-Host "Open chrome://extensions and load the extension from:" -ForegroundColor Yellow
Write-Host "  $(Join-Path $installDir 'extension')" -ForegroundColor Gray
Write-Host "Then copy the extension ID (32 lowercase letters a-p)." -ForegroundColor Yellow
Write-Host ""

$extId = Read-Host "Paste extension ID here"

if ($extId -notmatch '^[a-p]{32}$') {
    Write-Host ""
    Write-Host "ERROR: bad extension ID format (expect 32 lowercase a-p)" -ForegroundColor Red
    Write-Host ""
    Read-Host "Press Enter to exit"
    exit 1
}

# ---- Generate native host JSON ----
$launcherPath = (Join-Path $installDir "fastdl-launcher.exe").Replace('\', '\\')
$hostJson = @"
{
  "name": "com.fastdl.launcher",
  "description": "FastDL Native Messaging Host",
  "path": "$launcherPath",
  "type": "stdio",
  "allowed_origins": [
    "chrome-extension://$extId/"
  ]
}
"@
$hostJsonPath = Join-Path $installDir "com.fastdl.launcher.json"
Set-Content -Path $hostJsonPath -Value $hostJson -Encoding UTF8
Write-Host "Generated Native Host manifest" -ForegroundColor Green

# ---- Register ----
$chromeKey = "HKCU\Software\Google\Chrome\NativeMessagingHosts\com.fastdl.launcher"
$edgeKey   = "HKCU\Software\Microsoft\Edge\NativeMessagingHosts\com.fastdl.launcher"
reg add $chromeKey /ve /t REG_SZ /d "$hostJsonPath" /f | Out-Null
reg add $edgeKey   /ve /t REG_SZ /d "$hostJsonPath" /f | Out-Null
Write-Host "Registered in Chrome and Edge" -ForegroundColor Green

# ---- Project mode only: startup shortcut + start menu ----
if ($isProjectMode) {
    $ws = New-Object -ComObject WScript.Shell

    $startup = [Environment]::GetFolderPath('Startup')
    $lnk = $ws.CreateShortcut((Join-Path $startup "FastDL.lnk"))
    $lnk.TargetPath = Join-Path $installDir "fastdl.exe"
    $lnk.WorkingDirectory = $installDir
    $lnk.Save()

    $startMenu = [Environment]::GetFolderPath('Programs')
    $smLnk = $ws.CreateShortcut((Join-Path $startMenu "FastDL.lnk"))
    $smLnk.TargetPath = Join-Path $installDir "fastdl.exe"
    $smLnk.WorkingDirectory = $installDir
    $smLnk.Save()

    Write-Host "Created shortcuts" -ForegroundColor Green

    # 只有在项目模式下才启动（已安装模式启动会在后面单独做）
    if (-not (Get-Process fastdl -ErrorAction SilentlyContinue)) {
        Start-Process (Join-Path $installDir "fastdl.exe")
        Write-Host "Started FastDL" -ForegroundColor Green
    }
} else {
    # 已安装模式：如果没跑就启动
    if (-not (Get-Process fastdl -ErrorAction SilentlyContinue)) {
        Start-Process (Join-Path $installDir "fastdl.exe")
        Write-Host "Started FastDL" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host " Done!" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Now test: kill fastdl.exe, click a download in browser," -ForegroundColor Gray
Write-Host "and it should auto-start and download." -ForegroundColor Gray
Write-Host ""
Read-Host "Press Enter to exit"