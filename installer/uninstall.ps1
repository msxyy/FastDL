$ErrorActionPreference = 'SilentlyContinue'

Write-Host "Uninstalling FastDL..." -ForegroundColor Cyan

Get-Process fastdl, fastdl-launcher | Stop-Process -Force

reg delete "HKCU\Software\Google\Chrome\NativeMessagingHosts\com.fastdl.launcher" /f | Out-Null
reg delete "HKCU\Software\Microsoft\Edge\NativeMessagingHosts\com.fastdl.launcher"   /f | Out-Null

$startup = [Environment]::GetFolderPath('Startup')
Remove-Item (Join-Path $startup "FastDL.lnk") -Force
$startMenu = [Environment]::GetFolderPath('Programs')
Remove-Item (Join-Path $startMenu "FastDL.lnk") -Force

Remove-Item (Join-Path $env:LOCALAPPDATA "FastDL") -Recurse -Force

Write-Host "Uninstall complete." -ForegroundColor Green
Write-Host "Remove the browser extension manually from chrome://extensions" -ForegroundColor Yellow