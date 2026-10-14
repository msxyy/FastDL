# ============================================================
# FastDL - Generate RSA keypair for fixed extension ID
# Pure PowerShell, no external tools required
# Writes manifest.json as UTF-8 WITHOUT BOM
# ============================================================

$ErrorActionPreference = 'Stop'

$projectRoot  = Split-Path -Parent $PSScriptRoot
$privPath     = Join-Path $projectRoot "extension-key.xml"
$pubPath      = Join-Path $projectRoot "extension-key.txt"
$manifestPath = Join-Path $projectRoot "extension\manifest.json"

# ---- DER encoding helpers ----
function DerLength([int]$n) {
    $out = New-Object System.Collections.Generic.List[byte]
    if ($n -lt 0x80) {
        $out.Add([byte]$n)
    } elseif ($n -lt 0x100) {
        $out.Add([byte]0x81); $out.Add([byte]$n)
    } elseif ($n -lt 0x10000) {
        $out.Add([byte]0x82)
        $out.Add([byte](($n -shr 8) -band 0xFF))
        $out.Add([byte]($n -band 0xFF))
    } else {
        throw "DER length too large: $n"
    }
    return ,$out.ToArray()
}

function DerInteger([byte[]]$bytes) {
    $i = 0
    while ($i -lt ($bytes.Length - 1) -and $bytes[$i] -eq 0) { $i++ }
    $trimmed = $bytes[$i..($bytes.Length - 1)]
    if (($trimmed[0] -band 0x80) -ne 0) {
        $trimmed = [byte[]]@(0) + $trimmed
    }
    $out = New-Object System.Collections.Generic.List[byte]
    $out.Add([byte]0x02)
    foreach ($b in (DerLength $trimmed.Length)) { $out.Add($b) }
    foreach ($b in $trimmed) { $out.Add($b) }
    return ,$out.ToArray()
}

function DerSequence([byte[]]$content) {
    $out = New-Object System.Collections.Generic.List[byte]
    $out.Add([byte]0x30)
    foreach ($b in (DerLength $content.Length)) { $out.Add($b) }
    foreach ($b in $content) { $out.Add($b) }
    return ,$out.ToArray()
}

# ---- Generate key ----
Write-Host "Generating 2048-bit RSA keypair..." -ForegroundColor Cyan
$rsa = New-Object System.Security.Cryptography.RSACryptoServiceProvider 2048
$params = $rsa.ExportParameters($false)

# ---- Build SPKI (SubjectPublicKeyInfo) ----
$modInt = DerInteger $params.Modulus
$expInt = DerInteger $params.Exponent
$rsaPub = DerSequence ($modInt + $expInt)

# BIT STRING { unused=0, RSAPublicKey }
$bitContent = [byte[]]@(0) + $rsaPub
$bitOut = New-Object System.Collections.Generic.List[byte]
$bitOut.Add([byte]0x03)
foreach ($b in (DerLength $bitContent.Length)) { $bitOut.Add($b) }
foreach ($b in $bitContent) { $bitOut.Add($b) }
$bitString = $bitOut.ToArray()

# AlgorithmIdentifier { OID rsaEncryption, NULL }
$algContent = [byte[]]@(0x06,0x09,0x2A,0x86,0x48,0x86,0xF7,0x0D,0x01,0x01,0x01,0x05,0x00)
$algId = DerSequence $algContent

# SPKI = SEQUENCE { AlgorithmIdentifier, BIT STRING }
$spki = DerSequence ($algId + $bitString)
$pubB64 = [Convert]::ToBase64String($spki)

# ---- Save keys (ASCII, no BOM) ----
$ascii = New-Object System.Text.ASCIIEncoding
[System.IO.File]::WriteAllText($pubPath, $pubB64, $ascii)
Write-Host "Public key saved:  $pubPath" -ForegroundColor Green

$privXml = $rsa.ToXmlString($true)
[System.IO.File]::WriteAllText($privPath, $privXml, $ascii)
Write-Host "Private key saved: $privPath" -ForegroundColor Green

$rsa.Dispose()

# ---- Inject "key" into manifest.json (UTF-8 no BOM) ----
if (-not (Test-Path $manifestPath)) {
    Write-Host "ERROR: manifest.json not found: $manifestPath" -ForegroundColor Red
    exit 1
}

# 读取时兼容 BOM：先用 .NET 按 UTF-8 读
$json = [System.IO.File]::ReadAllText($manifestPath, [System.Text.Encoding]::UTF8)
$manifest = $json | ConvertFrom-Json

# 移除旧的 key（如果有）
if ($manifest.PSObject.Properties['key']) {
    $manifest.PSObject.Properties.Remove('key')
}

# 加 key 字段
$manifest | Add-Member -NotePropertyName 'key' -NotePropertyValue $pubB64 -Force

# 序列化并写回：UTF-8 WITHOUT BOM
$newJson = $manifest | ConvertTo-Json -Depth 20
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($manifestPath, $newJson, $utf8NoBom)

Write-Host "manifest.json updated (UTF-8 no BOM)" -ForegroundColor Green

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host " Done" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Next:" -ForegroundColor Yellow
Write-Host "  1. Open chrome://extensions" -ForegroundColor White
Write-Host "  2. Reload 'FastDL Bridge'" -ForegroundColor White
Write-Host "  3. A NEW extension ID appears - copy it" -ForegroundColor White
Write-Host "  4. Run install.ps1 with the NEW ID" -ForegroundColor White
Write-Host ""
Write-Host "IMPORTANT: back up extension-key.xml!" -ForegroundColor Red
Write-Host "  It is the private key. Keep it safe." -ForegroundColor Red
Write-Host ""