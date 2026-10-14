Add-Type -AssemblyName System.Drawing

function New-CircleIcon {
    param([int]$size, [string]$outPath)
    $bmp = New-Object System.Drawing.Bitmap $size, $size
    $g = [System.Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = 'AntiAlias'
    $brush = New-Object System.Drawing.SolidBrush ([System.Drawing.Color]::FromArgb(59, 130, 246))
    $pad = [Math]::Max(1, [int]($size * 0.06))
    $g.FillEllipse($brush, $pad, $pad, $size - 2*$pad, $size - 2*$pad)
    $bmp.Save($outPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $g.Dispose(); $bmp.Dispose(); $brush.Dispose()
}

New-CircleIcon -size 16  -outPath "icon16.png"
New-CircleIcon -size 48  -outPath "icon48.png"
New-CircleIcon -size 128 -outPath "icon128.png"

Write-Host "Icons generated."