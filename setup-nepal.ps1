$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

if (-not (Get-Command py -ErrorAction SilentlyContinue) -and -not (Get-Command python -ErrorAction SilentlyContinue)) {
    if (-not (Get-Command winget -ErrorAction SilentlyContinue)) {
        Write-Error "Install Python 3.12, then run this setup again."
    }
    winget install --id Python.Python.3.12 --exact --accept-package-agreements --accept-source-agreements
    $env:Path = [Environment]::GetEnvironmentVariable("Path", "Machine") + ";" + [Environment]::GetEnvironmentVariable("Path", "User")
}

$venv = Join-Path $scriptDir ".venv"
$launcher = Get-Command py -ErrorAction SilentlyContinue
if ($launcher) {
    & $launcher.Source -3 -m venv $venv
} else {
    & (Get-Command python).Source -m venv $venv
}
& (Join-Path $venv "Scripts\python.exe") -m pip install --disable-pip-version-check --quiet certifi
Write-Host "Setup complete. Double-click upload-nepal.cmd or run:"
Write-Host "  upload-nepal.cmd E:\"
