# Application directory and bot script
$AppPath = "C:\val-mal-bot"
$BotScript = "main.py"

# Kill the running bot by filtering processes whose command line contains the bot script name
Write-Output "Stopping existing bot processes..."
$processes = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match $BotScript }
foreach ($process in $processes) {
    Stop-Process -Id $process.ProcessId -Force
}

# Change to the application directory
Set-Location $AppPath

# Pull the latest code from Git
Write-Output "Updating from Git..."
git pull

# Install or update the required packages
Write-Output "Installing requirements..."
uv self update
uv sync

# Restart the bot
Write-Output "Restarting the bot..."
uv run $BotScript