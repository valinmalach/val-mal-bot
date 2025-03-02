# Application directory and bot script
$AppPath = "C:\val-mal-bot"
$BotScript = "main.py"

# Kill the running bot by filtering processes whose command line contains the bot script name
Write-Host "Stopping existing bot processes..."
$processes = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match $BotScript }
foreach ($process in $processes) {
    Stop-Process -Id $process.ProcessId -Force
}

# Change to the application directory
Set-Location $AppPath

# Pull the latest code from Git
Write-Host "Updating from Git..."
git pull

# Install or update the required packages
Write-Host "Installing requirements..."
python -m pip install -r requirements.txt --upgrade

# Restart the bot
Write-Host "Restarting the bot..."
python $BotScript