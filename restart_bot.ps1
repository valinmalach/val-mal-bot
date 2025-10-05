# Application directory and bot script
$AppPath = "C:\val-mal-bot"
$BotScript = "main.py"

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
Write-Output "Restarting the API and bot..."
uv run $BotScript