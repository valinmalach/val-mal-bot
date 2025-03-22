# Application directory and bot script
$AppPath = "C:\val-mal-bot"

# Change to the application directory
Set-Location $AppPath

# Pull the latest code from Git
Write-Host "Updating from Git..."
git pull