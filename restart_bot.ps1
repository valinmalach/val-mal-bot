# Application directory and bot script
$AppPath = "C:\val-mal-bot"
$BotScript = "main.py"

Write-Output "Stopping existing bot processes..."

# Find the FastAPI process and send SIGTERM for graceful shutdown
$processes = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match $BotScript }
foreach ($process in $processes) {
    Write-Output "Sending graceful shutdown signals to FastAPI process $($process.ProcessId)..."
    
    # First try SIGINT (Ctrl+C equivalent - most reliable for Python apps)
    try {
        Write-Output "Attempting SIGINT (Ctrl+C)..."
        # Use taskkill to send CTRL+C signal
        Start-Process -FilePath "taskkill" -ArgumentList "/PID", $process.ProcessId -Wait -NoNewWindow -ErrorAction Stop
        
        # Wait 10 seconds for SIGINT to work
        $timeout = 10
        $elapsed = 0
        while ((Get-Process -Id $process.ProcessId -ErrorAction SilentlyContinue) -and ($elapsed -lt $timeout)) {
            Start-Sleep -Seconds 1
            $elapsed++
        }
        
        if (-not (Get-Process -Id $process.ProcessId -ErrorAction SilentlyContinue)) {
            Write-Output "Process terminated gracefully with SIGINT"
            continue
        }
        
        Write-Output "SIGINT timeout, trying SIGTERM..."
    } catch {
        Write-Output "SIGINT failed: $($_.Exception.Message), trying SIGTERM..."
    }
    
    # If SIGINT failed or timed out, try SIGTERM
    try {
        Write-Output "Attempting SIGTERM..."
        Stop-Process -Id $process.ProcessId -ErrorAction Stop
        
        # Wait another 10 seconds for SIGTERM to work
        $timeout = 10
        $elapsed = 0
        while ((Get-Process -Id $process.ProcessId -ErrorAction SilentlyContinue) -and ($elapsed -lt $timeout)) {
            Start-Sleep -Seconds 1
            $elapsed++
        }
        
        if (-not (Get-Process -Id $process.ProcessId -ErrorAction SilentlyContinue)) {
            Write-Output "Process terminated gracefully with SIGTERM"
            continue
        }
        
        Write-Output "SIGTERM timeout, forcing termination..."
    } catch {
        Write-Output "SIGTERM failed: $($_.Exception.Message), forcing termination..."
    }
    
    # Last resort: Force kill
    try {
        Write-Output "Force stopping process..."
        Stop-Process -Id $process.ProcessId -Force
        Write-Output "Process force terminated"
    } catch {
        Write-Output "Failed to force stop process: $($_.Exception.Message)"
    }
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