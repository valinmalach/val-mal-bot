#!/bin/bash

# Path to your Python application
APP_PATH="/home/valinmalach/val-mal-bot"
BOT_PROCESS_NAME="python3 main.py"

# Kill the running bot (this could also be done using pkill)
pkill -f "$BOT_PROCESS_NAME"

# Wait for the process to terminate completely
sleep 2

# Change to the application directory
cd "$APP_PATH" || exit

# Pull the latest code from Git
echo "Updating from Git..."
git pull

# Install the required packages
echo "Installing requirements..."
pip3 install -r requirements.txt -U

# Restart the bot
echo "Restarting the bot..."
nohup python3 main.py &
