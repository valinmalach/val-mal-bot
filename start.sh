#!/bin/bash
set -e

PORT="${PORT:-8000}"

# 1. Authenticate and start loclx in the background
if [ -n "$LOCLX_TOKEN" ]; then
    # loclx wants the label only, but APP_URL is a full https:// URL.
    SUBDOMAIN="${APP_URL#*://}"
    SUBDOMAIN="${SUBDOMAIN%%.*}"
    loclx authtoken "$LOCLX_TOKEN"
    loclx tunnel http --to "$PORT" --subdomain "$SUBDOMAIN" &
fi

# 2. Apply database migrations
echo "Running database migrations..."
uv run alembic upgrade head

# 3. Seed data (Ensure your seed script checks if data exists first)
echo "Running database seed..."
uv run python seed.py

# 4. Launch main application
echo "Starting application..."
exec uv run main.py