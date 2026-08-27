#!/bin/bash
set -e

PORT="${PORT:-8000}"

# 1. Authenticate and start loclx in the background
if [ -n "$LOCLX_TOKEN" ]; then
    # loclx has no `authtoken` subcommand; anything it does not recognise falls
    # through to the web GUI, which blocks on :54537 and never returns.
    export ACCESS_TOKEN="$LOCLX_TOKEN"

    # loclx exits 0 even when it rejects the token, so the text is the signal.
    status="$(loclx account status 2>&1)"
    echo "$status"
    case "$status" in
        *"access token is invalid"*|*"not logged in"*)
            echo "loclx rejected LOCLX_TOKEN, aborting." >&2; exit 1 ;;
        *Error:*)
            echo "WARNING: loclx could not reach its API, tunnel may not come up." >&2 ;;
    esac

    if [ -n "$APP_URL" ] && [ "${APP_URL%/}" != "https://${LOCLX_SUBDOMAIN}.loclx.io" ]; then
        echo "WARNING: APP_URL is ${APP_URL%/} but the tunnel serves https://${LOCLX_SUBDOMAIN}.loclx.io" >&2
    fi

    # -r is --raw-mode here; on the http subcommand it means --https-redirect.
    # Without it loclx draws a terminal UI that has no terminal to draw to.
    args=(tunnel -r http --to "127.0.0.1:$PORT")
    [ -n "$LOCLX_SUBDOMAIN" ] && args+=(--subdomain "$LOCLX_SUBDOMAIN")
    loclx "${args[@]}" &

    echo "Waiting for the tunnel to register..."
    up=""
    for _ in $(seq 15); do
        listing="$(loclx tunnel list 2>&1)"
        case "$listing" in
            *"${LOCLX_SUBDOMAIN:-loclx.io}"*) up=1; break ;;
        esac
        sleep 2
    done
    if [ -n "$up" ]; then
        echo "$listing"
    else
        echo "WARNING: no tunnel after 30s, Twitch webhooks will not arrive." >&2
        echo "$listing" >&2
    fi
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
