#!/bin/bash
RUN_DIR="/run/gps-project"
LOG_FILE="${RUN_DIR}/cloudflared.log"
URL_FILE="${RUN_DIR}/public_url"

# Ensure run directory exists
mkdir -p "$RUN_DIR"
rm -f "$URL_FILE"

echo "Starting cloudflared quick tunnel..."
# Launch cloudflared tunnel in the background
cloudflared tunnel --url http://localhost:8080 > "$LOG_FILE" 2>&1 &
CF_PID=$!

# Clean up on exit
cleanup() {
    echo "Stopping cloudflared..."
    kill "$CF_PID" 2>/dev/null || true
    rm -f "$URL_FILE"
    exit 0
}
trap cleanup SIGTERM SIGINT

# Extract the trycloudflare URL from logs
echo "Waiting for trycloudflare.com URL..."
for i in {1..60}; do
    # Check if cloudflared process is still alive
    if ! kill -0 "$CF_PID" 2>/dev/null; then
        echo "Error: cloudflared exited unexpectedly. Check $LOG_FILE"
        exit 1
    fi

    if [ -f "$LOG_FILE" ]; then
        # Exclude 'api.trycloudflare.com' and extract the random tunnel URL
        URL=$(grep -oE 'https://[a-zA-Z0-9\-]+\.trycloudflare\.com' "$LOG_FILE" | grep -v 'api.trycloudflare.com' | head -n 1)
        if [ ! -z "$URL" ]; then
            echo "$URL" > "$URL_FILE"
            echo "Successfully generated public URL: $URL"
            break
        fi
    fi
    sleep 1
done

if [ ! -f "$URL_FILE" ]; then
    echo "Warning: Timeout waiting for quick tunnel URL. Check $LOG_FILE"
fi

# Wait for cloudflared process
wait "$CF_PID"
