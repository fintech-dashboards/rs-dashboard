#!/bin/bash
# RS Dashboard - Mac/Linux Startup Script

set -e

echo ""
echo "=========================================="
echo "  RS Dashboard - Starting..."
echo "=========================================="
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "[1/3] Installing uv package manager..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # Add to current session PATH
    export PATH="$HOME/.local/bin:$PATH"
    echo "  ✓ uv installed"
else
    echo "[1/3] uv already installed ✓"
fi

# Sync dependencies
echo "[2/3] Syncing dependencies..."
uv sync
echo "  ✓ Dependencies ready"

# Run the app
echo "[3/3] Starting RS Dashboard..."
echo ""
echo "  → Opening http://localhost:5001"
echo "  → Press Ctrl+C to stop"
echo ""

# Open browser after short delay (background)
(sleep 2 && open http://localhost:5001 2>/dev/null || xdg-open http://localhost:5001 2>/dev/null || true) &

# Run the FastAPI app
uv run python -m uvicorn api.main:app --host 0.0.0.0 --port 5001 --reload
