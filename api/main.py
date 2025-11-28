"""FastAPI application entry point"""
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from contextlib import asynccontextmanager
import uvicorn
from pathlib import Path
import sys

from .routes import pages, charts, admin, htmx
from db import init_db as init_central_db, cleanup_on_startup

ROOT_DIR = Path(__file__).parent.parent
TEMPLATES_DIR = ROOT_DIR / "templates"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize centralized database
    try:
        print("\n[STARTUP] Initializing database...")

        # Initialize centralized database (contains all tables)
        init_central_db()
        print("  ✓ Centralized database (rs_metrics.db) initialized")

        # Clear stale tasks from previous session
        cleanup_on_startup()

        # Clean up old task records (older than 7 days)
        from db import cleanup_old_tasks
        deleted = cleanup_old_tasks(days=7)
        if deleted > 0:
            print(f"  ✓ Cleaned up {deleted} old task records")

        # Fetch SPY prices if not already fetched
        from db import get_price_count, get_settings
        spy_count = get_price_count('SPY')
        if spy_count == 0:
            print("  → Fetching SPY benchmark prices...")
            sys.path.insert(0, str(ROOT_DIR / "price-engine"))
            from yfinance_provider import fetch_ticker_data
            settings = get_settings()
            start_date = settings.get('start_date', '2024-01-01')
            fetch_ticker_data('SPY', start_date=start_date, skip_info=True)
            print("  ✓ SPY price fetch queued")
        else:
            print(f"  ✓ SPY benchmark ready ({spy_count} prices)")

        print("[STARTUP] Database ready!\n")
    except Exception as e:
        print(f"✗ Database initialization error: {e}")
        import traceback
        traceback.print_exc()
    yield
    # Shutdown: cleanup if needed

app = FastAPI(title="RS Dashboard", lifespan=lifespan)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Include routers
app.include_router(pages.router)
app.include_router(charts.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(htmx.router, prefix="/api")

# Static files (if any)
# app.mount("/static", StaticFiles(directory="static"), name="static")

def main():
    uvicorn.run(app, host="0.0.0.0", port=5001, reload=True)

if __name__ == "__main__":
    main()
