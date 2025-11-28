"""Admin action endpoints"""
from fastapi import APIRouter, UploadFile, File, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import csv
import io
import sys

from ..services import task_service, price_service
from db import get_all_task_statuses, get_pipeline_status

# Add calc-engine to path for settings
CALC_ENGINE_DIR = Path(__file__).parent.parent.parent / "calc-engine"
sys.path.insert(0, str(CALC_ENGINE_DIR))

router = APIRouter(tags=["admin"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent.parent / "templates"))


@router.post("/upload", response_class=HTMLResponse)
async def upload_tickers(request: Request, file: UploadFile = File(...)):
    """Upload CSV of tickers and start full pipeline"""
    import re

    content = await file.read()
    text = content.decode('utf-8-sig')
    reader = csv.DictReader(io.StringIO(text))

    # Find ticker column (case-insensitive)
    if not reader.fieldnames:
        print("[UPLOAD] ERROR: Empty CSV or no headers")
        status = get_pipeline_status()
        return templates.TemplateResponse("partials/pipeline_stages.html", {
            "request": request,
            "pipeline": status
        })

    ticker_col = None
    for col in reader.fieldnames:
        if col and col.lower() in ('ticker', 'symbol', 'tickers', 'symbols'):
            ticker_col = col
            break

    if not ticker_col:
        print(f"[UPLOAD] ERROR: No ticker/symbol column found. Columns: {reader.fieldnames}")
        status = get_pipeline_status()
        return templates.TemplateResponse("partials/pipeline_stages.html", {
            "request": request,
            "pipeline": status
        })

    # Parse and validate tickers
    tickers = set()  # Use set to dedupe
    for row in reader:
        ticker = row.get(ticker_col, '').strip().upper()

        # Skip empty, NA, null values
        if not ticker or ticker in ('', 'NA', 'N/A', 'NULL', 'NAN', 'NONE', '-'):
            continue

        # Validate ticker format (1-10 alphanumeric chars, may have dots/hyphens)
        if re.match(r'^[A-Z0-9\.\-]{1,10}$', ticker):
            tickers.add(ticker)
        else:
            print(f"[UPLOAD] Skipping invalid ticker: {ticker}")

    tickers = list(tickers)
    print(f"[UPLOAD] Parsed {len(tickers)} valid tickers")

    if not tickers:
        print("[UPLOAD] No valid tickers found")
        status = get_pipeline_status()
        return templates.TemplateResponse("partials/pipeline_stages.html", {
            "request": request,
            "pipeline": status
        })

    # Add tickers to DB
    price_service.add_tickers_to_db(tickers)

    # Start full pipeline (prices → returns → RS) with auto-advance
    task_service.start_refresh_all_pipeline()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.post("/refresh-all", response_class=HTMLResponse)
async def refresh_all(request: Request):
    """Full pipeline: fetch prices → calc returns → calc RS"""
    task_service.start_refresh_all_pipeline()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.post("/recalculate-rs", response_class=HTMLResponse)
async def recalculate_rs(request: Request):
    """Delete RS scores and recalculate"""
    task_service.clear_rs_scores()
    task_service.queue_rs_calculation()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.post("/recalculate-all", response_class=HTMLResponse)
async def recalculate_all(request: Request):
    """Delete returns + RS, recalculate from prices"""
    task_service.clear_returns()
    task_service.clear_rs_scores()
    task_service.start_recalculate_pipeline()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.post("/clear-rs-history", response_class=HTMLResponse)
async def clear_rs_history(request: Request):
    """Delete all RS scores (keep prices)"""
    task_service.clear_rs_scores()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.post("/clear-returns", response_class=HTMLResponse)
async def clear_returns(request: Request):
    """Delete sector/industry returns"""
    task_service.clear_returns()

    # Return HTML partial for HTMX
    status = get_pipeline_status()
    return templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })


@router.get("/tasks/status")
async def get_tasks_status():
    """Get all task statuses from centralized database"""
    return get_all_task_statuses()


@router.get("/pipeline/status")
async def get_pipeline_status_json():
    """Get aggregated pipeline status"""
    return get_pipeline_status()


@router.get("/settings/rs")
async def get_rs_settings():
    """Get RS calculation settings"""
    from settings import get_settings, DEFAULT_SETTINGS
    settings = get_settings()
    # Merge with defaults for any missing keys
    return {**DEFAULT_SETTINGS, **settings}


@router.get("/settings/rs-html", response_class=HTMLResponse)
async def get_rs_settings_html(request: Request):
    """Get RS settings as HTML partial"""
    from settings import get_settings, DEFAULT_SETTINGS
    settings = {**DEFAULT_SETTINGS, **get_settings()}
    return templates.TemplateResponse("partials/rs_settings.html", {
        "request": request,
        "settings": settings
    })


@router.post("/settings/rs", response_class=HTMLResponse)
async def update_rs_settings(
    request: Request,
    q1_weight: float = Form(...),
    q2_weight: float = Form(...),
    q3_weight: float = Form(...),
    q4_weight: float = Form(...)
):
    """Update RS calculation weights and trigger full RS recalculation"""
    from settings import update_settings

    # Normalize weights to sum to 1.0
    total = q1_weight + q2_weight + q3_weight + q4_weight
    if total > 0:
        update_settings({
            'q1_weight': round(q1_weight / total, 2),
            'q2_weight': round(q2_weight / total, 2),
            'q3_weight': round(q3_weight / total, 2),
            'q4_weight': round(q4_weight / total, 2),
        })

    # Clear all RS scores and trigger recalculation with new weights
    # queue_rs_calculation calls calculate_all_rs which does stocks, sectors, AND industries
    task_service.clear_rs_scores()
    task_service.queue_rs_calculation()

    # Return updated settings partial
    from settings import get_settings, DEFAULT_SETTINGS
    settings = {**DEFAULT_SETTINGS, **get_settings()}
    return templates.TemplateResponse("partials/rs_settings.html", {
        "request": request,
        "settings": settings,
        "saved": True,
        "recalculating": True
    })
