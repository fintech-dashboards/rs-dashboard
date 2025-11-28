"""HTMX fragment endpoints"""
from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from ..services import price_service, task_service
from db import get_connection

router = APIRouter(tags=["htmx"])
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent.parent / "templates"))


@router.get("/pipeline-status-html", response_class=HTMLResponse)
async def get_pipeline_status(request: Request):
    """Pipeline status for HTMX polling"""
    status = task_service.get_pipeline_status()

    # Check if any stage is running
    any_running = (
        status['stocks']['prices']['status'] == 'running' or
        status['stocks']['rs_score']['status'] == 'running' or
        status['sectors']['returns']['status'] == 'running' or
        status['sectors']['rs_score']['status'] == 'running' or
        status['industries']['returns']['status'] == 'running' or
        status['industries']['rs_score']['status'] == 'running'
    )

    response = templates.TemplateResponse("partials/pipeline_stages.html", {
        "request": request,
        "pipeline": status
    })

    # Trigger ticker table refresh when pipeline is active
    if any_running:
        response.headers["HX-Trigger"] = "refreshTickerTable"

    return response


@router.get("/ticker-table-html", response_class=HTMLResponse)
async def get_ticker_table(
    request: Request,
    filter: str = Query(default="all"),
    sector: str = Query(default="all"),
    industry: str = Query(default="all"),
    search: str = Query(default="")
):
    """Simple endpoint - returns all filtered tickers. Client handles sort/pagination."""
    # Get filtered tickers
    tickers = price_service.get_filtered_tickers(filter, sector, industry, search)

    # BATCH QUERY 1: Get price counts for all tickers at once
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, COUNT(*) as price_count FROM prices
        GROUP BY symbol
    """)
    price_counts = {row[0]: row[1] for row in cursor.fetchall()}

    # BATCH QUERY 2: Get latest RS scores for all stocks at once
    cursor.execute("""
        SELECT entity_name, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'stock'
        AND date = (SELECT MAX(date) FROM rs_scores WHERE entity_type = 'stock')
    """)
    rs_data = {row[0]: {'score': row[1], 'percentile': row[2]} for row in cursor.fetchall()}
    conn.close()

    # Enhance tickers with batched data
    enhanced_tickers = []
    for ticker in tickers:
        symbol = ticker['symbol']
        price_count = price_counts.get(symbol, 0)
        rs_info = rs_data.get(symbol)

        enhanced_tickers.append({
            **ticker,
            'price_status': 'complete' if price_count > 0 else 'pending',
            'price_count': price_count,
            'rs_status': 'complete' if rs_info else 'pending',
            'rs_score': rs_info['score'] if rs_info else None,
            'rs_percentile': int(rs_info['percentile']) if rs_info else 0,
        })

    return templates.TemplateResponse("partials/ticker_table.html", {
        "request": request,
        "tickers": enhanced_tickers
    })
