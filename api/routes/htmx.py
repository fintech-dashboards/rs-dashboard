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
    search: str = Query(default=""),
    page: int = Query(default=1),
    per_page: int = Query(default=50),
    sort_by: str = Query(default="symbol"),
    sort_dir: str = Query(default="asc")
):
    """Filtered ticker table for HTMX with pagination and sorting"""
    # Get filtered tickers
    tickers = price_service.get_filtered_tickers(filter, sector, industry, search)

    # BATCH QUERY 1: Get price counts for all tickers at once (no N+1)
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT symbol, COUNT(*) as price_count FROM prices
        GROUP BY symbol
    """)
    price_counts = {row[0]: row[1] for row in cursor.fetchall()}

    # BATCH QUERY 2: Get latest RS scores for all stocks at once (no N+1)
    cursor.execute("""
        SELECT entity_name, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'stock'
        AND date = (SELECT MAX(date) FROM rs_scores WHERE entity_type = 'stock')
    """)
    rs_data = {row[0]: {'score': row[1], 'percentile': row[2]} for row in cursor.fetchall()}
    conn.close()

    # Enhance tickers with batched data (O(1) lookup per ticker)
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
            'price_chart': [],
            'rs_chart': [],
            'history_days': price_count,
            'rs_updated_at': None
        })

    # Server-side sorting (sort full dataset before pagination)
    sort_key_map = {
        'symbol': lambda x: x['symbol'] or '',
        'name': lambda x: x['name'] or '',
        'sector': lambda x: x['sector'] or '',
        'industry': lambda x: x['industry'] or '',
        'rs_score': lambda x: x['rs_score'] if x['rs_score'] is not None else -999,
        'rs_percentile': lambda x: x['rs_percentile'] if x['rs_percentile'] else 0,
        'price_count': lambda x: x['price_count'] or 0
    }

    if sort_by in sort_key_map:
        reverse = sort_dir == 'desc'
        enhanced_tickers.sort(key=sort_key_map[sort_by], reverse=reverse)

    # Implement pagination (after sorting)
    total_items = len(enhanced_tickers)
    total_pages = (total_items + per_page - 1) // per_page if per_page > 0 else 1
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    paginated_tickers = enhanced_tickers[start_idx:end_idx]

    return templates.TemplateResponse("partials/ticker_table.html", {
        "request": request,
        "tickers": paginated_tickers,
        "total_pages": total_pages,
        "page": page,
        "per_page": per_page,
        "total_items": total_items,
        "filter": filter,
        "sector": sector,
        "industry": industry,
        "search": search,
        "sort_by": sort_by,
        "sort_dir": sort_dir
    })
