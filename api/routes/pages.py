"""HTML page routes"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from ..services import price_service, rs_service

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent.parent / "templates"))


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Redirect based on data state"""
    # Exclude SPY (benchmark added by default) when checking for user tickers
    tickers = price_service.get_all_tickers()
    user_tickers = [t for t in tickers if t['symbol'] != 'SPY']
    if len(user_tickers) == 0:
        return RedirectResponse(url="/admin")
    return RedirectResponse(url="/dashboard")


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Sectors view (default dashboard)"""
    sectors = rs_service.get_sector_rankings()
    sparklines = rs_service.get_sector_sparklines()
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "view": "sectors",
        "sectors": sectors,
        "sparklines": sparklines
    })


@router.get("/sector/{sector_name}", response_class=HTMLResponse)
async def sector_detail(request: Request, sector_name: str):
    """Industries in a sector"""
    industries = rs_service.get_industry_rankings(sector_name)
    sparklines = rs_service.get_industry_sparklines(sector_name)
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "view": "industries",
        "current_sector": sector_name,
        "industries": industries,
        "sparklines": sparklines
    })


@router.get("/industry/{industry_name}", response_class=HTMLResponse)
async def industry_detail(request: Request, industry_name: str):
    """Stocks in an industry"""
    all_stocks_rs = rs_service.get_stock_rankings(None)
    sparklines = rs_service.get_stock_sparklines(None)
    sector = price_service.get_sector_for_industry(industry_name)

    # Get all tickers and filter by industry
    all_tickers = price_service.get_all_tickers()
    industry_symbols = {t['symbol'] for t in all_tickers if t['industry'] == industry_name}

    # Filter stocks to only those in this industry
    stocks_rs = [s for s in all_stocks_rs if s['symbol'] in industry_symbols]

    # Add ticker names and keep percentile as number for template comparisons
    ticker_map = {t['symbol']: t['name'] for t in all_tickers}
    stocks = [{
        **s,
        'name': ticker_map.get(s['symbol'], ''),
        'rs_percentile': int(s['percentile']) if s.get('percentile') else 0
    } for s in stocks_rs]

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "view": "stocks",
        "current_sector": sector,
        "current_industry": industry_name,
        "stocks": stocks,
        "sparklines": sparklines
    })


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin panel"""
    from ..services import task_service
    from db import get_connection

    # Get basic ticker info
    base_tickers = price_service.get_all_tickers()
    # Exclude SPY (benchmark) when checking if user has added tickers
    user_tickers = [t for t in base_tickers if t['symbol'] != 'SPY']
    show_upload_modal = len(user_tickers) == 0

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
    tickers = []
    for ticker in base_tickers:
        symbol = ticker['symbol']
        price_count = price_counts.get(symbol, 0)
        rs_info = rs_data.get(symbol)

        tickers.append({
            **ticker,
            'price_status': 'complete' if price_count > 0 else 'pending',
            'price_count': price_count,
            'rs_status': 'complete' if rs_info else 'pending',
            'rs_score': rs_info['score'] if rs_info else None,
            'rs_percentile': int(rs_info['percentile']) if rs_info and rs_info['percentile'] else 0,
            'price_chart': [],
            'rs_chart': [],
            'history_days': price_count,  # Number of price records available
            'rs_updated_at': None
        })

    stats = price_service.get_ticker_stats()
    pipeline = task_service.get_pipeline_status()

    # Get RS settings
    import sys
    from pathlib import Path
    calc_engine_dir = Path(__file__).parent.parent.parent / "calc-engine"
    sys.path.insert(0, str(calc_engine_dir))
    from settings import get_settings, DEFAULT_SETTINGS
    rs_settings = {**DEFAULT_SETTINGS, **get_settings()}

    return templates.TemplateResponse("admin.html", {
        "request": request,
        "tickers": tickers,
        "stats": stats,
        "pipeline": pipeline,
        "settings": rs_settings,
        "total_pages": 1,
        "page": 1,
        "per_page": len(tickers),
        "total_items": len(tickers),
        "show_upload_modal": show_upload_modal
    })
