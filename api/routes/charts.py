"""Chart data API endpoints"""
from fastapi import APIRouter, Query
from datetime import datetime, timedelta

from ..services import price_service, rs_service

router = APIRouter(tags=["charts"])


@router.get("/chart/ticker/{symbol}")
async def get_ticker_chart(symbol: str, days: int = Query(default=180)):
    """Get OHLC + RS data for stock chart"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    ohlc = price_service.get_ohlc_data(symbol, start_date, end_date)
    rs_with_dates = rs_service.get_rs_history_with_dates('stock', symbol, start_date, end_date)

    # Build date-aligned RS scores (null for missing dates)
    rs_by_date = {r['date']: r['rs_score'] for r in rs_with_dates}
    dates = [row['date'] for row in ohlc]
    aligned_rs = [rs_by_date.get(d) for d in dates]

    return {
        "dates": dates,
        "ohlc": [{"o": row['open'], "h": row['high'], "l": row['low'], "c": row['close']} for row in ohlc],
        "rs_scores": aligned_rs
    }


@router.get("/chart/sector/{sector_name}")
async def get_sector_chart(sector_name: str, days: int = Query(default=180)):
    """Get baselined returns for sector vs benchmark (6 months default)"""
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    sector_returns = price_service.get_sector_returns_baselined(sector_name, start_date)
    benchmark_returns = price_service.get_benchmark_returns_baselined(start_date)

    return {
        "dates": sector_returns['dates'],
        "returns": sector_returns['cumulative'],
        "benchmark_returns": benchmark_returns['cumulative'],
        "sector": sector_name,
        "start_date": start_date
    }


@router.get("/chart/industry/{industry_name}")
async def get_industry_chart(industry_name: str, days: int = Query(default=180)):
    """Get baselined returns for industry vs benchmark (6 months default)"""
    start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

    industry_returns = price_service.get_industry_returns_baselined(industry_name, start_date)
    benchmark_returns = price_service.get_benchmark_returns_baselined(start_date)

    return {
        "dates": industry_returns['dates'],
        "returns": industry_returns['cumulative'],
        "benchmark_returns": benchmark_returns['cumulative'],
        "industry": industry_name,
        "start_date": start_date
    }


@router.get("/chart/all-sectors")
async def get_all_sectors_chart(days: int = Query(default=90)):
    """Get strength % for all sectors (compare chart)"""
    sectors_data = rs_service.get_all_sectors_strength(days)
    return {"sectors": sectors_data}


@router.get("/chart/all-industries")
async def get_all_industries_chart(sector: str, days: int = Query(default=90)):
    """Get RS scores for all industries in a sector (compare chart)"""
    industries_data = rs_service.get_all_industries_strength(sector, days)
    return {"industries": industries_data, "sector": sector}
