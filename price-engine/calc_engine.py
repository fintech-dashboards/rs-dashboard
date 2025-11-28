"""Calculation engine for sector and industry return aggregation"""
from collections import defaultdict
from typing import List
from datetime import datetime

from tasks import submit_task
from db import (
    get_symbols_by_sector, get_symbols_by_industry,
    get_prices, save_sector_returns, save_industry_returns,
    create_task_status, update_task_status
)


def _log(msg: str):
    """Log with timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [CALC] {msg}")


def _calculate_equal_weighted_returns(symbols: List[str]) -> List[dict]:
    """Calculate equal-weighted average daily returns across symbols"""
    if not symbols:
        return []

    returns_by_date = defaultdict(list)

    for symbol in symbols:
        prices = get_prices(symbol)
        for price in prices:
            if price['daily_return'] is not None:
                returns_by_date[price['date']].append(price['daily_return'])

    results = []
    for date in sorted(returns_by_date.keys()):
        returns = returns_by_date[date]
        if returns:
            avg_return = round(sum(returns) / len(returns), 6)
            results.append({
                'date': date,
                'avg_return': avg_return,
                'stock_count': len(returns)
            })

    return results


def _do_calculate_sector_returns(task_id: str, sector: str) -> dict:
    """Internal function that runs in background thread"""
    try:
        _log(f"SECTOR [{sector}] Starting...")
        create_task_status(task_id, 'calc_sector', sector)

        symbols = get_symbols_by_sector(sector)
        _log(f"SECTOR [{sector}] Found {len(symbols)} symbols: {symbols[:5]}{'...' if len(symbols) > 5 else ''}")
        update_task_status(task_id, 'running', progress=f'Found {len(symbols)} symbols')

        if not symbols:
            _log(f"SECTOR [{sector}] No symbols found, skipping")
            update_task_status(task_id, 'completed', progress='No symbols found')
            return {'sector': sector, 'task_id': task_id, 'returns_count': 0, 'symbols_count': 0}

        update_task_status(task_id, 'running', progress='Calculating returns')
        returns = _calculate_equal_weighted_returns(symbols)

        if returns:
            save_sector_returns(sector, returns)
            _log(f"SECTOR [{sector}] Saved {len(returns)} daily returns")

        update_task_status(task_id, 'completed', progress=f'Calculated {len(returns)} dates')
        _log(f"SECTOR [{sector}] Done ✓")
        return {'sector': sector, 'task_id': task_id, 'returns_count': len(returns), 'symbols_count': len(symbols)}

    except Exception as e:
        _log(f"SECTOR [{sector}] ERROR: {e}")
        update_task_status(task_id, 'failed', error=str(e))
        raise


def _do_calculate_industry_returns(task_id: str, industry: str) -> dict:
    """Internal function that runs in background thread"""
    try:
        _log(f"INDUSTRY [{industry}] Starting...")
        create_task_status(task_id, 'calc_industry', industry)

        symbols = get_symbols_by_industry(industry)
        _log(f"INDUSTRY [{industry}] Found {len(symbols)} symbols")
        update_task_status(task_id, 'running', progress=f'Found {len(symbols)} symbols')

        if not symbols:
            _log(f"INDUSTRY [{industry}] No symbols found, skipping")
            update_task_status(task_id, 'completed', progress='No symbols found')
            return {'industry': industry, 'task_id': task_id, 'returns_count': 0, 'symbols_count': 0}

        update_task_status(task_id, 'running', progress='Calculating returns')
        returns = _calculate_equal_weighted_returns(symbols)

        if returns:
            save_industry_returns(industry, returns)
            _log(f"INDUSTRY [{industry}] Saved {len(returns)} daily returns")

        update_task_status(task_id, 'completed', progress=f'Calculated {len(returns)} dates')
        _log(f"INDUSTRY [{industry}] Done ✓")
        return {'industry': industry, 'task_id': task_id, 'returns_count': len(returns), 'symbols_count': len(symbols)}

    except Exception as e:
        _log(f"INDUSTRY [{industry}] ERROR: {e}")
        update_task_status(task_id, 'failed', error=str(e))
        raise


def calculate_sector_returns(sector: str) -> str:
    """Queue task to calculate sector returns - returns immediately with task_id"""
    return submit_task(_do_calculate_sector_returns, sector)


def calculate_industry_returns(industry: str) -> str:
    """Queue task to calculate industry returns - returns immediately with task_id"""
    return submit_task(_do_calculate_industry_returns, industry)
