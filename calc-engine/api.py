"""Public API for calc-engine RS calculations"""
from typing import List, Dict, Any, Optional

from db import (
    get_task_status,
    get_rs_scores as _get_rs_scores,
    get_rs_scores_by_date as _get_rs_scores_by_date,
    get_all_dates
)
from settings import (
    get_settings as _get_settings,
    update_settings as _update_settings,
    get_benchmark
)
from rs_calculator import (
    calculate_stock_rs as _calc_stock,
    calculate_sector_rs as _calc_sector,
    calculate_industry_rs as _calc_industry
)
from tasks import submit_task


def calculate_stock_rs(dates: List[str]) -> str:
    """Queue task to calculate stock RS scores.

    Args:
        dates: List of dates to calculate RS for (YYYY-MM-DD format)

    Returns:
        task_id: Use get_task_status(task_id) to check progress
    """
    return _calc_stock(dates)


def calculate_sector_rs(dates: List[str]) -> str:
    """Queue task to calculate sector RS scores.

    Args:
        dates: List of dates to calculate RS for (YYYY-MM-DD format)

    Returns:
        task_id: Use get_task_status(task_id) to check progress
    """
    return _calc_sector(dates)


def calculate_industry_rs(dates: List[str]) -> str:
    """Queue task to calculate industry RS scores.

    Args:
        dates: List of dates to calculate RS for (YYYY-MM-DD format)

    Returns:
        task_id: Use get_task_status(task_id) to check progress
    """
    return _calc_industry(dates)


def _do_calculate_all_rs(task_id: str, dates: List[str]) -> dict:
    """Internal function to calculate all RS types sequentially."""
    from db import create_task_status, update_task_status

    try:
        create_task_status(task_id, 'calc_all_rs', f'{len(dates)} dates')
        update_task_status(task_id, 'running', progress='Calculating stock RS')

        # Calculate stocks
        from rs_calculator import _do_calculate_stock_rs, _do_calculate_sector_rs, _do_calculate_industry_rs
        import uuid

        stock_result = _do_calculate_stock_rs(str(uuid.uuid4()), dates)
        update_task_status(task_id, 'running', progress=f'Stock RS done: {stock_result.get("count", 0)} scores')

        sector_result = _do_calculate_sector_rs(str(uuid.uuid4()), dates)
        update_task_status(task_id, 'running', progress=f'Sector RS done: {sector_result.get("count", 0)} scores')

        industry_result = _do_calculate_industry_rs(str(uuid.uuid4()), dates)

        total = (stock_result.get('count', 0) +
                 sector_result.get('count', 0) +
                 industry_result.get('count', 0))

        update_task_status(task_id, 'completed', progress=f'Total: {total} RS scores calculated')
        return {
            'task_id': task_id,
            'stock_count': stock_result.get('count', 0),
            'sector_count': sector_result.get('count', 0),
            'industry_count': industry_result.get('count', 0),
            'total': total
        }

    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        raise


def calculate_all_rs(dates: List[str]) -> str:
    """Queue task to calculate RS scores for stocks, sectors, and industries.

    Args:
        dates: List of dates to calculate RS for (YYYY-MM-DD format)

    Returns:
        task_id: Use get_task_status(task_id) to check progress
    """
    return submit_task(_do_calculate_all_rs, dates)


def get_rs_scores(entity_type: str, entity_name: str = None,
                  start_date: str = None, end_date: str = None) -> List[dict]:
    """Get RS scores with optional filters.

    Args:
        entity_type: 'stock', 'sector', or 'industry'
        entity_name: Optional symbol/sector/industry name
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)

    Returns:
        List of RS score records
    """
    return _get_rs_scores(entity_type, entity_name, start_date, end_date)


def get_rs_scores_by_date(date: str, entity_type: str = None) -> List[dict]:
    """Get all RS scores for a specific date.

    Args:
        date: Date in YYYY-MM-DD format
        entity_type: Optional filter by 'stock', 'sector', or 'industry'

    Returns:
        List of RS score records sorted by rs_score descending
    """
    return _get_rs_scores_by_date(date, entity_type)


def get_settings() -> Dict[str, Any]:
    """Get current RS calculation settings."""
    return _get_settings()


def update_settings(updates: Dict[str, Any]) -> None:
    """Update RS calculation settings.

    Args:
        updates: Dictionary of settings to update. Valid keys:
            - benchmark: Benchmark symbol (default: 'SPY')
            - q1_weight: Q1 weight (default: 0.4)
            - q2_weight: Q2 weight (default: 0.2)
            - q3_weight: Q3 weight (default: 0.2)
            - q4_weight: Q4 weight (default: 0.2)
            - lookback_days: Lookback period (default: 252)
            - min_data_points: Minimum data points (default: 120)
    """
    _update_settings(updates)


def get_available_dates() -> List[str]:
    """Get all dates available for RS calculation (from price data)."""
    return get_all_dates()


def get_status(task_id: str) -> Optional[dict]:
    """Get status of a calculation task.

    Args:
        task_id: Task ID returned by calculate_* functions

    Returns:
        Task status dict with keys: task_id, task_type, target, status, progress, error
    """
    return get_task_status(task_id)
