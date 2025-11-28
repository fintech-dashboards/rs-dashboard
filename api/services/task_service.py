"""Service for triggering engine tasks and pipeline orchestration"""
import sys
import uuid
from pathlib import Path
from typing import List, Dict
from datetime import datetime

from db import (
    get_pipeline_status as db_get_pipeline_status,
    get_settings as db_get_settings,
    get_connection,
    get_all_sectors,
    get_all_industries,
    get_all_symbols,
    save_batch_state,
    update_batch_stage,
    complete_batch,
    get_active_batches,
    check_tasks_completed,
    clear_task_statuses,
)

# Add engine paths
PRICE_ENGINE_DIR = Path(__file__).parent.parent.parent / "price-engine"
CALC_ENGINE_DIR = Path(__file__).parent.parent.parent / "calc-engine"
sys.path.insert(0, str(PRICE_ENGINE_DIR))
sys.path.insert(0, str(CALC_ENGINE_DIR))


def start_refresh_all_pipeline() -> str:
    """Start full refresh pipeline: prices → returns → RS"""
    # Clear old task statuses for fresh counts
    clear_task_statuses()

    batch_id = str(uuid.uuid4())

    from yfinance_provider import fetch_ticker_data

    # Stage 1: Queue price fetch
    symbols = get_all_symbols()
    settings = db_get_settings()
    start_date = settings.get('start_date', '2024-01-01')
    end_date = datetime.now().strftime('%Y-%m-%d')

    price_task_ids = []
    for symbol in symbols:
        task_id = fetch_ticker_data(symbol, start_date=start_date, end_date=end_date)
        price_task_ids.append(task_id)

    # Save batch state - stage 1 running
    save_batch_state(batch_id, stage=1, status='running', price_tasks=price_task_ids)

    return batch_id


def _start_stage2_returns(batch_id: str) -> List[str]:
    """Start stage 2: Calculate sector/industry returns"""
    from calc_engine import calculate_sector_returns, calculate_industry_returns

    sectors = get_all_sectors()
    industries = get_all_industries()

    return_task_ids = []

    for sector in sectors:
        if sector and sector != 'Unknown':
            task_id = calculate_sector_returns(sector)
            return_task_ids.append(task_id)

    for industry in industries:
        if industry and industry != 'Unknown':
            task_id = calculate_industry_returns(industry)
            return_task_ids.append(task_id)

    update_batch_stage(batch_id, stage=2, status='running', return_tasks=return_task_ids)
    return return_task_ids


def _start_stage3_rs(batch_id: str) -> str:
    """Start stage 3: Calculate RS scores"""
    import importlib.util

    # Load calc-engine modules
    api_path = CALC_ENGINE_DIR / "api.py"
    spec = importlib.util.spec_from_file_location("calc_engine_api", api_path)
    calc_api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(calc_api)

    settings_path = CALC_ENGINE_DIR / "settings.py"
    spec = importlib.util.spec_from_file_location("calc_engine_settings", settings_path)
    calc_settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(calc_settings)

    dates = calc_api.get_available_dates()
    backfill = calc_settings.get_backfill_days()
    calc_dates = dates[-backfill:] if len(dates) > backfill else dates

    rs_task_id = calc_api.calculate_all_rs(calc_dates)
    update_batch_stage(batch_id, stage=3, status='running', rs_task=rs_task_id)
    return rs_task_id


def check_and_advance_pipeline() -> None:
    """Check active batches and advance to next stage if current is complete"""
    batches = get_active_batches()

    for batch in batches:
        batch_id = batch['batch_id']
        stage = batch['stage']

        if stage == 1:
            # Check if all price tasks are done
            if check_tasks_completed(batch['price_tasks']):
                print(f"[PIPELINE] Batch {batch_id[:8]}: Stage 1 complete, starting stage 2")
                _start_stage2_returns(batch_id)

        elif stage == 2:
            # Check if all return tasks are done
            if check_tasks_completed(batch['return_tasks']):
                print(f"[PIPELINE] Batch {batch_id[:8]}: Stage 2 complete, starting stage 3")
                _start_stage3_rs(batch_id)

        elif stage == 3:
            # Check if RS task is done
            rs_task = batch['rs_task']
            if rs_task and check_tasks_completed([rs_task]):
                print(f"[PIPELINE] Batch {batch_id[:8]}: Stage 3 complete, pipeline done")
                complete_batch(batch_id)


def get_pipeline_status() -> Dict:
    """Get pipeline status and advance stages if needed"""
    # Check and advance pipeline on every status poll
    check_and_advance_pipeline()
    return db_get_pipeline_status()


def queue_price_fetch(symbols: List[str]) -> List[str]:
    """Queue price fetch tasks for symbols"""
    from yfinance_provider import fetch_ticker_data

    settings = db_get_settings()
    start_date = settings.get('start_date', '2024-01-01')
    end_date = datetime.now().strftime('%Y-%m-%d')

    task_ids = []
    for symbol in symbols:
        task_id = fetch_ticker_data(symbol, start_date=start_date, end_date=end_date)
        task_ids.append(task_id)

    return task_ids


def queue_rs_calculation() -> str:
    """Queue RS calculation for backfill days"""
    import importlib.util

    api_path = CALC_ENGINE_DIR / "api.py"
    spec = importlib.util.spec_from_file_location("calc_engine_api", api_path)
    calc_api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(calc_api)

    settings_path = CALC_ENGINE_DIR / "settings.py"
    spec = importlib.util.spec_from_file_location("calc_engine_settings", settings_path)
    calc_settings = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(calc_settings)

    dates = calc_api.get_available_dates()
    backfill = calc_settings.get_backfill_days()
    calc_dates = dates[-backfill:] if len(dates) > backfill else dates

    return calc_api.calculate_all_rs(calc_dates)


def start_recalculate_pipeline() -> str:
    """Start pipeline to recalculate returns and RS from existing prices"""
    # Clear old task statuses for fresh counts
    clear_task_statuses()

    batch_id = str(uuid.uuid4())

    # Start directly at stage 2 (skip price fetch)
    from calc_engine import calculate_sector_returns, calculate_industry_returns

    sectors = get_all_sectors()
    industries = get_all_industries()

    return_task_ids = []

    for sector in sectors:
        if sector and sector != 'Unknown':
            task_id = calculate_sector_returns(sector)
            return_task_ids.append(task_id)

    for industry in industries:
        if industry and industry != 'Unknown':
            task_id = calculate_industry_returns(industry)
            return_task_ids.append(task_id)

    # Save batch at stage 2
    save_batch_state(batch_id, stage=2, status='running', return_tasks=return_task_ids)

    return batch_id


def clear_rs_scores() -> int:
    """Delete all RS scores"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM rs_scores")
        count = cursor.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def clear_returns() -> int:
    """Delete sector and industry returns"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM sector_returns")
        cursor.execute("DELETE FROM industry_returns")
        count = cursor.rowcount
        conn.commit()
        return count
    finally:
        conn.close()
