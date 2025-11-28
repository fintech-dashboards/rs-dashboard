"""Test pipeline for calc-engine RS calculation"""
import os
import time
from pathlib import Path

# Remove existing DB to test fresh
BASE_DIR = Path(__file__).parent
db_path = BASE_DIR / 'rs_scores.db'
if db_path.exists():
    os.remove(db_path)
    print(f"Removed existing {db_path}")

from api import (
    calculate_stock_rs, calculate_sector_rs, calculate_industry_rs,
    calculate_all_rs, get_rs_scores, get_rs_scores_by_date,
    get_settings, get_available_dates, get_status
)
from db import get_rs_connection


def wait_for_task(task_id: str, poll_interval: float = 0.5) -> dict:
    """Poll until task completes."""
    while True:
        status = get_status(task_id)
        if status and status['status'] in ('completed', 'failed'):
            return status
        time.sleep(poll_interval)


def run_pipeline():
    print("=" * 70)
    print("CALC-ENGINE RS PIPELINE TEST")
    print("=" * 70)

    # Check settings
    print("\n--- Settings ---")
    settings = get_settings()
    for key, value in settings.items():
        print(f"  {key}: {value}")

    # Get available dates
    dates = get_available_dates()
    print(f"\nFound {len(dates)} dates in price data")

    if not dates:
        print("ERROR: No price data found. Run price-engine first.")
        return

    # Use last 30 dates for testing
    test_dates = dates[-30:] if len(dates) > 30 else dates
    print(f"Using {len(test_dates)} dates for RS calculation")
    print(f"  From: {test_dates[0]}")
    print(f"  To:   {test_dates[-1]}")

    # =========================================================================
    # PHASE 1: Calculate Stock RS
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: Calculate Stock RS")
    print("=" * 70)

    task_id = calculate_stock_rs(test_dates)
    print(f"Queued task: {task_id}")

    status = wait_for_task(task_id)
    print(f"Status: {status['status']}")
    print(f"Progress: {status['progress']}")
    if status['error']:
        print(f"Error: {status['error']}")

    # =========================================================================
    # PHASE 2: Calculate Sector RS
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 2: Calculate Sector RS")
    print("=" * 70)

    task_id = calculate_sector_rs(test_dates)
    print(f"Queued task: {task_id}")

    status = wait_for_task(task_id)
    print(f"Status: {status['status']}")
    print(f"Progress: {status['progress']}")
    if status['error']:
        print(f"Error: {status['error']}")

    # =========================================================================
    # PHASE 3: Calculate Industry RS
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 3: Calculate Industry RS")
    print("=" * 70)

    task_id = calculate_industry_rs(test_dates)
    print(f"Queued task: {task_id}")

    status = wait_for_task(task_id)
    print(f"Status: {status['status']}")
    print(f"Progress: {status['progress']}")
    if status['error']:
        print(f"Error: {status['error']}")

    # =========================================================================
    # VERIFY RESULTS
    # =========================================================================
    print("\n" + "=" * 70)
    print("RESULTS VERIFICATION")
    print("=" * 70)

    conn = get_rs_connection()
    cursor = conn.cursor()

    # Count by entity type
    cursor.execute('''
        SELECT entity_type, COUNT(*) as count, COUNT(DISTINCT entity_name) as entities, COUNT(DISTINCT date) as dates
        FROM rs_scores
        GROUP BY entity_type
    ''')
    print("\nRS Scores by Entity Type:")
    for row in cursor.fetchall():
        print(f"  {row['entity_type']:10} | {row['count']:6} scores | {row['entities']:3} entities | {row['dates']:3} dates")

    # Sample stock RS for latest date
    latest_date = test_dates[-1]
    print(f"\nTop 10 Stock RS for {latest_date}:")
    cursor.execute('''
        SELECT entity_name, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'stock' AND date = ?
        ORDER BY rs_score DESC
        LIMIT 10
    ''', (latest_date,))
    for row in cursor.fetchall():
        print(f"  {row['entity_name']:6} | RS: {row['rs_score']:6.2f} | Pctl: {row['percentile']:3}")

    # Sample sector RS for latest date
    print(f"\nSector RS for {latest_date}:")
    cursor.execute('''
        SELECT entity_name, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'sector' AND date = ?
        ORDER BY rs_score DESC
    ''', (latest_date,))
    for row in cursor.fetchall():
        print(f"  {row['entity_name']:20} | RS: {row['rs_score']:6.2f} | Pctl: {row['percentile']:3}")

    # Sample industry RS for latest date
    print(f"\nTop 5 Industry RS for {latest_date}:")
    cursor.execute('''
        SELECT entity_name, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'industry' AND date = ?
        ORDER BY rs_score DESC
        LIMIT 5
    ''', (latest_date,))
    for row in cursor.fetchall():
        print(f"  {row['entity_name']:30} | RS: {row['rs_score']:6.2f} | Pctl: {row['percentile']:3}")

    # Task status summary
    print("\nTask Status Summary:")
    cursor.execute('SELECT task_type, status, COUNT(*) as count FROM task_status GROUP BY task_type, status')
    for row in cursor.fetchall():
        print(f"  {row['task_type']:20} | {row['status']:10} | {row['count']}")

    conn.close()

    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    run_pipeline()
