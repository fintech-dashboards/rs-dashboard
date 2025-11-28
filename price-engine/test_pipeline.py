"""Pipeline test: Queue tasks, track by ID, poll status"""
import os
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path

# Remove existing DB to test fresh
BASE_DIR = Path(__file__).parent
db_path = BASE_DIR / 'prices.db'
if db_path.exists():
    os.remove(db_path)
    print(f"Removed existing {db_path}")

from yfinance_provider import fetch_ticker_data
from calc_engine import calculate_sector_returns, calculate_industry_returns
from db import get_task_status, get_all_sectors, get_all_industries, get_prices_connection, get_metrics_connection


def load_tickers_from_csv(csv_path: str):
    """Load ticker symbols from CSV file"""
    tickers = []
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get('Ticker', '').strip()
            if ticker:
                tickers.append(ticker)
    return tickers


def wait_for_tasks(task_ids: list[str], poll_interval: float = 0.5) -> tuple[int, int]:
    """Poll until all tasks complete. Returns (completed, failed) counts."""
    pending = set(task_ids)
    completed = 0
    failed = 0

    while pending:
        time.sleep(poll_interval)
        for task_id in list(pending):
            status = get_task_status(task_id)
            if status and status['status'] in ('completed', 'failed'):
                pending.discard(task_id)
                if status['status'] == 'completed':
                    completed += 1
                    print(f"  [{completed + failed}/{len(task_ids)}] {status['target']}: {status['progress']}")
                else:
                    failed += 1
                    print(f"  [{completed + failed}/{len(task_ids)}] {status['target']}: FAILED - {status['error']}")

    return completed, failed


def run_pipeline():
    print("=" * 70)
    print("PRICE ENGINE PIPELINE")
    print("=" * 70)

    csv_path = BASE_DIR / 'test_data' / 'tickerlist_test.csv'
    tickers = load_tickers_from_csv(csv_path)
    print(f"\nLoaded {len(tickers)} tickers")

    end = datetime.now().strftime('%Y-%m-%d')
    start = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    # =========================================================================
    # PHASE 1: Queue ALL ticker tasks (returns immediately)
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 1: Queue ticker fetch tasks")
    print("=" * 70)

    task_ids = []
    for ticker in tickers:
        task_id = fetch_ticker_data(ticker, start_date=start, end_date=end)
        task_ids.append(task_id)
        print(f"  Queued {ticker}: {task_id}")

    print(f"\nAll {len(task_ids)} tasks queued. Polling for completion...")
    completed, failed = wait_for_tasks(task_ids)
    print(f"\nPhase 1 done: {completed} completed, {failed} failed")

    # =========================================================================
    # PHASE 2: Queue sector calculations (only after ALL prices done)
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 2: Queue sector return calculations")
    print("=" * 70)

    sectors = [s for s in get_all_sectors() if s and s != 'Unknown']
    print(f"Found {len(sectors)} sectors: {sectors}")

    task_ids = []
    for sector in sectors:
        task_id = calculate_sector_returns(sector)
        task_ids.append(task_id)
        print(f"  Queued {sector}: {task_id}")

    print(f"\nPolling for completion...")
    wait_for_tasks(task_ids)

    # =========================================================================
    # PHASE 3: Queue industry calculations
    # =========================================================================
    print("\n" + "=" * 70)
    print("PHASE 3: Queue industry return calculations")
    print("=" * 70)

    industries = [i for i in get_all_industries() if i and i != 'Unknown']
    print(f"Found {len(industries)} industries")

    task_ids = []
    for industry in industries:
        task_id = calculate_industry_returns(industry)
        task_ids.append(task_id)
        print(f"  Queued {industry}: {task_id}")

    print(f"\nPolling for completion...")
    wait_for_tasks(task_ids)

    # =========================================================================
    # FINAL STATUS
    # =========================================================================
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE")
    print("=" * 70)

    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT task_type, status, COUNT(*) as cnt FROM task_status GROUP BY task_type, status')
    for row in cursor.fetchall():
        print(f"  {row['task_type']:20} | {row['status']:10} | {row['cnt']}")

    cursor.execute('SELECT COUNT(*) FROM tickers')
    print(f"\nTickers: {cursor.fetchone()[0]}")
    cursor.execute('SELECT COUNT(*) FROM prices')
    print(f"Prices: {cursor.fetchone()[0]}")
    conn.close()

    # Metrics from separate database
    metrics_conn = get_metrics_connection()
    metrics_cursor = metrics_conn.cursor()
    metrics_cursor.execute('SELECT COUNT(*) FROM sector_returns')
    print(f"Sector returns: {metrics_cursor.fetchone()[0]}")
    metrics_cursor.execute('SELECT COUNT(*) FROM industry_returns')
    print(f"Industry returns: {metrics_cursor.fetchone()[0]}")
    metrics_conn.close()


if __name__ == '__main__':
    run_pipeline()
