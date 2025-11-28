"""Centralized database management for RS metrics app"""
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List

from .models import (
    ALL_TABLES,
    ALL_INDEXES,
    DEFAULT_SETTINGS,
)

# Database path - single database file for everything in /db/data
BASE_DIR = Path(__file__).parent.resolve()
DATA_DIR = BASE_DIR / 'data'
DB_PATH = DATA_DIR / 'rs_metrics.db'

# Ensure the data directory exists
DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_connection():
    """Get connection to centralized database with WAL mode"""
    conn = sqlite3.connect(str(DB_PATH.resolve()))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def get_prices_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def get_rs_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def get_metrics_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def init_db():
    """Initialize centralized database with all tables and indexes"""
    print(f"[DB] Initializing database at: {DB_PATH}")
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Create all tables
        for table_sql in ALL_TABLES:
            cursor.execute(table_sql)

        # Create all indexes
        for index_sql in ALL_INDEXES:
            cursor.execute(index_sql)

        # Insert default settings if not exists
        for key, value in DEFAULT_SETTINGS:
            cursor.execute(
                '''INSERT OR IGNORE INTO settings (key, value, updated_at)
                   VALUES (?, ?, ?)''',
                (key, value, datetime.now().isoformat())
            )

        # Insert benchmark ticker (SPY) if not exists
        cursor.execute(
            '''INSERT OR IGNORE INTO tickers (symbol, name, sector, industry, updated_at)
               VALUES (?, ?, ?, ?, ?)''',
            ('SPY', 'SPDR S&P 500 ETF Trust', 'Index', 'Index', datetime.now().isoformat())
        )

        conn.commit()
        print(f"[DB] Database initialized successfully at {DB_PATH}")
    except Exception as e:
        print(f"[DB] Error initializing database: {e}")
        raise
    finally:
        conn.close()


# =============================================================================
# Task Status Functions (centralized)
# =============================================================================

def create_task_status(task_id: str, task_type: str, target: str, symbol: str = None) -> None:
    """Create a new task status entry"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO task_status
            (task_id, task_type, target, symbol, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'running', ?, ?)
        ''', (task_id, task_type, target, symbol, datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def update_task_status(task_id: str, status: str, progress: str = None, error: str = None) -> None:
    """Update task status"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE task_status
            SET status = ?, progress = ?, error = ?, updated_at = ?
            WHERE task_id = ?
        ''', (status, progress, error, datetime.now().isoformat(), task_id))
        conn.commit()
    finally:
        conn.close()


def get_task_status(task_id: str) -> Optional[dict]:
    """Get task status by ID"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_status WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_all_task_statuses(task_type: str = None, status: str = None) -> List[dict]:
    """Get all task statuses with optional filtering"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = 'SELECT * FROM task_status WHERE 1=1'
        params = []

        if task_type:
            query += ' AND task_type = ?'
            params.append(task_type)
        if status:
            query += ' AND status = ?'
            params.append(status)

        query += ' ORDER BY updated_at DESC'
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def clear_task_statuses() -> None:
    """Clear all task statuses (call before starting new pipeline)"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM task_status")
        conn.commit()
    finally:
        conn.close()


def cleanup_on_startup() -> None:
    """Clear stale tasks and batches on app startup"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        # Clear all task statuses (queued, running, completed)
        cursor.execute("DELETE FROM task_status")
        # Clear incomplete batch tasks (running/pending batches from previous session)
        cursor.execute("DELETE FROM batch_tasks WHERE status NOT IN ('completed', 'error')")
        conn.commit()
        print("  ✓ Cleared stale tasks and batches")
    finally:
        conn.close()


def cleanup_old_tasks(days: int = 7) -> int:
    """Clean up old task records to prevent database bloat.
    
    Args:
        days: Keep task records for this many days (default: 7)
    
    Returns:
        Number of records deleted
    """
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM task_status 
            WHERE status IN ('completed', 'failed')
            AND datetime(updated_at) < datetime('now', '-' || ? || ' days')
        """, (days,))
        deleted = cursor.rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()


def get_pipeline_status() -> dict:
    """Get entity-based pipeline status with data counts"""
    conn = get_connection()
    try:
        cursor = conn.cursor()

        # Check for active batch and its current stage
        cursor.execute('''
            SELECT stage, status FROM batch_tasks
            WHERE status NOT IN ('completed', 'error')
            ORDER BY started_at DESC LIMIT 1
        ''')
        active_batch = cursor.fetchone()
        active_stage = active_batch[0] if active_batch else 0

        # Get task stats grouped by type (running + completed counts)
        cursor.execute('''
            SELECT task_type,
                   SUM(CASE WHEN status = 'running' THEN 1 ELSE 0 END) as running,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                   COUNT(*) as total
            FROM task_status
            GROUP BY task_type
        ''')
        task_stats = {row[0]: {'running': row[1] or 0, 'completed': row[2] or 0, 'total': row[3] or 0} for row in cursor.fetchall()}

        # Entity counts
        cursor.execute("SELECT COUNT(*) FROM tickers WHERE symbol != 'SPY'")
        stock_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT sector) FROM tickers WHERE sector IS NOT NULL AND sector != 'Index'")
        sector_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT industry) FROM tickers WHERE industry IS NOT NULL AND industry != 'Index'")
        industry_count = cursor.fetchone()[0]

        # Historical data days
        cursor.execute("SELECT COUNT(DISTINCT date) FROM prices")
        price_days = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT date) FROM sector_returns")
        sector_return_days = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT date) FROM industry_returns")
        industry_return_days = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT date) FROM rs_scores WHERE entity_type = 'stock'")
        stock_rs_days = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT date) FROM rs_scores WHERE entity_type = 'sector'")
        sector_rs_days = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(DISTINCT date) FROM rs_scores WHERE entity_type = 'industry'")
        industry_rs_days = cursor.fetchone()[0]

        # Determine status based on active batch stage
        # Stage 1 = prices, Stage 2 = returns, Stage 3 = RS scores
        # If batch is in stage 1, stages 2 & 3 should show "pending" (waiting)

        # Task progress counts
        fetch_stats = task_stats.get('fetch_ticker', {'running': 0, 'completed': 0, 'total': 0})
        sector_stats = task_stats.get('calc_sector', {'running': 0, 'completed': 0, 'total': 0})
        industry_stats = task_stats.get('calc_industry', {'running': 0, 'completed': 0, 'total': 0})
        stock_rs_stats = task_stats.get('calc_stock_rs', {'running': 0, 'completed': 0, 'total': 0})
        sector_rs_stats = task_stats.get('calc_sector_rs', {'running': 0, 'completed': 0, 'total': 0})
        industry_rs_stats = task_stats.get('calc_industry_rs', {'running': 0, 'completed': 0, 'total': 0})

        def get_item_status(item_stage, has_data, running_count=0):
            """Get status for an item based on its stage, data, and actual running tasks"""
            # If there are actually running tasks for this item, it's running
            if running_count > 0:
                return 'running'
            if active_stage == 0:
                # No active batch - show complete if data exists
                return 'complete' if has_data else 'pending'
            elif active_stage < item_stage:
                # This stage hasn't started yet - show pending (waiting)
                return 'pending'
            elif active_stage == item_stage:
                # Stage is active but no running tasks - check if data exists
                return 'complete' if has_data else 'running'
            else:
                # This stage completed in current batch - complete
                return 'complete'

        return {
            'stocks': {
                'total': stock_count,
                'prices': {
                    'status': get_item_status(1, price_days > 0, fetch_stats['running']),
                    'days': price_days,
                    'completed': fetch_stats['completed'],
                    'task_total': stock_count
                },
                'rs_score': {
                    'status': get_item_status(3, stock_rs_days > 0, stock_rs_stats['running']),
                    'days': stock_rs_days,
                    'completed': stock_rs_stats['completed'],
                    'task_total': stock_rs_stats['total'] or 1
                }
            },
            'sectors': {
                'total': sector_count,
                'returns': {
                    'status': get_item_status(2, sector_return_days > 0, sector_stats['running']),
                    'days': sector_return_days,
                    'completed': sector_stats['completed'],
                    'task_total': sector_stats['total'] or sector_count
                },
                'rs_score': {
                    'status': get_item_status(3, sector_rs_days > 0, sector_rs_stats['running']),
                    'days': sector_rs_days,
                    'completed': sector_rs_stats['completed'],
                    'task_total': sector_rs_stats['total'] or 1
                }
            },
            'industries': {
                'total': industry_count,
                'returns': {
                    'status': get_item_status(2, industry_return_days > 0, industry_stats['running']),
                    'days': industry_return_days,
                    'completed': industry_stats['completed'],
                    'task_total': industry_stats['total'] or industry_count
                },
                'rs_score': {
                    'status': get_item_status(3, industry_rs_days > 0, industry_rs_stats['running']),
                    'days': industry_rs_days,
                    'completed': industry_rs_stats['completed'],
                    'task_total': industry_rs_stats['total'] or 1
                }
            }
        }
    finally:
        conn.close()


# =============================================================================
# Ticker Functions
# =============================================================================

def get_ticker(symbol: str) -> Optional[dict]:
    """Get ticker info"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT symbol, name, sector, industry FROM tickers WHERE symbol = ?',
            (symbol.upper(),)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def save_ticker(symbol: str, name: str, sector: str, industry: str):
    """Save ticker info"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO tickers (symbol, name, sector, industry, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (symbol.upper(), name, sector, industry, datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def get_all_tickers() -> List[dict]:
    """Get all tickers"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM tickers ORDER BY symbol')
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_symbols_by_sector(sector: str) -> List[str]:
    """Get all symbols in a sector"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT symbol FROM tickers WHERE sector = ?', (sector,))
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_symbols_by_industry(industry: str) -> List[str]:
    """Get all symbols in an industry"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT symbol FROM tickers WHERE industry = ?', (industry,))
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_sectors() -> List[str]:
    """Get list of unique sectors"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL')
        return [row['sector'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_industries() -> List[str]:
    """Get list of unique industries"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT industry FROM tickers WHERE industry IS NOT NULL')
        return [row['industry'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_dates() -> List[str]:
    """Get all unique dates from prices table"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT date FROM prices ORDER BY date')
        return [row['date'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_symbols() -> List[str]:
    """Get all stock symbols"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT symbol FROM tickers WHERE symbol != "SPY"')
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


# =============================================================================
# Price Functions
# =============================================================================

def save_prices(symbol: str, prices: List[dict]):
    """Bulk insert/update prices"""
    if not prices:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO prices
            (symbol, date, open, high, low, close, adjclose, volume, daily_return)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            (
                symbol.upper(),
                p['date'],
                p.get('open'),
                p.get('high'),
                p.get('low'),
                p.get('close'),
                p.get('adjclose'),
                p.get('volume'),
                p.get('daily_return')
            )
            for p in prices
        ])
        conn.commit()
    finally:
        conn.close()


def get_prices(symbol: str, start_date: str = None, end_date: str = None) -> List[dict]:
    """Retrieve prices for date range"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = 'SELECT * FROM prices WHERE symbol = ?'
        params = [symbol.upper()]

        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)
        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)

        query += ' ORDER BY date ASC'
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_price_count(symbol: str) -> int:
    """Get count of prices for a symbol"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT COUNT(*) as count FROM prices WHERE symbol = ?',
            (symbol.upper(),)
        )
        row = cursor.fetchone()
        return row['count'] if row else 0
    finally:
        conn.close()


def get_last_price_date(symbol: str) -> Optional[str]:
    """Get most recent price date for symbol"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            'SELECT MAX(date) as last_date FROM prices WHERE symbol = ?',
            (symbol.upper(),)
        )
        row = cursor.fetchone()
        return row['last_date'] if row and row['last_date'] else None
    finally:
        conn.close()


# =============================================================================
# RS Scores Functions
# =============================================================================

def save_rs_scores(scores: List[dict]) -> None:
    """Bulk save RS scores"""
    if not scores:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO rs_scores
            (entity_type, entity_name, date, rs_score, percentile, weighted_return)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', [
            (s['entity_type'], s['entity_name'], s['date'],
             s['rs_score'], s['percentile'], s.get('weighted_return'))
            for s in scores
        ])
        conn.commit()
    finally:
        conn.close()


def get_rs_scores(entity_type: str, entity_name: str = None,
                  start_date: str = None, end_date: str = None) -> List[dict]:
    """Get RS scores with optional filters"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        query = 'SELECT * FROM rs_scores WHERE entity_type = ?'
        params = [entity_type]

        if entity_name:
            query += ' AND entity_name = ?'
            params.append(entity_name)
        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)
        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)

        query += ' ORDER BY date ASC'
        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_rs_scores_by_date(date: str, entity_type: str = None) -> List[dict]:
    """Get all RS scores for a specific date"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        if entity_type:
            cursor.execute(
                'SELECT * FROM rs_scores WHERE date = ? AND entity_type = ? ORDER BY rs_score DESC',
                (date, entity_type)
            )
        else:
            cursor.execute(
                'SELECT * FROM rs_scores WHERE date = ? ORDER BY entity_type, rs_score DESC',
                (date,)
            )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


# =============================================================================
# Settings Functions
# =============================================================================

def get_settings() -> dict:
    """Get all settings as dictionary"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM settings')
        return {row['key']: row['value'] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_setting(key: str) -> Optional[str]:
    """Get a single setting value"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row['value'] if row else None
    finally:
        conn.close()


def update_setting(key: str, value: str) -> None:
    """Update a single setting"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', (key, str(value), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Metrics Functions
# =============================================================================

def save_sector_returns(sector: str, returns: List[dict]) -> None:
    """Save sector return time series"""
    if not returns:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO sector_returns (sector, date, avg_return, stock_count)
            VALUES (?, ?, ?, ?)
        ''', [(sector, r['date'], r['avg_return'], r['stock_count']) for r in returns])
        conn.commit()
    finally:
        conn.close()


def save_industry_returns(industry: str, returns: List[dict]) -> None:
    """Save industry return time series"""
    if not returns:
        return

    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO industry_returns (industry, date, avg_return, stock_count)
            VALUES (?, ?, ?, ?)
        ''', [(industry, r['date'], r['avg_return'], r['stock_count']) for r in returns])
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Pipeline Batch Functions
# =============================================================================

def save_batch_state(batch_id: str, stage: int, status: str,
                     price_tasks: List[str] = None,
                     return_tasks: List[str] = None,
                     rs_task: str = None) -> None:
    """Save pipeline batch state to database"""
    import json
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO batch_tasks
            (batch_id, stage, status, price_tasks, return_tasks, rs_task, started_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            batch_id,
            stage,
            status,
            json.dumps(price_tasks or []),
            json.dumps(return_tasks or []),
            rs_task,
            datetime.now().isoformat()
        ))
        conn.commit()
    finally:
        conn.close()


def update_batch_stage(batch_id: str, stage: int, status: str,
                       return_tasks: List[str] = None,
                       rs_task: str = None) -> None:
    """Update batch stage and associated tasks"""
    import json
    conn = get_connection()
    try:
        cursor = conn.cursor()
        if return_tasks is not None:
            cursor.execute('''
                UPDATE batch_tasks
                SET stage = ?, status = ?, return_tasks = ?
                WHERE batch_id = ?
            ''', (stage, status, json.dumps(return_tasks), batch_id))
        elif rs_task is not None:
            cursor.execute('''
                UPDATE batch_tasks
                SET stage = ?, status = ?, rs_task = ?
                WHERE batch_id = ?
            ''', (stage, status, rs_task, batch_id))
        else:
            cursor.execute('''
                UPDATE batch_tasks
                SET stage = ?, status = ?
                WHERE batch_id = ?
            ''', (stage, status, batch_id))
        conn.commit()
    finally:
        conn.close()


def complete_batch(batch_id: str) -> None:
    """Mark batch as completed"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE batch_tasks
            SET status = 'completed', completed_at = ?
            WHERE batch_id = ?
        ''', (datetime.now().isoformat(), batch_id))
        conn.commit()
    finally:
        conn.close()


def get_active_batches() -> List[dict]:
    """Get all active (non-completed) batches"""
    import json
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM batch_tasks
            WHERE status != 'completed' AND status != 'error'
            ORDER BY started_at DESC
        ''')
        batches = []
        for row in cursor.fetchall():
            batch = dict(row)
            batch['price_tasks'] = json.loads(batch['price_tasks'] or '[]')
            batch['return_tasks'] = json.loads(batch['return_tasks'] or '[]')
            batches.append(batch)
        return batches
    finally:
        conn.close()


def get_batch_state(batch_id: str) -> Optional[dict]:
    """Get batch state by ID"""
    import json
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM batch_tasks WHERE batch_id = ?', (batch_id,))
        row = cursor.fetchone()
        if row:
            batch = dict(row)
            batch['price_tasks'] = json.loads(batch['price_tasks'] or '[]')
            batch['return_tasks'] = json.loads(batch['return_tasks'] or '[]')
            return batch
        return None
    finally:
        conn.close()


def check_tasks_completed(task_ids: List[str]) -> bool:
    """Check if all tasks in list are completed"""
    if not task_ids:
        return True
    conn = get_connection()
    try:
        cursor = conn.cursor()
        placeholders = ','.join('?' * len(task_ids))
        cursor.execute(f'''
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM task_status
            WHERE task_id IN ({placeholders})
        ''', task_ids)
        row = cursor.fetchone()
        return row['total'] > 0 and row['total'] == row['completed']
    finally:
        conn.close()
