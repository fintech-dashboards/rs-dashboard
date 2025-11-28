"""SQLite databases for price engine"""
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import sys

# Add parent directory to path to import centralized db
sys.path.insert(0, str(Path(__file__).parent.parent))

# Database path - use centralized database in /db/data folder
BASE_DIR = Path(__file__).parent.parent.resolve() / 'db' / 'data'
BASE_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = BASE_DIR / 'rs_metrics.db'


def get_connection():
    """Get connection to centralized database"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_prices_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def get_metrics_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def init_db():
    """Initialize both databases"""
    # prices.db - tickers, prices, task_status
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tickers (
                symbol TEXT PRIMARY KEY,
                name TEXT,
                sector TEXT,
                industry TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                adjclose REAL,
                volume INTEGER,
                daily_return REAL,
                PRIMARY KEY (symbol, date)
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_prices_symbol_date
            ON prices(symbol, date DESC)
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_status (
                task_id TEXT PRIMARY KEY,
                task_type TEXT,
                target TEXT,
                status TEXT,
                progress TEXT,
                created_at TEXT,
                updated_at TEXT,
                error TEXT
            )
        ''')

        # Add indexes for performance
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_status_type_status
            ON task_status(task_type, status)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_task_status_updated_at
            ON task_status(updated_at DESC)
        ''')

        # Batch orchestration tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_tasks (
                batch_id TEXT PRIMARY KEY,
                stage INTEGER,
                status TEXT,
                price_tasks TEXT,
                return_tasks TEXT,
                rs_task TEXT,
                started_at TEXT,
                completed_at TEXT,
                error TEXT
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT
            )
        ''')

        # Insert default settings
        default_settings = [
            ('start_date', '2024-01-01'),
            ('benchmark', 'SPY'),
            ('lookback_days', '252'),      # 4 quarters for RS calc
            ('backfill_days', '63'),       # Calculate RS for last 3 months
        ]
        for key, value in default_settings:
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', (key, value, datetime.now().isoformat()))

        conn.commit()
    finally:
        conn.close()

    # price_metrics.db - sector_returns, industry_returns
    conn = get_metrics_connection()
    try:
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sector_returns (
                sector TEXT NOT NULL,
                date TEXT NOT NULL,
                avg_return REAL,
                stock_count INTEGER,
                PRIMARY KEY (sector, date)
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS industry_returns (
                industry TEXT NOT NULL,
                date TEXT NOT NULL,
                avg_return REAL,
                stock_count INTEGER,
                PRIMARY KEY (industry, date)
            )
        ''')

        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Ticker Functions (prices.db)
# =============================================================================

def get_ticker(symbol: str) -> Optional[dict]:
    """Get ticker info from cache"""
    conn = get_prices_connection()
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
    """Save ticker info to cache"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO tickers (symbol, name, sector, industry, updated_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (symbol.upper(), name, sector, industry, datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def get_symbols_by_sector(sector: str) -> List[str]:
    """Get all symbols in a sector"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT symbol FROM tickers WHERE sector = ?', (sector,))
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_symbols_by_industry(industry: str) -> List[str]:
    """Get all symbols in an industry"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT symbol FROM tickers WHERE industry = ?', (industry,))
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_sectors() -> List[str]:
    """Get list of unique sectors"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL')
        return [row['sector'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_industries() -> List[str]:
    """Get list of unique industries"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT industry FROM tickers WHERE industry IS NOT NULL')
        return [row['industry'] for row in cursor.fetchall()]
    finally:
        conn.close()


# =============================================================================
# Price Functions (prices.db)
# =============================================================================

def get_last_price_date(symbol: str) -> Optional[str]:
    """Get most recent price date for symbol"""
    conn = get_prices_connection()
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


def save_prices(symbol: str, prices: List[dict]):
    """Bulk insert/update prices"""
    if not prices:
        return

    conn = get_prices_connection()
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
    """Retrieve cached prices for date range"""
    conn = get_prices_connection()
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
    conn = get_prices_connection()
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


# =============================================================================
# Task Status Functions (centralized database)
# =============================================================================

def create_task_status(task_id: str, task_type: str, target: str) -> None:
    """Create a new task status entry in centralized database"""
    try:
        from db import create_task_status as central_create_task_status
        # Extract symbol if this is a fetch_ticker task (target is the symbol)
        symbol = target if task_type == 'fetch_ticker' else None
        central_create_task_status(task_id, task_type, target, symbol)
    except ImportError:
        # Fallback to local database if centralized db not available
        _local_create_task_status(task_id, task_type, target)


def update_task_status(task_id: str, status: str, progress: str = None, error: str = None) -> None:
    """Update task status in centralized database"""
    try:
        from db import update_task_status as central_update_task_status
        central_update_task_status(task_id, status, progress, error)
    except ImportError:
        # Fallback to local database if centralized db not available
        _local_update_task_status(task_id, status, progress, error)


def get_task_status(task_id: str) -> Optional[dict]:
    """Get task status by ID"""
    try:
        from db import get_task_status as central_get_task_status
        return central_get_task_status(task_id)
    except ImportError:
        # Fallback to local database if centralized db not available
        return _local_get_task_status(task_id)


def _local_create_task_status(task_id: str, task_type: str, target: str) -> None:
    """Local task status entry (fallback)"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO task_status
            (task_id, task_type, target, status, created_at, updated_at)
            VALUES (?, ?, ?, 'running', ?, ?)
        ''', (task_id, task_type, target, datetime.now().isoformat(), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def _local_update_task_status(task_id: str, status: str, progress: str = None, error: str = None) -> None:
    """Local task status update (fallback)"""
    conn = get_prices_connection()
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


def _local_get_task_status(task_id: str) -> Optional[dict]:
    """Get task status by ID (fallback)"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_status WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# =============================================================================
# Metrics Functions (price_metrics.db)
# =============================================================================

def save_sector_returns(sector: str, returns: List[dict]) -> None:
    """Save sector return time series"""
    if not returns:
        return

    conn = get_metrics_connection()
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

    conn = get_metrics_connection()
    try:
        cursor = conn.cursor()
        cursor.executemany('''
            INSERT OR REPLACE INTO industry_returns (industry, date, avg_return, stock_count)
            VALUES (?, ?, ?, ?)
        ''', [(industry, r['date'], r['avg_return'], r['stock_count']) for r in returns])
        conn.commit()
    finally:
        conn.close()


def get_sector_returns(sector: str, start_date: str = None, end_date: str = None) -> List[dict]:
    """Get sector returns for date range"""
    conn = get_metrics_connection()
    try:
        cursor = conn.cursor()
        query = 'SELECT * FROM sector_returns WHERE sector = ?'
        params = [sector]

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


def get_industry_returns(industry: str, start_date: str = None, end_date: str = None) -> List[dict]:
    """Get industry returns for date range"""
    conn = get_metrics_connection()
    try:
        cursor = conn.cursor()
        query = 'SELECT * FROM industry_returns WHERE industry = ?'
        params = [industry]

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


# =============================================================================
# Settings Functions (prices.db)
# =============================================================================

def get_settings() -> dict:
    """Get all settings as dictionary"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM settings')
        return {row['key']: row['value'] for row in cursor.fetchall()}
    finally:
        conn.close()


def get_setting(key: str) -> Optional[str]:
    """Get a single setting value"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row['value'] if row else None
    finally:
        conn.close()


def update_setting(key: str, value: str) -> None:
    """Update a single setting"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', (key, str(value), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def update_settings(updates: dict) -> None:
    """Update multiple settings"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        for key, value in updates.items():
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', (key, str(value), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def get_all_symbols() -> List[str]:
    """Get all stock symbols"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT symbol FROM tickers')
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


# Note: Database initialized by centralized db module on app startup
