"""SQLite database for calc-engine RS scores"""
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


def get_rs_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def get_prices_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def get_metrics_connection():
    """Alias for get_connection() for backward compatibility"""
    return get_connection()


def init_db():
    """Initialize RS scores database schema"""
    conn = get_rs_connection()
    try:
        cursor = conn.cursor()

        # Settings table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT
            )
        ''')

        # RS scores table (unified for stocks, sectors, industries)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rs_scores (
                entity_type TEXT NOT NULL,
                entity_name TEXT NOT NULL,
                date TEXT NOT NULL,
                rs_score REAL,
                percentile INTEGER,
                weighted_return REAL,
                PRIMARY KEY (entity_type, entity_name, date)
            )
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_rs_scores_date
            ON rs_scores(date)
        ''')

        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_rs_scores_type_date
            ON rs_scores(entity_type, date)
        ''')

        # Task status tracking
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

        # Insert default settings if not exists
        default_settings = [
            ('benchmark', 'SPY'),
            ('q1_weight', '0.4'),
            ('q2_weight', '0.2'),
            ('q3_weight', '0.2'),
            ('q4_weight', '0.2'),
            ('lookback_days', '252'),      # 4 quarters for RS calc
            ('backfill_days', '63'),       # Calculate RS for last 3 months
            ('min_data_points', '120'),
        ]
        for key, value in default_settings:
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value, updated_at)
                VALUES (?, ?, ?)
            ''', (key, value, datetime.now().isoformat()))

        conn.commit()
    finally:
        conn.close()


# =============================================================================
# Task Status Functions (centralized database)
# =============================================================================

def create_task_status(task_id: str, task_type: str, target: str) -> None:
    """Create a new task status entry in centralized database"""
    try:
        from db import create_task_status as central_create_task_status
        central_create_task_status(task_id, task_type, target)
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
    """Create a new task status entry (fallback)"""
    conn = get_rs_connection()
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
    """Update task status (fallback)"""
    conn = get_rs_connection()
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
    conn = get_rs_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM task_status WHERE task_id = ?', (task_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


# =============================================================================
# RS Scores Functions
# =============================================================================

def save_rs_scores(scores: List[dict]) -> None:
    """Bulk save RS scores"""
    if not scores:
        return

    conn = get_rs_connection()
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
    conn = get_rs_connection()
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
    conn = get_rs_connection()
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
# Price Data Functions (read from price-engine)
# =============================================================================

def get_all_symbols() -> List[str]:
    """Get all stock symbols from price-engine"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT symbol FROM tickers WHERE symbol != "SPY"')
        return [row['symbol'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_dates() -> List[str]:
    """Get all unique dates from prices table"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT date FROM prices ORDER BY date')
        return [row['date'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_sectors() -> List[str]:
    """Get all unique sectors"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT sector FROM tickers WHERE sector IS NOT NULL AND sector != 'Unknown'")
        return [row['sector'] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_industries() -> List[str]:
    """Get all unique industries"""
    conn = get_prices_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT industry FROM tickers WHERE industry IS NOT NULL AND industry != 'Unknown'")
        return [row['industry'] for row in cursor.fetchall()]
    finally:
        conn.close()


# Note: Database initialized by centralized db module on app startup
