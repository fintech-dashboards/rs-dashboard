"""Database schema definitions for RS metrics app"""

# Task Status Table (unified for price-engine and calc-engine)
TASK_STATUS_TABLE = '''
    CREATE TABLE IF NOT EXISTS task_status (
        task_id TEXT PRIMARY KEY,
        task_type TEXT NOT NULL,
        target TEXT NOT NULL,
        symbol TEXT,
        status TEXT NOT NULL DEFAULT 'running',
        progress TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        error TEXT
    )
'''

TASK_STATUS_INDEXES = [
    '''CREATE INDEX IF NOT EXISTS idx_task_status_type_status
       ON task_status(task_type, status)''',
    '''CREATE INDEX IF NOT EXISTS idx_task_status_updated_at
       ON task_status(updated_at DESC)''',
    '''CREATE INDEX IF NOT EXISTS idx_task_status_symbol
       ON task_status(symbol)''',
]

# Tickers Table (from price-engine)
TICKERS_TABLE = '''
    CREATE TABLE IF NOT EXISTS tickers (
        symbol TEXT PRIMARY KEY,
        name TEXT,
        sector TEXT,
        industry TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
'''

# Prices Table (from price-engine)
PRICES_TABLE = '''
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
'''

PRICES_INDEXES = [
    '''CREATE INDEX IF NOT EXISTS idx_prices_symbol_date
       ON prices(symbol, date DESC)''',
]

# Sector Returns Table (from price-engine)
SECTOR_RETURNS_TABLE = '''
    CREATE TABLE IF NOT EXISTS sector_returns (
        sector TEXT NOT NULL,
        date TEXT NOT NULL,
        avg_return REAL,
        stock_count INTEGER,
        PRIMARY KEY (sector, date)
    )
'''

# Industry Returns Table (from price-engine)
INDUSTRY_RETURNS_TABLE = '''
    CREATE TABLE IF NOT EXISTS industry_returns (
        industry TEXT NOT NULL,
        date TEXT NOT NULL,
        avg_return REAL,
        stock_count INTEGER,
        PRIMARY KEY (industry, date)
    )
'''

# RS Scores Table (from calc-engine)
RS_SCORES_TABLE = '''
    CREATE TABLE IF NOT EXISTS rs_scores (
        entity_type TEXT NOT NULL,
        entity_name TEXT NOT NULL,
        date TEXT NOT NULL,
        rs_score REAL,
        percentile INTEGER,
        weighted_return REAL,
        PRIMARY KEY (entity_type, entity_name, date)
    )
'''

RS_SCORES_INDEXES = [
    '''CREATE INDEX IF NOT EXISTS idx_rs_scores_date
       ON rs_scores(date)''',
    '''CREATE INDEX IF NOT EXISTS idx_rs_scores_type_date
       ON rs_scores(entity_type, date)''',
]

# Batch Tasks Table (for orchestration tracking)
BATCH_TASKS_TABLE = '''
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
'''

# Settings Table
SETTINGS_TABLE = '''
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
    )
'''

# All tables in order of creation (respecting foreign key dependencies)
ALL_TABLES = [
    TASK_STATUS_TABLE,
    TICKERS_TABLE,
    PRICES_TABLE,
    SECTOR_RETURNS_TABLE,
    INDUSTRY_RETURNS_TABLE,
    RS_SCORES_TABLE,
    BATCH_TASKS_TABLE,
    SETTINGS_TABLE,
]

# All indexes
ALL_INDEXES = TASK_STATUS_INDEXES + PRICES_INDEXES + RS_SCORES_INDEXES

# Default settings
DEFAULT_SETTINGS = [
    ('benchmark', 'SPY'),
    ('q1_weight', '0.4'),
    ('q2_weight', '0.2'),
    ('q3_weight', '0.2'),
    ('q4_weight', '0.2'),
    ('lookback_days', '252'),
    ('backfill_days', '63'),
    ('min_data_points', '120'),
    ('start_date', '2024-01-01'),
]
