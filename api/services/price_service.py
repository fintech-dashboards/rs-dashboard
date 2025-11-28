"""Service for reading price-engine data"""
from typing import List, Dict, Optional
from db import get_connection

PRICE_ENGINE_DIR = None  # Kept for backward compatibility


def get_prices_connection():
    """Get connection to centralized database"""
    return get_connection()


def get_metrics_connection():
    """Get connection to centralized database"""
    return get_connection()


def get_ticker_count() -> int:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tickers")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def get_all_tickers() -> List[Dict]:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, name, sector, industry FROM tickers ORDER BY symbol")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def add_tickers_to_db(symbols: List[str]) -> int:
    """Add ticker symbols to DB (fast - no yfinance fetch, pipeline will get info)"""
    conn = get_prices_connection()
    cursor = conn.cursor()
    added = 0

    for symbol in symbols:
        # Just add symbol, pipeline will fetch info from yfinance
        cursor.execute("""
            INSERT OR IGNORE INTO tickers (symbol, name, sector, industry)
            VALUES (?, ?, '', '')
        """, (symbol, symbol))
        if cursor.rowcount > 0:
            added += 1

    conn.commit()
    conn.close()
    print(f"[UPLOAD] Added {added} new tickers to DB")
    return added


def get_ticker_stats() -> Dict:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tickers")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT symbol) FROM prices")
    with_prices = cursor.fetchone()[0]
    conn.close()
    return {"total": total, "with_prices": with_prices}


def get_all_tickers() -> List[Dict]:
    """Get all tickers - filtering done client-side"""
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol, name, sector, industry FROM tickers ORDER BY symbol")
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_ohlc_data(symbol: str, start_date: str, end_date: str) -> List[Dict]:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, open, high, low, close, volume
        FROM prices
        WHERE symbol = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, (symbol.upper(), start_date, end_date))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_sector_for_industry(industry: str) -> Optional[str]:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT sector FROM tickers WHERE industry = ?", (industry,))
    row = cursor.fetchone()
    conn.close()
    return row['sector'] if row else None


def get_sector_returns_baselined(sector: str, start_date: str) -> Dict:
    """Get cumulative returns baselined to 100 at start_date"""
    conn = get_metrics_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, avg_return FROM sector_returns
        WHERE sector = ? AND date >= ?
        ORDER BY date
    """, (sector, start_date))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "cumulative": []}

    dates = [row['date'] for row in rows]
    daily_returns = [row['avg_return'] for row in rows]

    # Calculate cumulative return baselined to 100
    cumulative = [100.0]
    for ret in daily_returns[1:]:
        cumulative.append(cumulative[-1] * (1 + (ret or 0)))

    return {"dates": dates, "cumulative": cumulative}


def get_industry_returns_baselined(industry: str, start_date: str) -> Dict:
    """Get cumulative returns baselined to 100 at start_date"""
    conn = get_metrics_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, avg_return FROM industry_returns
        WHERE industry = ? AND date >= ?
        ORDER BY date
    """, (industry, start_date))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "cumulative": []}

    dates = [row['date'] for row in rows]
    daily_returns = [row['avg_return'] for row in rows]

    cumulative = [100.0]
    for ret in daily_returns[1:]:
        cumulative.append(cumulative[-1] * (1 + (ret or 0)))

    return {"dates": dates, "cumulative": cumulative}


def get_benchmark_returns_baselined(start_date: str) -> Dict:
    """Get SPY cumulative returns baselined to 100"""
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, daily_return FROM prices
        WHERE symbol = 'SPY' AND date >= ?
        ORDER BY date
    """, (start_date,))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return {"dates": [], "cumulative": []}

    dates = [row['date'] for row in rows]
    daily_returns = [row['daily_return'] for row in rows]

    cumulative = [100.0]
    for ret in daily_returns[1:]:
        cumulative.append(cumulative[-1] * (1 + (ret or 0)))

    return {"dates": dates, "cumulative": cumulative}


def get_all_symbols() -> List[str]:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT symbol FROM tickers")
    symbols = [row['symbol'] for row in cursor.fetchall()]
    conn.close()
    return symbols


def get_sector_ohlc(sector: str, start_date: str, end_date: str) -> List[Dict]:
    """Get aggregated OHLC for a sector (equal-weighted average of component stocks, baselined to 100)"""
    conn = get_prices_connection()
    cursor = conn.cursor()

    # Get daily aggregated OHLC for the sector
    cursor.execute("""
        SELECT p.date,
               AVG(p.open) as avg_open,
               AVG(p.high) as avg_high,
               AVG(p.low) as avg_low,
               AVG(p.close) as avg_close
        FROM prices p
        JOIN tickers t ON p.symbol = t.symbol
        WHERE t.sector = ? AND p.date >= ? AND p.date <= ?
        GROUP BY p.date
        ORDER BY p.date
    """, (sector, start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    # Baseline to 100 using first day's close
    first_close = rows[0]['avg_close']
    if not first_close or first_close == 0:
        return []

    scale = 100.0 / first_close

    return [{
        'date': row['date'],
        'open': row['avg_open'] * scale,
        'high': row['avg_high'] * scale,
        'low': row['avg_low'] * scale,
        'close': row['avg_close'] * scale
    } for row in rows]


def get_industry_ohlc(industry: str, start_date: str, end_date: str) -> List[Dict]:
    """Get aggregated OHLC for an industry (equal-weighted average of component stocks, baselined to 100)"""
    conn = get_prices_connection()
    cursor = conn.cursor()

    # Get daily aggregated OHLC for the industry
    cursor.execute("""
        SELECT p.date,
               AVG(p.open) as avg_open,
               AVG(p.high) as avg_high,
               AVG(p.low) as avg_low,
               AVG(p.close) as avg_close
        FROM prices p
        JOIN tickers t ON p.symbol = t.symbol
        WHERE t.industry = ? AND p.date >= ? AND p.date <= ?
        GROUP BY p.date
        ORDER BY p.date
    """, (industry, start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    # Baseline to 100 using first day's close
    first_close = rows[0]['avg_close']
    if not first_close or first_close == 0:
        return []

    scale = 100.0 / first_close

    return [{
        'date': row['date'],
        'open': row['avg_open'] * scale,
        'high': row['avg_high'] * scale,
        'low': row['avg_low'] * scale,
        'close': row['avg_close'] * scale
    } for row in rows]


def get_benchmark_ohlc(start_date: str, end_date: str) -> List[Dict]:
    """Get SPY OHLC baselined to 100"""
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, open, high, low, close
        FROM prices
        WHERE symbol = 'SPY' AND date >= ? AND date <= ?
        ORDER BY date
    """, (start_date, end_date))
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return []

    # Baseline to 100 using first day's close
    first_close = rows[0]['close']
    if not first_close or first_close == 0:
        return []

    scale = 100.0 / first_close

    return [{
        'date': row['date'],
        'open': row['open'] * scale,
        'high': row['high'] * scale,
        'low': row['low'] * scale,
        'close': row['close'] * scale
    } for row in rows]


def get_settings() -> Dict:
    conn = get_prices_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM settings")
    settings = {row['key']: row['value'] for row in cursor.fetchall()}
    conn.close()
    return settings
