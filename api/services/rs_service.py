"""Service for reading calc-engine RS data"""
from typing import List, Dict, Optional
from db import get_connection

CALC_ENGINE_DIR = None  # Kept for backward compatibility


def get_rs_connection():
    """Get connection to centralized database"""
    return get_connection()


def get_sector_rankings() -> List[Dict]:
    """Get sectors ranked by RS"""
    conn = get_rs_connection()
    cursor = conn.cursor()

    # Get latest date
    cursor.execute("SELECT MAX(date) FROM rs_scores WHERE entity_type = 'sector'")
    latest_date = cursor.fetchone()[0]
    if not latest_date:
        return []

    cursor.execute("""
        SELECT entity_name as sector, rs_score as avg_rs, percentile
        FROM rs_scores
        WHERE entity_type = 'sector' AND date = ?
        ORDER BY rs_score DESC
    """, (latest_date,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_industry_rankings(sector: str) -> List[Dict]:
    """Get industries in a sector ranked by RS"""
    conn = get_rs_connection()
    cursor = conn.cursor()

    # First get the latest date
    cursor.execute("SELECT MAX(date) FROM rs_scores WHERE entity_type = 'industry'")
    latest_date = cursor.fetchone()[0]
    if not latest_date:
        conn.close()
        return []

    # Get industries for this sector from centralized db (tickers table)
    cursor.execute("SELECT DISTINCT industry FROM tickers WHERE sector = ?", (sector,))
    industries = [row[0] for row in cursor.fetchall()]

    if not industries:
        conn.close()
        return []

    # Get RS rankings for those industries
    placeholders = ','.join(['?' for _ in industries])
    cursor.execute(f"""
        SELECT entity_name as industry, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'industry' AND date = ? AND entity_name IN ({placeholders})
        ORDER BY rs_score DESC
    """, [latest_date] + industries)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_stock_rankings(industry: str = None) -> List[Dict]:
    """Get stocks ranked by RS (industry param ignored - filter on client)"""
    conn = get_rs_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(date) FROM rs_scores WHERE entity_type = 'stock'")
    latest_date = cursor.fetchone()[0]
    if not latest_date:
        return []

    cursor.execute("""
        SELECT entity_name as symbol, rs_score, percentile
        FROM rs_scores
        WHERE entity_type = 'stock' AND date = ?
        ORDER BY rs_score DESC
    """, (latest_date,))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_rs_history(entity_type: str, entity_name: str, start_date: str, end_date: str) -> List[float]:
    """Get RS score history for an entity"""
    conn = get_rs_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT rs_score FROM rs_scores
        WHERE entity_type = ? AND entity_name = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, (entity_type, entity_name, start_date, end_date))
    scores = [row['rs_score'] for row in cursor.fetchall()]
    conn.close()
    return scores


def get_rs_history_with_dates(entity_type: str, entity_name: str, start_date: str, end_date: str) -> List[dict]:
    """Get RS score history with dates for an entity"""
    conn = get_rs_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, rs_score FROM rs_scores
        WHERE entity_type = ? AND entity_name = ? AND date >= ? AND date <= ?
        ORDER BY date
    """, (entity_type, entity_name, start_date, end_date))
    rows = [{'date': row['date'], 'rs_score': row['rs_score']} for row in cursor.fetchall()]
    conn.close()
    return rows


def get_sector_sparklines() -> Dict[str, List[float]]:
    """Get 12-week RS history for all sectors"""
    return _get_sparklines('sector', 12 * 5)


def get_industry_sparklines(sector: str = None) -> Dict[str, List[float]]:
    """Get 12-week RS history for industries"""
    return _get_sparklines('industry', 12 * 5)


def get_stock_sparklines(industry: str = None) -> Dict[str, List[float]]:
    """Get 12-week RS history for stocks (industry param ignored - filter on client)"""
    return _get_sparklines('stock', 12 * 5)


def _get_sparklines(entity_type: str, days: int) -> Dict[str, List[float]]:
    """Get sparkline data for entities"""
    conn = get_rs_connection()
    cursor = conn.cursor()

    # Get dates for sparkline
    cursor.execute("""
        SELECT DISTINCT date FROM rs_scores
        WHERE entity_type = ?
        ORDER BY date DESC LIMIT ?
    """, (entity_type, days))
    dates = [row['date'] for row in cursor.fetchall()]
    dates.reverse()

    if not dates:
        return {}

    # Get scores for each entity
    placeholders = ','.join(['?'] * len(dates))
    cursor.execute(f"""
        SELECT entity_name, date, rs_score FROM rs_scores
        WHERE entity_type = ? AND date IN ({placeholders})
        ORDER BY entity_name, date
    """, [entity_type] + dates)

    sparklines = {}
    for row in cursor.fetchall():
        name = row['entity_name']
        if name not in sparklines:
            sparklines[name] = []
        sparklines[name].append(row['rs_score'] or 0)

    conn.close()
    return sparklines


def get_all_sectors_strength(days: int) -> Dict[str, Dict]:
    """Get strength % history for all sectors"""
    conn = get_rs_connection()
    cursor = conn.cursor()

    # Get dates
    cursor.execute("""
        SELECT DISTINCT date FROM rs_scores
        WHERE entity_type = 'sector'
        ORDER BY date DESC LIMIT ?
    """, (days,))
    dates = [row['date'] for row in cursor.fetchall()]
    dates.reverse()

    # Get sector data
    if not dates:
        return {}

    placeholders = ','.join(['?'] * len(dates))
    cursor.execute(f"""
        SELECT entity_name, date, rs_score FROM rs_scores
        WHERE entity_type = 'sector' AND date IN ({placeholders})
        ORDER BY entity_name, date
    """, dates)

    sectors = {}
    for row in cursor.fetchall():
        name = row['entity_name']
        if name not in sectors:
            sectors[name] = {"dates": [], "strength_pct": []}
        sectors[name]["dates"].append(row['date'])
        sectors[name]["strength_pct"].append(row['rs_score'] or 0)

    conn.close()
    return sectors


def get_all_industries_strength(sector: str, days: int) -> Dict[str, Dict]:
    """Get RS score history for all industries in a sector"""
    conn = get_rs_connection()
    cursor = conn.cursor()

    # Get industries for this sector
    cursor.execute("SELECT DISTINCT industry FROM tickers WHERE sector = ?", (sector,))
    industries = [row[0] for row in cursor.fetchall()]

    if not industries:
        conn.close()
        return {}

    # Get dates
    cursor.execute("""
        SELECT DISTINCT date FROM rs_scores
        WHERE entity_type = 'industry'
        ORDER BY date DESC LIMIT ?
    """, (days,))
    dates = [row['date'] for row in cursor.fetchall()]
    dates.reverse()

    if not dates:
        conn.close()
        return {}

    # Get industry data
    date_placeholders = ','.join(['?'] * len(dates))
    industry_placeholders = ','.join(['?'] * len(industries))
    cursor.execute(f"""
        SELECT entity_name, date, rs_score FROM rs_scores
        WHERE entity_type = 'industry'
          AND date IN ({date_placeholders})
          AND entity_name IN ({industry_placeholders})
        ORDER BY entity_name, date
    """, dates + industries)

    result = {}
    for row in cursor.fetchall():
        name = row['entity_name']
        if name not in result:
            result[name] = {"dates": [], "strength_pct": []}
        result[name]["dates"].append(row['date'])
        result[name]["strength_pct"].append(row['rs_score'] or 0)

    conn.close()
    return result
