#!/usr/bin/env python3
"""
RS Calculation Verification Script

Randomly samples stocks, sectors, and industries and verifies RS calculations
by manually computing from raw data and comparing against database values.

Run from project root: python tests/verify_rs_calculation.py
"""
import sys
from pathlib import Path
import random
import sqlite3
import numpy as np

# Database path
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "db" / "data" / "rs_metrics.db"


def get_connection():
    """Get database connection"""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def get_weights():
    """Get RS weights from settings"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT key, value FROM settings WHERE key LIKE 'q%_weight'")
    weights = {}
    for row in cursor.fetchall():
        weights[row['key']] = float(row['value'])
    conn.close()

    # Return as array [Q1, Q2, Q3, Q4]
    return np.array([
        weights.get('q1_weight', 0.4),
        weights.get('q2_weight', 0.2),
        weights.get('q3_weight', 0.2),
        weights.get('q4_weight', 0.2)
    ])


def get_benchmark_symbol():
    """Get benchmark symbol from settings"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM settings WHERE key = 'benchmark'")
    row = cursor.fetchone()
    conn.close()
    return row['value'] if row else 'SPY'


def calculate_quarterly_returns_from_daily(daily_returns: np.ndarray) -> np.ndarray:
    """Calculate quarterly returns from daily returns using cumulative product.

    Args:
        daily_returns: 1D array of daily returns (most recent first after reversing)

    Returns:
        Array of 4 quarterly returns [Q1, Q2, Q3, Q4]
    """
    n = len(daily_returns)
    if n < 20:
        return np.zeros(4)

    quarterly_returns = np.zeros(4)

    # Q1: most recent 63 days (end of array)
    # Q2: days 63-126 from end
    # Q3: days 126-189 from end
    # Q4: days 189-252 from end
    quarters = [
        (max(0, n - 63), n),           # Q1
        (max(0, n - 126), max(0, n - 63)),   # Q2
        (max(0, n - 189), max(0, n - 126)),  # Q3
        (max(0, n - 252), max(0, n - 189)),  # Q4
    ]

    for q, (start, end) in enumerate(quarters):
        if end <= start or end - start < 20:
            continue
        segment = daily_returns[start:end]
        # Clean NaN
        segment = np.nan_to_num(segment, nan=0.0)
        # Cumulative return: (1+r1)*(1+r2)*... - 1
        quarterly_returns[q] = np.prod(1 + segment) - 1

    return quarterly_returns


def verify_stock_rs(symbol: str, date: str, weights: np.ndarray, benchmark: str):
    """Manually calculate and verify RS for a stock"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get 252 days of daily returns ending at date
    cursor.execute("""
        SELECT date, daily_return FROM prices
        WHERE symbol = ? AND date <= ? AND daily_return IS NOT NULL
        ORDER BY date DESC LIMIT 252
    """, (symbol, date))
    stock_rows = cursor.fetchall()

    cursor.execute("""
        SELECT date, daily_return FROM prices
        WHERE symbol = ? AND date <= ? AND daily_return IS NOT NULL
        ORDER BY date DESC LIMIT 252
    """, (benchmark, date))
    bench_rows = cursor.fetchall()

    # Get stored RS score
    cursor.execute("""
        SELECT rs_score, percentile, weighted_return FROM rs_scores
        WHERE entity_type = 'stock' AND entity_name = ? AND date = ?
    """, (symbol, date))
    db_row = cursor.fetchone()
    conn.close()

    if not stock_rows or not bench_rows:
        return None, None, "No price data"

    if not db_row:
        return None, None, "No RS score in DB"

    # Reverse to chronological order for calculation
    stock_returns = np.array([r['daily_return'] for r in reversed(stock_rows)])
    bench_returns = np.array([r['daily_return'] for r in reversed(bench_rows)])

    # Calculate quarterly returns
    stock_q = calculate_quarterly_returns_from_daily(stock_returns)
    bench_q = calculate_quarterly_returns_from_daily(bench_returns)

    # Apply weights
    stock_weighted = np.dot(weights, stock_q)
    bench_weighted = np.dot(weights, bench_q)

    # Calculate RS score
    if bench_weighted > -1:
        calculated_rs = (1 + stock_weighted) / (1 + bench_weighted) * 100
    else:
        calculated_rs = 100.0

    db_rs = db_row['rs_score']
    diff = abs(calculated_rs - db_rs)

    return calculated_rs, db_rs, {
        'stock_q': stock_q.tolist(),
        'bench_q': bench_q.tolist(),
        'stock_weighted': stock_weighted,
        'bench_weighted': bench_weighted,
        'diff': diff,
        'days_used': len(stock_returns)
    }


def verify_sector_rs(sector: str, date: str, weights: np.ndarray, benchmark: str):
    """Manually calculate and verify RS for a sector"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get sector daily returns
    cursor.execute("""
        SELECT date, avg_return FROM sector_returns
        WHERE sector = ? AND date <= ?
        ORDER BY date DESC LIMIT 252
    """, (sector, date))
    sector_rows = cursor.fetchall()

    # Get benchmark daily returns
    cursor.execute("""
        SELECT date, daily_return FROM prices
        WHERE symbol = ? AND date <= ? AND daily_return IS NOT NULL
        ORDER BY date DESC LIMIT 252
    """, (benchmark, date))
    bench_rows = cursor.fetchall()

    # Get stored RS score
    cursor.execute("""
        SELECT rs_score, percentile, weighted_return FROM rs_scores
        WHERE entity_type = 'sector' AND entity_name = ? AND date = ?
    """, (sector, date))
    db_row = cursor.fetchone()
    conn.close()

    if not sector_rows or not bench_rows:
        return None, None, "No return data"

    if not db_row:
        return None, None, "No RS score in DB"

    # Reverse to chronological order
    sector_returns = np.array([r['avg_return'] for r in reversed(sector_rows)])
    bench_returns = np.array([r['daily_return'] for r in reversed(bench_rows)])

    # Calculate quarterly returns
    sector_q = calculate_quarterly_returns_from_daily(sector_returns)
    bench_q = calculate_quarterly_returns_from_daily(bench_returns)

    # Apply weights
    sector_weighted = np.dot(weights, sector_q)
    bench_weighted = np.dot(weights, bench_q)

    # Calculate RS
    if bench_weighted > -1:
        calculated_rs = (1 + sector_weighted) / (1 + bench_weighted) * 100
    else:
        calculated_rs = 100.0

    db_rs = db_row['rs_score']
    diff = abs(calculated_rs - db_rs)

    return calculated_rs, db_rs, {
        'sector_q': sector_q.tolist(),
        'bench_q': bench_q.tolist(),
        'sector_weighted': sector_weighted,
        'bench_weighted': bench_weighted,
        'diff': diff,
        'days_used': len(sector_returns)
    }


def verify_industry_rs(industry: str, date: str, weights: np.ndarray, benchmark: str):
    """Manually calculate and verify RS for an industry"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get industry daily returns
    cursor.execute("""
        SELECT date, avg_return FROM industry_returns
        WHERE industry = ? AND date <= ?
        ORDER BY date DESC LIMIT 252
    """, (industry, date))
    industry_rows = cursor.fetchall()

    # Get benchmark daily returns
    cursor.execute("""
        SELECT date, daily_return FROM prices
        WHERE symbol = ? AND date <= ? AND daily_return IS NOT NULL
        ORDER BY date DESC LIMIT 252
    """, (benchmark, date))
    bench_rows = cursor.fetchall()

    # Get stored RS score
    cursor.execute("""
        SELECT rs_score, percentile, weighted_return FROM rs_scores
        WHERE entity_type = 'industry' AND entity_name = ? AND date = ?
    """, (industry, date))
    db_row = cursor.fetchone()
    conn.close()

    if not industry_rows or not bench_rows:
        return None, None, "No return data"

    if not db_row:
        return None, None, "No RS score in DB"

    # Reverse to chronological order
    industry_returns = np.array([r['avg_return'] for r in reversed(industry_rows)])
    bench_returns = np.array([r['daily_return'] for r in reversed(bench_rows)])

    # Calculate quarterly returns
    industry_q = calculate_quarterly_returns_from_daily(industry_returns)
    bench_q = calculate_quarterly_returns_from_daily(bench_returns)

    # Apply weights
    industry_weighted = np.dot(weights, industry_q)
    bench_weighted = np.dot(weights, bench_q)

    # Calculate RS
    if bench_weighted > -1:
        calculated_rs = (1 + industry_weighted) / (1 + bench_weighted) * 100
    else:
        calculated_rs = 100.0

    db_rs = db_row['rs_score']
    diff = abs(calculated_rs - db_rs)

    return calculated_rs, db_rs, {
        'industry_q': industry_q.tolist(),
        'bench_q': bench_q.tolist(),
        'industry_weighted': industry_weighted,
        'bench_weighted': bench_weighted,
        'diff': diff,
        'days_used': len(industry_returns)
    }


def main():
    print("=" * 70)
    print("RS CALCULATION VERIFICATION")
    print("=" * 70)

    # Get settings
    weights = get_weights()
    benchmark = get_benchmark_symbol()

    print(f"\nWeights: Q1={weights[0]:.2f}, Q2={weights[1]:.2f}, Q3={weights[2]:.2f}, Q4={weights[3]:.2f}")
    print(f"Benchmark: {benchmark}")

    conn = get_connection()
    cursor = conn.cursor()

    # Get latest date with RS scores
    cursor.execute("SELECT MAX(date) FROM rs_scores WHERE entity_type = 'stock'")
    latest_date = cursor.fetchone()[0]
    print(f"Latest RS date: {latest_date}")

    # Sample random stocks
    cursor.execute("""
        SELECT DISTINCT entity_name FROM rs_scores
        WHERE entity_type = 'stock' AND date = ?
    """, (latest_date,))
    all_stocks = [r[0] for r in cursor.fetchall()]

    cursor.execute("""
        SELECT DISTINCT entity_name FROM rs_scores
        WHERE entity_type = 'sector' AND date = ?
    """, (latest_date,))
    all_sectors = [r[0] for r in cursor.fetchall()]

    cursor.execute("""
        SELECT DISTINCT entity_name FROM rs_scores
        WHERE entity_type = 'industry' AND date = ?
    """, (latest_date,))
    all_industries = [r[0] for r in cursor.fetchall()]
    conn.close()

    # Verify stocks
    print("\n" + "=" * 70)
    print("STOCK VERIFICATION (5 random samples)")
    print("=" * 70)

    sample_stocks = random.sample(all_stocks, min(5, len(all_stocks)))
    stock_errors = []

    for symbol in sample_stocks:
        calc_rs, db_rs, details = verify_stock_rs(symbol, latest_date, weights, benchmark)
        if calc_rs is None:
            print(f"\n{symbol}: {details}")
        else:
            status = "OK" if details['diff'] < 0.5 else "MISMATCH"
            stock_errors.append(details['diff'])
            print(f"\n{symbol}:")
            print(f"  Calculated: {calc_rs:.2f}")
            print(f"  Database:   {db_rs:.2f}")
            print(f"  Diff:       {details['diff']:.4f} [{status}]")
            print(f"  Days used:  {details['days_used']}")
            print(f"  Stock Q:    {[f'{q:.4f}' for q in details['stock_q']]}")
            print(f"  Bench Q:    {[f'{q:.4f}' for q in details['bench_q']]}")

    # Verify sectors
    print("\n" + "=" * 70)
    print("SECTOR VERIFICATION (3 random samples)")
    print("=" * 70)

    sample_sectors = random.sample(all_sectors, min(3, len(all_sectors)))
    sector_errors = []

    for sector in sample_sectors:
        calc_rs, db_rs, details = verify_sector_rs(sector, latest_date, weights, benchmark)
        if calc_rs is None:
            print(f"\n{sector}: {details}")
        else:
            status = "OK" if details['diff'] < 0.5 else "MISMATCH"
            sector_errors.append(details['diff'])
            print(f"\n{sector}:")
            print(f"  Calculated: {calc_rs:.2f}")
            print(f"  Database:   {db_rs:.2f}")
            print(f"  Diff:       {details['diff']:.4f} [{status}]")
            print(f"  Days used:  {details['days_used']}")
            print(f"  Sector Q:   {[f'{q:.4f}' for q in details['sector_q']]}")
            print(f"  Bench Q:    {[f'{q:.4f}' for q in details['bench_q']]}")

    # Verify industries
    print("\n" + "=" * 70)
    print("INDUSTRY VERIFICATION (3 random samples)")
    print("=" * 70)

    sample_industries = random.sample(all_industries, min(3, len(all_industries)))
    industry_errors = []

    for industry in sample_industries:
        calc_rs, db_rs, details = verify_industry_rs(industry, latest_date, weights, benchmark)
        if calc_rs is None:
            print(f"\n{industry}: {details}")
        else:
            status = "OK" if details['diff'] < 0.5 else "MISMATCH"
            industry_errors.append(details['diff'])
            print(f"\n{industry}:")
            print(f"  Calculated: {calc_rs:.2f}")
            print(f"  Database:   {db_rs:.2f}")
            print(f"  Diff:       {details['diff']:.4f} [{status}]")
            print(f"  Days used:  {details['days_used']}")
            print(f"  Industry Q: {[f'{q:.4f}' for q in details['industry_q']]}")
            print(f"  Bench Q:    {[f'{q:.4f}' for q in details['bench_q']]}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    all_errors = stock_errors + sector_errors + industry_errors
    if all_errors:
        avg_err = np.mean(all_errors)
        max_err = np.max(all_errors)
        passed = sum(1 for e in all_errors if e < 0.5)
        total = len(all_errors)

        print(f"Samples tested: {total}")
        print(f"Passed (<0.5 diff): {passed}/{total}")
        print(f"Average error: {avg_err:.4f}")
        print(f"Max error: {max_err:.4f}")

        if max_err < 0.5:
            print("\n✓ ALL CALCULATIONS VERIFIED")
        else:
            print(f"\n✗ SOME CALCULATIONS HAVE ERRORS > 0.5")
    else:
        print("No samples could be verified")


if __name__ == "__main__":
    main()
