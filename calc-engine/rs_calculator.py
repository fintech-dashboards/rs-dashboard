"""RS Calculator - NumPy-optimized Relative Strength calculation"""
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple
from datetime import datetime, timedelta

from db import (
    get_prices_connection, get_metrics_connection,
    save_rs_scores, create_task_status, update_task_status,
    get_all_symbols, get_all_sectors, get_all_industries
)
from settings import get_weight_array, get_benchmark, get_lookback_days, get_min_data_points
from tasks import submit_task


def _log(msg: str):
    """Log with timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [RS] {msg}")


def _load_price_matrix(start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.Series]:
    """Load all stock prices and benchmark prices in a single query.

    Returns:
        price_matrix: DataFrame with dates as index, symbols as columns, close prices as values
        benchmark_prices: Series with dates as index, close prices as values
    """
    conn = get_prices_connection()
    try:
        # Load all stock prices
        prices_df = pd.read_sql(f"""
            SELECT symbol, date, close
            FROM prices
            WHERE date >= ? AND date <= ?
            ORDER BY date, symbol
        """, conn, params=(start_date, end_date))

        if prices_df.empty:
            return pd.DataFrame(), pd.Series()

        # Pivot to matrix: rows=dates, columns=symbols
        price_matrix = prices_df.pivot_table(
            index='date',
            columns='symbol',
            values='close',
            aggfunc='first'
        )

        # Get benchmark (SPY) prices
        benchmark = get_benchmark()
        if benchmark in price_matrix.columns:
            benchmark_prices = price_matrix[benchmark].copy()
            # Remove benchmark from stock matrix
            price_matrix = price_matrix.drop(columns=[benchmark])
        else:
            # Load benchmark separately
            benchmark_df = pd.read_sql("""
                SELECT date, close FROM prices
                WHERE symbol = ? AND date >= ? AND date <= ?
                ORDER BY date
            """, conn, params=(benchmark, start_date, end_date))
            benchmark_prices = benchmark_df.set_index('date')['close']

        return price_matrix, benchmark_prices
    finally:
        conn.close()


def _load_sector_returns(start_date: str, end_date: str) -> pd.DataFrame:
    """Load sector daily returns from price_metrics.db"""
    conn = get_metrics_connection()
    try:
        df = pd.read_sql("""
            SELECT sector, date, avg_return
            FROM sector_returns
            WHERE date >= ? AND date <= ?
            ORDER BY date, sector
        """, conn, params=(start_date, end_date))

        if df.empty:
            return pd.DataFrame()

        # Pivot: rows=dates, columns=sectors
        return df.pivot_table(
            index='date',
            columns='sector',
            values='avg_return',
            aggfunc='first'
        )
    finally:
        conn.close()


def _load_industry_returns(start_date: str, end_date: str) -> pd.DataFrame:
    """Load industry daily returns from price_metrics.db"""
    conn = get_metrics_connection()
    try:
        df = pd.read_sql("""
            SELECT industry, date, avg_return
            FROM industry_returns
            WHERE date >= ? AND date <= ?
            ORDER BY date, industry
        """, conn, params=(start_date, end_date))

        if df.empty:
            return pd.DataFrame()

        # Pivot: rows=dates, columns=industries
        return df.pivot_table(
            index='date',
            columns='industry',
            values='avg_return',
            aggfunc='first'
        )
    finally:
        conn.close()


def _load_benchmark_returns(start_date: str, end_date: str) -> pd.Series:
    """Load benchmark daily returns"""
    conn = get_prices_connection()
    try:
        benchmark = get_benchmark()
        df = pd.read_sql("""
            SELECT date, daily_return
            FROM prices
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(benchmark, start_date, end_date))

        if df.empty:
            return pd.Series(dtype=float)

        return df.set_index('date')['daily_return']
    finally:
        conn.close()


def _load_benchmark_prices(start_date: str, end_date: str) -> pd.Series:
    """Load benchmark close prices"""
    conn = get_prices_connection()
    try:
        benchmark = get_benchmark()
        df = pd.read_sql("""
            SELECT date, close
            FROM prices
            WHERE symbol = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, conn, params=(benchmark, start_date, end_date))

        if df.empty:
            return pd.Series(dtype=float)

        return df.set_index('date')['close']
    finally:
        conn.close()


def _calculate_quarterly_returns_from_prices(price_window: np.ndarray) -> np.ndarray:
    """Calculate quarterly returns for all stocks from price window.

    Args:
        price_window: 2D array of shape (n_days, n_stocks)

    Returns:
        quarterly_returns: 2D array of shape (4, n_stocks)
    """
    # Validate input
    if price_window.size == 0:
        return np.zeros((4, 1))
    
    n_days = price_window.shape[0]
    n_stocks = price_window.shape[1] if price_window.ndim > 1 else 1

    if price_window.ndim == 1:
        price_window = price_window.reshape(-1, 1)

    # Return zeros if insufficient data
    if n_days < 20:
        return np.zeros((4, n_stocks))

    quarterly_returns = np.zeros((4, n_stocks))

    # Q1: most recent 63 days (days 0-62 from end)
    # Q2: days 63-125 from end
    # Q3: days 126-188 from end
    # Q4: days 189-251 from end
    quarters = [
        (max(0, n_days - 63), n_days),           # Q1
        (max(0, n_days - 126), max(0, n_days - 63)),   # Q2
        (max(0, n_days - 189), max(0, n_days - 126)),  # Q3
        (max(0, n_days - 252), max(0, n_days - 189)),  # Q4
    ]

    for q, (start_idx, end_idx) in enumerate(quarters):
        if end_idx <= start_idx or end_idx - start_idx < 20:
            continue

        first_prices = price_window[start_idx, :]
        last_prices = price_window[end_idx - 1, :]

        # Vectorized return calculation with NaN handling
        with np.errstate(divide='ignore', invalid='ignore'):
            returns = np.where(
                (first_prices > 0) & ~np.isnan(first_prices) & ~np.isnan(last_prices),
                (last_prices / first_prices) - 1,
                0.0
            )
        quarterly_returns[q, :] = returns

    return quarterly_returns


def _calculate_quarterly_returns_from_daily_returns(returns_window: np.ndarray) -> np.ndarray:
    """Calculate quarterly returns from daily returns using cumulative product.

    Args:
        returns_window: 2D array of shape (n_days, n_entities)

    Returns:
        quarterly_returns: 2D array of shape (4, n_entities)
    """
    # Validate input
    if returns_window.size == 0:
        return np.zeros((4, 1))
    
    n_days = returns_window.shape[0]
    n_entities = returns_window.shape[1] if returns_window.ndim > 1 else 1

    if returns_window.ndim == 1:
        returns_window = returns_window.reshape(-1, 1)

    # Return zeros if insufficient data
    if n_days < 20:
        return np.zeros((4, n_entities))

    quarterly_returns = np.zeros((4, n_entities))

    quarters = [
        (max(0, n_days - 63), n_days),
        (max(0, n_days - 126), max(0, n_days - 63)),
        (max(0, n_days - 189), max(0, n_days - 126)),
        (max(0, n_days - 252), max(0, n_days - 189)),
    ]

    for q, (start_idx, end_idx) in enumerate(quarters):
        if end_idx <= start_idx or end_idx - start_idx < 20:
            continue

        segment = returns_window[start_idx:end_idx, :]

        # Fill NaN with 0 for calculation
        segment_clean = np.nan_to_num(segment, nan=0.0)

        # Cumulative product: (1 + r1) * (1 + r2) * ... - 1
        cum_return = np.prod(1 + segment_clean, axis=0) - 1
        quarterly_returns[q, :] = cum_return

    return quarterly_returns


def _get_benchmark_quarterly_returns(bench_window_values: np.ndarray, weights: np.ndarray) -> float:
    """Get benchmark quarterly returns as scalar.
    
    Args:
        bench_window_values: 1D array of benchmark prices/returns
        weights: 1D array of quarterly weights
    
    Returns:
        Scalar benchmark weighted return
    """
    q_returns = _calculate_quarterly_returns_from_daily_returns(bench_window_values)
    # q_returns is (4, 1) - extract column to get (4,)
    if q_returns.ndim == 2:
        q_returns = q_returns[:, 0]
    return float(np.dot(weights, q_returns))


def _calculate_percentiles(scores: np.ndarray) -> np.ndarray:
    """Calculate percentile rankings for scores.

    Args:
        scores: 1D array of RS scores

    Returns:
        percentiles: 1D array of percentile values (0-100)
    """
    if len(scores) == 0:
        return np.array([])

    # Use pandas rank for percentile calculation
    percentiles = pd.Series(scores).rank(pct=True) * 100
    return percentiles.astype(int).values


def _do_calculate_stock_rs(task_id: str, dates: List[str]) -> dict:
    """Calculate RS scores for all stocks on given dates.

    This is the main calculation function that runs in a background thread.
    """
    try:
        _log(f"STOCK RS Starting for {len(dates)} dates...")
        create_task_status(task_id, 'calc_stock_rs', f'{len(dates)} dates')

        weights = get_weight_array()
        lookback = get_lookback_days()
        min_points = get_min_data_points()
        _log(f"STOCK RS Settings: lookback={lookback}, min_points={min_points}")

        # Calculate date range including lookback
        dates_sorted = sorted(dates)
        earliest_date = datetime.strptime(dates_sorted[0], '%Y-%m-%d')
        lookback_start = (earliest_date - timedelta(days=lookback + 50)).strftime('%Y-%m-%d')
        _log(f"STOCK RS Date range: {lookback_start} to {dates_sorted[-1]}")

        update_task_status(task_id, 'running', progress='Loading price data')

        # Single query to load all prices
        price_matrix, benchmark_prices = _load_price_matrix(lookback_start, dates_sorted[-1])
        _log(f"STOCK RS Price matrix: {price_matrix.shape if not price_matrix.empty else 'EMPTY'}")

        if price_matrix.empty:
            _log("STOCK RS ERROR: No price data found!")
            update_task_status(task_id, 'failed', error='No price data found')
            return {'task_id': task_id, 'error': 'No price data'}

        # If benchmark not found in matrix, load separately
        if benchmark_prices.empty:
            _log(f"STOCK RS Loading benchmark {get_benchmark()} separately...")
            benchmark_prices = _load_benchmark_prices(lookback_start, dates_sorted[-1])

        if benchmark_prices.empty:
            _log(f"STOCK RS ERROR: No benchmark ({get_benchmark()}) data!")
            update_task_status(task_id, 'failed', error=f'No benchmark ({get_benchmark()}) data found')
            return {'task_id': task_id, 'error': 'No benchmark data'}

        symbols = price_matrix.columns.tolist()
        batch_results = []
        total_saved = 0
        BATCH_SIZE = 10000  # Save every 10k records to prevent memory bloat

        # Adjust min_points based on available data
        available_days = len(price_matrix)
        effective_min_points = min(min_points, available_days // 2)
        if effective_min_points < 60:
            effective_min_points = 60  # Absolute minimum

        update_task_status(task_id, 'running', progress=f'Processing {len(dates_sorted)} dates, {len(symbols)} stocks')

        # Process each date
        for i, date_str in enumerate(dates_sorted):
            if (i + 1) % 10 == 0:
                update_task_status(task_id, 'running', progress=f'Processing {i+1}/{len(dates_sorted)}: {date_str}')

            # Get lookback window ending at this date
            date_mask = price_matrix.index <= date_str
            window = price_matrix.loc[date_mask].tail(lookback)

            if len(window) < effective_min_points:
                continue

            # Get benchmark window
            bench_mask = benchmark_prices.index <= date_str
            bench_window = benchmark_prices.loc[bench_mask].tail(lookback)

            if len(bench_window) < effective_min_points:
                continue

            # Calculate quarterly returns for all stocks (vectorized)
            stock_q_returns = _calculate_quarterly_returns_from_prices(window.values)

            # Calculate benchmark quarterly returns as scalar
            bench_weighted = _get_benchmark_quarterly_returns(bench_window.values, weights)

            # Apply weights: np.dot(4,) @ (4, n_stocks) = (n_stocks,)
            stock_weighted = np.dot(weights, stock_q_returns)
            
            # Validate shapes before proceeding
            if stock_weighted.ndim != 1:
                _log(f"STOCK RS WARNING: Unexpected stock_weighted shape {stock_weighted.shape}, skipping date {date_str}")
                continue
            
            if len(stock_weighted) != len(symbols):
                _log(f"STOCK RS ERROR: stock_weighted length {len(stock_weighted)} != symbols {len(symbols)}")
                continue

            # Calculate RS scores
            with np.errstate(divide='ignore', invalid='ignore'):
                rs_scores = np.where(
                    bench_weighted > -1,
                    (1 + stock_weighted) / (1 + bench_weighted) * 100,
                    100.0
                )

            # Filter valid scores (10-500 range)
            valid_mask = (rs_scores >= 10) & (rs_scores <= 500) & ~np.isnan(rs_scores)

            if not np.any(valid_mask):
                continue

            valid_scores = rs_scores[valid_mask]
            valid_symbols = [symbols[j] for j in range(len(symbols)) if valid_mask[j]]
            valid_weighted = stock_weighted[valid_mask]
            
            # Validate indexing
            if len(valid_scores) != len(valid_symbols) or len(valid_scores) != len(valid_weighted):
                _log(f"STOCK RS ERROR: Length mismatch - scores:{len(valid_scores)} symbols:{len(valid_symbols)} weighted:{len(valid_weighted)}")
                continue

            # Calculate percentiles
            percentiles = _calculate_percentiles(valid_scores)

            # Create result records
            for sym, score, pctl, wret in zip(valid_symbols, valid_scores, percentiles, valid_weighted):
                batch_results.append({
                    'entity_type': 'stock',
                    'entity_name': sym,
                    'date': date_str,
                    'rs_score': round(float(score), 2),
                    'percentile': int(pctl),
                    'weighted_return': round(float(wret), 6)
                })

            # Batch save to prevent memory bloat
            if len(batch_results) >= BATCH_SIZE:
                save_rs_scores(batch_results)
                total_saved += len(batch_results)
                batch_results = []  # Clear memory

        # Save remaining results
        if batch_results:
            save_rs_scores(batch_results)
            total_saved += len(batch_results)

        update_task_status(task_id, 'completed', progress=f'Calculated {total_saved} RS scores')
        return {'task_id': task_id, 'count': total_saved, 'dates': len(dates_sorted)}

    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        raise


def _do_calculate_sector_rs(task_id: str, dates: List[str]) -> dict:
    """Calculate RS scores for all sectors on given dates."""
    try:
        create_task_status(task_id, 'calc_sector_rs', f'{len(dates)} dates')

        weights = get_weight_array()
        lookback = get_lookback_days()
        min_points = get_min_data_points()

        dates_sorted = sorted(dates)
        earliest_date = datetime.strptime(dates_sorted[0], '%Y-%m-%d')
        lookback_start = (earliest_date - timedelta(days=lookback + 50)).strftime('%Y-%m-%d')

        update_task_status(task_id, 'running', progress='Loading sector returns')

        # Load sector returns and benchmark returns
        sector_returns = _load_sector_returns(lookback_start, dates_sorted[-1])
        benchmark_returns = _load_benchmark_returns(lookback_start, dates_sorted[-1])

        if sector_returns.empty:
            update_task_status(task_id, 'completed', progress='No sector return data found')
            return {'task_id': task_id, 'count': 0, 'error': 'No sector data'}

        if benchmark_returns.empty:
            update_task_status(task_id, 'failed', error=f'No benchmark ({get_benchmark()}) returns found')
            return {'task_id': task_id, 'error': 'No benchmark data'}

        sectors = sector_returns.columns.tolist()
        batch_results = []
        total_saved = 0
        BATCH_SIZE = 10000

        for i, date_str in enumerate(dates_sorted):
            update_task_status(task_id, 'running', progress=f'Processing {i+1}/{len(dates_sorted)}: {date_str}')

            # Get lookback window
            date_mask = sector_returns.index <= date_str
            sector_window = sector_returns.loc[date_mask].tail(lookback)

            bench_mask = benchmark_returns.index <= date_str
            bench_window = benchmark_returns.loc[bench_mask].tail(lookback)

            if len(sector_window) < min_points or len(bench_window) < min_points:
                continue

            # Calculate quarterly returns from daily returns
            sector_q_returns = _calculate_quarterly_returns_from_daily_returns(sector_window.values)
            bench_weighted = _get_benchmark_quarterly_returns(bench_window.values, weights)

            # Apply weights
            sector_weighted = np.dot(weights, sector_q_returns)
            
            # Validate shapes
            if sector_weighted.ndim != 1:
                continue
            if len(sector_weighted) != len(sectors):
                continue

            # Calculate RS scores
            with np.errstate(divide='ignore', invalid='ignore'):
                rs_scores = np.where(
                    bench_weighted > -1,
                    (1 + sector_weighted) / (1 + bench_weighted) * 100,
                    100.0
                )

            # Filter valid
            valid_mask = (rs_scores >= 10) & (rs_scores <= 500) & ~np.isnan(rs_scores)

            if not np.any(valid_mask):
                continue

            valid_scores = rs_scores[valid_mask]
            valid_sectors = [sectors[i] for i in range(len(sectors)) if valid_mask[i]]
            valid_weighted = sector_weighted[valid_mask]
            
            # Validate indexing
            if len(valid_scores) != len(valid_sectors) or len(valid_scores) != len(valid_weighted):
                continue

            percentiles = _calculate_percentiles(valid_scores)

            for sec, score, pctl, wret in zip(valid_sectors, valid_scores, percentiles, valid_weighted):
                batch_results.append({
                    'entity_type': 'sector',
                    'entity_name': sec,
                    'date': date_str,
                    'rs_score': round(float(score), 2),
                    'percentile': int(pctl),
                    'weighted_return': round(float(wret), 6)
                })

            # Batch save to prevent memory bloat
            if len(batch_results) >= BATCH_SIZE:
                save_rs_scores(batch_results)
                total_saved += len(batch_results)
                batch_results = []

        if batch_results:
            save_rs_scores(batch_results)
            total_saved += len(batch_results)

        update_task_status(task_id, 'completed', progress=f'Calculated {total_saved} sector RS scores')
        return {'task_id': task_id, 'count': total_saved, 'dates': len(dates_sorted)}

    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        raise


def _do_calculate_industry_rs(task_id: str, dates: List[str]) -> dict:
    """Calculate RS scores for all industries on given dates."""
    try:
        create_task_status(task_id, 'calc_industry_rs', f'{len(dates)} dates')

        weights = get_weight_array()
        lookback = get_lookback_days()
        min_points = get_min_data_points()

        dates_sorted = sorted(dates)
        earliest_date = datetime.strptime(dates_sorted[0], '%Y-%m-%d')
        lookback_start = (earliest_date - timedelta(days=lookback + 50)).strftime('%Y-%m-%d')

        update_task_status(task_id, 'running', progress='Loading industry returns')

        # Load industry returns and benchmark returns
        industry_returns = _load_industry_returns(lookback_start, dates_sorted[-1])
        benchmark_returns = _load_benchmark_returns(lookback_start, dates_sorted[-1])

        if industry_returns.empty:
            update_task_status(task_id, 'completed', progress='No industry return data found')
            return {'task_id': task_id, 'count': 0, 'error': 'No industry data'}

        if benchmark_returns.empty:
            update_task_status(task_id, 'failed', error=f'No benchmark ({get_benchmark()}) returns found')
            return {'task_id': task_id, 'error': 'No benchmark data'}

        industries = industry_returns.columns.tolist()
        batch_results = []
        total_saved = 0
        BATCH_SIZE = 10000

        for i, date_str in enumerate(dates_sorted):
            update_task_status(task_id, 'running', progress=f'Processing {i+1}/{len(dates_sorted)}: {date_str}')

            date_mask = industry_returns.index <= date_str
            ind_window = industry_returns.loc[date_mask].tail(lookback)

            bench_mask = benchmark_returns.index <= date_str
            bench_window = benchmark_returns.loc[bench_mask].tail(lookback)

            if len(ind_window) < min_points or len(bench_window) < min_points:
                continue

            # Calculate quarterly returns
            ind_q_returns = _calculate_quarterly_returns_from_daily_returns(ind_window.values)
            bench_weighted = _get_benchmark_quarterly_returns(bench_window.values, weights)

            # Apply weights
            ind_weighted = np.dot(weights, ind_q_returns)
            
            # Validate shapes
            if ind_weighted.ndim != 1:
                continue
            if len(ind_weighted) != len(industries):
                continue

            # Calculate RS
            with np.errstate(divide='ignore', invalid='ignore'):
                rs_scores = np.where(
                    bench_weighted > -1,
                    (1 + ind_weighted) / (1 + bench_weighted) * 100,
                    100.0
                )

            valid_mask = (rs_scores >= 10) & (rs_scores <= 500) & ~np.isnan(rs_scores)

            if not np.any(valid_mask):
                continue

            valid_scores = rs_scores[valid_mask]
            valid_industries = [industries[i] for i in range(len(industries)) if valid_mask[i]]
            valid_weighted = ind_weighted[valid_mask]
            
            # Validate indexing
            if len(valid_scores) != len(valid_industries) or len(valid_scores) != len(valid_weighted):
                continue

            percentiles = _calculate_percentiles(valid_scores)

            for ind, score, pctl, wret in zip(valid_industries, valid_scores, percentiles, valid_weighted):
                batch_results.append({
                    'entity_type': 'industry',
                    'entity_name': ind,
                    'date': date_str,
                    'rs_score': round(float(score), 2),
                    'percentile': int(pctl),
                    'weighted_return': round(float(wret), 6)
                })

            # Batch save to prevent memory bloat
            if len(batch_results) >= BATCH_SIZE:
                save_rs_scores(batch_results)
                total_saved += len(batch_results)
                batch_results = []

        if batch_results:
            save_rs_scores(batch_results)
            total_saved += len(batch_results)

        update_task_status(task_id, 'completed', progress=f'Calculated {total_saved} industry RS scores')
        return {'task_id': task_id, 'count': total_saved, 'dates': len(dates_sorted)}

    except Exception as e:
        update_task_status(task_id, 'failed', error=str(e))
        raise


# =============================================================================
# Public API Functions (return task_id immediately)
# =============================================================================

def calculate_stock_rs(dates: List[str]) -> str:
    """Queue task to calculate stock RS scores - returns immediately with task_id"""
    return submit_task(_do_calculate_stock_rs, dates)


def calculate_sector_rs(dates: List[str]) -> str:
    """Queue task to calculate sector RS scores - returns immediately with task_id"""
    return submit_task(_do_calculate_sector_rs, dates)


def calculate_industry_rs(dates: List[str]) -> str:
    """Queue task to calculate industry RS scores - returns immediately with task_id"""
    return submit_task(_do_calculate_industry_rs, dates)
