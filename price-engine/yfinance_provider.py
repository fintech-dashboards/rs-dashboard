"""Fetch ticker data and OHLCV price history with SQLite caching"""
from datetime import datetime, timedelta
import time
import yfinance as yf

from tasks import submit_task, rate_limit
from db import (
    get_ticker, save_ticker, get_last_price_date,
    save_prices, get_prices,
    create_task_status, update_task_status
)

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def _log(msg: str):
    """Log with timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [PRICE] {msg}")


def _fetch_with_retry(func, symbol: str, *args):
    """Execute yfinance call with rate limiting and retry on rate limit errors"""
    for attempt in range(MAX_RETRIES):
        try:
            rate_limit()  # Enforce minimum delay between requests
            return func(symbol, *args)
        except Exception as e:
            error_msg = str(e).lower()
            if 'rate' in error_msg or 'too many' in error_msg or '429' in error_msg:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    _log(f"TICKER [{symbol}] Rate limited, waiting {wait_time}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(wait_time)
                    continue
            raise


def _fetch_ticker_info_from_yfinance(symbol: str) -> dict:
    """Fetch ticker metadata from yfinance"""
    ticker = yf.Ticker(symbol)
    info = ticker.info
    return {
        'name': info.get('longName') or info.get('shortName') or symbol,
        'sector': info.get('sector') or 'Unknown',
        'industry': info.get('industry') or 'Unknown'
    }


def _fetch_ticker_info_with_retry(symbol: str) -> dict:
    """Fetch ticker info with rate limiting and retry"""
    return _fetch_with_retry(lambda s: _fetch_ticker_info_from_yfinance(s), symbol)


def _fetch_prices_from_yfinance(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch OHLCV prices from yfinance for date range"""
    ticker = yf.Ticker(symbol)
    hist = ticker.history(start=start_date, end=end_date, auto_adjust=False)

    if hist.empty:
        return []

    prices = []
    prev_adjclose = None

    for idx, row in hist.iterrows():
        adjclose = float(row['Adj Close'])

        daily_return = None
        if prev_adjclose is not None and prev_adjclose != 0:
            try:
                daily_return = round((adjclose - prev_adjclose) / prev_adjclose, 6)
            except (ZeroDivisionError, TypeError):
                daily_return = None

        prices.append({
            'date': idx.strftime('%Y-%m-%d'),
            'open': float(row['Open']),
            'high': float(row['High']),
            'low': float(row['Low']),
            'close': float(row['Close']),
            'adjclose': adjclose,
            'volume': int(row['Volume']) if row['Volume'] else 0,
            'daily_return': daily_return
        })

        prev_adjclose = adjclose

    return prices


def _fetch_prices_with_retry(symbol: str, start_date: str, end_date: str) -> list[dict]:
    """Fetch prices with rate limiting and retry"""
    return _fetch_with_retry(
        lambda s, sd, ed: _fetch_prices_from_yfinance(s, sd, ed),
        symbol, start_date, end_date
    )


def _do_fetch_ticker_data(task_id: str, symbol: str, start_date: str, end_date: str, skip_info: bool) -> dict:
    """Internal function that runs in background thread"""
    try:
        symbol = symbol.upper()
        today = datetime.now().strftime('%Y-%m-%d')
        _log(f"TICKER [{symbol}] Starting...")

        create_task_status(task_id, 'fetch_ticker', symbol)

        if not end_date:
            end_date = today
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

        result = {'symbol': symbol, 'task_id': task_id}

        # 1. Handle ticker info
        if not skip_info:
            cached_ticker = get_ticker(symbol)
            # Fetch from yfinance if no cached ticker OR if sector/industry are empty
            # Valid values: actual sector/industry names, 'Index' (for ETFs like SPY)
            valid_values = lambda v: v and v not in ('', 'Unknown')
            needs_enrichment = (
                not cached_ticker or
                not valid_values(cached_ticker.get('sector')) or
                not valid_values(cached_ticker.get('industry'))
            )
            if cached_ticker and not needs_enrichment:
                _log(f"TICKER [{symbol}] Using cached info: {cached_ticker['sector']}/{cached_ticker['industry']}")
                result.update({
                    'name': cached_ticker['name'],
                    'sector': cached_ticker['sector'],
                    'industry': cached_ticker['industry']
                })
            else:
                _log(f"TICKER [{symbol}] Fetching info from yfinance (enriching missing sector/industry)...")
                update_task_status(task_id, 'running', progress='Fetching ticker info')
                ticker_info = _fetch_ticker_info_with_retry(symbol)
                save_ticker(symbol, ticker_info['name'], ticker_info['sector'], ticker_info['industry'])
                _log(f"TICKER [{symbol}] Saved: {ticker_info['sector']}/{ticker_info['industry']}")
                result.update(ticker_info)

        # 2. Determine what prices to fetch
        last_price_date = get_last_price_date(symbol)

        if last_price_date:
            last_date = datetime.strptime(last_price_date, '%Y-%m-%d')
            today_date = datetime.strptime(today, '%Y-%m-%d')
            days_diff = (today_date - last_date).days

            if days_diff >= 1:
                fetch_start = last_price_date
                _log(f"TICKER [{symbol}] Fetching prices from {fetch_start} ({days_diff} days behind)")
                update_task_status(task_id, 'running', progress=f'Fetching prices from {fetch_start}')

                new_prices = _fetch_prices_with_retry(symbol, fetch_start, end_date)

                if new_prices:
                    cached_prices = get_prices(symbol)
                    if cached_prices and new_prices:
                        last_cached = cached_prices[-1]
                        if last_cached['adjclose'] and last_cached['adjclose'] != 0:
                            first_new = new_prices[0]
                            first_new['daily_return'] = round(
                                (first_new['adjclose'] - last_cached['adjclose']) / last_cached['adjclose'],
                                6
                            )
                    save_prices(symbol, new_prices)
                    _log(f"TICKER [{symbol}] Saved {len(new_prices)} new prices")
            else:
                _log(f"TICKER [{symbol}] Already up to date (last: {last_price_date})")
        else:
            _log(f"TICKER [{symbol}] No prices, fetching full history from {start_date}")
            update_task_status(task_id, 'running', progress=f'Fetching prices from {start_date}')
            new_prices = _fetch_prices_with_retry(symbol, start_date, end_date)
            if new_prices:
                save_prices(symbol, new_prices)
                _log(f"TICKER [{symbol}] Saved {len(new_prices)} prices")

        # 3. Return all prices from DB
        result['prices'] = get_prices(symbol, start_date, end_date)

        update_task_status(task_id, 'completed', progress=f'Fetched {len(result["prices"])} prices')
        _log(f"TICKER [{symbol}] Done ✓ ({len(result['prices'])} total prices)")
        return result

    except Exception as e:
        _log(f"TICKER [{symbol}] ERROR: {e}")
        update_task_status(task_id, 'failed', error=str(e))
        raise


def fetch_ticker_data(symbol: str, start_date: str = None, end_date: str = None, skip_info: bool = False) -> str:
    """Queue task to fetch ticker data - returns immediately with task_id

    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL')
        start_date: Start date 'YYYY-MM-DD' (default: 1 year ago)
        end_date: End date 'YYYY-MM-DD' (default: today)
        skip_info: Skip fetching ticker metadata

    Returns:
        task_id: Use get_task_status(task_id) to check progress
    """
    return submit_task(_do_fetch_ticker_data, symbol, start_date, end_date, skip_info)
