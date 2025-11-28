"""Task executor for price engine - ThreadPoolExecutor for background tasks"""
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Thread pool for concurrent task execution (single worker to avoid rate limits)
_executor = ThreadPoolExecutor(max_workers=1)

# Rate limiter: minimum delay between yfinance API calls
_rate_lock = threading.Lock()
_last_request_time = 0
MIN_REQUEST_INTERVAL = 0.5  # 500ms between requests


def rate_limit():
    """Enforce minimum interval between API requests"""
    global _last_request_time
    with _rate_lock:
        now = time.time()
        elapsed = now - _last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        _last_request_time = time.time()

# Track submitted futures by task_id
_futures = {}


def submit_task(func, *args, **kwargs) -> str:
    """Submit a task for background execution

    Returns:
        task_id: UUID string to track the task
    """
    task_id = str(uuid.uuid4())
    future = _executor.submit(func, task_id, *args, **kwargs)
    _futures[task_id] = future
    return task_id


def get_task_result(task_id: str, timeout: float = None):
    """Get result of a completed task (blocks if not done)"""
    future = _futures.get(task_id)
    if future:
        return future.result(timeout=timeout)
    return None


def is_task_done(task_id: str) -> bool:
    """Check if task is done without blocking"""
    future = _futures.get(task_id)
    if future:
        return future.done()
    return True  # Unknown task = done
