"""Task executor for calc-engine - ThreadPoolExecutor for background tasks"""
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict

# Thread pool for concurrent task execution (single worker for yfinance to avoid rate limits)
_executor = ThreadPoolExecutor(max_workers=1)

# Track submitted futures by task_id (limited to 100 most recent)
_futures = OrderedDict()
_MAX_FUTURES = 100

# Rate limiter (shared with price-engine for API calls)
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


def submit_task(func, *args, **kwargs) -> str:
    """Submit a task for background execution

    Returns:
        task_id: UUID string to track the task
    """
    global _futures
    
    task_id = str(uuid.uuid4())
    future = _executor.submit(func, task_id, *args, **kwargs)
    _futures[task_id] = future
    
    # Clean up old completed futures to prevent memory leak
    if len(_futures) > _MAX_FUTURES:
        # Remove oldest completed futures
        to_remove = []
        for old_id, old_future in list(_futures.items()):
            if old_future.done():
                to_remove.append(old_id)
                if len(_futures) - len(to_remove) <= _MAX_FUTURES:
                    break
        
        for old_id in to_remove:
            del _futures[old_id]
    
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
