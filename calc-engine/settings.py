"""Settings module for calc-engine RS configuration"""
from datetime import datetime
from typing import Dict, Any
import numpy as np

from db import get_rs_connection


# Default settings
DEFAULT_SETTINGS = {
    'benchmark': 'SPY',
    'q1_weight': 0.4,
    'q2_weight': 0.2,
    'q3_weight': 0.2,
    'q4_weight': 0.2,
    'lookback_days': 252,
    'min_data_points': 120,
}


def get_settings() -> Dict[str, Any]:
    """Get all settings as a dictionary"""
    conn = get_rs_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT key, value FROM settings')
        rows = cursor.fetchall()

        settings = {}
        for row in rows:
            key = row['key']
            value = row['value']
            # Convert numeric values
            if key in ('q1_weight', 'q2_weight', 'q3_weight', 'q4_weight'):
                settings[key] = float(value)
            elif key in ('lookback_days', 'min_data_points'):
                settings[key] = int(value)
            else:
                settings[key] = value

        return settings
    finally:
        conn.close()


def get_setting(key: str) -> Any:
    """Get a single setting value"""
    conn = get_rs_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        if not row:
            return DEFAULT_SETTINGS.get(key)

        value = row['value']
        # Convert numeric values
        if key in ('q1_weight', 'q2_weight', 'q3_weight', 'q4_weight'):
            return float(value)
        elif key in ('lookback_days', 'min_data_points', 'backfill_days'):
            return int(value)
        return value
    finally:
        conn.close()


def update_setting(key: str, value: Any) -> None:
    """Update a single setting"""
    conn = get_rs_connection()
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO settings (key, value, updated_at)
            VALUES (?, ?, ?)
        ''', (key, str(value), datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def update_settings(updates: Dict[str, Any]) -> None:
    """Update multiple settings at once"""
    conn = get_rs_connection()
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


def get_weights() -> Dict[str, float]:
    """Get quarter weights as dictionary"""
    settings = get_settings()
    return {
        'q1': settings.get('q1_weight', 0.4),
        'q2': settings.get('q2_weight', 0.2),
        'q3': settings.get('q3_weight', 0.2),
        'q4': settings.get('q4_weight', 0.2),
    }


def get_weight_array() -> np.ndarray:
    """Get quarter weights as NumPy array for vectorized operations"""
    weights = get_weights()
    return np.array([weights['q1'], weights['q2'], weights['q3'], weights['q4']])


def get_benchmark() -> str:
    """Get benchmark symbol"""
    return get_setting('benchmark') or 'SPY'


def get_lookback_days() -> int:
    """Get lookback period in days"""
    return get_setting('lookback_days') or 252


def get_min_data_points() -> int:
    """Get minimum data points required for valid RS"""
    return get_setting('min_data_points') or 120


def get_backfill_days() -> int:
    """Get number of days to backfill RS calculations (default: 63 = ~3 months)"""
    return get_setting('backfill_days') or 63
