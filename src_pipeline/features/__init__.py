"""
Bristol Gate Feature Engineering

Feature engineering utilities for creating ML-ready features from financial and economic data.
"""

from .feature_utils import (
    apply_savgol_filter,
    load_and_prepare_data,
    load_metadata,
    get_symbol_metadata_details,
    combine_and_save_metadata,
    save_featured_data
)
from .interpolate_data import DuckDBInterpolator

__all__ = [
    'apply_savgol_filter',
    'load_and_prepare_data',
    'load_metadata',
    'get_symbol_metadata_details',
    'combine_and_save_metadata',
    'save_featured_data',
    'DuckDBInterpolator'
] 