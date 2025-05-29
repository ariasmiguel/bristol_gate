"""
Bristol Gate - Financial Data Pipeline

A comprehensive financial and economic data pipeline using DuckDB and Parquet
for optimal performance and advanced feature engineering.

Organized modules:
- pipelines: Main pipeline orchestrators
- core: Core infrastructure and database operations  
- fetchers: Data source fetchers
- utils: Specialized utility classes
- features: Feature engineering utilities
"""

# Import main pipeline entry points
from .pipelines import (
    DataCollectionPipeline,
    create_aggregate_series_from_interpolated_data,
    run_silver_pipeline,
    run_full_pipeline
)

# Import core infrastructure
from .core import (
    BaseDataFetcher,
    SymbolManager,
    DataPipelineManager,
    DuckDBManager,
    DateUtils
)

# Make key classes available at package level for backward compatibility
__all__ = [
    # Main pipeline classes
    'DataCollectionPipeline',
    'create_aggregate_series_from_interpolated_data', 
    'run_silver_pipeline',
    'run_full_pipeline',
    
    # Core infrastructure
    'BaseDataFetcher',
    'SymbolManager', 
    'DataPipelineManager',
    'DuckDBManager',
    'DateUtils'
]

# Package metadata
__version__ = "2.0.0"
__author__ = "Miguel Arias"
__description__ = "Financial data pipeline with DuckDB and advanced feature engineering"
