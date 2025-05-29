"""
DuckDB-based Data Interpolation for Bristol Gate Pipeline

This module provides functionality to extract data from DuckDB staging tables,
transform it to wide format, and perform interpolation for daily frequency analysis.

Key improvements over CSV-based approach:
- Direct DuckDB queries for better performance
- SQL-based pivoting for efficiency  
- Configurable date filtering
- Support for incremental processing
- Parquet output for better performance and compression
"""

import time
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import duckdb

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DuckDBInterpolator:
    """Handles data extraction and interpolation from DuckDB staging tables"""
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        """
        Initialize the interpolator with DuckDB connection
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        
    def connect(self) -> bool:
        """Establish connection to DuckDB database"""
        try:
            if not self.db_path.exists():
                logger.error(f"Database file not found: {self.db_path}")
                return False
                
            self.con = duckdb.connect(database=str(self.db_path), read_only=True)
            logger.info(f"Connected to DuckDB: {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            return False
    
    def get_wide_format_data_direct(self, 
                                   filter_start_date: str = '1950-01-01',
                                   usrec_symbol: str = 'USREC') -> pd.DataFrame:
        """
        Extract and pivot data directly to wide format using SQL
        
        This is the most efficient approach - uses DuckDB's optimized PIVOT
        to transform from staging tables directly to wide format.
        
        Args:
            filter_start_date: Start date for filtering (YYYY-MM-DD format)
            usrec_symbol: Symbol for recession indicator (for special handling)
            
        Returns:
            Wide format DataFrame with date index and symbol_metric columns
        """
        if not self.con:
            logger.error("No database connection. Call connect() first.")
            return pd.DataFrame()
            
        try:
            # Complex SQL query that normalizes all sources and pivots directly
            query = f"""
            WITH normalized_data AS (
                -- Yahoo Finance data (multi-metric per symbol)
                SELECT date, symbol || '_open' as symbol_metric, open as value 
                FROM stg_yahoo WHERE open IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_high' as symbol_metric, high as value 
                FROM stg_yahoo WHERE high IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_low' as symbol_metric, low as value 
                FROM stg_yahoo WHERE low IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_close' as symbol_metric, close as value 
                FROM stg_yahoo WHERE close IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_volume' as symbol_metric, volume as value 
                FROM stg_yahoo WHERE volume IS NOT NULL
                
                -- FRED data (single value per series)
                UNION ALL
                SELECT date, series_id as symbol_metric, value 
                FROM stg_fred WHERE value IS NOT NULL
                
                -- EIA data (single value per series)
                UNION ALL
                SELECT date, series_id as symbol_metric, value 
                FROM stg_eia WHERE value IS NOT NULL
                
                -- Baker Hughes data (already in metric format)
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_baker WHERE value IS NOT NULL
                
                -- FINRA data (already in metric format)
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_finra WHERE value IS NOT NULL
                
                -- S&P 500 data (already in metric format)
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_sp500 WHERE value IS NOT NULL
                
                -- USDA data (already in metric format)
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_usda WHERE value IS NOT NULL
            ),
            
            -- Create date spine for interpolation
            date_range AS (
                SELECT DISTINCT date
                FROM normalized_data
                WHERE date >= '{filter_start_date}'
            ),
            
            -- Get all unique symbols for cross join
            all_symbols AS (
                SELECT DISTINCT symbol_metric
                FROM normalized_data
            ),
            
            -- Create full grid and left join actual data
            full_grid AS (
                SELECT d.date, s.symbol_metric, n.value
                FROM date_range d
                CROSS JOIN all_symbols s
                LEFT JOIN normalized_data n 
                    ON d.date = n.date AND s.symbol_metric = n.symbol_metric
            )
            
            -- Pivot to wide format
            PIVOT full_grid ON symbol_metric USING FIRST(value)
            ORDER BY date
            """
            
            logger.info("Executing direct SQL pivot query...")
            start_time = time.time()
            
            df_wide = self.con.execute(query).df()
            
            exec_time = time.time() - start_time
            logger.info(f"SQL pivot completed in {exec_time:.2f} seconds")
            logger.info(f"Wide format shape: {df_wide.shape}")
            
            if df_wide.empty:
                logger.warning("Query returned empty DataFrame")
                return df_wide
                
            # Set date as index
            df_wide['date'] = pd.to_datetime(df_wide['date'])
            df_wide.set_index('date', inplace=True)
            
            return df_wide
            
        except Exception as e:
            logger.error(f"Error in direct SQL pivot: {e}")
            return pd.DataFrame()
    
    def get_wide_format_data_staged(self, 
                                   filter_start_date: str = '1950-01-01') -> pd.DataFrame:
        """
        Alternative approach: Extract to long format first, then pivot with pandas
        
        This approach is more flexible for debugging and custom transformations,
        but less efficient than direct SQL pivot.
        
        Args:
            filter_start_date: Start date for filtering (YYYY-MM-DD format)
            
        Returns:
            Wide format DataFrame with date index and symbol_metric columns
        """
        if not self.con:
            logger.error("No database connection. Call connect() first.")
            return pd.DataFrame()
            
        try:
            # Get data in long format first
            query = f"""
            SELECT date, symbol_metric, value
            FROM (
                -- Same normalized_data CTE as above
                SELECT date, symbol || '_open' as symbol_metric, open as value 
                FROM stg_yahoo WHERE open IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_high' as symbol_metric, high as value 
                FROM stg_yahoo WHERE high IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_low' as symbol_metric, low as value 
                FROM stg_yahoo WHERE low IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_close' as symbol_metric, close as value 
                FROM stg_yahoo WHERE close IS NOT NULL
                UNION ALL
                SELECT date, symbol || '_volume' as symbol_metric, volume as value 
                FROM stg_yahoo WHERE volume IS NOT NULL
                UNION ALL
                SELECT date, series_id as symbol_metric, value 
                FROM stg_fred WHERE value IS NOT NULL
                UNION ALL
                SELECT date, series_id as symbol_metric, value 
                FROM stg_eia WHERE value IS NOT NULL
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_baker WHERE value IS NOT NULL
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_finra WHERE value IS NOT NULL
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_sp500 WHERE value IS NOT NULL
                UNION ALL
                SELECT date, 
                       CASE WHEN metric = 'value' THEN symbol 
                            ELSE symbol || '_' || metric END as symbol_metric, 
                       value 
                FROM stg_usda WHERE value IS NOT NULL
            ) 
            WHERE date >= '{filter_start_date}'
            ORDER BY date, symbol_metric
            """
            
            logger.info("Extracting data in long format...")
            df_long = self.con.execute(query).df()
            logger.info(f"Long format shape: {df_long.shape}")
            
            if df_long.empty:
                logger.warning("Query returned empty DataFrame")
                return df_long
            
            # Convert to wide format using pandas pivot
            logger.info("Pivoting to wide format with pandas...")
            df_long['date'] = pd.to_datetime(df_long['date'])
            
            df_wide = df_long.pivot_table(
                index='date',
                columns='symbol_metric',
                values='value',
                aggfunc='mean'  # Handle duplicates if any
            )
            
            logger.info(f"Wide format shape after pivot: {df_wide.shape}")
            return df_wide
            
        except Exception as e:
            logger.error(f"Error in staged pivot approach: {e}")
            return pd.DataFrame()
    
    def interpolate_and_process(self, 
                               df_wide: pd.DataFrame,
                               usrec_symbol: str = 'USREC') -> pd.DataFrame:
        """
        Apply interpolation and special processing to wide format data
        
        Args:
            df_wide: Wide format DataFrame with date index
            usrec_symbol: Symbol for recession indicator (gets forward fill)
            
        Returns:
            Processed DataFrame with interpolation applied
        """
        if df_wide.empty:
            logger.warning("Empty DataFrame provided for interpolation")
            return df_wide
            
        logger.info("Starting interpolation and processing...")
        
        # Create a copy to avoid modifying original
        df_processed = df_wide.copy()
        
        # Ensure we have daily frequency
        if not df_processed.empty:
            min_date = df_processed.index.min()
            max_date = df_processed.index.max()
            
            if pd.notna(min_date) and pd.notna(max_date):
                daily_range = pd.date_range(start=min_date, end=max_date, freq='D')
                df_processed = df_processed.reindex(daily_range)
                logger.info(f"Reindexed to daily frequency: {len(daily_range)} days")
        
        # Handle USREC with forward fill (recession indicator)
        if usrec_symbol in df_processed.columns:
            logger.info(f"Applying forward fill to {usrec_symbol}")
            df_processed[usrec_symbol] = df_processed[usrec_symbol].ffill()
        
        # Apply linear interpolation to all other numeric columns
        numeric_cols = df_processed.select_dtypes(include=[np.number]).columns
        interpolation_cols = [col for col in numeric_cols if col != usrec_symbol]
        
        if interpolation_cols:
            logger.info(f"Applying linear interpolation to {len(interpolation_cols)} columns")
            df_processed[interpolation_cols] = df_processed[interpolation_cols].interpolate(
                method='linear', 
                limit_direction='both'
            )
        
        # Log interpolation results
        total_nulls_before = df_wide.isnull().sum().sum()
        total_nulls_after = df_processed.isnull().sum().sum()
        logger.info(f"Nulls before interpolation: {total_nulls_before}")
        logger.info(f"Nulls after interpolation: {total_nulls_after}")
        
        return df_processed
    
    def run_interpolation(self, 
                         output_path: str = 'data/silver/interpolated_wide_data.parquet',
                         filter_start_date: str = '1950-01-01',
                         usrec_symbol: str = 'USREC',
                         method: str = 'direct') -> pd.DataFrame:
        """
        Complete interpolation pipeline
        
        Args:
            output_path: Where to save the interpolated data (Parquet format)
            filter_start_date: Start date for filtering
            usrec_symbol: Recession indicator symbol  
            method: 'direct' (SQL pivot) or 'staged' (pandas pivot)
            
        Returns:
            Final interpolated DataFrame
        """
        logger.info("🚀 Starting DuckDB-based interpolation pipeline")
        logger.info(f"📅 Filter start date: {filter_start_date}")
        logger.info(f"🔧 Method: {method}")
        
        start_time = time.time()
        
        try:
            # Connect to database
            if not self.connect():
                return pd.DataFrame()
            
            # Extract and pivot data
            if method == 'direct':
                df_wide = self.get_wide_format_data_direct(filter_start_date, usrec_symbol)
            else:
                df_wide = self.get_wide_format_data_staged(filter_start_date)
            
            if df_wide.empty:
                logger.error("No data extracted from database")
                return df_wide
            
            # Apply interpolation
            df_interpolated = self.interpolate_and_process(df_wide, usrec_symbol)
            
            # Save results (optional)
            if not df_interpolated.empty and output_path:
                output_path = Path(output_path)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                logger.info(f"💾 Saving results to {output_path}")
                # Save as Parquet for better performance and compression
                df_interpolated.to_parquet(
                    output_path, 
                    engine='pyarrow',
                    compression='snappy',
                    index=True
                )
                
                # Log file size for comparison
                file_size_mb = output_path.stat().st_size / (1024 * 1024)
                logger.info(f"📦 Parquet file size: {file_size_mb:.2f} MB")
                
                # Summary statistics
                logger.info(f"📊 Final shape: {df_interpolated.shape}")
                logger.info(f"📅 Date range: {df_interpolated.index.min()} to {df_interpolated.index.max()}")
                logger.info(f"📈 Columns: {len(df_interpolated.columns)}")
                
                # Check for common symbols
                gspc_cols = [col for col in df_interpolated.columns if '^GSPC' in col.upper()]
                if gspc_cols:
                    logger.info(f"🏛️ S&P 500 columns found: {gspc_cols}")
                
                if usrec_symbol in df_interpolated.columns:
                    nulls = df_interpolated[usrec_symbol].isnull().sum()
                    logger.info(f"📉 {usrec_symbol} nulls after processing: {nulls}")
            
            total_time = time.time() - start_time
            logger.info(f"⏱️ Total execution time: {total_time:.2f} seconds")
            logger.info("✅ Interpolation pipeline completed successfully!")
            
            return df_interpolated
            
        except Exception as e:
            logger.error(f"❌ Error in interpolation pipeline: {e}")
            return pd.DataFrame()
        
        finally:
            if self.con:
                self.con.close()
                logger.info("🔌 Database connection closed")

# Convenience functions for backward compatibility
def interpolate_data_from_duckdb(
    db_path: str = 'bristol_gate.duckdb',
    filter_start_date: str = '1950-01-01',
    usrec_symbol: str = 'USREC',
    method: str = 'direct'
) -> pd.DataFrame:
    """
    Main function for interpolating data from DuckDB staging tables
    
    Args:
        db_path: Path to DuckDB database
        filter_start_date: Start date for filtering (YYYY-MM-DD format)
        usrec_symbol: Symbol for recession indicator
        method: 'direct' for SQL pivot, 'staged' for pandas pivot
        
    Returns:
        Interpolated wide format DataFrame
    """
    interpolator = DuckDBInterpolator(db_path)
    return interpolator.run_interpolation(
        filter_start_date=filter_start_date,
        usrec_symbol=usrec_symbol,
        method=method
    )

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='DuckDB-based Data Interpolation Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python interpolate_data.py                                    # Use direct SQL method, save to data/silver/
    python interpolate_data.py --method staged                   # Use pandas pivot method  
    python interpolate_data.py --start-date 2020-01-01           # Filter from specific date
    python interpolate_data.py --output data/silver/my_data.parquet  # Custom output file

Methods:
    direct: Uses DuckDB's PIVOT for maximum efficiency (recommended)
    staged: Extracts to long format first, then pivots with pandas (debugging)

Output:
    • Saves in Parquet format for better performance and compression
    • Uses Snappy compression for optimal balance of speed/size
    • Stored in data/silver/ following medallion architecture
        """
    )
    
    parser.add_argument('--db-path', default='bristol_gate.duckdb',
                       help='Path to DuckDB database file')
    parser.add_argument('--output', default='data/silver/interpolated_wide_data.parquet',
                       help='Output Parquet file path')
    parser.add_argument('--start-date', default='1950-01-01',
                       help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--usrec-symbol', default='USREC',
                       help='Recession indicator symbol')
    parser.add_argument('--method', choices=['direct', 'staged'], default='direct',
                       help='Interpolation method')
    
    args = parser.parse_args()
    
    # Run interpolation
    result = interpolate_data_from_duckdb(
        db_path=args.db_path,
        filter_start_date=args.start_date,
        usrec_symbol=args.usrec_symbol,
        method=args.method
    )
    
    if not result.empty:
        print(f"\n🎉 Success! Interpolated data saved to: {args.output}")
        print(f"📊 Shape: {result.shape}")
        print(f"📅 Date range: {result.index.min()} to {result.index.max()}")
    else:
        print("\n❌ Interpolation failed or returned empty data") 