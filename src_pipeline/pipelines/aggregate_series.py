"""
Aggregate Series Creator for Bristol Gate Pipeline

This module creates computed/aggregate series from interpolated wide-format data
and inserts the new symbol metadata into the DuckDB symbols table.
"""

import polars as pl
import pandas as pd
import duckdb
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import shutil

from ..core.date_utils import DateUtils

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_timestamped_path(base_path: str) -> tuple[Path, Path]:
    """
    Generate timestamped file path and latest file path
    
    Args:
        base_path: Base path like 'data/silver/final_aggregated_data.parquet'
        
    Returns:
        Tuple of (timestamped_path, latest_path)
    """
    base_path = Path(base_path)
    
    # Use DateUtils for consistent timestamp formatting
    timestamp = DateUtils.generate_timestamp_string()
    
    # Create timestamped filename
    stem = base_path.stem
    suffix = base_path.suffix
    
    timestamped_name = f"{stem}_{timestamp}{suffix}"
    timestamped_path = base_path.parent / timestamped_name
    
    # Keep the original path as "latest"
    latest_path = base_path
    
    return timestamped_path, latest_path

class AggregateSeriesCreator:
    """
    Creates aggregate/computed series from interpolated wide-format data
    and manages symbol metadata in DuckDB
    """
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        """
        Initialize with DuckDB connection
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.aggregations_config = self._get_aggregations_config()
        self.con: Optional[duckdb.DuckDBPyConnection] = None
    
    def connect(self) -> bool:
        """Establish connection to DuckDB database"""
        try:
            if not self.db_path.exists():
                logger.error(f"Database file not found: {self.db_path}")
                return False
                
            self.con = duckdb.connect(database=str(self.db_path), read_only=False)
            logger.info(f"Connected to DuckDB: {self.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            return False
    
    def get_interpolated_data_direct(self, 
                                   filter_start_date: str = '1950-01-01',
                                   usrec_symbol: str = 'USREC') -> pd.DataFrame:
        """
        Get interpolated data directly using the existing connection
        
        Args:
            filter_start_date: Start date for filtering (YYYY-MM-DD format)
            usrec_symbol: Symbol for recession indicator (for special handling)
            
        Returns:
            Interpolated wide format DataFrame with date index
        """
        if not self.con:
            logger.error("No database connection. Call connect() first.")
            return pd.DataFrame()
            
        try:
            logger.info("📊 Extracting and interpolating data from DuckDB...")
            
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
            
            df_wide = self.con.execute(query).df()
            logger.info(f"Raw query result shape: {df_wide.shape}")
            logger.info(f"Columns: {list(df_wide.columns)}")
            
            if df_wide.empty:
                logger.warning("Query returned empty DataFrame")
                return df_wide
            
            # Check if date column exists
            if 'date' not in df_wide.columns:
                logger.error("❌ Date column missing from query result")
                logger.error(f"Available columns: {list(df_wide.columns)}")
                return pd.DataFrame()
                
            # Convert date column to datetime and set as index
            df_wide['date'] = pd.to_datetime(df_wide['date'])
            df_wide.set_index('date', inplace=True)
            logger.info(f"Date index set. Shape: {df_wide.shape}")
            
            # Apply interpolation
            df_interpolated = self._apply_interpolation(df_wide, usrec_symbol)
            
            logger.info(f"✅ Interpolation complete. Final shape: {df_interpolated.shape}")
            return df_interpolated
            
        except Exception as e:
            logger.error(f"Error in data extraction and interpolation: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _apply_interpolation(self, df_wide: pd.DataFrame, usrec_symbol: str = 'USREC') -> pd.DataFrame:
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
            
        logger.info("🔄 Applying interpolation...")
        
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
        import numpy as np
        numeric_cols = df_processed.select_dtypes(include=[np.number]).columns
        interpolation_cols = [col for col in numeric_cols if col != usrec_symbol]
        
        if interpolation_cols:
            logger.info(f"Applying linear interpolation to {len(interpolation_cols)} columns")
            df_processed[interpolation_cols] = df_processed[interpolation_cols].interpolate(
                method='linear', 
                limit_direction='both'
            )
        
        return df_processed
    
    def create_aggregate_series(self, 
                              df_interpolated: pd.DataFrame) -> pd.DataFrame:
        """
        Create aggregate series from interpolated data and update DuckDB symbols table
        
        Args:
            df_interpolated: Wide format DataFrame with date index from interpolation
            
        Returns:
            Enhanced DataFrame with original + aggregate series
        """
        logger.info("🧮 Starting aggregate series creation...")
        
        if df_interpolated.empty:
            logger.warning("Empty DataFrame provided")
            return df_interpolated
        
        # Ensure the DataFrame has the expected structure
        logger.info(f"Input DataFrame shape: {df_interpolated.shape}")
        logger.info(f"Index name: {df_interpolated.index.name}")
        logger.info(f"Sample columns: {list(df_interpolated.columns)[:5]}")
        
        # Convert to Polars for efficient computation
        # Reset index to get date as a column, then convert
        df_with_date = df_interpolated.reset_index()
        
        # Ensure date column is properly named
        if df_with_date.columns[0] != 'date':
            df_with_date.columns = ['date'] + list(df_with_date.columns[1:])
        
        logger.info(f"DataFrame for Polars conversion shape: {df_with_date.shape}")
        logger.info(f"Columns: {list(df_with_date.columns)[:5]}")
        
        df_data_pl = pl.from_pandas(df_with_date)
        
        # Create aggregate series
        new_symbols_metadata = []
        successful_aggregates = 0
        skipped_aggregates = 0
        
        for new_col_name, agg_details in self.aggregations_config.items():
            try:
                # Check if all required components exist
                missing_components = [comp for comp in agg_details["components"] 
                                    if comp not in df_data_pl.columns]
                
                if missing_components:
                    logger.warning(f"⚠️ Skipping {new_col_name}: missing {missing_components}")
                    skipped_aggregates += 1
                    continue
                
                # Calculate the new series
                series_expr = agg_details["expr_lambda"](df_data_pl)
                df_data_pl = df_data_pl.with_columns(series_expr.alias(new_col_name))
                
                # Create symbols table entry (matching DuckDB schema)
                new_symbol_entry = {
                    "symbol": new_col_name,
                    "source": "Calc",
                    "description": agg_details["description"],
                    "unit": agg_details["unit"]
                }
                new_symbols_metadata.append(new_symbol_entry)
                successful_aggregates += 1
                
                if successful_aggregates % 10 == 0:
                    logger.info(f"✅ Created {successful_aggregates} aggregate series...")
                    
            except Exception as e:
                logger.error(f"❌ Error creating {new_col_name}: {e}")
                skipped_aggregates += 1
                continue
        
        logger.info(f"✅ Successfully created {successful_aggregates} aggregate series")
        logger.info(f"⚠️ Skipped {skipped_aggregates} aggregate series due to missing data or errors")
        
        # Insert new symbols into DuckDB symbols table
        if new_symbols_metadata and self.con:
            self._insert_symbols_to_duckdb(new_symbols_metadata)
        
        # Convert back to pandas with date index
        df_final_pandas = df_data_pl.to_pandas()
        
        # Ensure date column exists before setting as index
        if 'date' not in df_final_pandas.columns:
            logger.error("❌ Date column missing in final DataFrame")
            logger.error(f"Available columns: {list(df_final_pandas.columns)}")
            return pd.DataFrame()
        
        df_data_final = df_final_pandas.set_index('date')
        
        logger.info(f"🎉 Aggregate series creation complete!")
        logger.info(f"📊 Final dataset shape: {df_data_final.shape}")
        
        return df_data_final
    
    def _insert_symbols_to_duckdb(self, symbols_metadata: List[Dict[str, Any]]) -> bool:
        """
        Insert new symbols metadata into DuckDB symbols table
        
        Args:
            symbols_metadata: List of symbol metadata dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not self.con:
                logger.error("No database connection available")
                return False
            
            # Convert to DataFrame for easier handling
            df_symbols = pd.DataFrame(symbols_metadata)
            
            # Check for existing symbols to avoid duplicates
            existing_symbols_df = self.con.execute("SELECT symbol FROM symbols").df()
            existing_symbols = set(existing_symbols_df['symbol'].tolist()) if not existing_symbols_df.empty else set()
            
            # Filter out existing symbols
            new_symbols_df = df_symbols[~df_symbols['symbol'].isin(existing_symbols)]
            
            if new_symbols_df.empty:
                logger.info("📊 All symbols already exist in database")
                return True
            
            logger.info(f"📊 Inserting {len(new_symbols_df)} new symbols into database...")
            
            # Insert new symbols
            self.con.execute("""
                INSERT INTO symbols (symbol, source, description, unit)
                SELECT symbol, source, description, unit FROM new_symbols_df
            """)
            
            logger.info(f"✅ Successfully inserted {len(new_symbols_df)} symbols into database")
            
            # Verify insertion
            total_symbols = self.con.execute("SELECT COUNT(*) as count FROM symbols").df()['count'].iloc[0]
            logger.info(f"📊 Total symbols in database: {total_symbols}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting symbols into database: {e}")
            return False
    
    def run_full_pipeline(self,
                         filter_start_date: str = '1950-01-01',
                         usrec_symbol: str = 'USREC',
                         output_path: Optional[str] = None,
                         close_connection: bool = True) -> pd.DataFrame:
        """
        Complete pipeline: interpolation → aggregate series → database updates
        Uses a single DuckDB connection throughout
        
        Args:
            filter_start_date: Start date for filtering
            usrec_symbol: Recession indicator symbol
            output_path: Optional path to save final data (Parquet format)
            close_connection: Whether to close the connection when done (default: True)
            
        Returns:
            Final DataFrame with original + aggregate series
        """
        logger.info("🚀 Starting full aggregate series pipeline...")
        
        try:
            # Connect to database
            if not self.connect():
                return pd.DataFrame()
            
            # Step 1: Get interpolated data using existing connection
            logger.info("📊 Step 1: Extracting and interpolating data...")
            df_interpolated = self.get_interpolated_data_direct(
                filter_start_date=filter_start_date,
                usrec_symbol=usrec_symbol
            )
            
            if df_interpolated.empty:
                logger.error("❌ Data extraction/interpolation returned empty data")
                return df_interpolated
            
            # Step 2: Create aggregate series
            logger.info("🧮 Step 2: Creating aggregate series...")
            df_enhanced = self.create_aggregate_series(df_interpolated)
            
            if df_enhanced.empty:
                logger.error("❌ Aggregate series creation failed")
                return df_enhanced
            
            # Step 3: Save final results with timestamps (optional)
            if output_path:
                timestamped_path, latest_path = generate_timestamped_path(output_path)
                
                # Ensure output directory exists
                timestamped_path.parent.mkdir(parents=True, exist_ok=True)
                
                logger.info(f"💾 Saving enhanced data with timestamp...")
                
                # Save timestamped version
                df_enhanced.to_parquet(
                    timestamped_path,
                    engine='pyarrow',
                    compression='snappy',
                    index=True
                )
                
                # Create/update latest version
                shutil.copy2(timestamped_path, latest_path)
                
                # Log file sizes
                timestamped_size_mb = timestamped_path.stat().st_size / (1024 * 1024)
                latest_size_mb = latest_path.stat().st_size / (1024 * 1024)
                
                logger.info(f"📦 Timestamped file: {timestamped_path} ({timestamped_size_mb:.2f} MB)")
                logger.info(f"📦 Latest file: {latest_path} ({latest_size_mb:.2f} MB)")
                logger.info(f"🕐 Timestamp: {DateUtils.format_current_datetime()}")
            
            logger.info("🎉 Full pipeline completed successfully!")
            logger.info(f"📊 Final shape: {df_enhanced.shape}")
            
            return df_enhanced
            
        except Exception as e:
            logger.error(f"❌ Error in full pipeline: {e}")
            return pd.DataFrame()
        
        finally:
            if self.con and close_connection:
                self.con.close()
                logger.info("🔌 Database connection closed")
            elif self.con and not close_connection:
                logger.debug("🔌 Database connection kept open for caller")
    
    def _get_aggregations_config(self) -> Dict[str, Dict[str, Any]]:
        """
        Define all aggregate series calculations
        
        Returns:
            Dictionary with aggregation definitions
        """
        return {
            "RSALESAGG": {
                "components": ["RRSFS", "RSALES"],
                "expr_lambda": lambda df: (pl.col("RRSFS") + pl.col("RSALES")) / 2,
                "description": "Real Retail and Food Services Sales (Mean of RRSFS and RSALES)",
                "unit": "Millions of Dollars"
            },
            "BUSLOANS_minus_BUSLOANSNSA": {
                "components": ["BUSLOANS", "BUSLOANSNSA"],
                "expr_lambda": lambda df: pl.col("BUSLOANS") - pl.col("BUSLOANSNSA"),
                "description": "Business Loans (Monthly) SA - NSA",
                "unit": "Billions of U.S. Dollars"
            },
            "BUSLOANS_minus_BUSLOANSNSA_by_GDP": {
                "components": ["BUSLOANS_minus_BUSLOANSNSA", "GDP"],
                "expr_lambda": lambda df: (pl.col("BUSLOANS_minus_BUSLOANSNSA") / pl.col("GDP")) * 100,
                "description": "Business Loans (Monthly) SA - NSA divided by GDP",
                "unit": "Percent"
            },
            "BUSLOANS_by_GDP": {
                "components": ["BUSLOANS", "GDP"],
                "expr_lambda": lambda df: (pl.col("BUSLOANS") / pl.col("GDP")) * 100,
                "description": "Business Loans (Monthly, SA) Normalized by GDP",
                "unit": "Percent"
            },
            "BUSLOANS_INTEREST": {
                "components": ["BUSLOANS", "DGS10"],
                "expr_lambda": lambda df: (pl.col("BUSLOANS") * pl.col("DGS10")) / 100,
                "description": "Business Loans (Monthly, SA) Adjusted Interest Burdens (using DGS10)",
                "unit": "Calculated Billions of U.S. Dollars"
            },
            "BUSLOANS_INTEREST_by_GDP": {
                "components": ["BUSLOANS_INTEREST", "GDP"],
                "expr_lambda": lambda df: (pl.col("BUSLOANS_INTEREST") / pl.col("GDP")) * 100,
                "description": "Business Loans (Monthly, SA) Adjusted Interest Burden Divided by GDP",
                "unit": "Percent"
            },
            "W875RX1_by_GDP": {
                "components": ["W875RX1", "GDP"],
                "expr_lambda": lambda df: (pl.col("W875RX1") / pl.col("GDP")) * 100,
                "description": "Real Personal Income Normalized by GDP",
                "unit": "Percent"
            },
            "PI_by_GDP": {
                "components": ["PI", "GDP"],
                "expr_lambda": lambda df: (pl.col("PI") / pl.col("GDP")) * 100,
                "description": "Personal Income (SA) Normalized by GDP",
                "unit": "Percent"
            },
            "CPROFIT_by_GDP": {
                "components": ["CPROFIT", "GDP"],
                "expr_lambda": lambda df: (pl.col("CPROFIT") / pl.col("GDP")) * 100,
                "description": "National income: Corporate profits before tax (with IVA and CCAdj) Normalized by GDP",
                "unit": "Percent"
            },
            "TOTLNNSA": {
                "components": ["BUSLOANS", "REALLNNSA", "CONSUMERNSA"],
                "expr_lambda": lambda df: pl.col("BUSLOANS") + pl.col("REALLNNSA") + pl.col("CONSUMERNSA"),
                "description": "Total Loans, Not Seasonally Adjusted (BUSLOANS + REALLNNSA + CONSUMERNSA)",
                "unit": "Billions of U.S. Dollars"
            },
            "TOTLNNSA_by_GDP": {
                "components": ["TOTLNNSA", "GDP"],
                "expr_lambda": lambda df: (pl.col("TOTLNNSA") / pl.col("GDP")) * 100,
                "description": "Total Loans, Not Seasonally Adjusted, divided by GDP",
                "unit": "Percent"
            },
            "WRESBAL_by_GDP": {
                "components": ["WRESBAL", "GDP"],
                "expr_lambda": lambda df: (pl.col("WRESBAL") / pl.col("GDP")) * 100,
                "description": "Reserve Balances with Federal Reserve Banks Divided by GDP",
                "unit": "Percent"
            },
            "DGS30_to_DGS10": {
                "components": ["DGS30", "DGS10"],
                "expr_lambda": lambda df: pl.col("DGS30") - pl.col("DGS10"),
                "description": "Yield Curve: 30-Year Treasury Constant Maturity Minus 10-Year Treasury Constant Maturity",
                "unit": "Percent"
            },
            "DGS10_to_DGS2": {
                "components": ["DGS10", "DGS2"],
                "expr_lambda": lambda df: pl.col("DGS10") - pl.col("DGS2"),
                "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity",
                "unit": "Percent"
            },
            "DGS10_to_TB3MS": {
                "components": ["DGS10", "TB3MS"],
                "expr_lambda": lambda df: pl.col("DGS10") - pl.col("TB3MS"),
                "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 3-Month Treasury Bill Secondary Market Rate",
                "unit": "Percent"
            },
            "AAA_div_DGS10": {
                "components": ["AAA", "DGS10"],
                "expr_lambda": lambda df: pl.col("AAA") / pl.col("DGS10"),
                "description": "Moody's Seasoned Aaa Corporate Bond Yield Relative to 10-Year Treasury Constant Maturity (AAA/DGS10)",
                "unit": "Ratio"
            },
            "UNEMPLOY_by_POPTHM": {
                "components": ["UNEMPLOY", "POPTHM"],
                "expr_lambda": lambda df: (pl.col("UNEMPLOY") / pl.col("POPTHM")) * 100,
                "description": "Unemployment Level (SA) / Population",
                "unit": "%"
            },
            "U6_to_U3": {
                "components": ["U6RATE", "UNRATE"],
                "expr_lambda": lambda df: pl.col("U6RATE") - pl.col("UNRATE"),
                "description": "U-6 Unemployment Rate Minus U-3 Unemployment Rate (U6RATE - UNRATE)",
                "unit": "%"
            },
            "DCOILWTICO_by_PPIACO": {
                "components": ["DCOILWTICO", "PPIACO"],
                "expr_lambda": lambda df: pl.col("DCOILWTICO") / pl.col("PPIACO"),
                "description": "Crude Oil WTI Price Normalized by Producer Price Index: All Commodities",
                "unit": "$/bbl/Index"
            },
            "GDP_by_POPTHM": {
                "components": ["GDP", "POPTHM"],
                "expr_lambda": lambda df: (pl.col("GDP") * 1_000_000) / pl.col("POPTHM"),
                "description": "GDP per Capita",
                "unit": "$/person"
            },
            "GDP_by_CPIAUCSL": {
                "components": ["GDP", "CPIAUCSL"],
                "expr_lambda": lambda df: pl.col("GDP") / (pl.col("CPIAUCSL") / 100),
                "description": "GDP Deflated by CPI (CPIAUCSL)",
                "unit": "Billions of Constant Dollars"
            },
            "GDP_by_CPIAUCSL_by_POPTHM": {
                "components": ["GDP_by_CPIAUCSL", "POPTHM"],
                "expr_lambda": lambda df: (pl.col("GDP_by_CPIAUCSL") * 1_000_000) / pl.col("POPTHM"),
                "description": "GDP Deflated by CPI, per Capita",
                "unit": "Constant $/Person"
            },
            "GSPC_Close_by_MDY_Close": {
                "components": ["^GSPC_close", "MDY_close"],
                "expr_lambda": lambda df: pl.col("^GSPC_close") / pl.col("MDY_close"),
                "description": "S&P 500 Close Normalized by S&P MidCap 400 Close",
                "unit": "Ratio"
            },
            "QQQ_Close_by_MDY_Close": {
                "components": ["QQQ_close", "MDY_close"],
                "expr_lambda": lambda df: pl.col("QQQ_close") / pl.col("MDY_close"),
                "description": "Nasdaq 100 Close (QQQ) Normalized by S&P MidCap 400 Close",
                "unit": "Ratio"
            },
            "GSPC_DailySwing": {
                "components": ["^GSPC_high", "^GSPC_low", "^GSPC_open"],
                "expr_lambda": lambda df: (pl.col("^GSPC_high") - pl.col("^GSPC_low")) / pl.col("^GSPC_open").replace(0, None),
                "description": "S&P 500 (GSPC) Daily Swing: (High - Low) / Open",
                "unit": "Ratio"
            },
            "GSPC_Close_by_GDPDEF": {
                "components": ["^GSPC_close", "GDPDEF"],
                "expr_lambda": lambda df: pl.col("^GSPC_close") / (pl.col("GDPDEF") / 100),
                "description": "S&P 500 (GSPC) Close Deflated by GDP Deflator",
                "unit": "Constant Dollars"
            },
            "GSPC_open_by_GDPDEF": {
                "components": ["^GSPC_open", "GDPDEF"],
                "expr_lambda": lambda df: pl.col("^GSPC_open") / (pl.col("GDPDEF") / 100),
                "description": "S&P 500 (GSPC) Open Deflated by GDP Deflator",
                "unit": "Constant Dollars"
            },
            "HOUST_div_POPTHM": {
                "components": ["HOUST", "POPTHM"],
                "expr_lambda": lambda df: pl.col("HOUST") / pl.col("POPTHM"),
                "description": "Housing Starts per Capita (Thousands of Units SAAR / Thousands of Persons)",
                "unit": "Starts per 1000 Persons"
            },
            "MSPUS_times_HOUST": {
                "components": ["MSPUS", "HOUST"],
                "expr_lambda": lambda df: (pl.col("MSPUS") * pl.col("HOUST")) / 1000,
                "description": "Median Sales Price of New Houses Sold times Housing Starts (Value of New Construction Started)",
                "unit": "Millions of Dollars"
            },
            "FARMINCOME_by_GDP": {
                "components": ["USDA_NET_FARM_INCOME", "GDP"],
                "expr_lambda": lambda df: (pl.col("USDA_NET_FARM_INCOME") / pl.col("GDP")) * 100,
                "description": "Farm Income (Annual, NSA) Divided by GDP",
                "unit": "Percent"
            },
            "GSG_Close_by_GDPDEF": {
                "components": ["GSG_close", "GDPDEF"],
                "expr_lambda": lambda df: pl.col("GSG_close") / pl.col("GDPDEF"),
                "description": "GSCI Commodity-Indexed Trust (GSG Close) Normalized by GDP Deflator",
                "unit": "Ratio"
            },
            "GSG_Close_by_GSPC_Close": {
                "components": ["GSG_close", "^GSPC_close"],
                "expr_lambda": lambda df: pl.col("GSG_close") / pl.col("^GSPC_close"),
                "description": "GSCI Commodity-Indexed Trust (GSG Close) Normalized by S&P 500 Close (GSPC Close)",
                "unit": "Ratio"
            }
        }

# Convenience function for backward compatibility
def create_aggregate_series_from_interpolated_data(
    db_path: str = 'bristol_gate.duckdb',
    filter_start_date: str = '1950-01-01',
    usrec_symbol: str = 'USREC',
    interpolation_method: str = 'direct',  # Not used anymore, kept for compatibility
    output_path: Optional[str] = 'data/silver/final_aggregated_data.parquet'
) -> pd.DataFrame:
    """
    Create aggregate series using the full integrated pipeline
    
    Args:
        db_path: Path to DuckDB database
        filter_start_date: Start date for filtering
        usrec_symbol: Recession indicator symbol
        interpolation_method: Ignored (kept for compatibility)
        output_path: Optional path to save final data (Parquet format with timestamps)
        
    Returns:
        Enhanced DataFrame with original + aggregate series
    """
    creator = AggregateSeriesCreator(db_path)
    return creator.run_full_pipeline(
        filter_start_date=filter_start_date,
        usrec_symbol=usrec_symbol,
        output_path=output_path
    )

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Aggregate Series Creator for Bristol Gate Pipeline (DuckDB Integration)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python -m src_pipeline.aggregate_series                           # Full pipeline with defaults
    python -m src_pipeline.aggregate_series --start-date 2020-01-01   # Filter from specific date
    python -m src_pipeline.aggregate_series --output data/silver/final_aggregated_data.parquet   # Save to custom file

What this does:
    • Loads data from DuckDB staging tables
    • Applies interpolation to create daily time series
    • Creates aggregate/computed series (ratios, differences, normalized values)
    • Inserts new symbol metadata into DuckDB symbols table
    • Saves timestamped and latest versions of enhanced dataset

File Output:
    • Creates timestamped file: final_aggregated_data_20250128_143022.parquet
    • Creates/updates latest file: final_aggregated_data.parquet
        """
    )
    
    parser.add_argument('--db-path', default='bristol_gate.duckdb',
                       help='Path to DuckDB database file')
    parser.add_argument('--start-date', default='1950-01-01',
                       help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--usrec-symbol', default='USREC',
                       help='Recession indicator symbol')
    parser.add_argument('--output', default='data/silver/final_aggregated_data.parquet',
                       help='Output path for enhanced data Parquet file')
    
    args = parser.parse_args()
    
    # Run full pipeline
    result = create_aggregate_series_from_interpolated_data(
        db_path=args.db_path,
        filter_start_date=args.start_date,
        usrec_symbol=args.usrec_symbol,
        output_path=args.output
    )
    
    if not result.empty:
        print(f"\n🎉 Success! Enhanced data with aggregate series created!")
        print(f"📊 Shape: {result.shape}")
        print(f"📅 Date range: {result.index.min()} to {result.index.max()}")
        print(f"💾 Files created with timestamp and latest versions")
    else:
        print("\n❌ Pipeline failed or returned empty data")
        exit(1) 