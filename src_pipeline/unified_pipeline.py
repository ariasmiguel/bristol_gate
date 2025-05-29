"""
Unified Data Pipeline - Bristol Gate
Flexible pipeline with two modes:
1. Load from silver layer (fast for development/testing)
2. Run complete pipeline from DuckDB (full production pipeline)

Integrates with existing feature calculation logic from features.py and features_parallel.py
"""

import time
import pandas as pd
import polars as pl
import logging
import concurrent.futures
import math
import duckdb
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime

from .interpolate_data import DuckDBInterpolator
from .aggregate_series import AggregateSeriesCreator, generate_timestamped_path
from .feature_utils import (
    apply_savgol_filter,
    get_symbol_metadata_details
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DomainFeaturesCreator:
    """
    Creates domain-specific derived features based on financial and economic knowledge
    
    This class handles:
    - Combination features (e.g., TOTLNNSA + WRESBAL)
    - Yield curve features (e.g., GDP_YoY - DGS1)
    - Normalized/ratio features (e.g., S&P 500 / GDP)
    - Return and equity calculations
    - Advanced smoothing and derivatives
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__ + '.DomainFeaturesCreator')
        
        # Domain-specific feature definitions from the original R script
        self.domain_features = {
            "TOTLNNSA_plus_WRESBAL": {
                "components": ["TOTLNNSA", "WRESBAL"],
                "expr_lambda": lambda df: pl.col("TOTLNNSA") + pl.col("WRESBAL"),
                "description": "Total Loans Plus All Reserves (TOTLNNSA + WRESBAL)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "TOTLLNSA_plus_WRESBAL": {
                "components": ["TOTLLNSA", "WRESBAL"],
                "expr_lambda": lambda df: pl.col("TOTLLNSA") + pl.col("WRESBAL"),
                "description": "Total Loans Plus All Reserves (TOTLLNSA + WRESBAL)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "GDP_YoY_to_DGS1": {
                "components": ["GDP_YoY", "DGS1"],
                "expr_lambda": lambda df: pl.col("GDP_YoY") - pl.col("DGS1"),
                "description": "Economic Yield Curve, GDP YoY and 1-Year Tr (GDP_YoY-DGS1)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "GDP_YoY_to_TB3MS": {
                "components": ["GDP_YoY", "TB3MS"],
                "expr_lambda": lambda df: pl.col("GDP_YoY") - pl.col("TB3MS"),
                "description": "Economic Yield Curve, GDP YoY and 3-Month Tr (GDP_YoY-TB3MS)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "OPHNFB_YoY_to_DGS1": {
                "components": ["OPHNFB_YoY", "DGS1"],
                "expr_lambda": lambda df: pl.col("OPHNFB_YoY") - pl.col("DGS1"),
                "description": "Productivity Yield Curve, Real output YoY and 1-Year Tr (OPHNFB_YoY-DGS1)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "^GSPC_open_mva200_norm": {
                "components": ["^GSPC_open", "^GSPC_open_mva200"],
                "expr_lambda": lambda df: 100 * (pl.col("^GSPC_open") / pl.col("^GSPC_open_mva200").replace(0, None)),
                "description": "S&P 500 normalized by 200 SMA",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "^GSPC_open_mva050_mva200": {
                "components": ["^GSPC_open_mva050", "^GSPC_open_mva200"],
                "expr_lambda": lambda df: pl.col("^GSPC_open_mva050") - pl.col("^GSPC_open_mva200"),
                "description": "S&P 500 50 SMA - 200 SMA",
                "label_y": "Dollars",
                "source": "CalcFeat"
            },
            "^GSPC_open_mva050_mva200_sig": {
                "components": ["^GSPC_open_mva050_mva200"],
                "expr_lambda": lambda df: (pl.col("^GSPC_open_mva050_mva200") > 0).cast(pl.Int8),
                "description": "Signal S&P 500 50 SMA - 200 SMA (1 if > 0, else 0)",
                "label_y": "-",
                "source": "CalcFeat"
            },
            "UNRATE_smooth_21": {
                "components": ["UNRATE"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("UNRATE"), window_length=21, polyorder=3, deriv=0),
                "description": "Smoothed Civilian Unemployment Rate U-3 (21-period Savitzky-Golay)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "UNRATE_smooth_der2": {
                "components": ["UNRATE"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("UNRATE"), window_length=501, polyorder=3, deriv=2),
                "description": "2nd Derivative of Smoothed U-3 (501-period Savitzky-Golay, p=3, m=2)",
                "label_y": "Percent/period^2",
                "source": "CalcFeat"
            },
            "U6RATE_smooth_21": {
                "components": ["U6RATE"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("U6RATE"), window_length=21, polyorder=3, deriv=0),
                "description": "Smoothed Total Unemployed U-6 (21-period Savitzky-Golay)",
                "label_y": "Percent",
                "source": "CalcFeat"
            },
            "U6RATE_smooth_der2": {
                "components": ["U6RATE"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("U6RATE"), window_length=501, polyorder=3, deriv=2),
                "description": "2nd Derivative of Smoothed U-6 (501-period Savitzky-Golay, p=3, m=2)",
                "label_y": "Percent/period^2",
                "source": "CalcFeat"
            },
            "GSPC_open_Log_smooth_der": {
                "components": ["^GSPC_open_Log"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("^GSPC_open_Log"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed Log Scale S&P 500 Open (501-period Savitzky-Golay, p=3, m=1)",
                "label_y": "log-points/period",
                "source": "CalcFeat"
            },
            "GSPC_open_by_GDPDEF_Log_smooth_der": {
                "components": ["GSPC_open_by_GDPDEF_Log"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("GSPC_open_by_GDPDEF_Log"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed Log S&P 500 Open (Real) by GDP Deflator (501-period Savitzky-Golay, p=3, m=1)",
                "label_y": "log-points/period",
                "source": "CalcFeat"
            },
            "GSPC_open_Log_smooth_der_der": {
                "components": ["GSPC_open_Log_smooth_der"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("GSPC_open_Log_smooth_der"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed GSPC_open_Log_smooth_der (effectively 2nd order effect on GSPC_open_Log)",
                "label_y": "log-points/period^2",
                "source": "CalcFeat"
            },
            "NCBDBIQ027S_Log_Der": {
                "components": ["NCBDBIQ027S_Log"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("NCBDBIQ027S_Log"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed Log Nonfinancial Corporate Business Debt (NCBDBIQ027S_Log)",
                "label_y": "log-points/period",
                "source": "CalcFeat"
            },
            "BUSLOANS_Log_Der": {
                "components": ["BUSLOANS_Log"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("BUSLOANS_Log"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed Log Commercial and Industrial Loans (BUSLOANS_Log)",
                "label_y": "log-points/period",
                "source": "CalcFeat"
            },
            "GPDI_Log_Der": {
                "components": ["GPDI_Log"],
                "expr_lambda": lambda df: apply_savgol_filter(df.get_column("GPDI_Log"), window_length=501, polyorder=3, deriv=1),
                "description": "Derivative of Smoothed Log Gross Private Domestic Investment (GPDI_Log)",
                "label_y": "log-points/period",
                "source": "CalcFeat"
            },
            "GDPSP500": {
                "components": ["^GSPC_close", "GDP"],
                "expr_lambda": lambda df: (pl.col("^GSPC_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
                "description": "S&P 500 (^GSPC_close) / GDP, interpolated",
                "label_y": "Ratio ($/$)",
                "source": "Ratio"
            },
            "RLGSP500": {
                "components": ["RLG_close", "GDP"],
                "expr_lambda": lambda df: (pl.col("RLG_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
                "description": "Russell 2000 (RLG_close) / GDP, interpolated",
                "label_y": "Ratio ($/$)",
                "source": "Ratio"
            },
            "DJISP500": {
                "components": ["DJI_close", "GDP"],
                "expr_lambda": lambda df: (pl.col("DJI_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
                "description": "Dow Jones Industrial Average (DJI_close) / GDP, interpolated",
                "label_y": "Ratio ($/$)",
                "source": "Ratio"
            },
            "GPDI_by_GDP": {
                "components": ["GPDI", "GDP"],
                "expr_lambda": lambda df: (pl.col("GPDI") / pl.col("GDP").replace(0, None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
                "description": "Gross Private Domestic Investment/GDP",
                "label_y": "Ratio ($/$)",
                "source": "Ratio"
            },
            "ret_base": {
                "components": ["^GSPC_close"],
                "expr_lambda": lambda df: pl.col("^GSPC_close").pct_change().fill_null(0),
                "description": "S&P 500 Rate of Change",
                "label_y": "Percent",
                "source": "Calc"
            },
            "ret_base_short_TB3MS": {
                "components": ["TB3MS"],
                "expr_lambda": lambda df: (pl.col("TB3MS") / 365).fill_null(0),
                "description": "Daily Return from 3-Month T-Bill (TB3MS/365)",
                "label_y": "Percent",
                "source": "Calc"
            },
            "eq_base": {
                "components": ["ret_base"],
                "expr_lambda": lambda df: (1 + pl.col("ret_base")).cum_prod(),
                "description": "Equity Return, 100% long S&P 500",
                "label_y": "$1 Invested",
                "source": "Calc"
            },
            "eq_base_short_TB3MS": {
                "components": ["ret_base_short_TB3MS"],
                "expr_lambda": lambda df: (
                    1 + (
                        pl.col("ret_base_short_TB3MS").cum_sum() - 
                        pl.col("ret_base_short_TB3MS").cum_sum().filter(pl.col("ret_base_short_TB3MS").cum_sum().is_not_null()).first().fill_null(0)
                    )
                ),
                "description": "Cumulative Equity from $1 Invested in 3-Month T-Bill (simple interest basis)",
                "label_y": "$1 Invested",
                "source": "Calc"
            }
        }
    
    def create_domain_features(self, 
                              df_data: pl.DataFrame,
                              overall_min_date: Any,
                              overall_max_date: Any) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """
        Create domain-specific derived features from existing data
        
        Args:
            df_data: Polars DataFrame with existing features
            overall_min_date: Minimum date for metadata
            overall_max_date: Maximum date for metadata
            
        Returns:
            Tuple of (updated_dataframe, new_metadata_list)
        """
        self.logger.info(f"🧮 Creating {len(self.domain_features)} domain-specific features...")
        
        new_metadata = []
        processed_df = df_data.clone()
        
        # Get date range for metadata
        series_start_date_str = str(overall_min_date)[:10] if overall_min_date else "Unknown"
        series_end_date_str = str(overall_max_date)[:10] if overall_max_date else "Unknown"
        
        created_count = 0
        skipped_count = 0
        
        # Process features in order (important for dependencies)
        for feature_name, feature_def in self.domain_features.items():
            try:
                # Check if feature already exists
                if feature_name in processed_df.columns:
                    self.logger.debug(f"Feature '{feature_name}' already exists, skipping creation")
                    skipped_count += 1
                    continue
                
                # Check if all required components are available
                missing_components = [comp for comp in feature_def["components"] if comp not in processed_df.columns]
                if missing_components:
                    self.logger.warning(f"Skipping '{feature_name}' - missing components: {missing_components}")
                    skipped_count += 1
                    continue
                
                # Create the feature
                self.logger.debug(f"Creating domain feature: {feature_name}")
                feature_expr_or_series = feature_def["expr_lambda"](processed_df)
                
                if isinstance(feature_expr_or_series, pl.Expr):
                    processed_df = processed_df.with_columns(feature_expr_or_series.alias(feature_name))
                elif isinstance(feature_expr_or_series, pl.Series):
                    processed_df = processed_df.with_columns(feature_expr_or_series.alias(feature_name))
                else:
                    self.logger.error(f"Invalid return type for '{feature_name}': {type(feature_expr_or_series)}")
                    skipped_count += 1
                    continue
                
                # Create metadata
                metadata_entry = {
                    "symbol": feature_name,
                    "source": feature_def.get("source", "CalcFeat"),
                    "description": feature_def["description"],
                    "label_y": feature_def["label_y"],
                    "series_start": series_start_date_str,
                    "series_end": series_end_date_str
                }
                new_metadata.append(metadata_entry)
                created_count += 1
                
                if created_count % 5 == 0:
                    self.logger.debug(f"Created {created_count} domain features so far...")
                
            except Exception as e:
                self.logger.error(f"Error creating domain feature '{feature_name}': {e}")
                skipped_count += 1
                continue
        
        self.logger.info(f"✅ Domain features complete: {created_count} created, {skipped_count} skipped")
        
        if created_count > 0:
            sample_features = list(self.domain_features.keys())[:5]
            self.logger.info(f"   Sample domain features: {', '.join(sample_features)}")
            if len(self.domain_features) > 5:
                self.logger.info(f"   ... and {len(self.domain_features) - 5} more")
        
        return processed_df, new_metadata

class UnifiedDataPipeline:
    """
    Unified pipeline with flexible data loading options
    
    Modes:
    - 'silver': Load from existing silver layer parquet files (fast)
    - 'full': Run complete pipeline from DuckDB staging tables (comprehensive)
    """
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        self.db_path = db_path
        self.interpolator = DuckDBInterpolator(db_path)
        self.aggregate_creator = AggregateSeriesCreator(db_path)
        self.domain_features_creator = DomainFeaturesCreator()
        
    def run_pipeline(self,
                    mode: str = 'silver',
                    silver_data_path: str = 'data/silver/final_aggregated_data.parquet',
                    silver_metadata_path: Optional[str] = None,
                    filter_start_date: str = '1950-01-01',
                    usrec_symbol: str = 'USREC',
                    n_days_year: int = 365,
                    use_parallel: bool = True,
                    num_workers: int = 6,
                    output_path: str = 'data/silver/featured_data.parquet',
                    metadata_output_path: str = 'data/silver/featured_symbols_metadata.csv',
                    save_intermediates: bool = False,
                    update_symbols_table: bool = True,
                    create_domain_features: bool = True) -> pd.DataFrame:
        """
        Unified pipeline with flexible data loading
        
        Args:
            mode: 'silver' (load from parquet) or 'full' (run complete pipeline)
            silver_data_path: Path to final aggregated data parquet file
            silver_metadata_path: Optional path to metadata (auto-detected if None)
            filter_start_date: Start date for analysis (only used in 'full' mode)
            usrec_symbol: Recession indicator symbol  
            n_days_year: Days per year for YoY calculations
            use_parallel: Use parallel feature calculation
            num_workers: Number of parallel workers (ignored if use_parallel=False)
            output_path: Final featured data output path (Parquet format)
            metadata_output_path: Metadata output path
            save_intermediates: Save intermediate results (only used in 'full' mode)
            update_symbols_table: Insert new feature symbols into DuckDB symbols table
            create_domain_features: Create domain-specific derived features
            
        Returns:
            Final featured dataset ready for analysis
        """
        logger.info(f"🚀 Starting unified data pipeline in '{mode}' mode")
        start_time = time.time()
        
        try:
            # Step 1: Load or create aggregated data
            if mode == 'silver':
                df_aggregated_pd, df_metadata_pd = self._load_from_silver(
                    silver_data_path, silver_metadata_path
                )
                if df_aggregated_pd.empty:
                    logger.error("❌ Failed to load data from silver layer")
                    return pd.DataFrame()
                    
            elif mode == 'full':
                df_aggregated_pd, df_metadata_pd = self._run_full_pipeline_to_aggregates(
                    filter_start_date, usrec_symbol, save_intermediates
                )
                if df_aggregated_pd.empty:
                    logger.error("❌ Full pipeline failed or returned empty data")
                    return pd.DataFrame()
                    
            else:
                raise ValueError(f"Invalid mode '{mode}'. Must be 'silver' or 'full'")
            
            # Step 2: Convert to Polars for feature calculation
            logger.info("🔄 Converting to Polars format for feature calculation...")
            df_aggregated_pl = pl.from_pandas(df_aggregated_pd.reset_index())
            
            # Step 3: Calculate basic features using existing feature calculation logic
            logger.info("🧮 Calculating basic features...")
            features_start = time.time()
            
            if use_parallel:
                logger.info(f"⚡ Using parallel processing with {num_workers} workers")
                df_featured_pl, basic_metadata = self._calculate_features_parallel_integrated(
                    df_aggregated_pl, df_metadata_pd, n_days_year, num_workers
                )
            else:
                logger.info("🔄 Using sequential processing")
                df_featured_pl, basic_metadata = self._calculate_features_sequential_integrated(
                    df_aggregated_pl, df_metadata_pd, n_days_year
                )
                
            features_time = time.time() - features_start
            logger.info(f"✅ Basic feature calculation complete: {df_featured_pl.shape} ({features_time:.2f}s)")
            
            # Step 4: Create domain-specific features
            domain_metadata = []
            if create_domain_features:
                logger.info("🧠 Creating domain-specific derived features...")
                domain_start = time.time()
                
                overall_min_date = df_featured_pl["date"].min()
                overall_max_date = df_featured_pl["date"].max()
                
                df_featured_pl, domain_metadata = self.domain_features_creator.create_domain_features(
                    df_featured_pl, overall_min_date, overall_max_date
                )
                
                domain_time = time.time() - domain_start
                logger.info(f"✅ Domain features complete: {df_featured_pl.shape} ({domain_time:.2f}s)")
            else:
                logger.info("⏭️ Skipping domain-specific features creation")
            
            # Step 5: Convert back to pandas and save
            logger.info(f"💾 Saving final results...")
            df_featured_pd = df_featured_pl.to_pandas().set_index('date')
            
            # Save final results as timestamped Parquet only
            timestamped_path, _ = generate_timestamped_path(output_path)
            
            # Ensure output directory exists
            timestamped_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save only timestamped version
            df_featured_pd.to_parquet(
                timestamped_path,
                engine='pyarrow',
                compression='snappy',
                index=True
            )
            
            # Log file size
            timestamped_size_mb = timestamped_path.stat().st_size / (1024 * 1024)
            
            logger.info(f"📦 Saved timestamped file: {timestamped_path.name} ({timestamped_size_mb:.2f} MB)")
            
            # Step 6: Save combined metadata and update symbols table
            all_new_metadata = basic_metadata + domain_metadata
            if df_metadata_pd is not None and all_new_metadata:
                combined_metadata = self._combine_metadata(df_metadata_pd, all_new_metadata)
                metadata_path = Path(metadata_output_path)
                metadata_path.parent.mkdir(parents=True, exist_ok=True)
                combined_metadata.to_csv(metadata_path, index=False)
                logger.info(f"💾 Saved combined metadata to: {metadata_path}")
                
                # Step 7: Update DuckDB symbols table with new feature symbols
                if update_symbols_table:
                    self._update_symbols_table(all_new_metadata)
            
            total_time = time.time() - start_time
            logger.info(f"🎉 Unified pipeline completed successfully! ({total_time:.2f}s total)")
            logger.info(f"📊 Final dataset: {df_featured_pd.shape}")
            logger.info(f"📅 Date range: {df_featured_pd.index.min()} to {df_featured_pd.index.max()}")
            logger.info(f"💾 Saved to: {timestamped_path}")
            
            # Summary of feature types created
            basic_feature_cols = [col for col in df_featured_pd.columns if any(suffix in col for suffix in ['_YoY', '_Log', '_mva', '_Smooth'])]
            domain_feature_cols = [metadata['symbol'] for metadata in domain_metadata]
            
            logger.info(f"🧮 Feature summary:")
            logger.info(f"   • Basic features: {len(basic_feature_cols)} (YoY, Log, MVA, Smoothing)")
            logger.info(f"   • Domain features: {len(domain_feature_cols)} (Economic, Technical, Ratios)")
            logger.info(f"   • Total features: {len(basic_feature_cols) + len(domain_feature_cols)}")
            
            return df_featured_pd
            
        except Exception as e:
            logger.error(f"❌ Error in unified pipeline: {e}")
            import traceback
            traceback.print_exc()
            raise
        
        finally:
            # Clean up connections
            if hasattr(self.interpolator, 'con') and self.interpolator.con:
                self.interpolator.con.close()
            if hasattr(self.aggregate_creator, 'con') and self.aggregate_creator.con:
                self.aggregate_creator.con.close()
            logger.info("🔌 Database connections closed")
    
    def _update_symbols_table(self, new_metadata: List[Dict[str, Any]]) -> bool:
        """
        Insert new feature symbols into DuckDB symbols table
        
        Args:
            new_metadata: List of feature metadata dictionaries
            
        Returns:
            True if successful, False otherwise
        """
        if not new_metadata:
            logger.info("📊 No new feature symbols to add to database")
            return True
            
        try:
            # Connect to database
            con = duckdb.connect(database=str(self.db_path), read_only=False)
            
            # Convert to DataFrame for easier handling
            df_new_symbols = pd.DataFrame(new_metadata)
            
            # Ensure we have the required columns matching the symbols table schema
            required_columns = ['symbol', 'source', 'description', 'unit']
            
            # Map our metadata to the symbols table schema
            symbols_data = []
            for _, row in df_new_symbols.iterrows():
                symbols_data.append({
                    'symbol': row.get('symbol', ''),
                    'source': row.get('source', 'Calc'),
                    'description': row.get('description', ''),
                    'unit': row.get('label_y', 'Value')  # Map label_y to unit
                })
            
            df_symbols_to_insert = pd.DataFrame(symbols_data)
            
            # Check for existing symbols to avoid duplicates
            try:
                existing_symbols_df = con.execute("SELECT symbol FROM symbols").df()
                existing_symbols = set(existing_symbols_df['symbol'].tolist()) if not existing_symbols_df.empty else set()
            except Exception as e:
                logger.warning(f"⚠️ Could not check existing symbols: {e}. Proceeding with insert...")
                existing_symbols = set()
            
            # Filter out existing symbols
            new_symbols_df = df_symbols_to_insert[~df_symbols_to_insert['symbol'].isin(existing_symbols)]
            
            if new_symbols_df.empty:
                logger.info("📊 All feature symbols already exist in database")
                con.close()
                return True
            
            logger.info(f"📊 Inserting {len(new_symbols_df)} new feature symbols into database...")
            
            # Insert new symbols
            con.execute("""
                INSERT INTO symbols (symbol, source, description, unit)
                SELECT symbol, source, description, unit FROM new_symbols_df
            """)
            
            logger.info(f"✅ Successfully inserted {len(new_symbols_df)} feature symbols into database")
            
            # Verify insertion
            total_symbols = con.execute("SELECT COUNT(*) as count FROM symbols").df()['count'].iloc[0]
            logger.info(f"📊 Total symbols in database: {total_symbols}")
            
            # Show breakdown of new symbol types
            basic_symbols = new_symbols_df[new_symbols_df['source'] == 'Calc']['symbol'].tolist()
            domain_symbols = new_symbols_df[new_symbols_df['source'].isin(['CalcFeat', 'Ratio'])]['symbol'].tolist()
            
            if basic_symbols:
                logger.info(f"🧮 Basic feature symbols: {len(basic_symbols)} (sample: {', '.join(basic_symbols[:3])})")
            if domain_symbols:
                logger.info(f"🧠 Domain feature symbols: {len(domain_symbols)} (sample: {', '.join(domain_symbols[:3])})")
            
            con.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error updating symbols table: {e}")
            return False
    
    def _load_from_silver(self, 
                         data_path: str, 
                         metadata_path: Optional[str] = None) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Load aggregated data from silver layer parquet files
        
        Args:
            data_path: Path to aggregated data parquet file (can be pattern or specific file)
            metadata_path: Optional path to metadata CSV (auto-detected if None)
            
        Returns:
            Tuple of (aggregated_data_df, metadata_df)
        """
        logger.info(f"📂 Loading data from silver layer: {data_path}")
        
        # Auto-detect if default path doesn't exist
        data_path = Path(data_path)
        if not data_path.exists() and data_path.name == "final_aggregated_data.parquet":
            logger.info("🔍 Default file not found, searching for latest timestamped file...")
            auto_detected_path = self._find_latest_silver_file(data_path.parent)
            if auto_detected_path:
                data_path = Path(auto_detected_path)
                logger.info(f"✅ Auto-detected: {data_path.name}")
            else:
                logger.error(f"❌ No suitable silver files found in {data_path.parent}")
                return pd.DataFrame(), None
        
        if not data_path.exists():
            logger.error(f"Silver data file not found: {data_path}")
            return pd.DataFrame(), None
        
        try:
            df_data = pd.read_parquet(data_path, engine='pyarrow')
            
            # Ensure date index
            if 'date' in df_data.columns:
                df_data['date'] = pd.to_datetime(df_data['date'])
                df_data.set_index('date', inplace=True)
            elif not isinstance(df_data.index, pd.DatetimeIndex):
                df_data.index = pd.to_datetime(df_data.index)
            
            logger.info(f"✅ Loaded silver data: {df_data.shape}")
            
            # Log file info
            file_size_mb = data_path.stat().st_size / (1024 * 1024)
            logger.info(f"📦 File size: {file_size_mb:.1f} MB")
            
        except Exception as e:
            logger.error(f"❌ Error loading silver data: {e}")
            return pd.DataFrame(), None
        
        # Load metadata if available
        df_metadata = None
        if metadata_path:
            metadata_path = Path(metadata_path)
        else:
            # Auto-detect metadata file
            possible_metadata_paths = [
                data_path.parent / "aggregated_symbols_metadata.csv",
                data_path.parent / "final_aggregated_metadata.csv",
                Path("data/silver/aggregated_symbols_metadata.csv")
            ]
            metadata_path = next((p for p in possible_metadata_paths if p.exists()), None)
        
        if metadata_path and metadata_path.exists():
            try:
                df_metadata = pd.read_csv(metadata_path)
                logger.info(f"✅ Loaded metadata: {df_metadata.shape}")
            except Exception as e:
                logger.warning(f"⚠️ Could not load metadata from {metadata_path}: {e}")
        else:
            logger.warning("⚠️ No metadata file found - will create mock metadata for features")
            
        return df_data, df_metadata
    
    def _find_latest_silver_file(self, silver_dir: Path) -> Optional[str]:
        """
        Find the latest final_aggregated_data file in silver directory
        
        Priority order:
        1. Latest timestamped file (final_aggregated_data_YYYYMMDD_HHMMSS.parquet)
        2. Generic file (final_aggregated_data.parquet)
        3. Any file containing "final_aggregated_data"
        """
        if not silver_dir.exists():
            return None
        
        # Look for timestamped files first
        timestamped_pattern = "final_aggregated_data_*.parquet"
        timestamped_files = list(silver_dir.glob(timestamped_pattern))
        
        if timestamped_files:
            # Sort by filename (which includes timestamp) and return the latest
            latest_file = max(timestamped_files, key=lambda p: p.name)
            return str(latest_file)
        
        # Fall back to generic file
        generic_file = silver_dir / "final_aggregated_data.parquet"
        if generic_file.exists():
            return str(generic_file)
        
        # Last resort: any file containing "final_aggregated_data"
        pattern = "*final_aggregated_data*.parquet"
        all_matching_files = list(silver_dir.glob(pattern))
        
        if all_matching_files:
            latest_file = max(all_matching_files, key=lambda p: p.stat().st_mtime)
            return str(latest_file)
        
        return None
    
    def _run_full_pipeline_to_aggregates(self, 
                                        filter_start_date: str,
                                        usrec_symbol: str,
                                        save_intermediates: bool) -> Tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        """
        Run the complete pipeline from DuckDB to aggregated data
        
        Returns:
            Tuple of (aggregated_data_df, metadata_df)
        """
        logger.info("🔄 Running full pipeline from DuckDB staging tables...")
        
        try:
            # Run the aggregate series creator (which includes interpolation)
            df_aggregated_pd = self.aggregate_creator.run_full_pipeline(
                filter_start_date=filter_start_date,
                usrec_symbol=usrec_symbol,
                output_path='data/silver/final_aggregated_data.parquet' if save_intermediates else None
            )
            
            if df_aggregated_pd.empty:
                logger.error("❌ Aggregate series creation failed")
                return pd.DataFrame(), None
            
            # Try to load metadata from database with a fresh connection
            df_metadata = None
            try:
                # Create a fresh connection since the previous one was closed
                con = duckdb.connect(database=str(self.db_path), read_only=True)
                symbols_df = con.execute("SELECT * FROM symbols").df()
                df_metadata = symbols_df
                logger.info(f"✅ Loaded metadata from database: {df_metadata.shape}")
                con.close()
            except Exception as e:
                logger.warning(f"⚠️ Could not load metadata from database: {e}")
                logger.info("📊 Continuing without database metadata - will use default metadata")
                # Create minimal metadata for the pipeline to continue
                df_metadata = None
            
            return df_aggregated_pd, df_metadata
            
        except Exception as e:
            logger.error(f"❌ Error in full pipeline: {e}")
            return pd.DataFrame(), None
    
    def _calculate_features_sequential_integrated(self, 
                                                df_data: pl.DataFrame,
                                                df_metadata: Optional[pd.DataFrame],
                                                n_days_year: int) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """
        Sequential feature calculation using logic from features.py
        """
        logger.info("Starting sequential feature calculation...")
        
        # Prepare metadata
        if df_metadata is not None:
            df_symbols_meta = pl.from_pandas(df_metadata)
        else:
            df_symbols_meta = self._create_mock_metadata(df_data)
        
        all_new_feature_metadata = []
        columns_to_process = [col for col in df_data.columns if col != "date"]
        
        overall_min_date = df_data["date"].min()
        overall_max_date = df_data["date"].max()
        
        # Collect all expressions for bulk application
        all_feature_expressions = []
        
        logger.info(f"Processing {len(columns_to_process)} symbols sequentially...")
        
        for i, str_symbol_original in enumerate(columns_to_process):
            if i % 100 == 0:
                logger.info(f"Processing symbol {i+1}/{len(columns_to_process)}: {str_symbol_original}")
            
            description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
                str_symbol_original, df_symbols_meta, overall_min_date, overall_max_date
            )
            
            # YoY features (expressions)
            yoy_exprs, yoy_meta = self._calculate_yoy_features_expr(
                str_symbol_original, description_r, series_start_date_str, series_end_date_str, n_days_year
            )
            all_feature_expressions.extend(yoy_exprs)
            all_new_feature_metadata.extend(yoy_meta)
            
            # Log transform (expression)
            log_expr, log_meta = self._calculate_log_transform_expr(
                str_symbol_original, description_r, label_y_r, series_start_date_str, series_end_date_str
            )
            all_feature_expressions.append(log_expr)
            all_new_feature_metadata.append(log_meta)
            
            # MVA features (expressions)
            mva_exprs, mva_meta = self._calculate_mva_features_expr(
                str_symbol_original, description_r, label_y_r, 
                series_start_date_str, series_end_date_str, n_days_year, df_data
            )
            all_feature_expressions.extend(mva_exprs)
            all_new_feature_metadata.extend(mva_meta)
        
        # Apply all expressions at once
        if all_feature_expressions:
            logger.info(f"Applying {len(all_feature_expressions)} feature expressions...")
            df_data = df_data.with_columns(all_feature_expressions)
        
        # Savitzky-Golay features (need to be done iteratively)
        savgol_series_to_add = []
        for i, str_symbol_original in enumerate(columns_to_process):
            if i % 100 == 0:
                logger.info(f"Processing Savitzky-Golay {i+1}/{len(columns_to_process)}: {str_symbol_original}")
            
            description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
                str_symbol_original, df_symbols_meta, overall_min_date, overall_max_date
            )
            
            original_series = df_data.get_column(str_symbol_original)
            savgol_series_list, savgol_meta = self._calculate_savgol_features_series(
                original_series, str_symbol_original, description_r, label_y_r,
                series_start_date_str, series_end_date_str, n_days_year
            )
            savgol_series_to_add.extend(savgol_series_list)
            all_new_feature_metadata.extend(savgol_meta)
        
        if savgol_series_to_add:
            logger.info(f"Adding {len(savgol_series_to_add)} Savitzky-Golay features...")
            df_data = df_data.with_columns(savgol_series_to_add)
        
        return df_data, all_new_feature_metadata
    
    def _calculate_features_parallel_integrated(self, 
                                              df_data: pl.DataFrame,
                                              df_metadata: Optional[pd.DataFrame],
                                              n_days_year: int,
                                              num_workers: int) -> Tuple[pl.DataFrame, List[Dict[str, Any]]]:
        """
        Parallel feature calculation using logic from features_parallel.py
        """
        logger.info(f"Starting parallel feature calculation with {num_workers} workers...")
        
        # Prepare metadata
        if df_metadata is not None:
            df_symbols_meta = pl.from_pandas(df_metadata)
        else:
            df_symbols_meta = self._create_mock_metadata(df_data)
        
        columns_to_process = [col for col in df_data.columns if col != "date"]
        overall_min_date = df_data["date"].min()
        overall_max_date = df_data["date"].max()
        
        all_new_feature_metadata = []
        all_new_series_to_add = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_symbol = {
                executor.submit(
                    self._process_symbol_features,
                    symbol,
                    df_data.get_column(symbol).clone(),
                    df_symbols_meta,
                    overall_min_date,
                    overall_max_date,
                    n_days_year
                ): symbol for symbol in columns_to_process
            }
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                
                if completed % 100 == 0:
                    logger.info(f"Completed {completed}/{len(columns_to_process)} symbols")
                
                try:
                    _, new_series_for_symbol, new_metadata_for_symbol = future.result()
                    all_new_series_to_add.extend(new_series_for_symbol)
                    all_new_feature_metadata.extend(new_metadata_for_symbol)
                except Exception as exc:
                    logger.error(f"Symbol {symbol} generated an exception: {exc}")
                    import traceback
                    traceback.print_exc()
        
        # Add all new series
        if all_new_series_to_add:
            valid_series = [s for s in all_new_series_to_add if s is not None and isinstance(s, pl.Series)]
            logger.info(f"Adding {len(valid_series)} new feature series to DataFrame...")
            df_data = df_data.with_columns(valid_series)
        
        return df_data, all_new_feature_metadata
    
    def _process_symbol_features(self, 
                               symbol_original: str,
                               original_series_data: pl.Series,
                               df_symbols_meta: pl.DataFrame, 
                               overall_min_date: Any,
                               overall_max_date: Any,
                               n_days_year: int) -> Tuple[str, List[pl.Series], List[Dict[str, Any]]]:
        """
        Process all features for a single symbol (parallel worker function)
        Based on logic from features_parallel.py
        """
        description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
            symbol_original, df_symbols_meta, overall_min_date, overall_max_date
        )
        
        current_symbol_new_series = []
        current_symbol_new_metadata = []

        # YoY features
        yoy_series_list, yoy_meta_list = self._calculate_yoy_features_series(
            original_series_data, symbol_original, description_r, 
            series_start_date_str, series_end_date_str, n_days_year
        )
        current_symbol_new_series.extend(yoy_series_list)
        current_symbol_new_metadata.extend(yoy_meta_list)

        # Savitzky-Golay features
        savgol_series_list, savgol_meta_list = self._calculate_savgol_features_series(
            original_series_data, symbol_original, description_r, label_y_r, 
            series_start_date_str, series_end_date_str, n_days_year
        )
        current_symbol_new_series.extend(savgol_series_list)
        current_symbol_new_metadata.extend(savgol_meta_list)

        # Log transform
        log_series, log_meta_dict = self._calculate_log_transform_series(
            original_series_data, symbol_original, description_r, label_y_r, 
            series_start_date_str, series_end_date_str
        )
        current_symbol_new_series.append(log_series)
        current_symbol_new_metadata.append(log_meta_dict)
        
        # MVA features
        mva_series_list, mva_meta_list = self._calculate_mva_features_series(
            original_series_data, symbol_original, description_r, label_y_r, 
            series_start_date_str, series_end_date_str, n_days_year
        )
        current_symbol_new_series.extend(mva_series_list)
        current_symbol_new_metadata.extend(mva_meta_list)
        
        return symbol_original, current_symbol_new_series, current_symbol_new_metadata
    
    # Feature calculation helper methods (expressions for sequential processing)
    def _calculate_yoy_features_expr(self, symbol_original: str, description_r: str, 
                                   series_start_date_str: str, series_end_date_str: str, 
                                   n_days_year: int) -> Tuple[List[pl.Expr], List[Dict[str, Any]]]:
        """Calculate YoY features as expressions for bulk application"""
        feature_exprs = []
        feature_metadata = []

        # YoY
        new_col_yoy = f"{symbol_original}_YoY"
        feature_exprs.append(
            (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year)) - 1) * 100).alias(new_col_yoy)
        )
        feature_metadata.append({
            "symbol": new_col_yoy, "source": "Calc",
            "description": f"{description_r}\nYear over Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })

        # YoY4
        new_col_yoy4 = f"{symbol_original}_YoY4"
        feature_exprs.append(
            (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year * 4)) - 1) * 100).alias(new_col_yoy4)
        )
        feature_metadata.append({
            "symbol": new_col_yoy4, "source": "Calc",
            "description": f"{description_r}\n4 Year over 4 Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })

        # YoY5
        new_col_yoy5 = f"{symbol_original}_YoY5"
        feature_exprs.append(
            (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year * 5)) - 1) * 100).alias(new_col_yoy5)
        )
        feature_metadata.append({
            "symbol": new_col_yoy5, "source": "Calc",
            "description": f"{description_r}\n5 Year over 5 Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })
        
        return feature_exprs, feature_metadata
    
    def _calculate_log_transform_expr(self, symbol_original: str, description_r: str, 
                                    label_y_r: str, series_start_date_str: str, 
                                    series_end_date_str: str) -> Tuple[pl.Expr, Dict[str, Any]]:
        """Calculate log transform as expression"""
        new_col_log = f"{symbol_original}_Log"
        log_expr = (
            pl.when(pl.col(symbol_original) > 0)
            .then(pl.col(symbol_original).log())
            .otherwise(None)
            .interpolate()
            .alias(new_col_log)
        )
        log_metadata = {
            "symbol": new_col_log, "source": "Calc",
            "description": f"Log of {description_r}", "label_y": f"log({label_y_r})",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        }
        return log_expr, log_metadata
    
    def _calculate_mva_features_expr(self, symbol_original: str, description_r: str, 
                                   label_y_r: str, series_start_date_str: str, 
                                   series_end_date_str: str, n_days_year: int,
                                   df_data: pl.DataFrame) -> Tuple[List[pl.Expr], List[Dict[str, Any]]]:
        """Calculate MVA features as expressions"""
        feature_exprs = []
        feature_metadata = []

        mva_configs = {
            "_mva365": n_days_year,
            "_mva200": 200,
            "_mva050": 50
        }

        original_series = df_data.get_column(symbol_original)
        for suffix, window in mva_configs.items():
            new_col_mva = f"{symbol_original}{suffix}"
            if original_series.drop_nulls().len() >= window:
                feature_exprs.append(
                    pl.col(symbol_original).rolling_mean(window_size=window, min_periods=1).interpolate().alias(new_col_mva)
                )
                feature_metadata.append({
                    "symbol": new_col_mva, "source": "Calc",
                    "description": f"{description_r} {window} Day MA", "label_y": f"{label_y_r} {window} Day MA",
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
            else:
                feature_exprs.append(pl.lit(None, dtype=pl.Float64).alias(new_col_mva))
                feature_metadata.append({
                    "symbol": new_col_mva, "source": "Calc (Skipped)",
                    "description": f"{description_r} {window} Day MA - SKIPPED", "label_y": f"{label_y_r} {window} Day MA",
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
                
        return feature_exprs, feature_metadata
    
    # Feature calculation helper methods (series for parallel processing)
    def _calculate_yoy_features_series(self, original_series: pl.Series, symbol_name_prefix: str,
                                     description_r: str, series_start_date_str: str, 
                                     series_end_date_str: str, n_days_year: int) -> Tuple[List[pl.Series], List[Dict[str, Any]]]:
        """Calculate YoY features as series for parallel processing"""
        feature_series_list = []
        feature_metadata = []

        new_col_yoy_name = f"{symbol_name_prefix}_YoY"
        yoy_series = (((original_series / original_series.shift(n_days_year)) - 1) * 100).alias(new_col_yoy_name)
        feature_series_list.append(yoy_series)
        feature_metadata.append({
            "symbol": new_col_yoy_name, "source": "Calc",
            "description": f"{description_r}\nYear over Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })

        new_col_yoy4_name = f"{symbol_name_prefix}_YoY4"
        yoy4_series = (((original_series / original_series.shift(n_days_year * 4)) - 1) * 100).alias(new_col_yoy4_name)
        feature_series_list.append(yoy4_series)
        feature_metadata.append({
            "symbol": new_col_yoy4_name, "source": "Calc",
            "description": f"{description_r}\n4 Year over 4 Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })

        new_col_yoy5_name = f"{symbol_name_prefix}_YoY5"
        yoy5_series = (((original_series / original_series.shift(n_days_year * 5)) - 1) * 100).alias(new_col_yoy5_name)
        feature_series_list.append(yoy5_series)
        feature_metadata.append({
            "symbol": new_col_yoy5_name, "source": "Calc",
            "description": f"{description_r}\n5 Year over 5 Year", "label_y": "Percent",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        })
        
        return feature_series_list, feature_metadata

    def _calculate_log_transform_series(self, original_series: pl.Series, symbol_name_prefix: str,
                                      description_r: str, label_y_r: str, series_start_date_str: str, 
                                      series_end_date_str: str) -> Tuple[pl.Series, Dict[str, Any]]:
        """Calculate log transform as series"""
        new_col_log_name = f"{symbol_name_prefix}_Log"

        def safe_log(val):
            if val is not None and val > 0:
                try:
                    return math.log(val)
                except (ValueError, TypeError):
                    return None
            return None

        series_for_log = original_series
        if not original_series.dtype.is_float():
            series_for_log = original_series.cast(pl.Float64, strict=False)

        log_transformed_values = series_for_log.map_elements(safe_log, return_dtype=pl.Float64)
        log_series = log_transformed_values.interpolate().alias(new_col_log_name)
        
        log_metadata = {
            "symbol": new_col_log_name, "source": "Calc",
            "description": f"Log of {description_r}", "label_y": f"log({label_y_r})",
            "series_start": series_start_date_str, "series_end": series_end_date_str
        }
        return log_series, log_metadata

    def _calculate_mva_features_series(self, original_series: pl.Series, symbol_name_prefix: str,
                                     description_r: str, label_y_r: str, series_start_date_str: str, 
                                     series_end_date_str: str, n_days_year: int) -> Tuple[List[pl.Series], List[Dict[str, Any]]]:
        """Calculate MVA features as series"""
        feature_series_list = []
        feature_metadata = []

        mva_configs = {
            "_mva365": n_days_year,
            "_mva200": 200,
            "_mva050": 50
        }

        for suffix, window in mva_configs.items():
            new_col_mva_name = f"{symbol_name_prefix}{suffix}"
            if original_series.drop_nulls().len() >= window:
                mva_series = original_series.rolling_mean(window_size=window, min_periods=1).interpolate().alias(new_col_mva_name)
                feature_series_list.append(mva_series)
                feature_metadata.append({
                    "symbol": new_col_mva_name, "source": "Calc",
                    "description": f"{description_r} {window} Day MA", "label_y": f"{label_y_r} {window} Day MA",
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
            else:
                feature_series_list.append(pl.Series(new_col_mva_name, [None] * len(original_series), dtype=pl.Float64))
                feature_metadata.append({
                    "symbol": new_col_mva_name, "source": "Calc (Skipped)",
                    "description": f"{description_r} {window} Day MA - SKIPPED", "label_y": f"{label_y_r} {window} Day MA",
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
                
        return feature_series_list, feature_metadata

    def _calculate_savgol_features_series(self, original_series: pl.Series, symbol_original: str,
                                        description_r: str, label_y_r: str, series_start_date_str: str, 
                                        series_end_date_str: str, n_days_year: int) -> Tuple[List[pl.Series], List[Dict[str, Any]]]:
        """Calculate Savitzky-Golay features as series"""
        feature_series_list = []
        feature_metadata = []

        savgol_configs = [
            {"suffix": "_Smooth", "window": n_days_year, "poly": 3, "deriv": 0, "desc_prefix": "Savitsky-Golay Smoothed"},
            {"suffix": "_Smooth_short", "window": 15, "poly": 3, "deriv": 0, "desc_prefix": "Savitsky-Golay Smoothed"},
            {"suffix": "_SmoothDer", "window": 501, "poly": 3, "deriv": 1, "desc_prefix": "Derivative of Savitsky-Golay Smoothed"},
        ]

        for config in savgol_configs:
            new_col_name = f"{symbol_original}{config['suffix']}"
            window = config['window']
            poly = config['poly']

            filtered_series = apply_savgol_filter(
                original_series.alias(new_col_name),
                window_length=window,
                polyorder=poly,
                deriv=config['deriv']
            )
            
            if filtered_series.is_null().all() and not original_series.is_null().all():
                feature_series_list.append(pl.Series(new_col_name, [None] * len(original_series), dtype=pl.Float64))
                feature_metadata.append({
                    "symbol": new_col_name, "source": "Calc (Skipped/Failed)",
                    "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r} - SKIPPED/FAILED",
                    "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
            else:
                feature_series_list.append(filtered_series)
                feature_metadata.append({
                    "symbol": new_col_name, "source": "Calc",
                    "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r}",
                    "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                    "series_start": series_start_date_str, "series_end": series_end_date_str
                })
                
        return feature_series_list, feature_metadata
    
    def _create_mock_metadata(self, df_data: pl.DataFrame) -> pl.DataFrame:
        """Create basic metadata for symbols when original metadata isn't available"""
        symbols = [col for col in df_data.columns if col != "date"]
        
        mock_metadata = []
        for symbol in symbols:
            mock_metadata.append({
                "symbol": symbol,
                "description": f"Data for {symbol}",
                "label_y": "Value",
                "series_start": df_data["date"].min(),
                "series_end": df_data["date"].max(),
                "source": "Unknown"
            })
        
        return pl.DataFrame(mock_metadata)
    
    def _combine_metadata(self, original_metadata: pd.DataFrame, 
                         new_metadata: List[Dict[str, Any]]) -> pd.DataFrame:
        """Combine original metadata with new feature metadata"""
        if new_metadata:
            new_metadata_df = pd.DataFrame(new_metadata)
            combined = pd.concat([original_metadata, new_metadata_df], ignore_index=True)
            return combined
        return original_metadata

# Convenience functions for easy usage
def run_silver_pipeline(silver_data_path: str = 'data/silver/final_aggregated_data.parquet',
                       parallel: bool = True,
                       num_workers: int = 6,
                       output_path: str = 'data/silver/featured_data.parquet',
                       create_domain_features: bool = True) -> pd.DataFrame:
    """
    Quick pipeline that loads from silver layer and calculates features
    
    Args:
        silver_data_path: Path to final aggregated data parquet file
        parallel: Use parallel processing for features
        num_workers: Number of parallel workers
        output_path: Final output path (Parquet format)
        create_domain_features: Create domain-specific derived features
        
    Returns:
        Final featured dataset ready for analysis
    """
    pipeline = UnifiedDataPipeline()
    return pipeline.run_pipeline(
        mode='silver',
        silver_data_path=silver_data_path,
        use_parallel=parallel,
        num_workers=num_workers,
        output_path=output_path,
        create_domain_features=create_domain_features
    )

def run_full_pipeline(db_path: str = 'bristol_gate.duckdb',
                     start_date: str = '1950-01-01',
                     parallel: bool = True,
                     num_workers: int = 6,
                     save_intermediates: bool = False,
                     output_path: str = 'data/silver/featured_data.parquet',
                     create_domain_features: bool = True) -> pd.DataFrame:
    """
    Complete pipeline from DuckDB to featured dataset
    
    Args:
        db_path: Path to DuckDB database
        start_date: Start date for analysis
        parallel: Use parallel processing for features
        num_workers: Number of parallel workers
        save_intermediates: Save interpolated and aggregate data
        output_path: Final output path (Parquet format)
        create_domain_features: Create domain-specific derived features
        
    Returns:
        Final featured dataset ready for analysis
    """
    pipeline = UnifiedDataPipeline(db_path)
    return pipeline.run_pipeline(
        mode='full',
        filter_start_date=start_date,
        use_parallel=parallel,
        num_workers=num_workers,
        save_intermediates=save_intermediates,
        output_path=output_path,
        create_domain_features=create_domain_features
    )

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Unified Data Pipeline - Bristol Gate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Load from silver layer (fast for development)
    python -m src_pipeline.unified_pipeline --mode silver
    
    # Run complete pipeline from DuckDB (comprehensive)
    python -m src_pipeline.unified_pipeline --mode full --start-date 2020-01-01
    
    # Use sequential processing instead of parallel
    python -m src_pipeline.unified_pipeline --mode silver --sequential
    
    # Save intermediate files for debugging
    python -m src_pipeline.unified_pipeline --mode full --save-intermediates

Modes:
    silver: Load data from silver layer parquet files (fastest)
    full:   Run complete pipeline from DuckDB staging tables (most comprehensive)

Output:
    • Featured dataset saved as timestamped Parquet in silver layer
    • New feature symbols automatically added to DuckDB symbols table
    • Comprehensive metadata with feature descriptions
    • Performance metrics and timing information
        """
    )
    
    parser.add_argument('--mode', choices=['silver', 'full'], default='silver',
                       help='Pipeline mode: silver (load from parquet) or full (from DuckDB)')
    parser.add_argument('--db-path', default='bristol_gate.duckdb',
                       help='Path to DuckDB database file')
    parser.add_argument('--silver-data', default='data/silver/final_aggregated_data.parquet',
                       help='Path to silver layer aggregated data file')
    parser.add_argument('--start-date', default='1950-01-01',
                       help='Start date for filtering (only used in full mode)')
    parser.add_argument('--output', default='data/silver/featured_data.parquet',
                       help='Output path for featured data (Parquet format)')
    parser.add_argument('--sequential', action='store_true',
                       help='Use sequential processing instead of parallel')
    parser.add_argument('--workers', type=int, default=6,
                       help='Number of parallel workers')
    parser.add_argument('--save-intermediates', action='store_true',
                       help='Save intermediate results (only used in full mode)')
    parser.add_argument('--no-symbols-update', action='store_true',
                       help='Skip updating DuckDB symbols table with new features')
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = UnifiedDataPipeline(args.db_path)
    result = pipeline.run_pipeline(
        mode=args.mode,
        silver_data_path=args.silver_data,
        filter_start_date=args.start_date,
        use_parallel=not args.sequential,
        num_workers=args.workers,
        output_path=args.output,
        save_intermediates=args.save_intermediates,
        update_symbols_table=not args.no_symbols_update,
        create_domain_features=not args.no_domain_features
    )
    
    if not result.empty:
        print(f"\n🎉 Success! Featured dataset ready for analysis!")
        print(f"📊 Shape: {result.shape}")
        print(f"📅 Date range: {result.index.min()} to {result.index.max()}")
        print(f"💾 Saved to silver layer as Parquet with timestamp")
    else:
        print("\n❌ Pipeline failed or returned empty data")
        exit(1) 