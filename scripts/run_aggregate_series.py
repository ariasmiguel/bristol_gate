#!/usr/bin/env python3
"""
Aggregate Series Creation Entry Point
Standalone script to create aggregate series using the integrated DuckDB pipeline
Saves output in Parquet format with timestamps to data/silver/ following medallion architecture
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

from src_pipeline.pipelines.aggregate_series import create_aggregate_series_from_interpolated_data
from src_pipeline.core.date_utils import DateUtils

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main function for aggregate series creation using DuckDB integration"""
    parser = argparse.ArgumentParser(
        description='Create Aggregate Series using DuckDB Integration Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_aggregate_series.py                                       # Full pipeline with defaults
    python run_aggregate_series.py --start-date 2020-01-01               # Filter from specific date  
    python run_aggregate_series.py --output data/silver/final_data.parquet  # Save to custom file
    python run_aggregate_series.py --method staged                       # Use pandas pivot method

What this does:
    • Loads data directly from DuckDB staging tables
    • Applies interpolation to create daily time series  
    • Creates aggregate/computed series (ratios, differences, normalized values)
    • Inserts new symbol metadata into DuckDB symbols table
    • Saves enhanced dataset in Parquet format to data/silver/

File Output:
    • Creates timestamped file: final_aggregated_data_YYYYMMDD_HHMMSS.parquet
    • Creates/updates latest file: final_aggregated_data.parquet
    
Key improvements:
    • No intermediate CSV files needed
    • Direct DuckDB integration for better performance  
    • Automatic symbol metadata management
    • Parquet format for efficient storage and faster loading
    • Timestamped files for audit trail
    • Follows medallion architecture (bronze → silver → gold)
        """
    )
    
    parser.add_argument('--db-path', default='bristol_gate.duckdb',
                       help='Path to DuckDB database file')
    parser.add_argument('--start-date', default='1950-01-01',
                       help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--usrec-symbol', default='USREC',
                       help='Recession indicator symbol for special handling')
    parser.add_argument('--method', choices=['direct', 'staged'], default='direct',
                       help='Interpolation method: direct (SQL pivot) or staged (pandas pivot)')
    parser.add_argument('--output', default='data/silver/final_aggregated_data.parquet',
                       help='Output Parquet file path (will add timestamp)')
    parser.add_argument('--skip-save', action='store_true',
                       help='Skip saving Parquet output (useful for testing)')
    
    args = parser.parse_args()
    
    # Validate database file exists
    db_path = Path(args.db_path)
    if not db_path.exists():
        logger.error(f"❌ Database file not found: {db_path}")
        logger.error("💡 Run 'python setup_duckdb.py' first to initialize the database")
        sys.exit(1)
    
    # Ensure output directory exists
    if not args.skip_save:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"📁 Silver layer directory ready: {output_path.parent}")
    
    logger.info("🧮 DuckDB-Integrated Aggregate Series Creator")
    logger.info(f"⏰ Started at: {DateUtils.format_current_datetime()}")
    logger.info(f"🗃️ Database: {db_path}")
    logger.info(f"📅 Filter start: {args.start_date}")
    logger.info(f"🔧 Method: {args.method}")
    logger.info(f"📉 USREC symbol: {args.usrec_symbol}")
    
    if not args.skip_save:
        logger.info(f"💾 Output (Parquet with timestamp): {args.output}")
    else:
        logger.info("⚠️ Parquet output will be skipped")
    
    try:
        # Run integrated pipeline
        result = create_aggregate_series_from_interpolated_data(
            db_path=str(db_path),
            filter_start_date=args.start_date,
            usrec_symbol=args.usrec_symbol,
            interpolation_method=args.method,
            output_path=args.output if not args.skip_save else None
        )
        
        if result.empty:
            logger.error("❌ Pipeline returned empty data")
            sys.exit(1)
        
        logger.info("🎉 Pipeline completed successfully!")
        logger.info(f"📊 Final dataset shape: {result.shape}")
        logger.info(f"📅 Date range: {result.index.min()} to {result.index.max()}")
        logger.info(f"📈 Total columns: {len(result.columns)}")
        
        # Show sample of aggregate series created
        calc_columns = [col for col in result.columns if any(
            agg_name in col for agg_name in [
                'RSALESAGG', 'BUSLOANS_by_GDP', 'DGS10_to_DGS2', 
                'GSPC_Close_by_MDY_Close', 'GDP_by_POPTHM'
            ]
        )]
        
        if calc_columns:
            logger.info(f"🧮 Sample aggregate series: {calc_columns[:5]}")
        
        if not args.skip_save:
            logger.info(f"💾 Enhanced data saved with timestamp and latest versions")
        
        logger.info("✅ Aggregate series and symbols metadata updated in DuckDB!")
        logger.info("🥈 Data saved to Silver layer (medallion architecture)")
        
        # Show efficiency gains vs CSV
        logger.info("💡 Benefits of timestamped Parquet format:")
        logger.info("   • ~3-5x smaller file size vs CSV")
        logger.info("   • ~10-50x faster loading for analytics")
        logger.info("   • Preserves data types and metadata")
        logger.info("   • Column-oriented storage for better compression")
        logger.info("   • Full audit trail with timestamps")
        logger.info("   • Always-available 'latest' version for downstream processes")
        
    except KeyboardInterrupt:
        logger.info("⏹️ Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Process failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 