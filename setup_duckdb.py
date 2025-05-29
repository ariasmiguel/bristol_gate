#!/usr/bin/env python3
"""
DuckDB Database Initialization Script

This script initializes the DuckDB database for the Bristol Gate data pipeline by:
1. Loading environment variables from .env file
2. Connecting to DuckDB database file
3. Executing the SQL initialization script
4. Optionally loading symbols reference data
5. Providing detailed feedback and error handling

Usage:
    python setup_duckdb.py [--load-symbols]

Options:
    --load-symbols    Also load symbols.csv into the symbols table
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

# Import from our pipeline module
try:
    from src_pipeline.duckdb_functions import DuckDBInitializer
except ImportError as e:
    print(f"Error: Could not import from pipeline module: {e}")
    print("Please ensure you're running from the project root directory")
    print("and that the src_pipeline module is available")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def format_duration(start_time: datetime, end_time: datetime) -> str:
    """Format duration in a human-readable way"""
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    
    if total_seconds < 1:
        return f"{total_seconds*1000:.0f}ms"
    elif total_seconds < 60:
        return f"{total_seconds:.2f}s"
    else:
        minutes = int(total_seconds // 60)
        seconds = total_seconds % 60
        return f"{minutes}m {seconds:.2f}s"

def main():
    """Main execution function for DuckDB database initialization"""
    # Record overall start time
    overall_start_time = datetime.now()
    
    parser = argparse.ArgumentParser(
        description='Initialize DuckDB database for Bristol Gate pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python setup_duckdb.py                    # Initialize database and tables only
    python setup_duckdb.py --load-symbols     # Initialize and load symbols data
    python setup_duckdb.py --sql-file custom.sql --symbols-file custom.csv

Features:
    • Creates bristol_gate.duckdb file in current directory
    • Sets up all staging tables with exact schema compatibility
    • Automatically exports data to data/bronze/*.parquet files
    • No server dependencies - everything runs locally!
        """
    )
    
    parser.add_argument('--load-symbols', action='store_true', 
                       help='Also load symbols.csv into the symbols table')
    parser.add_argument('--sql-file', default='sql/duckdb_init.sql',
                       help='Path to SQL initialization file (default: sql/duckdb_init.sql)')
    parser.add_argument('--symbols-file', default='data/symbols.csv',
                       help='Path to symbols CSV file (default: data/symbols.csv)')
    
    args = parser.parse_args()
    
    # Initialize the setup
    initializer = DuckDBInitializer()
    success = True
    step_times = {}
    
    try:
        logger.info("🦆 Starting DuckDB initialization...")
        logger.info(f"⏱️  Start time: {overall_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Step 1: Load environment
        logger.info("📋 Step 1: Loading environment variables...")
        step_start = datetime.now()
        if not initializer.load_environment():
            success = False
            return
        step_end = datetime.now()
        step_times['Environment Loading'] = format_duration(step_start, step_end)
        logger.info(f"⏱️  Step 1 completed in {step_times['Environment Loading']}")
        
        # Step 2: Connect to DuckDB
        logger.info("🔌 Step 2: Connecting to DuckDB...")
        step_start = datetime.now()
        if not initializer.connect_to_duckdb():
            success = False
            return
        step_end = datetime.now()
        step_times['Database Connection'] = format_duration(step_start, step_end)
        logger.info(f"⏱️  Step 2 completed in {step_times['Database Connection']}")
        
        # Step 3: Execute SQL script (uses default path if none specified)
        logger.info("📝 Step 3: Executing SQL initialization script...")
        step_start = datetime.now()
        if args.sql_file == 'sql/duckdb_init.sql':
            # Use the default parameter in the method
            if not initializer.execute_sql_file():
                success = False
                return
        else:
            # Use custom SQL file
            sql_file = Path(args.sql_file)
            if not initializer.execute_sql_file(sql_file):
                success = False
                return
        step_end = datetime.now()
        step_times['SQL Execution'] = format_duration(step_start, step_end)
        logger.info(f"⏱️  Step 3 completed in {step_times['SQL Execution']}")
        
        # Step 4: Load symbols data (if requested)
        if args.load_symbols:
            logger.info("📊 Step 4: Loading symbols reference data...")
            step_start = datetime.now()
            symbols_file = Path(args.symbols_file)
            if not initializer.load_symbols_data(symbols_file):
                success = False
                return
            step_end = datetime.now()
            step_times['Symbols Loading'] = format_duration(step_start, step_end)
            logger.info(f"⏱️  Step 4 completed in {step_times['Symbols Loading']}")
        
        # Step 5: Verify setup
        logger.info("✅ Step 5: Verifying database setup...")
        step_start = datetime.now()
        if not initializer.verify_setup():
            success = False
            return
        step_end = datetime.now()
        step_times['Setup Verification'] = format_duration(step_start, step_end)
        logger.info(f"⏱️  Step 5 completed in {step_times['Setup Verification']}")
        
        # Calculate total time
        overall_end_time = datetime.now()
        total_duration = format_duration(overall_start_time, overall_end_time)
        
        logger.info("🎉 DuckDB initialization completed successfully!")
        logger.info(f"📁 Database file: {Path('bristol_gate.duckdb').absolute()}")
        logger.info(f"📁 Bronze layer: {Path('data/bronze/').absolute()}")
        
        if not args.load_symbols:
            logger.info("💡 Tip: Run with --load-symbols to also load the symbols reference data")
        
        # Display timing summary
        logger.info("")
        logger.info("⏱️  Performance Summary:")
        logger.info("=" * 60)
        for step_name, duration in step_times.items():
            logger.info(f"  {step_name:<25}: {duration:>8}")
        logger.info("=" * 60)
        logger.info(f"  {'Total Duration':<25}: {total_duration:>8}")
        logger.info(f"  {'End time':<25}: {overall_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        end_time = datetime.now()
        duration = format_duration(overall_start_time, end_time)
        logger.info(f"❌ Initialization cancelled by user after {duration}")
        success = False
    except Exception as e:
        end_time = datetime.now()
        duration = format_duration(overall_start_time, end_time)
        logger.error(f"❌ Unexpected error during initialization: {e}")
        logger.error(f"⏱️  Failed after {duration}")
        success = False
    finally:
        # Final cleanup with timing
        cleanup_start = datetime.now()
        initializer.close_connection()
        cleanup_end = datetime.now()
        cleanup_time = format_duration(cleanup_start, cleanup_end)
        logger.info(f"🧹 Cleanup completed in {cleanup_time}")
    
    if not success:
        final_time = datetime.now()
        total_duration = format_duration(overall_start_time, final_time)
        logger.error(f"❌ Initialization failed after {total_duration}")
        sys.exit(1)

if __name__ == '__main__':
    main() 