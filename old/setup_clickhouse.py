#!/usr/bin/env python3
"""
ClickHouse Database Initialization Script

This script initializes the ClickHouse database for the Bristol Gate data pipeline by:
1. Loading environment variables from .env file
2. Connecting to ClickHouse
3. Executing the SQL initialization script
4. Optionally loading symbols reference data
5. Providing detailed feedback and error handling

Usage:
    python setup_clickhouse.py [--load-symbols]

Options:
    --load-symbols    Also load symbols.csv into the symbols table
"""

import sys
import argparse
import logging
from pathlib import Path

# Import from our pipeline module
try:
    from src_pipeline.clickhouse_functions import ClickHouseInitializer
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

def main():
    """Main execution function for ClickHouse database initialization"""
    parser = argparse.ArgumentParser(
        description='Initialize ClickHouse database for Bristol Gate pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python setup_clickhouse.py                    # Initialize database and tables only
    python setup_clickhouse.py --load-symbols     # Initialize and load symbols data
    python setup_clickhouse.py --sql-file custom.sql --symbols-file custom.csv
        """
    )
    
    parser.add_argument('--load-symbols', action='store_true', 
                       help='Also load symbols.csv into the symbols table')
    parser.add_argument('--sql-file', default='sql/init_tables.sql',
                       help='Path to SQL initialization file (default: sql/init_tables.sql)')
    parser.add_argument('--symbols-file', default='data/symbols.csv',
                       help='Path to symbols CSV file (default: data/symbols.csv)')
    
    args = parser.parse_args()
    
    # Initialize the setup
    initializer = ClickHouseInitializer()
    success = True
    
    try:
        logger.info("🚀 Starting ClickHouse initialization...")
        
        # Step 1: Load environment
        if not initializer.load_environment():
            success = False
            return
        
        # Step 2: Connect to ClickHouse
        if not initializer.connect_to_clickhouse():
            success = False
            return
        
        # Step 3: Execute SQL script (now uses default path if none specified)
        if args.sql_file == 'sql/init_tables.sql':
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
        
        # Step 4: Load symbols data (if requested)
        if args.load_symbols:
            symbols_file = Path(args.symbols_file)
            if not initializer.load_symbols_data(symbols_file):
                success = False
                return
        
        # Step 5: Verify setup
        if not initializer.verify_setup():
            success = False
            return
        
        logger.info("🎉 ClickHouse initialization completed successfully!")
        
        if not args.load_symbols:
            logger.info("💡 Tip: Run with --load-symbols to also load the symbols reference data")
        
    except KeyboardInterrupt:
        logger.info("❌ Initialization cancelled by user")
        success = False
    except Exception as e:
        logger.error(f"❌ Unexpected error during initialization: {e}")
        success = False
    finally:
        initializer.close_connection()
    
    if not success:
        logger.error("❌ Initialization failed")
        sys.exit(1)

if __name__ == '__main__':
    main() 