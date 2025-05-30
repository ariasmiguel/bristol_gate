#!/usr/bin/env python3
"""
DuckDB Functions for Bristol Gate Data Pipeline

This module provides DuckDB-related functionality including:
1. Database initialization and setup
2. Data extraction and upload functions
3. Table management and utilities

Classes:
    DuckDBInitializer: Handles database and table setup
    DuckDBManager: Handles data operations

Usage:
    python -m src_pipeline.duckdb_functions [--load-symbols]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import Optional
import pandas as pd

# Third-party imports
try:
    import duckdb
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Please install required packages:")
    print("pip install duckdb>=0.10.0 python-dotenv pandas")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DuckDBInitializer:
    """Handles DuckDB database initialization and setup"""
    
    def __init__(self):
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        self.db_path = Path('bristol_gate.duckdb')
        
    def load_environment(self) -> bool:
        """Load environment variables (primarily for API keys)"""
        logger.info("Loading environment variables...")
        
        # Load .env file
        env_path = Path('.env')
        if not env_path.exists():
            logger.warning(".env file not found. Using system environment variables.")
        else:
            load_dotenv(env_path)
            logger.info("Loaded .env file")
        
        # Log database path
        logger.info(f"DuckDB database path: {self.db_path.absolute()}")
        
        return True
    
    def connect_to_duckdb(self) -> bool:
        """Establish connection to DuckDB database file"""
        logger.info(f"Connecting to DuckDB database: {self.db_path}")
        
        try:
            # Connect to DuckDB database file
            self.con = duckdb.connect(database=str(self.db_path), read_only=False)
            
            # Test connection
            result = self.con.execute('SELECT 1').fetchone()
            logger.info("✅ Successfully connected to DuckDB")
            logger.info(f"Database file exists: {self.db_path.exists()}")
            logger.info(f"Database file size: {self.db_path.stat().st_size if self.db_path.exists() else 0} bytes")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to DuckDB: {e}")
            return False
    
    def execute_sql_file(self, sql_file_path: Path = Path('sql/duckdb_init.sql')) -> bool:
        """Execute SQL initialization script"""
        logger.info(f"Executing SQL script: {sql_file_path}")
        
        if not sql_file_path.exists():
            logger.error(f"❌ SQL file not found: {sql_file_path}")
            return False
        
        try:
            # Read SQL file
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            logger.info(f"Read SQL file ({len(sql_content)} characters)")
            
            # Remove comment lines first, then split statements
            lines = sql_content.split('\n')
            cleaned_lines = []
            
            for line in lines:
                # Skip comment lines and empty lines
                stripped = line.strip()
                if not stripped.startswith('--') and stripped:
                    cleaned_lines.append(line)
            
            # Join back and split on semicolons
            cleaned_content = '\n'.join(cleaned_lines)
            statements = [stmt.strip() for stmt in cleaned_content.split(';') if stmt.strip()]
            
            logger.info(f"Found {len(statements)} SQL statements to execute")
            
            # Execute each statement
            for i, statement in enumerate(statements, 1):
                try:
                    logger.info(f"Executing statement {i}/{len(statements)}")
                    logger.info(f"  {statement[:100]}{'...' if len(statement) > 100 else ''}")
                    
                    result = self.con.execute(statement)
                    
                    logger.info(f"  ✅ Statement {i} executed successfully")
                    
                    # Log results for statements that return data
                    if statement.upper().startswith(('SHOW', 'DESCRIBE', 'SELECT')):
                        try:
                            rows = result.fetchall()
                            if rows:
                                logger.info(f"  Result: {rows}")
                        except Exception:
                            # Some statements might not return fetchable results
                            pass
                        
                except Exception as e:
                    logger.error(f"❌ Error executing statement {i}: {e}")
                    logger.error(f"  Failed statement: {statement}")
                    return False
            
            logger.info("✅ Successfully executed all SQL statements")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error reading or executing SQL file: {e}")
            logger.error(f"  File path: {sql_file_path}")
            return False
    
    def load_symbols_data(self, symbols_file_path: Path) -> bool:
        """Load symbols reference data from CSV with detailed error handling"""
        logger.info(f"Loading symbols data from: {symbols_file_path}")
        
        if not symbols_file_path.exists():
            logger.error(f"❌ Symbols file not found: {symbols_file_path}")
            return False
        
        try:
            # Step 1: Read CSV file
            logger.info("Step 1: Reading CSV file...")
            df = pd.read_csv(symbols_file_path)
            logger.info(f"Successfully read CSV with {len(df)} rows and columns: {list(df.columns)}")
            
            # Step 2: Check if we have data
            if len(df) == 0:
                logger.error("❌ CSV file is empty")
                return False
            
            # Step 3: Rename columns to match database schema
            logger.info("Step 2: Renaming columns...")
            column_mapping = {
                'string.symbol': 'symbol',
                'string.source': 'source', 
                'string.description': 'description',
                'string.label.y': 'unit',
                'float.expense.ratio': 'expense_ratio'
            }
            
            # Check if required columns exist
            missing_columns = [col for col in column_mapping.keys() if col not in df.columns]
            if missing_columns:
                logger.error(f"❌ Missing required columns in CSV: {missing_columns}")
                logger.error(f"Available columns: {list(df.columns)}")
                return False
            
            df_clean = df.rename(columns=column_mapping)
            logger.info("Successfully renamed columns")
            
            # Step 4: Select only the columns we need (including expense_ratio)
            logger.info("Step 3: Selecting required columns...")
            df_clean = df_clean[['symbol', 'source', 'description', 'unit', 'expense_ratio']]
            
            # Step 5: Clean up any null values
            logger.info("Step 4: Cleaning null values...")
            df_clean = df_clean.fillna('')
            logger.info(f"Data cleaned. Final shape: {df_clean.shape}")
            
            # Step 6: Insert data into DuckDB using DataFrame
            logger.info("Step 5: Inserting data into DuckDB...")
            logger.info(f"Inserting {len(df_clean)} records into symbols table")
            
            # Register DataFrame as temporary table and insert
            self.con.register('temp_symbols', df_clean)
            self.con.execute('INSERT INTO symbols SELECT * FROM temp_symbols')
            self.con.unregister('temp_symbols')
            
            logger.info("✅ Data insertion completed")
            
            # Step 7: Verify the data was loaded
            logger.info("Step 6: Verifying data insertion...")
            count = self.con.execute('SELECT COUNT(*) FROM symbols').fetchone()[0]
            logger.info(f"✅ Successfully loaded {len(df_clean)} symbols into database")
            logger.info(f"Total symbols in database: {count}")
            
            # Step 8: Export to bronze parquet
            logger.info("Step 7: Exporting to bronze parquet...")
            self.export_to_bronze('symbols')
            
            return True
            
        except FileNotFoundError as e:
            logger.error(f"❌ File not found error: {e}")
            return False
        except pd.errors.EmptyDataError as e:
            logger.error(f"❌ CSV file is empty or corrupt: {e}")
            return False
        except KeyError as e:
            logger.error(f"❌ Column mapping error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Unexpected error loading symbols data: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False
    
    def export_to_bronze(self, table_name: str) -> bool:
        """Export table data to bronze parquet layer"""
        try:
            bronze_dir = Path('data/bronze')
            bronze_dir.mkdir(parents=True, exist_ok=True)
            
            parquet_file = bronze_dir / f'{table_name}.parquet'
            
            # Export to parquet (without schema prefix)
            self.con.execute(f"""
                COPY {table_name} 
                TO '{parquet_file}' 
                (FORMAT PARQUET)
            """)
            
            logger.info(f"✅ Exported {table_name} to {parquet_file}")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Failed to export {table_name} to bronze: {e}")
            return False
    
    def verify_setup(self) -> bool:
        """Verify the database setup was successful"""
        logger.info("Verifying database setup...")
        
        try:
            # Check tables
            tables_result = self.con.execute('SHOW TABLES').fetchall()
            tables = [row[0] for row in tables_result]
            
            expected_tables = [
                'symbols', 'stg_fred', 'stg_yahoo', 'stg_eia', 
                'stg_baker', 'stg_finra', 'stg_sp500', 'stg_usda', 'stg_occ', 'featured_data'
            ]
            
            missing_tables = []
            for table in expected_tables:
                if table not in tables:
                    missing_tables.append(table)
            
            if missing_tables:
                logger.error(f"❌ Missing tables: {missing_tables}")
                return False
            
            logger.info("✅ All expected tables found")
            
            # Show table counts
            for table in expected_tables:
                try:
                    count = self.con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                    logger.info(f"Table {table}: {count} rows")
                except Exception as e:
                    logger.warning(f"Could not get count for {table}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")
            return False
    
    def close_connection(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()
            logger.info("Closed DuckDB connection")


class DuckDBManager:
    """
    Handles DuckDB data operations for the Bristol Gate pipeline
    
    Functionality:
    - Data extraction from staging tables
    - Data upload and upsert operations
    - Table maintenance and optimization
    - Data quality checks
    - Bronze layer parquet exports
    """
    
    def __init__(self, db_path: str = 'bristol_gate.duckdb'):
        self.db_path = Path(db_path)
        self.con: Optional[duckdb.DuckDBPyConnection] = None
        
    def connect(self) -> bool:
        """Connect to DuckDB database"""
        try:
            self.con = duckdb.connect(database=str(self.db_path), read_only=False)
            return True
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            return False
    
    def upload_data(self, table_name: str, data: pd.DataFrame) -> bool:
        """Upload data to specified DuckDB table"""
        try:
            if not self.con:
                if not self.connect():
                    return False
            
            # Register DataFrame and insert (without schema prefix)
            temp_table = f'temp_{table_name}'
            self.con.register(temp_table, data)
            self.con.execute(f'INSERT INTO {table_name} SELECT * FROM {temp_table}')
            self.con.unregister(temp_table)
            
            # Export to bronze
            self.export_to_bronze(table_name)
            
            logger.info(f"✅ Uploaded {len(data)} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading data to {table_name}: {e}")
            return False
    
    def extract_data(self, table_name: str, filters: dict = None) -> pd.DataFrame:
        """Extract data from DuckDB table with optional filters"""
        try:
            if not self.con:
                if not self.connect():
                    return pd.DataFrame()
            
            query = f'SELECT * FROM {table_name}'
            
            if filters:
                conditions = []
                for column, value in filters.items():
                    if isinstance(value, str):
                        conditions.append(f"{column} = '{value}'")
                    else:
                        conditions.append(f"{column} = {value}")
                
                if conditions:
                    query += ' WHERE ' + ' AND '.join(conditions)
            
            return self.con.execute(query).df()
            
        except Exception as e:
            logger.error(f"❌ Error extracting data from {table_name}: {e}")
            return pd.DataFrame()
    
    def export_to_bronze(self, table_name: str) -> bool:
        """Export table data to bronze parquet layer"""
        try:
            bronze_dir = Path('data/bronze')
            bronze_dir.mkdir(parents=True, exist_ok=True)
            
            parquet_file = bronze_dir / f'{table_name}.parquet'
            
            # Export to parquet (without schema prefix)
            self.con.execute(f"""
                COPY {table_name} 
                TO '{parquet_file}' 
                (FORMAT PARQUET)
            """)
            
            logger.info(f"✅ Exported {table_name} to {parquet_file}")
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Failed to export {table_name} to bronze: {e}")
            return False
    
    def close(self):
        """Close DuckDB connection"""
        if self.con:
            self.con.close()


def main():
    """Main execution function for database initialization"""
    parser = argparse.ArgumentParser(description='Initialize DuckDB database for Bristol Gate pipeline')
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
    
    try:
        logger.info("🚀 Starting DuckDB initialization...")
        
        # Step 1: Load environment
        if not initializer.load_environment():
            success = False
            return
        
        # Step 2: Connect to DuckDB
        if not initializer.connect_to_duckdb():
            success = False
            return
        
        # Step 3: Execute SQL script
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
        
        logger.info("🎉 DuckDB initialization completed successfully!")
        
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