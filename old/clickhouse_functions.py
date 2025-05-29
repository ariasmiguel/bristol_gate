#!/usr/bin/env python3
"""
ClickHouse Functions for Bristol Gate Data Pipeline

This module provides ClickHouse-related functionality including:
1. Database initialization and setup
2. Data extraction and upload functions
3. Table management and utilities

Classes:
    ClickHouseInitializer: Handles database and table setup
    ClickHouseManager: Handles data operations (future)

Usage:
    python -m pipeline.clickhouse_functions [--load-symbols]
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
    import clickhouse_connect
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Please install required packages:")
    print("pip install clickhouse-connect python-dotenv pandas")
    sys.exit(1)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class ClickHouseInitializer:
    """Handles ClickHouse database initialization and setup"""
    
    def __init__(self):
        self.client: Optional[clickhouse_connect.driver.Client] = None
        self.config = {}
        
    def load_environment(self) -> bool:
        """Load and validate environment variables"""
        logger.info("Loading environment variables...")
        
        # Load .env file
        env_path = Path('.env')
        if not env_path.exists():
            logger.warning(".env file not found. Using system environment variables.")
        else:
            load_dotenv(env_path)
            logger.info("Loaded .env file")
        
        # Get ClickHouse configuration from environment variables only
        self.config = {
            'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
            'port': int(os.getenv('CLICKHOUSE_PORT', '8123')),
            'username': os.getenv('CLICKHOUSE_USER', 'default'),
            'password': os.getenv('CLICKHOUSE_PASSWORD', ''),
            'database': os.getenv('CLICKHOUSE_DATABASE', 'bristol_gate'),
            'secure': os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true',
            'verify': os.getenv('CLICKHOUSE_VERIFY', 'true').lower() == 'true',
            'compress': os.getenv('CLICKHOUSE_COMPRESS', 'true').lower() == 'true',
        }
        
        # Log configuration (without password)
        config_display = {k: v for k, v in self.config.items() if k != 'password'}
        config_display['password'] = '***' if self.config['password'] else '(empty)'
        logger.info(f"ClickHouse configuration: {config_display}")
        
        return True
    
    def connect_to_clickhouse(self) -> bool:
        """Establish connection to ClickHouse (without specifying database)"""
        logger.info("Connecting to ClickHouse...")
        
        try:
            # Connect without specifying a database - let the SQL script handle database creation/selection
            self.client = clickhouse_connect.get_client(
                host=self.config['host'],
                port=self.config['port'],
                username=self.config['username'],
                password=self.config['password'],
                secure=self.config['secure'],
                verify=self.config['verify'],
                compress=self.config['compress']
                # Note: No database parameter - we'll use the default database initially
            )
            
            # Test connection
            result = self.client.command('SELECT 1')
            logger.info("✅ Successfully connected to ClickHouse")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
            logger.error("Please check your ClickHouse configuration and ensure the server is running")
            return False
    
    def execute_sql_file(self, sql_file_path: Path = Path('sql/init_tables.sql')) -> bool:
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
                    
                    result = self.client.command(statement)
                    
                    logger.info(f"  ✅ Statement {i} executed successfully")
                    
                    # Log results for statements that return data
                    if result and not statement.upper().startswith(('CREATE', 'USE')):
                        logger.info(f"  Result: {result}")
                        
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
                'string.label.y': 'unit'
            }
            
            # Check if required columns exist
            missing_columns = [col for col in column_mapping.keys() if col not in df.columns]
            if missing_columns:
                logger.error(f"❌ Missing required columns in CSV: {missing_columns}")
                logger.error(f"Available columns: {list(df.columns)}")
                return False
            
            df_clean = df.rename(columns=column_mapping)
            logger.info("Successfully renamed columns")
            
            # Step 4: Select only the columns we need
            logger.info("Step 3: Selecting required columns...")
            df_clean = df_clean[['symbol', 'source', 'description', 'unit']]
            
            # Step 5: Clean up any null values
            logger.info("Step 4: Cleaning null values...")
            df_clean = df_clean.fillna('')
            logger.info(f"Data cleaned. Final shape: {df_clean.shape}")
            
            # Step 6: Insert data into ClickHouse
            logger.info("Step 5: Inserting data into ClickHouse...")
            logger.info(f"Inserting {len(df_clean)} records using DataFrame method")
            # Insert data into ClickHouse using DataFrame method
            self.client.insert_df('bristol_gate.symbols', df_clean)
            logger.info("✅ Data insertion completed")
            
            # Step 7: Verify the data was loaded
            logger.info("Step 6: Verifying data insertion...")
            count = self.client.command('SELECT COUNT(*) FROM bristol_gate.symbols')
            logger.info(f"✅ Successfully loaded {len(df_clean)} symbols into database")
            logger.info(f"Total symbols in database: {count}")
            
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
    
    def verify_setup(self) -> bool:
        """Verify the database setup was successful"""
        logger.info("Verifying database setup...")
        
        try:
            # Check if database exists
            databases = self.client.command('SHOW DATABASES')
            if 'bristol_gate' not in str(databases):
                logger.error("❌ bristol_gate database not found")
                return False
            
            # Check tables
            tables = self.client.command('SHOW TABLES FROM bristol_gate')
            expected_tables = [
                'symbols', 'stg_fred', 'stg_yahoo', 'stg_eia', 
                'stg_baker', 'stg_finra', 'stg_sp500', 'stg_usda'
            ]
            
            missing_tables = []
            for table in expected_tables:
                if table not in str(tables):
                    missing_tables.append(table)
            
            if missing_tables:
                logger.error(f"❌ Missing tables: {missing_tables}")
                return False
            
            logger.info("✅ All expected tables found")
            
            # Show table counts
            for table in expected_tables:
                try:
                    count = self.client.command(f'SELECT COUNT(*) FROM bristol_gate.{table}')
                    logger.info(f"Table {table}: {count} rows")
                except Exception as e:
                    logger.warning(f"Could not get count for {table}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error during verification: {e}")
            return False
    
    def close_connection(self):
        """Close ClickHouse connection"""
        if self.client:
            self.client.close()
            logger.info("Closed ClickHouse connection")


class ClickHouseManager:
    """
    Handles ClickHouse data operations for the Bristol Gate pipeline
    
    Future functionality:
    - Data extraction from staging tables
    - Data upload and upsert operations
    - Table maintenance and optimization
    - Data quality checks
    """
    
    def __init__(self):
        self.client: Optional[clickhouse_connect.driver.Client] = None
        # TODO: Implement data operations
        pass
    
    def upload_data(self, table_name: str, data: pd.DataFrame) -> bool:
        """Upload data to specified ClickHouse table"""
        # TODO: Implement data upload functionality
        pass
    
    def extract_data(self, table_name: str, filters: dict = None) -> pd.DataFrame:
        """Extract data from ClickHouse table with optional filters"""
        # TODO: Implement data extraction functionality
        pass
