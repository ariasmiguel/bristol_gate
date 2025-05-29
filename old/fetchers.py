import pandas as pd
import yfinance as yf
from fredapi import Fred
import os
import random
from datetime import datetime
from dotenv import load_dotenv
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from urllib.parse import urlparse, unquote
from dateutil.relativedelta import relativedelta
import requests
import re
import traceback

# Load environment variables from .env file
load_dotenv()

# Now import from myeia after environment variables are set
from myeia import API

def load_symbols_csv(file_path: str) -> pd.DataFrame:
    """
    Loads the symbols CSV file into a pandas DataFrame.

    Parameters:
    - file_path (str): The path to the CSV file.

    Returns:
    - pd.DataFrame: DataFrame containing the symbols data.
    """
    df = pd.read_csv(file_path)
    return df

def get_yahoo_data(symbol: str, start_date: datetime = datetime(1990, 1, 1), end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Fetches historical market data from Yahoo Finance.

    Parameters:
    - symbol (str): The ticker symbol for the stock.
    - start_date (datetime): The start date for the data.
    - end_date (datetime): The end date for the data.

    Returns:
    - pd.DataFrame: DataFrame containing the historical market data with standardized column order.
    """
    # Download the data
    df = yf.download(symbol, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    
    # Check if we have a MultiIndex in columns and flatten if needed
    if isinstance(df.columns, pd.MultiIndex):
        # Create a new DataFrame with flattened column names
        df.columns = df.columns.get_level_values(0)
    
    # Reset the index to make the date a column
    df = df.reset_index()
    
    # Rename the columns to match the desired structure
    column_mapping = {
        'Date': 'date',
        'Open': 'open', 
        'High': 'high', 
        'Low': 'low', 
        'Close': 'close', 
        'Adj Close': 'adj_close', 
        'Volume': 'volume'
    }
    
    # Only rename columns that exist
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df.rename(columns={old_col: new_col}, inplace=True)
    
    # Melt the dataframe to transform from wide to long format
    value_columns = [col for col in df.columns if col not in ['date']]
    result_df = pd.melt(
            df, 
            id_vars=['date'], 
            value_vars=value_columns,
            var_name='metric',
            value_name='value'
        )
    
    # Convert date to date format
    result_df['date'] = pd.to_datetime(result_df['date']).dt.date
    
    # Add the symbol column
    result_df['symbol'] = symbol
    
    # Reorder columns
    column_order = ['date', 'symbol', 'metric', 'value']
    result_df = result_df[column_order]
    
    return result_df

def get_fred_data(series_id: str, start_date: datetime = datetime(1990, 1, 1), end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Fetches a time series from the FRED API and returns it as a pandas DataFrame in long format.

    Parameters:
    - series_id (str): The FRED series ID to fetch.
    - start_date (datetime): The start date for the data.
    - end_date (datetime): The end date for the data.

    Returns:
    - pd.DataFrame: DataFrame with columns:
        - date: The date of the data point
        - symbol: The FRED series ID
        - metric: The type of data (always 'value' for FRED data)
        - value: The actual value
    """
    fred_api_key = os.getenv('FRED_API_KEY')
    fred = Fred(api_key=fred_api_key)
    max_retries = 3
    wait_time = 20  # Initial wait time in seconds

    for attempt in range(max_retries):
        try:
            data = fred.get_series(series_id, start_date, end_date)
            df = pd.DataFrame(data, columns=['value'])

            # Reset the index to make the date a column and rename it
            df = df.reset_index()
            df = df.rename(columns={'index': 'date'})

            # Add the symbol column
            df['symbol'] = series_id
            
            # Add the metric column (always 'value' for FRED data)
            df['metric'] = 'value'
            
            # Reorder columns to match the desired format
            column_order = ['date', 'symbol', 'metric', 'value']
            df = df[column_order]
            
            # Remove any rows with NaN values
            df = df.dropna(subset=['value'])
            
            return df
            
        except ValueError as e:
            if "Too Many Requests" in str(e):
                print(f"Rate limit hit. Waiting for {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            else:
                raise e
    raise Exception("Failed to fetch data from FRED after multiple attempts.")

def get_eia_data(series_id: str, start_date: datetime = datetime(1990, 1, 1), end_date: datetime = datetime.now()) -> pd.DataFrame:
    """
    Fetches a time series from the EIA API and returns it as a pandas DataFrame in long format.
    
    Parameters:
    - series_id (str): The EIA series ID to fetch.
    - start_date (datetime): The start date for the data.
    - end_date (datetime): The end date for the data.
    
    Returns:
    - pd.DataFrame: DataFrame with columns:
        - date: The date of the data point
        - symbol: The EIA series ID
        - metric: The type of data (always 'value' for EIA data)
        - value: The actual value
    """
    # Check if EIA_TOKEN is set
    eia_token = os.getenv('EIA_TOKEN')
    if not eia_token:
        raise ValueError("EIA_TOKEN environment variable is not set. Please set it to your EIA API key.")
    
    max_retries = 3
    wait_time = 20  # Initial wait time in seconds

    for attempt in range(max_retries):
        try:
            # Create API instance
            eia = API()
            
            # Format dates for API call
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            print(f"Fetching EIA data for series {series_id} from {start_str} to {end_str}")
            
            # Get data for the series
            df = eia.get_series(
                series_id=series_id,
                start_date=start_str,
                end_date=end_str
            )
            
            if df is None or df.empty:
                raise ValueError(f"No data returned from EIA API for series {series_id}")
            
            print(f"Successfully retrieved data. Shape: {df.shape}")
            
            # Reset the index to make the date a column and rename it
            df = df.reset_index()
            df = df.rename(columns={'index': 'date'})
            
            # Set the columns to lowercase
            df.columns = df.columns.str.lower()
            
            # Rename the second column to value
            if len(df.columns) < 2:
                raise ValueError(f"DataFrame has insufficient columns: {df.columns.tolist()}")
            
            df.rename(columns={df.columns[1]: 'value'}, inplace=True)
            
            # Add the symbol column
            df['symbol'] = series_id
            
            # Add the metric column (always 'value' for EIA data)
            df['metric'] = 'value'
            
            # Reorder columns to match the desired format
            column_order = ['date', 'symbol', 'metric', 'value']
            df = df[column_order]
            
            # Remove any rows with NaN values
            df = df.dropna(subset=['value'])
            
            print(f"Final processed data shape: {df.shape}")
            return df
            
        except Exception as e:
            error_msg = f"Error fetching data for {series_id}: {str(e)}"
            if isinstance(e, ValueError):
                error_msg += f"\nValueError details: {str(e)}"
            elif isinstance(e, KeyError):
                error_msg += f"\nKeyError details: {str(e)}"
            elif isinstance(e, AttributeError):
                error_msg += f"\nAttributeError details: {str(e)}"
            
            if attempt < max_retries - 1:
                print(f"{error_msg}\nRetrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                wait_time *= 2  # Exponential backoff
            else:
                print(f"Final attempt failed: {error_msg}")
                raise Exception(error_msg)

def get_baker_data(baker_url: str = "https://bakerhughesrigcount.gcs-web.com/na-rig-count",
                   file_name: str = "North America Rotary Rig Count (",
                   sheet_name: str = "US Oil & Gas Split",
                   skip_rows: int = 6,
                   download_dir: str = "data/baker_hughes") -> pd.DataFrame:
    """
    Downloads and processes Baker Hughes rig count data.
    
    Returns:
    - pd.DataFrame: DataFrame containing the Baker Hughes data with standardized column format.
    """
    # Make sure download directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    driver = None
    try:
        # Setup Chrome options
        print("Setting up Chrome options...")
        options = Options()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless=new")  # Headless mode
        options.add_argument("--window-size=1920,1080")
        
        # Add a realistic user agent
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={user_agent}")
        
        # Setup download preferences
        prefs = {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        
        # Initialize the driver
        print("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=options)
        
        # Apply stealth settings
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        
        # Navigate to the Baker Hughes page
        print("Navigating to Baker Hughes page...")
        driver.get(baker_url)
        time.sleep(random.uniform(3, 5))  # Wait for page to load
        
        # Check if access was granted
        if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
            print("Access denied. Try again later or adjust browser settings.")
            return None
            
        # Find the target link
        print("Searching for rig count spreadsheet link...")
        all_links = driver.find_elements(By.TAG_NAME, "a")
        target_link = None
        search_text = file_name
        
        for link in all_links:
            try:
                if search_text in link.text.strip():
                    target_link = link
                    print(f"Found link: {link.text}")
                    break
            except:
                continue
                
        if not target_link:
            print("Target link not found.")
            return None
            
        # Download the file
        print("Downloading Excel file...")
        target_link.click()
        
        # Wait for download to complete
        print("Waiting for download to complete...")
        time.sleep(20)  # Allow time for download
        
        # Find the most recent Excel file
        excel_files = [f for f in os.listdir(download_dir) if f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.xlsb')]
        if not excel_files:
            print("No Excel files found in download directory.")
            return None
            
        most_recent_file = max([os.path.join(download_dir, f) for f in excel_files], key=os.path.getctime)
        print(f"Processing file: {most_recent_file}")
        
        # Read the Excel file
        if most_recent_file.endswith('.xlsb'):
            df = pd.read_excel(most_recent_file, engine='pyxlsb', sheet_name=sheet_name, skiprows=skip_rows)
        else:
            df = pd.read_excel(most_recent_file, sheet_name=sheet_name, skiprows=skip_rows)
        
        # Convert Excel date numbers to datetime
        date_col = None
        for col in df.columns:
            if 'date' in col.lower():
                date_col = col
                break
                
        if not date_col:
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col].dtype):
                    # Check if values are in reasonable range for Excel dates
                    if df[col].min() > 20000 and df[col].max() < 50000:
                        date_col = col
                        break
        
        if date_col:
            df['date'] = pd.to_datetime(df[date_col], origin='1899-12-30', unit='D').dt.date
            if date_col != 'date':
                df = df.drop(columns=[date_col])
        
        # Add BKR_ prefix to all columns except date
        df.columns = ['date' if col == 'date' else f'BKR{col}' for col in df.columns]
        
        # Remove percentage columns
        percent_columns = [col for col in df.columns if '%' in col]
        if percent_columns:
            df = df.drop(columns=percent_columns)
        
        # Melt the dataframe to transform from wide to long format
        value_columns = [col for col in df.columns if col.startswith('BKR')]
        result_df = pd.melt(
            df, 
            id_vars=['date'], 
            value_vars=value_columns,
            var_name='metric',
            value_name='value'
        )
        
        # Add the symbol column (using the metric name as the symbol)
        result_df['symbol'] = result_df['metric']
        result_df['metric'] = 'rig_count'
        
        # Reorder columns to match the desired format
        column_order = ['date', 'symbol', 'metric', 'value']
        result_df = result_df[column_order]
        
        # Remove any rows with NaN values
        result_df = result_df.dropna(subset=['value'])
        
        return result_df
        
    except Exception as e:
        print(f"Error in get_baker_data: {str(e)}")
        return None
        
    finally:
        if driver:
            driver.quit()

def get_finra_data(
    url: str = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics",
    download_dir: str = "data/finra",
    headless: bool = True
) -> pd.DataFrame:
    """
    Fetches FINRA margin statistics and returns a standardized DataFrame.
    
    Parameters:
    - url (str): The URL of the FINRA margin statistics page.
    - download_dir (str): Directory where files will be downloaded.
    - headless (bool): Whether to run Chrome in headless mode.
    
    Returns:
    - pd.DataFrame: DataFrame with standardized columns.
    """
    # Make sure download directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    driver = None
    try:
        # Setup Chrome options
        print("Setting up Chrome options...")
        options = Options()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        
        # Add a realistic user agent
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={user_agent}")
        
        # Initialize the driver
        print("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=options)
        
        # Apply stealth settings
        stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        
        # Step 1: Navigate to the FINRA margin statistics page
        print(f"Navigating to FINRA page: {url}")
        driver.get(url)
        
        # Add a random delay to appear more human-like
        time.sleep(random.uniform(2, 4))
        
        # Check if access was granted
        if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
            print("Access denied. Try again later or adjust browser settings.")
            raise Exception("Access denied by website")
        
        print("Successfully accessed the page, extracting links...")
        
        # Step 2: Extract all links on the page
        all_links = driver.find_elements(By.TAG_NAME, "a")
        
        link_data = []
        for i, link in enumerate(all_links):
            try:
                href = link.get_attribute("href")
                text = link.text.strip()
                classes = link.get_attribute("class")
                link_data.append({
                    "index": i,
                    "text": text,
                    "href": href,
                    "class": classes
                })
            except Exception as e:
                print(f"Error processing link #{i}: {e}")
                
        # Step 3: Find the "DOWNLOAD THE DATA" link
        download_link = None
        for link_info in link_data:
            if "DOWNLOAD THE DATA" in link_info.get('text', '').upper():
                download_link = link_info
                break
        
        if not download_link:
            print("Could not find 'DOWNLOAD THE DATA' link")
            raise Exception("Download link not found")
        
        # Get the download URL
        download_url = download_link['href']
        print(f"Found download URL: {download_url}")
        
        # Step 4: Download the Excel file directly
        print(f"Downloading Excel file from: {download_url}")
        
        # Send a GET request to download the file
        headers = {
            "User-Agent": user_agent
        }
        
        response = requests.get(download_url, headers=headers)
        
        # Check if the request was successful
        if response.status_code != 200:
            print(f"Failed to download the file. Status code: {response.status_code}")
            raise Exception(f"Download failed with status code {response.status_code}")
        
        # Get the filename from the URL
        parsed_url = urlparse(download_url)
        filename = os.path.basename(parsed_url.path)
        
        # Save the file to the download directory
        file_path = os.path.join(download_dir, filename)
        with open(file_path, 'wb') as file:
            file.write(response.content)
        
        print(f"File downloaded successfully to: {file_path}")
        
        # Step 5: Read the Excel file with pandas
        try:
            df = pd.read_excel(file_path)
            print(f"Excel file read successfully. Shape: {df.shape}")
            
            # Create the date column from Year-Month
            # Convert Year-Month to datetime using the last day of each month
            df['date'] = pd.to_datetime(df['Year-Month'], format='%Y-%m')
            df['date'] = df['date'].apply(lambda x: (x + relativedelta(months=1, days=-1))).dt.date
            
            # Delete the Year-Month column
            df = df.drop(columns=['Year-Month'])
            
            # Rename the columns
            df.columns = [
                "FINRA/Margin_Debt",
                "FINRA/Free_Credit_Cash",
                "FINRA/Free_Credit_Margin",
                "date"
            ]
            
            # Melt the dataframe to transform from wide to long format
            df = df.melt(id_vars=['date'], var_name='metric', value_name='value')
            
            # Add the symbol column (using the metric name as the symbol)
            df['symbol'] = df['metric']
            df['metric'] = 'value'
        
            # Reorder columns to match the desired format
            column_order = ['date', 'symbol', 'metric', 'value']
            df = df[column_order]
            
            # Remove NaN values from value column
            df = df.dropna(subset=['value'])
            
            print(f"Processed FINRA data. Shape: {df.shape}")
            return df
            
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            raise
    
    except Exception as e:
        print(f"An error occurred in get_finra_data: {str(e)}")
        return None
    
    finally:
        # Close the browser
        if driver:
            print("Closing the browser...")
            driver.quit()

def get_sp500_data(
    sp500_url: str = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx",
    referer_url: str = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
    download_dir: str = "data/sp500"
) -> pd.DataFrame:
    """
    Downloads and processes S&P 500 earnings and estimates data.
    
    Returns:
    - pd.DataFrame: DataFrame with columns:
        - date: The date of the data point
        - symbol: The S&P 500 metric identifier
        - metric: The type of data (earnings, estimates, etc.)
        - value: The actual value
    """
    # Make sure download directory exists
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    driver = None
    try:
        # Set up Chrome options
        print("Setting up Chrome options...")
        options = Options()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        
        # Add a realistic user agent
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
        options.add_argument(f"--user-agent={user_agent}")
        
        # Setup download preferences
        prefs = {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        
        # Initialize the driver
        print("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=options)
        
        # First, visit the referer page to establish session
        print(f"Visiting referer page to establish session: {referer_url}")
        driver.get(referer_url)
        time.sleep(random.uniform(2, 4))
        
        # Now navigate to the download URL
        print(f"Downloading Excel file from: {sp500_url}")
        driver.get(sp500_url)
        time.sleep(random.uniform(4, 6))
        
        # Check if file is downloaded
        filename = "sp-500-eps-est.xlsx"
        file_path = os.path.join(download_dir, filename)
        max_wait_time = 10
        wait_time = 0
        while not os.path.exists(file_path) and wait_time < max_wait_time:
            time.sleep(1)
            wait_time += 1
            print(f"Waiting for download... ({wait_time}s)")
        
        if not os.path.exists(file_path):
            raise Exception("File download timed out or failed")
            
        print(f"File downloaded successfully to: {file_path}")
        
        # Column name mapping for quarterly data
        column_names = [
            "date",
            "op_earnings_per_share",
            "ar_earnings_per_share",
            "cash_dividends_per_share",
            "sales_per_share",
            "book_value_per_share",
            "capex_per_share",
            "price",
            "divisor"
        ]
        
        # Read the quarterly data
        print("Reading Quarterly Data sheet...")
        df_quarterly = pd.read_excel(
            file_path,
            sheet_name="QUARTERLY DATA",
            skiprows=5,
            names=column_names
        )
        
        # Melt the quarterly DataFrame
        print("Melting Quarterly DataFrame...")
        df_quarterly_melted = df_quarterly.melt(
            id_vars=['date'],
            value_vars=[
                'op_earnings_per_share',
                'ar_earnings_per_share',
                'cash_dividends_per_share',
                'sales_per_share',
                'book_value_per_share',
                'capex_per_share',
                'price',
                'divisor'
            ],
            var_name='metric',
            value_name='value'
        )
        
        # Remove NaN values
        df_quarterly_melted = df_quarterly_melted.dropna(subset=['value'])
        
        # Read the Estimates sheet
        print("\nReading Estimates & PEs sheet...")
        df_estimates_raw = pd.read_excel(file_path, sheet_name="ESTIMATES&PEs")
        
        # Find the row index containing "ACTUALS"
        actuals_row = df_estimates_raw[df_estimates_raw.iloc[:, 0] == "ACTUALS"].index[0]
        print(f"ACTUALS row index: {actuals_row}")
        
        # First, get only the data after ACTUALS row
        df_estimates = df_estimates_raw.iloc[actuals_row + 1:].copy()
        df_estimates = df_estimates.reset_index(drop=True)
        
        # Function to check if a string can be converted to datetime
        def is_valid_date(date_str):
            try:
                if pd.isna(date_str):
                    return False
                # Use mixed format parsing
                pd.to_datetime(str(date_str).strip(), format='mixed')
                return True
            except:
                return False
        
        # Filter rows where the first column contains valid dates
        valid_date_mask = df_estimates.iloc[:, 0].apply(is_valid_date)
        df_estimates = df_estimates[valid_date_mask].copy()
        df_estimates = df_estimates.reset_index(drop=True)
        
        # Convert first column to date format using mixed parsing
        df_estimates.iloc[:, 0] = pd.to_datetime(
            df_estimates.iloc[:, 0].astype(str).str.strip(), 
            format='mixed'
        ).dt.date
        
        # Remove columns that contain only NaN values
        df_estimates = df_estimates.dropna(axis=1, how='all')
        
        # Define the column names in order
        estimates_column_names = [
            'date',
            'sp500_price',
            'op_earnings_per_share',
            'ar_earnings_per_share',
            'op_earnings_pe',
            'ar_earnings_pe',
            'op_earnings_ttm',
            'ar_earnings_ttm'
        ]
        
        # Assign column names to the remaining columns
        num_cols = len(df_estimates.columns)
        df_estimates.columns = estimates_column_names[:num_cols]
        
        # Melt the estimates DataFrame
        print("Melting Estimates DataFrame...")
        df_estimates_melted = df_estimates.melt(
            id_vars=['date'],
            value_vars=[col for col in df_estimates.columns if col != 'date'],
            var_name='metric',
            value_name='value'
        )
        
        # Remove NaN values from estimates
        df_estimates_melted = df_estimates_melted.dropna(subset=['value'])
        
        # Convert quarterly data date to date format
        df_quarterly_melted['date'] = pd.to_datetime(df_quarterly_melted['date']).dt.date
        
        # Combine both melted dataframes
        print("\nCombining quarterly and estimates data...")
        df_combined = pd.concat([df_quarterly_melted, df_estimates_melted], ignore_index=True)
        
        # Sort the combined data by date descending
        df_combined = df_combined.sort_values('date', ascending=False)
        
        # Define the symbols we want to keep
        keep_symbols = [
            'op_earnings_per_share',
            'ar_earnings_per_share',
            'cash_dividends_per_share',
            'sales_per_share',
            'book_value_per_share',
            'capex_per_share',
            'op_earnings_pe',
            'ar_earnings_pe',
            'op_earnings_ttm',
            'ar_earnings_ttm'
        ]
        
        # Filter to keep only the desired symbols
        df_combined = df_combined[df_combined['metric'].isin(keep_symbols)]
        
        # Add the symbol column (using the metric name as the symbol)
        df_combined['symbol'] = 'SILVERBLATT_' + df_combined['metric']
        
        # Reorder columns to match the desired format
        column_order = ['date', 'symbol', 'metric', 'value']
        df_combined = df_combined[column_order]
        
        print("\nData processing completed successfully")
        print(f"Final shape: {df_combined.shape}")
        print("\nUnique symbols:")
        print(sorted(df_combined['symbol'].unique().tolist()))
        
        return df_combined
        
    except Exception as e:
        print(f"Error in get_sp500_data: {str(e)}")
        return None
        
    finally:
        if driver:
            driver.quit()

def get_usda_ers_data(
    usda_page_url: str = "https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/data-files-us-and-state-level-farm-income-and-wealth-statistics/",
    target_link_text_in_file: str = "U.S. farm sector financial indicators",
    target_metric_text_in_file: str = r'net\s+farm\s+income',
    sheet_name_in_file: str = 'Sheet1',
    download_dir: str = "data/usda",
    symbol_name: str = "USDA_NET_FARM_INCOME",
    headless: bool = True
) -> pd.DataFrame | None:
    r"""
    Downloads and processes USDA ERS farm income data for a specific metric.

    Parameters:
    - usda_page_url (str): URL of the USDA ERS data page.
    - target_link_text_in_file (str): Text to identify the download link for the Excel file.
    - target_metric_text_in_file (str): Regex pattern to identify the target metric row 
                                      (e.g., r'net\s+farm\s+income' for "Net farm income") in the Excel sheet.
    - sheet_name_in_file (str): Name of the sheet to process in the Excel file.
    - download_dir (str): Directory to download the file to.
    - symbol_name (str): Symbol to use for the output data (e.g., "USDA_NET_FARM_INCOME").
    - headless (bool): Whether to run Chrome in headless mode.

    Returns:
    - pd.DataFrame | None: DataFrame with columns ['date', 'symbol', 'metric', 'value'] or None if an error occurs.
    """

    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
        print(f"Created directory: {download_dir}")

    driver = None
    downloaded_file_path = None
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"

    try:
        print("Setting up Chrome options...")
        options = Options()
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-agent={user_agent}")
        prefs = {
            "download.default_directory": os.path.abspath(download_dir),
            "download.prompt_for_download": False, "download.directory_upgrade": True,
            "safebrowsing.enabled": False, "plugins.always_open_pdf_externally": True,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)

        print("Initializing Chrome driver...")
        driver = webdriver.Chrome(options=options)
        stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32",
                webgl_vendor="Intel Inc.", renderer="Intel Iris OpenGL Engine", fix_hairline=True)

        print(f"Navigating to USDA page: {usda_page_url}")
        driver.get(usda_page_url)
        time.sleep(random.uniform(4, 7)) # Allow page to load

        if "Access Denied" in driver.page_source or "Forbidden" in driver.page_source:
            raise Exception(f"Access denied by {usda_page_url}")
        
        print("Successfully accessed the page, searching for the download link...")
        all_links = driver.find_elements(By.TAG_NAME, "a")
        found_link_href = None
        print(f"Found {len(all_links)} links. Searching for link text containing '{target_link_text_in_file}' and ending with .xlsx or .xls.")

        for link_element in all_links:
            try:
                link_text_content = link_element.text.strip()
                href_content = link_element.get_attribute("href")
                if target_link_text_in_file.lower() in link_text_content.lower():
                    if href_content and (href_content.endswith(".xlsx") or href_content.endswith(".xls")):
                        found_link_href = href_content
                        print(f"Found matching direct file link: '{link_text_content}' with href: {found_link_href}")
                        break
            except Exception: # Ignore stale elements or other link processing issues
                continue
        
        if not found_link_href:
            print(f"Could not find a direct .xlsx or .xls download link containing the text: '{target_link_text_in_file}'.")
            return None

        print(f"Attempting to download from: {found_link_href}")
        parsed_url = urlparse(found_link_href)
        file_name = os.path.basename(unquote(parsed_url.path))
        if not file_name or not (file_name.endswith(".xlsx") or file_name.endswith(".xls")): # Ensure valid filename
            file_name = "usda_downloaded_data.xlsx" # Fallback filename
        
        downloaded_file_path = os.path.join(download_dir, file_name)

        headers = {'User-Agent': user_agent}
        response = requests.get(found_link_href, headers=headers, stream=True, timeout=60)
        response.raise_for_status() # Raise an exception for bad status codes
        with open(downloaded_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"File downloaded successfully to: {downloaded_file_path}")

    except Exception as e:
        print(f"An error occurred during web scraping/download: {str(e)}")
        traceback.print_exc()
        return None
    finally:
        if driver:
            print("Closing the browser...")
            driver.quit()

    # --- Data Processing Part ---
    if not (downloaded_file_path and os.path.exists(downloaded_file_path)):
        print("\nSkipping data processing: file not downloaded or not found.")
        return None

    print(f"\n--- Processing downloaded file: {downloaded_file_path} ---")
    try:
        df_full = pd.read_excel(downloaded_file_path, sheet_name=sheet_name_in_file, header=None)
        print(f"Full DataFrame loaded from sheet '{sheet_name_in_file}'. Shape: {df_full.shape}")

        # --- Dynamically find the Year Header Row ---
        year_header_row_index = -1
        year_data_start_col_index = -1 

        def is_potential_year_header(val):
            if pd.isna(val): return False
            s_val = str(val).strip()
            # Matches YYYY or YYYYF, YYYYE etc. (4-digit year possibly followed by a capital letter)
            return bool(re.fullmatch(r'(19\d{2}|20\d{2})[A-Z]?\b', s_val))

        for i, row_series in df_full.iterrows():
            potential_year_cells_count = 0
            first_year_col_in_row = -1
            # Iterate starting from the second column (index 1) as first is usually labels
            for col_idx, cell_val in enumerate(row_series.iloc[1:]): 
                if is_potential_year_header(cell_val):
                    potential_year_cells_count += 1
                    if first_year_col_in_row == -1:
                        first_year_col_in_row = col_idx + 1 # +1 because we sliced from iloc[1:]
            
            if potential_year_cells_count > 3: # Heuristic: needs at least 4 year-like headers
                year_header_row_index = i
                year_data_start_col_index = first_year_col_in_row
                print(f"Identified year header row at sheet index: {year_header_row_index}, year data starts at column index: {year_data_start_col_index}")
                print(f"Content of identified year row (from detected start col): \n{df_full.iloc[year_header_row_index, year_data_start_col_index:]}")
                break
        
        if year_header_row_index == -1 or year_data_start_col_index == -1:
            print("Could not dynamically identify the year header row based on heuristics.")
            return None

        year_header_values_raw = df_full.iloc[year_header_row_index, year_data_start_col_index:]
        
        def extract_year_int(value):
            if pd.isna(value): return pd.NA
            # Extract YYYY or YYYYF, remove rows that have 'Change' in the value
            s_value = str(value).strip()
            if 'change' in s_value.lower(): # Ensure case-insensitivity for 'Change'
                return pd.NA
            
            # Regex to capture the 4-digit year in group 1
            match = re.search(r'\b(19\d{2}|20\d{2})[A-Z]?\b', s_value) 
            if match:
                # Use group(1) which contains just the digits (e.g., "2024" from "2024F")
                return int(match.group(1)) 
            return pd.NA
        
        parsed_years_series = year_header_values_raw.apply(extract_year_int).dropna().astype('Int64')
        if parsed_years_series.empty:
            print("No valid years (YYYY format) found in the identified header row.")
            return None
        print(f"Parsed Years ({len(parsed_years_series)} values): {parsed_years_series.tolist()}")
        print(f"Original column indices of parsed years: {parsed_years_series.index.tolist()}")

        # --- Dynamically find the target metric row ---
        target_metric_row_index = -1
        actual_metric_name_found = ""

        for i, row_series in df_full.iterrows():
            if row_series.empty: continue # Skip empty rows
            if len(row_series) == 0: continue # ensure at least one cell
            first_cell_val_str = str(row_series.iloc[0]).strip() # First column for metric name
            
            is_target_metric_base = re.search(target_metric_text_in_file, first_cell_val_str, re.IGNORECASE)
            if is_target_metric_base:
                # Special handling for "net farm income" to avoid "net cash farm income"
                if "net farm income" in target_metric_text_in_file.lower() and \
                   "cash" not in target_metric_text_in_file.lower() and \
                   "cash" in first_cell_val_str.lower():
                    continue 
                
                target_metric_row_index = i
                actual_metric_name_found = first_cell_val_str
                print(f"Identified target metric '{actual_metric_name_found}' at sheet row index {target_metric_row_index}.")
                break
        
        if target_metric_row_index == -1:
            print(f"Could not find row for target metric using regex: '{target_metric_text_in_file}'.")
            return None

        metric_values_raw = df_full.iloc[target_metric_row_index, parsed_years_series.index]
        metric_values_numeric = pd.to_numeric(metric_values_raw, errors='coerce')
        print(f"Extracted raw values for '{actual_metric_name_found}': {metric_values_raw.tolist()}")
        print(f"Converted numeric values: {metric_values_numeric.tolist()}")
        
        final_dates = pd.to_datetime(parsed_years_series.astype(str) + "-01-01", errors='coerce').dt.date
        valid_data_mask = pd.notna(final_dates) & pd.notna(metric_values_numeric.values)
        
        if not valid_data_mask.any():
            print("No valid date/value pairs found after parsing and alignment.")
            return None

        result_df = pd.DataFrame({
            'date': final_dates[valid_data_mask],
            'symbol': symbol_name,
            'metric': 'value', 
            'value': metric_values_numeric.values[valid_data_mask] 
        })
        result_df = result_df[['date', 'symbol', 'metric', 'value']].reset_index(drop=True)

        print(f"\n--- Resulting DataFrame for '{actual_metric_name_found}' (metric name in sheet) ---")
        print(result_df.head())
        print(f"Resulting DataFrame shape: {result_df.shape}")
        return result_df

    except Exception as e:
        print(f"An error occurred during data processing: {str(e)}")
        traceback.print_exc()
        return None