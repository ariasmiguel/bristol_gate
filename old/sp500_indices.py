import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import random

def get_sp500_data(
    sp500_url: str = "https://www.spglobal.com/spdji/en/documents/additional-material/sp-500-eps-est.xlsx",
    referer_url: str = "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
    download_dir: str = "data/sp500"
) -> pd.DataFrame:
    """
    Downloads and processes S&P 500 earnings and estimates data.
    
    Args:
        sp500_url (str): URL for the S&P 500 Excel file
        referer_url (str): Referer URL for establishing session
        download_dir (str): Directory to save downloaded files
        
    Returns:
        pd.DataFrame: DataFrame containing the S&P 500 data with standardized column format
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
            var_name='symbol',
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
            var_name='symbol',
            value_name='value'
        )
        
        # Remove NaN values from estimates
        df_estimates_melted = df_estimates_melted.dropna(subset=['value'])
        
        # Convert quarterly data date to datetime.date for consistency
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
        df_combined = df_combined[df_combined['symbol'].isin(keep_symbols)]
        
        # Add placeholder columns for the standard format
        for col in ['open', 'high', 'low', 'close', 'adj_close', 'volume']:
            df_combined[col] = None
        
        # Reorder columns to match our standard format
        column_order = ['date', 'symbol', 'value', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        df_combined = df_combined[column_order]
        
        # Add SP500_ prefix to all symbols
        df_combined['symbol'] = 'SILVERBLATT_' + df_combined['symbol']
        
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

if __name__ == "__main__":
    # Example usage
    df = get_sp500_data()
    if df is not None:
        print("\nFirst few rows of data:")
        print(df.head()) 