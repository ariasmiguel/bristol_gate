import pandas as pd
import random
import time
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth

def get_baker_data(baker_url: str = "https://bakerhughesrigcount.gcs-web.com/na-rig-count",
                   file_name: str = "North America Rotary Rig Count (",
                   sheet_name: str = "US Oil & Gas Split",
                   skip_rows: int = 6,
                   data_dir: str = "data/baker"):
    """
    Downloads and processes Baker Hughes rig count data.
    
    Returns:
    - pd.DataFrame: DataFrame containing the Baker Hughes data with standardized column format.
    """
    # Make sure download directory exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
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
            "download.default_directory": os.path.abspath(data_dir),
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
        excel_files = [f for f in os.listdir(data_dir) if f.endswith('.xlsx') or f.endswith('.xls') or f.endswith('.xlsb')]
        if not excel_files:
            print("No Excel files found in download directory.")
            return None
            
        most_recent_file = max([os.path.join(data_dir, f) for f in excel_files], key=os.path.getctime)
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
            df['date'] = pd.to_datetime(df[date_col], origin='1899-12-30', unit='D')
            if date_col != 'date':
                df = df.drop(columns=[date_col])
        
        # Add BKR_ prefix to all columns except date
        df.columns = ['date' if col == 'date' else f'BKR{col}' for col in df.columns]
        
        # Remove percentage columns
        percent_columns = [col for col in df.columns if '%' in col]
        if percent_columns:
            df = df.drop(columns=percent_columns)
        
        # Standardize output format to match other get_*_data functions
        print("Transforming data from wide to long format...")
        
        # Melt the dataframe to transform from wide to long format
        # Keep 'date' column, use all BKR_* columns as variable columns
        value_columns = [col for col in df.columns if col.startswith('BKR')]
        result_df = pd.melt(
            df, 
            id_vars=['date'], 
            value_vars=value_columns,
            var_name='symbol',
            value_name='value'
        )
        
        # Add placeholder columns for the standard format
        for col in ['open', 'high', 'low', 'close', 'adj_close', 'volume']:
            result_df[col] = None
        
        # Reorder columns to match our standard format
        column_order = ['date', 'symbol', 'value', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
        result_df = result_df[column_order]
        
        # Save to CSV for reference
        # result_df.to_csv(os.path.join(download_dir, 'baker_hughes_data.csv'), index=False)
        # print(f"Data saved to {os.path.join(download_dir, 'baker_hughes_data.csv')}")
        
        return result_df
        
    except Exception as e:
        print(f"Error in get_baker_data: {str(e)}")
        return None
        
    finally:
        if driver:
            driver.quit()

if __name__ == "__main__":
    df = get_baker_data()
    if df is not None:
        print("Data retrieved successfully")
        print(f"Shape: {df.shape}")
        print("First few rows:")
        print(df) 