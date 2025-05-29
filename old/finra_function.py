import pandas as pd
import time
import os
import requests
from datetime import datetime
from urllib.parse import urlparse
from dateutil.relativedelta import relativedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
import random

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
            df['date'] = df['date'].apply(lambda x: (x + relativedelta(months=1, days=-1)))
            
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
            df = df.melt(id_vars=['date'], var_name='symbol', value_name='value')
            
            # Add missing columns required by the standard format
            for col in ['open', 'high', 'low', 'close', 'adj_close', 'volume']:
                df[col] = None
            
            # Reorder columns to match our standard format
            column_order = ['date', 'symbol', 'value', 'open', 'high', 'low', 'close', 'adj_close', 'volume']
            df = df[column_order]
            
            # Remove NaN values from value column
            df = df[df['value'].notna()]
            
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

# Test the function if running as a script
if __name__ == "__main__":
    try:
        print("Testing get_finra_data function...")
        df = get_finra_data()
        if df is not None:
            print("\nSample of processed data:")
            print(df.head())
            print(f"\nData shape: {df.shape}")
            
            # Save for verification
            csv_path = os.path.join("data/finra", "finra_data_test.csv")
            df.to_csv(csv_path, index=False)
            print(f"Saved test output to: {csv_path}")
        else:
            print("Function returned None")
    except Exception as e:
        print(f"Test failed: {str(e)}") 