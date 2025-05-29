import os
import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
from urllib.parse import urlparse, unquote
import pandas as pd
import re
import traceback # For more detailed error printing

def get_usda_ers_data(
    usda_page_url: str = "https://www.ers.usda.gov/data-products/farm-income-and-wealth-statistics/data-files-us-and-state-level-farm-income-and-wealth-statistics/",
    target_link_text_in_file: str = "U.S. farm sector financial indicators",
    target_metric_text_in_file: str = r'net\s+farm\s+income', # Raw string here too
    sheet_name_in_file: str = 'Sheet1',
    download_dir: str = "data/usda",
    symbol_name: str = "USDA_NET_FARM_INCOME",
    headless: bool = True
) -> pd.DataFrame | None:
    """
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
            first_cell_val_str = str(row_series.iloc[0]).strip() # First column for metric name
            
            is_target_metric_base = re.search(target_metric_text_in_file, first_cell_val_str, re.IGNORECASE)
            if is_target_metric_base:
                # Special handling for "net farm income" to avoid "net cash farm income"
                # if the target_metric_text_in_file is specific for "net farm income" (and doesn't itself contain "cash")
                if "net farm income" in target_metric_text_in_file.lower() and \
                   "cash" not in target_metric_text_in_file.lower() and \
                   "cash" in first_cell_val_str.lower():
                    # print(f"Skipping '{first_cell_val_str}' as it contains 'cash' but target is non-cash 'net farm income'.")
                    continue 
                
                target_metric_row_index = i
                actual_metric_name_found = first_cell_val_str
                print(f"Identified target metric '{actual_metric_name_found}' at sheet row index {target_metric_row_index}.")
                break
        
        if target_metric_row_index == -1:
            print(f"Could not find row for target metric using regex: '{target_metric_text_in_file}'.")
            return None

        # Extract metric values using the original column indices from parsed_years_series
        metric_values_raw = df_full.iloc[target_metric_row_index, parsed_years_series.index]
        metric_values_numeric = pd.to_numeric(metric_values_raw, errors='coerce')
        print(f"Extracted raw values for '{actual_metric_name_found}': {metric_values_raw.tolist()}")
        print(f"Converted numeric values: {metric_values_numeric.tolist()}")
        
        # --- Construct final DataFrame ---
        # Years are taken directly from parsed_years_series.values
        final_dates = pd.to_datetime(parsed_years_series.astype(str) + "-01-01", errors='coerce').dt.date
        
        # Filter out NaT dates and corresponding metric values (NaNs)
        # Both final_dates (numpy array) and metric_values_numeric (pandas Series) should align by position here
        valid_data_mask = pd.notna(final_dates) & pd.notna(metric_values_numeric.values)
        
        if not valid_data_mask.any():
            print("No valid date/value pairs found after parsing and alignment.")
            return None

        result_df = pd.DataFrame({
            'date': final_dates[valid_data_mask],
            'symbol': symbol_name,
            'metric': 'value', # Standardized: the 'symbol' carries the metric's identity
            'value': metric_values_numeric.values[valid_data_mask] 
        })
        # Ensure correct column order and reset index
        result_df = result_df[['date', 'symbol', 'metric', 'value']].reset_index(drop=True)

        print(f"\n--- Resulting DataFrame for '{actual_metric_name_found}' (metric name in sheet) ---")
        print(result_df.head())
        print(f"Resulting DataFrame shape: {result_df.shape}")
        return result_df

    except Exception as e:
        print(f"An error occurred during data processing: {str(e)}")
        traceback.print_exc()
        return None

if __name__ == '__main__':
    print("--- Running USDA ERS Data Scraper (Test Mode) ---")
    
    # Example usage to test the function:
    # You can change parameters here for testing different scenarios
    usda_data = get_usda_ers_data(
        target_metric_text_in_file=r'net farm income', # Test for "Net farm income" specifically
        symbol_name="USDA_NET_FARM_INCOME",
        headless=True # Set to False to watch browser, True for background run
    )
    
    if usda_data is not None and not usda_data.empty:
        print("\n--- Test Run Successful ---")
        print("Data Summary:")
        print(usda_data.info())
        print("\nFirst 5 rows of data:")
        print(usda_data.head())
        print("\nLast 5 rows of data:")
        print(usda_data.tail())
        
        # Optional: Save to CSV for inspection
        # output_csv_path = os.path.join("data", "usda_test_output.csv")
        # usda_data.to_csv(output_csv_path, index=False)
        # print(f"Sample data saved to {output_csv_path}")
    else:
        print("\n--- Test Run Failed or No Data Returned ---")