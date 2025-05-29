import pandas as pd
import time
import os
import requests
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium_stealth import stealth
import random

# Setup directory for downloads
download_dir = os.path.join(os.getcwd(), "data", "finra")
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

# Setup Chrome options
print("Setting up Chrome options...")
options = Options()
options.add_argument("--disable-extensions")
options.add_argument("--disable-gpu")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--no-sandbox")
options.add_argument("--headless=new")  # Headless mode is fine since we're not clicking
options.add_argument("--window-size=1920,1080")

# Add a realistic user agent
user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"
options.add_argument(f"--user-agent={user_agent}")

# Initialize the driver
print("Initializing Chrome driver...")
driver = webdriver.Chrome(options=options)

# Apply stealth settings
print("Applying stealth settings...")
stealth(driver,
    languages=["en-US", "en"],
    vendor="Google Inc.",
    platform="Win32",
    webgl_vendor="Intel Inc.",
    renderer="Intel Iris OpenGL Engine",
    fix_hairline=True,
)

try:
    # Step 1: Navigate to the FINRA margin statistics page
    url = "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"
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
            
    # Save link data to CSV
    links_df = pd.DataFrame(link_data)
    links_csv_path = os.path.join(download_dir, "finra_links.csv")
    links_df.to_csv(links_csv_path, index=False)
    print(f"Saved {len(links_df)} links to {links_csv_path}")
    
    # Step 3: Find the "DOWNLOAD THE DATA" link
    download_links = links_df[links_df['text'].str.contains("DOWNLOAD THE DATA", case=False, na=False)]
    
    if download_links.empty:
        print("Could not find 'DOWNLOAD THE DATA' link")
        raise Exception("Download link not found")
    
    # Get the download URL
    download_url = download_links.iloc[0]['href']
    print(f"Found download URL: {download_url}")
    
    # Step 4: Download the Excel file directly
    print(f"Downloading Excel file from: {download_url}")
    
    # Send a GET request to download the file
    headers = {
        "User-Agent": user_agent
    }
    
    response = requests.get(download_url, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
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
            
            # Display basic info about the data
            print("\nColumns:")
            print(df.columns.tolist())
            
            # Create the date column
            df['date'] = pd.to_datetime(df['Year-Month'], format='%Y-%m')
            # Get the last day of the month
            df['date'] = df['date'].apply(lambda x: x.replace(day=x.days_in_month))
            
            # Delete the Year-Month column
            df = df.drop(columns=['Year-Month'])
            
            # Rename the columns
            df.columns = [
                "finra_margin_debt",
                "finra_free_credit_cash",
                "finra_free_credit_margin",
                "date"
            ]
            
            # Melt the dataframe
            df = df.melt(id_vars=['date'], var_name='symbol', value_name='value')
            
            # Remove NaN values from value column
            df = df[df['value'].notna()]
            
            # Prints the dataframe
            print(df)
            
            # Save as CSV
            csv_path = os.path.join(download_dir, "finra_data.csv")
            df.to_csv(csv_path, index=False)
            print(f"Converted to CSV: {csv_path}")
            
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close the browser
    print("Closing the browser...")
    driver.quit()

print("Done.") 