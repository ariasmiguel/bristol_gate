from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time
import os
import pandas as pd

# --- Configuration ---
OCC_URL = "https://www.theocc.com/market-data/market-data-reports/volume-and-open-interest/historical-volume-statistics"
DOWNLOAD_DIR = os.path.join(os.getcwd(), "data", "occ_playwright")
TARGET_MONTH_YEAR = "May 2023"  # Example: "May 2023", "Oct 2019"
HEADLESS_MODE = False # Set to True for background execution

# Ensure download directory exists
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)
    print(f"Created directory: {DOWNLOAD_DIR}")

def fetch_occ_data_for_month(month_year_str):
    """
    Fetches OCC daily statistics for a specific month and year.
    """
    all_data = []
    print(f"\nAttempting to fetch data for: {month_year_str}")

    with sync_playwright() as p:
        browser = None
        try:
            browser = p.chromium.launch(headless=HEADLESS_MODE, slow_mo=50) # slow_mo for observing
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            )
            page = context.new_page()

            print(f"Navigating to {OCC_URL}...")
            page.goto(OCC_URL, timeout=60000) # 60 seconds timeout for navigation
            print("Page loaded.")

            # 1. Handle Cookie Consent (if it appears)
            try:
                print("Checking for cookie consent banner...")
                # More generic selectors for cookie buttons
                cookie_button_selectors = [
                    "button:has-text('Accept All Cookies')",
                    "button:has-text('Accept')",
                    "button:has-text('Allow All')",
                    "button:has-text('Got it')",
                    "//button[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accept')]"
                ]
                cookie_button_found = False
                for selector in cookie_button_selectors:
                    try:
                        print(f"Trying cookie selector: {selector}")
                        button = page.query_selector(selector)
                        if button and button.is_visible():
                            button.click(timeout=5000)
                            print(f"Clicked cookie consent button using: {selector}")
                            page.wait_for_timeout(2000) # Wait for banner to disappear
                            cookie_button_found = True
                            break
                    except PlaywrightTimeoutError:
                        print(f"Cookie selector {selector} timed out or not clickable.")
                    except Exception as e_cookie_click:
                        print(f"Error clicking cookie button with {selector}: {e_cookie_click}")
                if not cookie_button_found:
                     print("No cookie banner found or handled.")

            except Exception as e_cookie:
                print(f"An error occurred during cookie handling: {e_cookie}")


            # 2. Select "Daily Statistics (Listed by Month)"
            print("Selecting 'Daily Statistics (Listed by Month)'...")
            # The input's value is "RptCatDaily"
            # The label is "Daily Statistics (Listed by Month)"
            daily_stats_radio_label_selector = "label:has-text('Daily Statistics (Listed by Month)')"
            try:
                page.wait_for_selector(daily_stats_radio_label_selector, timeout=15000)
                page.click(daily_stats_radio_label_selector)
                print("Clicked 'Daily Statistics (Listed by Month)' label.")
                page.wait_for_timeout(3000) # Wait for any dynamic content to load after click
            except PlaywrightTimeoutError:
                print(f"Timeout waiting for '{daily_stats_radio_label_selector}'. Trying to click input directly.")
                # Fallback: try to click the input element itself if label fails
                daily_stats_radio_input_selector = "input[type='radio'][value='RptCatDaily']" # Check this value in dev tools
                try:
                    page.click(daily_stats_radio_input_selector, timeout=5000)
                    print("Clicked 'Daily Statistics' radio input directly.")
                    page.wait_for_timeout(3000)
                except PlaywrightTimeoutError:
                    print(f"Fatal: Could not find or click 'Daily Statistics' radio button (label or input).")
                    return None # Cannot proceed

            # 3. Set the Month and Year
            print(f"Setting month to: {month_year_str}")
            target_month_name = month_year_str.split(" ")[0] # e.g., "May"
            target_year_str = month_year_str.split(" ")[1]   # e.g., "2023"

            month_input_selector = "#monthYear-input"
            calendar_icon_selector = "button[aria-label='Calendar']" # Common aria-label for calendar buttons

            try:
                # Click the calendar icon or the input field to open the picker
                print("Opening month/year picker...")
                try:
                    # Try clicking the calendar icon first
                    calendar_button = page.locator(calendar_icon_selector)
                    if calendar_button.is_visible():
                        calendar_button.click()
                        print("Clicked calendar icon.")
                    else:
                        # Fallback to clicking the input field
                        page.locator(month_input_selector).click()
                        print("Clicked month input field to open picker.")
                except Exception as e_open_picker:
                    print(f"Could not click calendar icon or month input: {e_open_picker}. Trying input field directly.")
                    page.locator(month_input_selector).click() # Last resort click on input

                page.wait_for_timeout(1500) # Wait for picker to appear

                # Select the Year
                # The year is often in a clickable element, possibly a button or a span, often within a specific header of the picker
                # Example: A button with the current year displayed, clicking it might show a list of years.
                # From your screenshot, it looks like years might be directly selectable or via arrows.
                # We need to find how to change the year in the picker first.
                # For now, let's assume we can find a button for the target year.
                
                # Click on the part of the date picker that displays the current year to open year selection
                # This selector might need adjustment based on the actual HTML of the date picker.
                # Often it's a button in the header of the calendar.
                # Let's assume it shows something like "2025" (from your screenshot of May 2025 default)
                # We need to click this to bring up year selection.
                
                # First, click the year display in the picker to go to year selection view
                # Common pattern: picker has a header, year is a button there.
                # Example from a popular date picker: .MuiPickersCalendarHeader-label
                # We'll try a generic approach: find a button that contains the *current* year displayed in the picker
                # This is highly dependent on the date picker's structure.
                
                # Let's get the currently displayed year in the picker's header
                # The selector "//div[contains(@class, 'datepicker-header')]//button[contains(@class, 'year')]" is a guess
                # Or if the year is simply text, not a button initially
                # Let's assume a structure like:
                # <div class="datepicker-header">
                #   <button class="prev-year-button"></button>
                #   <span class="current-year-display">2025</span>  <-- Or this could be a button
                #   <button class="next-year-button"></button>
                # </div>
                # And clicking "current-year-display" lets you select a year from a list.

                print(f"Attempting to select year: {target_year_str}")
                # This selector needs to target the button/element in the datepicker header that shows the current year,
                # which, when clicked, usually opens a year selection view.
                # Example: current_year_in_picker_selector = "div.datepicker-header button.year-button" (very hypothetical)
                # From your screenshot, it looks like "2025" is displayed prominently.
                
                # General strategy:
                # 1. Click the displayed year in the picker to show the year list.
                # 2. Click the target year from the list.

                # Based on the screenshot, the year "2025" is visible. Let's try to click that.
                # It might be inside a button or a specific span.
                # A common pattern is a button in the picker header.
                # Let's try to find a button that looks like it's for selecting the year.
                # This is the most fragile part and will likely need inspecting the live HTML.
                year_display_in_picker_selector = "//div[contains(@class,'MuiPickersCalendarHeader')]//button[contains(@class,'MuiPickersCalendarHeader-label')]"
                # The above is a common Material UI pattern, might not be it.
                # Let's try a more generic one: find a button in the datepicker that might lead to year view
                
                # Simpler: If the date picker opens directly to month/year view without a separate year list step:
                # Look for a button or div that represents the target year.
                year_selector_in_list = f"button:has-text('{target_year_str}')" # If years are buttons
                # Or if years are divs/spans
                # year_selector_in_list = f"div.year-item:has-text('{target_year_str}')" 

                # Let's assume for now clicking the main year display opens the year grid.
                # This needs to be verified with dev tools.
                # For now, let's try to find the current year in the header, click it, then select the target year.

                # Looking at the image, "2025" is quite prominent.
                # Let's assume the picker header has a button for the year.
                # This requires inspecting the actual picker. For now, let's try a plausible selector
                # for the element that shows the current year in the picker (e.g., "2025" from your image)
                # We need to click this element to go into year selection mode.
                
                # It's often a button within a div that has "datepicker-years" or similar class
                # Or something like: page.click('//div[contains(@class, "datepicker-header")]//button[contains(@class, "year")]')
                # For now, this is a placeholder as the structure is unknown:
                
                # From your screenshot, the "2025" seems to be the element to click to change the year.
                # Let's try to click the element showing the current year, then the target year from a list.
                # This will be VERY specific to the date picker library used.
                # Tentative:
                # 1. Get current year displayed in picker (e.g., text of a specific element).
                # 2. If it's not target_year_str, click it.
                # 3. Then, find and click target_year_str from the year list.

                # Let's assume a direct click on year is possible from the grid
                # Often, a year grid shows up after clicking the current year display in the header.
                # This is the most likely part that needs live debugging with dev tools.
                # For now, let's assume a direct year click after picker opens:
                # Try to click the current year in the header to go to year selection mode
                # This needs to be dynamic; find the button/element that shows the year in the calendar header
                # Example from a common library:
                # current_year_header_button = page.locator(".MuiPickersCalendarHeader-labelContainer > button").first
                # current_year_header_button.click()
                # page.wait_for_timeout(500)
                # # Then click the target year from the list that appears
                # page.locator(f"button.MuiPickersYear-yearButton:has-text('{target_year_str}')").click()
                # print(f"Selected year {target_year_str}.")
                # page.wait_for_timeout(500)
                
                # **Simplified approach if typing works after all for the component used by OCC:**
                # If the component *does* respond to typing but needs a blur/enter:
                month_input_element = page.locator(month_input_selector)
                month_input_element.click() # Ensure it has focus
                page.wait_for_timeout(200)
                month_input_element.fill("") # Clear it
                page.wait_for_timeout(200)
                month_input_element.type(month_year_str, delay=50) # Type it
                page.wait_for_timeout(200)
                page.keyboard.press("Enter") # Try pressing Enter
                print(f"Typed '{month_year_str}' and pressed Enter.")
                page.wait_for_timeout(1000) # Give it a second to update


                # Select the Month (after year is set)
                print(f"Attempting to select month: {target_month_name}")
                # Months are usually buttons with text like "Jan", "Feb", or full names
                # Example: month_selector = f"button:has-text('{target_month_name_abbr}')" # e.g., "May"
                # We need to find the exact text or selector for the month buttons in the picker.
                # From your screenshot, it's "May".
                
                # This also depends on the picker structure.
                # For now, this is a placeholder:
                # month_button_selector = f"//button[contains(@class,'MuiPickersMonth-monthButton') and normalize-space(.)='{target_month_name[:3]}']"
                # page.locator(month_button_selector).click()
                # print(f"Selected month {target_month_name}.")
                # page.wait_for_timeout(500)

            except PlaywrightTimeoutError as e_month_year:
                print(f"Fatal: Timeout during month/year selection: {e_month_year}")
                # Try to capture a screenshot of the date picker state
                screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_datepicker_error_{month_year_str.replace(' ', '_')}.png")
                page.screenshot(path=screenshot_path)
                print(f"Debug screenshot saved to: {screenshot_path}")
                return None
            except Exception as e_general_picker:
                print(f"Fatal: General error during month/year selection: {e_general_picker}")
                screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_datepicker_general_error_{month_year_str.replace(' ', '_')}.png")
                page.screenshot(path=screenshot_path)
                print(f"Debug screenshot saved to: {screenshot_path}")
                return None

            # 4. Click the "View" button
            print("Clicking 'View' button...")
            view_button_selector = "button:has-text('View')"
            try:
                page.wait_for_selector(view_button_selector, timeout=10000, state='visible')
                page.click(view_button_selector)
                print("Clicked 'View' button.")
            except PlaywrightTimeoutError:
                print("Fatal: Timeout finding or clicking 'View' button.")
                return None

            # 5. Wait for the data table to load and extract data
            print("Waiting for data table to load...")
            table_selector = "table.dataTable" # Assuming it has a class 'dataTable' or just 'table'
            try:
                page.wait_for_selector(table_selector, timeout=20000) # Wait up to 20s for table
                print("Data table found.")

                # Extract table data
                # Using page.evaluate to run JavaScript in the browser context for robust table scraping
                table_data = page.evaluate(f"""() => {{
                    const table = document.querySelector('{table_selector}');
                    if (!table) return null;
                    const rows = Array.from(table.querySelectorAll('tr'));
                    const data = [];
                    // Assuming first row is header, adjust if not
                    const headerCells = Array.from(rows[0].querySelectorAll('th, td')).map(cell => cell.innerText.trim());
                    data.push(headerCells); // Add header row

                    for (let i = 1; i < rows.length; i++) {{
                        const cells = Array.from(rows[i].querySelectorAll('td')).map(cell => cell.innerText.trim());
                        if(cells.length > 0) {{ // Ensure row has cells
                           data.push(cells);
                        }}
                    }}
                    return data;
                }}""")

                if table_data and len(table_data) > 1:
                    header = table_data[0]
                    data_rows = table_data[1:]
                    df = pd.DataFrame(data_rows, columns=header)
                    all_data.append(df)
                    print(f"Successfully extracted {len(df)} rows of data for {month_year_str}.")
                else:
                    print(f"No data extracted from table for {month_year_str}.")
                    # Capture a screenshot if table is found but no data extracted
                    screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_no_data_{month_year_str.replace(' ', '_')}.png")
                    page.screenshot(path=screenshot_path)
                    print(f"Debug screenshot saved to: {screenshot_path}")


            except PlaywrightTimeoutError:
                print(f"Timeout waiting for data table ('{table_selector}') for {month_year_str}.")
                # Capture a screenshot if table doesn't appear
                screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_no_table_{month_year_str.replace(' ', '_')}.png")
                page.screenshot(path=screenshot_path)
                print(f"Debug screenshot saved to: {screenshot_path}")
                return None
            
            page.wait_for_timeout(2000) # Brief pause before closing

        except Exception as e:
            print(f"An unexpected error occurred for {month_year_str}: {e}")
            if page:
                screenshot_path = os.path.join(DOWNLOAD_DIR, f"error_{month_year_str.replace(' ', '_')}.png")
                try:
                    page.screenshot(path=screenshot_path)
                    print(f"Error screenshot saved to: {screenshot_path}")
                except Exception as e_shot:
                    print(f"Could not save error screenshot: {e_shot}")

        finally:
            if browser:
                print("Closing browser...")
                browser.close()
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df
    return None

if __name__ == "__main__":
    print("Starting OCC Scraper with Playwright (single month test)...")
    
    # Test with a single month
    df_month_data = fetch_occ_data_for_month(TARGET_MONTH_YEAR)

    if df_month_data is not None and not df_month_data.empty:
        print(f"\n--- Data for {TARGET_MONTH_YEAR} ---")
        print(df_month_data.head())
        
        # Save to CSV
        output_filename = f"occ_daily_stats_{TARGET_MONTH_YEAR.replace(' ', '_')}.csv"
        output_path = os.path.join(DOWNLOAD_DIR, output_filename)
        df_month_data.to_csv(output_path, index=False)
        print(f"\nData for {TARGET_MONTH_YEAR} saved to: {output_path}")
    else:
        print(f"\nNo data retrieved or an error occurred for {TARGET_MONTH_YEAR}.")

    print("\nScript finished.")