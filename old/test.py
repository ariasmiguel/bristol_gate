import pandas as pd

def check_for_duplicates(csv_path):
    """
    Read a CSV file and check for duplicate entries.
    
    Parameters:
    - csv_path (str): Path to the CSV file
    
    Returns:
    - dict: Dictionary containing duplicate information
    """
    print(f"Reading CSV file from {csv_path}...")
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Print basic info about the dataframe
    print(f"CSV loaded. Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Check for any completely duplicate rows
    duplicate_rows = df.duplicated().sum()
    print(f"Total duplicate rows: {duplicate_rows}")
    
    # Check for duplicates based on date and symbol
    # This is often what we care about in financial data - duplicate entries for the same symbol on same date
    date_symbol_dupes = df.duplicated(subset=['date', 'symbol'], keep='first').sum()
    print(f"Rows with duplicate date-symbol combinations: {date_symbol_dupes}")
    
    # Get the actual duplicate entries
    if date_symbol_dupes > 0:
        print("\nFirst 10 date-symbol duplicates:")
        dupes = df[df.duplicated(subset=['date', 'symbol'], keep=False)].sort_values(['symbol', 'date'])
        print(dupes.head(10))
        
        # Count duplicates by symbol
        print("\nDuplicate count by symbol:")
        dupe_counts = dupes.groupby('symbol').size().sort_values(ascending=False)
        print(dupe_counts.head(10))
    
    return {
        "total_rows": len(df),
        "duplicate_rows": duplicate_rows,
        "date_symbol_duplicates": date_symbol_dupes
    }

if __name__ == "__main__":
    # Path to the CSV file
    csv_path = 'data/combined_data.csv'
    
    # Check for duplicates
    results = check_for_duplicates(csv_path)
    
    # Summarize findings
    print("\nSummary:")
    print(f"Total rows: {results['total_rows']}")
    print(f"Total duplicate rows: {results['duplicate_rows']}")
    print(f"Rows with duplicate date-symbol combinations: {results['date_symbol_duplicates']}")
    
    if results['date_symbol_duplicates'] > 0:
        print("\nRecommendation: You may want to remove duplicate entries to avoid data quality issues.")
        print("To remove duplicates, you could use:")
        print("df = df.drop_duplicates(subset=['date', 'symbol'], keep='first')")
