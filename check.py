import duckdb
import pandas as pd

print("--- Inspecting DuckDB symbols table ---")
try:
    # Connect to DuckDB
    con = duckdb.connect('bristol_gate.duckdb', read_only=True)
    
    # Get basic info about symbols table
    symbols_df = con.execute("SELECT * FROM symbols").df()
    print(f"Total symbols in database: {len(symbols_df)}")
    print(f"Columns in symbols table: {symbols_df.columns.tolist()}")
    print()
    
    # Show first few rows
    print("Sample symbols (first 10):")
    print(symbols_df.head(10).to_string(index=False))
    print()
    
    # Check for specific problematic symbols mentioned in the warnings
    problematic_symbols = [
        'BKRGas_rig_count', 'BKRMisc_rig_count', 'BKROil_rig_count', 'BKRTotal_rig_count',
        'FINRA/Free_Credit_Cash', 'FINRA/Free_Credit_Margin', 'FINRA/Margin_Debt',
        'SILVERBLATT_ar_earnings_pe_ar_earnings_pe', 'USDA_NET_FARM_INCOME',
        'BKRGas_rig', 'BKRMisc_rig', 'BKROil_rig', 'BKRTotal_rig',  # potential roots
        'FINRA/Free_Credit', 'FINRA/Margin', 'SILVERBLATT_ar_earnings_pe_ar_earnings',
        'USDA_NET_FARM'
    ]
    
    print("Checking for problematic symbols:")
    found_symbols = []
    missing_symbols = []
    
    for symbol in problematic_symbols:
        symbol_exists = con.execute("SELECT COUNT(*) as count FROM symbols WHERE symbol = ?", [symbol]).df()['count'].iloc[0]
        if symbol_exists > 0:
            found_symbols.append(symbol)
            symbol_info = con.execute("SELECT symbol, source, description, unit FROM symbols WHERE symbol = ?", [symbol]).df()
            print(f"✅ FOUND: {symbol}")
            print(f"   Source: {symbol_info['source'].iloc[0]}")
            print(f"   Description: {symbol_info['description'].iloc[0][:100]}...")
            print(f"   Unit: {symbol_info['unit'].iloc[0]}")
        else:
            missing_symbols.append(symbol)
            print(f"❌ MISSING: {symbol}")
    
    print(f"\nSummary: {len(found_symbols)} found, {len(missing_symbols)} missing")
    
    # Check sources breakdown
    print("\nSources breakdown:")
    source_counts = con.execute("SELECT source, COUNT(*) as count FROM symbols GROUP BY source ORDER BY count DESC").df()
    print(source_counts.to_string(index=False))
    
    # Look for similar symbols (fuzzy search)
    if missing_symbols:
        print(f"\nLooking for similar symbols to missing ones:")
        for missing in missing_symbols[:5]:  # Check first 5 missing
            # Try to find symbols that contain parts of the missing symbol
            search_term = missing.split('_')[0] if '_' in missing else missing.split('/')[0] if '/' in missing else missing
            similar = con.execute("""
                SELECT symbol, description 
                FROM symbols 
                WHERE symbol LIKE ? OR description LIKE ?
                LIMIT 3
            """, [f'%{search_term}%', f'%{search_term}%']).df()
            
            if not similar.empty:
                print(f"   Similar to '{missing}':")
                for _, row in similar.iterrows():
                    print(f"     • {row['symbol']}: {row['description'][:50]}...")
    
    con.close()
    
except Exception as e:
    print(f"Error connecting to DuckDB or querying symbols table: {e}")
