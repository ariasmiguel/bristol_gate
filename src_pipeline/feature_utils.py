import polars as pl
import numpy as np
from scipy.signal import savgol_filter
import sys
from typing import Tuple, Any, List, Dict

# --- Helper function for Savitzky-Golay filter ---
# (Based on the version in features.py/features_parallel.py, renamed for generic use)
def apply_savgol_filter(
    s: pl.Series, 
    window_length: int, 
    polyorder: int, 
    deriv: int = 0
) -> pl.Series:
    """
    Applies Savitzky-Golay filter to a Polars Series.
    Handles NaNs by interpolating, then ffill/bfill.
    If series is all null or becomes all null after interpolation, returns nulls.
    Adjusts window_length to be odd and greater than polyorder.
    Casts to Float64 for filtering if not already float, then casts back.
    """
    series_name = s.name # Preserve original name for the output Series
    original_dtype = s.dtype

    if s.is_empty() or s.is_null().all(): # If empty or all nulls, return as is
        return s.cast(original_dtype, strict=False) if s.is_empty() else pl.Series(series_name, [None] * len(s), dtype=original_dtype, strict=False)

    # Interpolate, then fill ends.
    s_processed = s.interpolate().fill_null(strategy="backward").fill_null(strategy="forward")
    
    # If still all nulls (e.g., was all nulls and interpolation didn't change that)
    if s_processed.is_null().all():
        return pl.Series(series_name, [None] * len(s), dtype=original_dtype, strict=False)

    # Convert to NumPy array. Cast to float64 for savgol_filter if not already float.
    if not s_processed.dtype.is_float():
        s_np = s_processed.cast(pl.Float64).to_numpy()
    else:
        s_np = s_processed.to_numpy()

    # Ensure window_length is odd and > polyorder
    effective_window_length = window_length
    if effective_window_length <= polyorder:
        print(f"  Warning: SavGol window_length ({effective_window_length}) for series '{series_name}' is <= polyorder ({polyorder}). Adjusting window.")
        effective_window_length = polyorder + 1 + (polyorder % 2 == 0) # Make odd and larger
    
    if effective_window_length % 2 == 0:
        effective_window_length += 1
    
    # Ensure window_length is not greater than the number of non-NaN data points
    num_valid_points = np.count_nonzero(~np.isnan(s_np))
    if num_valid_points == 0 : # All NaN after processing (should not happen if initial check passed and fill_null worked)
         return pl.Series(series_name, [None] * len(s), dtype=original_dtype, strict=False)

    if effective_window_length > num_valid_points:
        print(f"  Warning: SavGol window_length ({effective_window_length}) for series '{series_name}' > number of valid data points ({num_valid_points}). Adjusting window_length.")
        effective_window_length = num_valid_points if num_valid_points % 2 != 0 else max(1, num_valid_points -1)
    
    if effective_window_length <= polyorder : 
         print(f"  Warning: Data length for series '{series_name}' too small for SavGol filter with polyorder {polyorder} after adjustments. Returning uninterpolated series (but filled).")
         return s_processed.cast(original_dtype, strict=False) # Return the prepped series

    try:
        # savgol_filter requires non-NaN data for calculation where it operates
        # The mode='interp' handles boundaries, but internal NaNs can be problematic.
        # We've filled NaNs in s_np, so it should be clean.
        filtered_array = savgol_filter(
            s_np, # s_np should be free of NaNs at this point
            window_length=effective_window_length,
            polyorder=polyorder,
            deriv=deriv,
            mode='interp' 
        )
        # Cast back to original dtype if possible, otherwise keep as float
        try:
            return pl.Series(series_name, filtered_array).cast(original_dtype, strict=False)
        except pl.PolarsError:
            return pl.Series(series_name, filtered_array) # Keep as float if cast fails
            
    except ValueError as e:
        print(f"  Warning: Savitzky-Golay failed for series {series_name} (window: {effective_window_length}, poly: {polyorder}, deriv: {deriv}). Error: {e}. Returning original (interpolated/filled) series.")
        return s_processed.cast(original_dtype, strict=False)


# --- Data Loading Functions ---
def load_and_prepare_data(data_path: str) -> pl.DataFrame:
    """Loads and prepares the main data CSV."""
    print(f"Loading data from: {data_path}")
    try:
        df = pl.read_csv(data_path, try_parse_dates=True) 
        
        if "date" not in df.columns:
            print(f"Error: 'date' column not found in {data_path}. Exiting.")
            sys.exit(1)
            
        if df["date"].dtype == pl.Datetime:
            df = df.with_columns(pl.col("date").dt.date())
        elif df["date"].dtype == pl.Object or df["date"].dtype == pl.Utf8:
            print(f"Warning: 'date' column was read as string ({df['date'].dtype}). Attempting explicit datetime parse.")
            # Try multiple formats for robustness
            try:
                df = df.with_columns(
                    pl.col("date").str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f", strict=False).cast(pl.Date, strict=False)
                )
            except pl.PolarsError: # Catches compute errors during strptime
                 print(f"Warning: Parsing date with T and fractional seconds failed for {data_path}. Retrying with %Y-%m-%d.")
                 df_reloaded = pl.read_csv(data_path) # Reload to avoid issues with partially modified column
                 df = df_reloaded.with_columns(
                     pl.col("date").str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
                 )

            if df.filter(pl.col("date").is_null()).height == df.height and df.height > 0: # Check if all dates are null after attempts
                 print(f"Error: All 'date' values are null after parsing attempts for {data_path}. Please check CSV. Exiting.")
                 sys.exit(1)

        elif df["date"].dtype != pl.Date:
             print(f"Error: 'date' column is of unexpected type {df['date'].dtype} after try_parse_dates for {data_path}. Exiting.")
             sys.exit(1)
        
        if df.select(pl.col("date").is_null().sum()).item() > 0:
            print(f"Warning: Some 'date' values are null after parsing attempts for {data_path}. Please check CSV.")

        print(f"Loaded data shape for {data_path}: {df.shape}")
        if df.is_empty():
            print(f"Error: Input data from {data_path} is empty. Exiting.")
            sys.exit(1)
        
        df = df.sort("date")
        if df.n_chunks() > 1:
            df = df.rechunk()
        return df
    except Exception as e:
        print(f"Error loading or initial processing of data from {data_path}: {e}. Exiting.")
        sys.exit(1)

def load_metadata(metadata_path: str) -> pl.DataFrame:
    """Loads the metadata CSV."""
    print(f"Loading metadata from: {metadata_path}")
    try:
        df_meta = pl.read_csv(metadata_path)
        print(f"Loaded metadata shape for {metadata_path}: {df_meta.shape}")
        if df_meta.is_empty():
            print(f"Warning: Metadata from {metadata_path} is empty.")
        return df_meta
    except Exception as e:
        print(f"Error loading metadata from {metadata_path}: {e}. Exiting.")
        sys.exit(1)

# --- Metadata Helper Function ---
def get_symbol_metadata_details(
    symbol_original: str, 
    df_symbols_meta: pl.DataFrame, 
    default_start_date: Any, 
    default_end_date: Any   
) -> Tuple[str, str, str, str, str]:
    """
    Retrieves metadata details for a given symbol.
    Returns: (description, label_y, series_start_date_str, series_end_date_str, symbol_root_for_meta)
    """
    description_r = ""
    label_y_r = "Value" 
    series_start_date_r = default_start_date
    series_end_date_r = default_end_date
    symbol_root_for_meta = symbol_original
    suffix_for_desc = ""

    parts = symbol_original.rsplit('_', 1)
    potential_root = parts[0]
    potential_suffix = parts[1].lower() if len(parts) > 1 else "" 
    
    yahoo_suffixes = ["open", "high", "low", "close", "adj_close", "volume"] 

    meta_row_root = None
    direct_match_meta = df_symbols_meta.filter(pl.col("symbol") == symbol_original)

    if not direct_match_meta.is_empty():
        meta_row_root = direct_match_meta
    elif potential_suffix in yahoo_suffixes:
        root_match_meta = df_symbols_meta.filter(pl.col("symbol") == potential_root)
        if not root_match_meta.is_empty():
            meta_row_root = root_match_meta
            symbol_root_for_meta = potential_root
            suffix_for_desc = f" ({parts[1]})" 
    
    if meta_row_root is not None and not meta_row_root.is_empty():
        try:
            desc_val = meta_row_root.select("description").row(0)[0]
            description_r = (desc_val if desc_val is not None else "") + suffix_for_desc
            
            # Fix: Use 'unit' column instead of 'label_y' to match DuckDB schema
            unit_columns = ["unit", "label_y"]  # Try both for compatibility
            label_val = None
            for col in unit_columns:
                if col in meta_row_root.columns:
                    label_val = meta_row_root.select(col).row(0)[0]
                    break
            label_y_r = label_val if label_val is not None else "Value"

            # Handle different possible date column names
            start_date_columns = ["series_start", "start_date"]
            end_date_columns = ["series_end", "end_date"]
            
            for col in start_date_columns:
                if col in meta_row_root.columns and meta_row_root.select(col).row(0)[0] is not None:
                    series_start_date_r = meta_row_root.select(col).row(0)[0]
                    break
                    
            for col in end_date_columns:
                if col in meta_row_root.columns and meta_row_root.select(col).row(0)[0] is not None:
                    series_end_date_r = meta_row_root.select(col).row(0)[0]
                    break
                    
        except Exception as e:
            print(f"  Warning: Could not retrieve all metadata details for {symbol_original} (root: {symbol_root_for_meta}). Using defaults. Error: {e}")
            if not description_r: description_r = f"{symbol_original} (Description not found)"
    else:
        print(f"  Warning: No metadata entry found for symbol '{symbol_original}' or its potential root '{potential_root}'. Using default description.")
        description_r = f"{symbol_original} (Original Series)"

    start_date_str = series_start_date_r.strftime('%Y-%m-%d') if hasattr(series_start_date_r, 'strftime') else str(series_start_date_r)
    end_date_str = series_end_date_r.strftime('%Y-%m-%d') if hasattr(series_end_date_r, 'strftime') else str(series_end_date_r)
    
    return description_r, label_y_r, start_date_str, end_date_str, symbol_root_for_meta

# --- Saving Functions ---
def combine_and_save_metadata(
    df_original_meta: pl.DataFrame, 
    new_metadata_rows: List[Dict[str, Any]], 
    output_path: str
):
    """Combines original metadata with new feature metadata and saves it."""
    df_symbols_meta_updated = df_original_meta.clone() # Start with a clone

    if new_metadata_rows:
        # Ensure all new metadata rows have a consistent schema before creating DataFrame
        # This is important if some rows lack optional keys like 'expense_ratio'
        all_keys = set()
        for row in new_metadata_rows:
            all_keys.update(row.keys())
        
        # Define a schema based on original metadata, falling back for new keys
        schema = {}
        if not df_original_meta.is_empty():
            schema = df_original_meta.schema.copy()
        
        for key in all_keys:
            if key not in schema:
                # Basic type inference for new keys (can be refined)
                sample_val = next((row[key] for row in new_metadata_rows if key in row and row[key] is not None), None)
                if isinstance(sample_val, float): schema[key] = pl.Float64
                elif isinstance(sample_val, int): schema[key] = pl.Int64
                else: schema[key] = pl.Utf8 # Default for others or if all are None


        # Pad rows to include all keys, with None for missing ones, before creating DataFrame
        padded_new_metadata_rows = []
        for row in new_metadata_rows:
            padded_row = {key: row.get(key) for key in schema.keys() if key in all_keys} # only add keys present in all_keys
            for key in schema.keys(): # Ensure all schema keys exist
                if key not in padded_row:
                    padded_row[key] = None
            padded_new_metadata_rows.append(padded_row)
            
        if padded_new_metadata_rows:
            df_new_meta = pl.DataFrame(padded_new_metadata_rows, schema_overrides=schema, strict=False)
            
            # Ensure columns are in the same order as original_meta for concat
            # And that all original columns are present
            final_new_meta_cols = []
            for col_name in df_original_meta.columns:
                final_new_meta_cols.append(col_name)
                if col_name not in df_new_meta.columns:
                    df_new_meta = df_new_meta.with_columns(pl.lit(None, dtype=df_original_meta[col_name].dtype).alias(col_name))
            
            df_new_meta = df_new_meta.select(final_new_meta_cols)
            df_symbols_meta_updated = pl.concat([df_symbols_meta_updated, df_new_meta], how="diagonal_relaxed")
        
    print(f"\nSaving updated symbols metadata to: {output_path}")
    try:
        df_symbols_meta_updated.write_csv(output_path)
        print(f"Updated metadata saved. Shape: {df_symbols_meta_updated.shape}")
    except Exception as e:
        print(f"Error saving updated metadata to {output_path}: {e}")

def save_featured_data(df_data: pl.DataFrame, output_path: str):
    """Saves the DataFrame with all features."""
    print(f"\nSaving featured data to: {output_path}")
    try:
        df_data.write_csv(output_path)
        print(f"Featured data saved. Shape: {df_data.shape}")
    except Exception as e:
        print(f"Error saving featured data to {output_path}: {e}") 