import time
import polars as pl
from typing import Tuple, Any, List, Dict
import sys
import concurrent.futures
import math # Add math import for log

# Import from the new utils file
from data_collectors.feature_utils import (
    apply_savgol_filter,
    load_and_prepare_data,
    load_metadata,
    get_symbol_metadata_details,
    combine_and_save_metadata,
    save_featured_data
)

# --- Configuration ---
DATA_PATH = "data/final_aggregated_data.csv"
METADATA_PATH = "data/aggregated_symbols_metadata.csv"
FEATURED_DATA_OUTPUT_PATH = "data/featured_data.csv"
FEATURED_METADATA_OUTPUT_PATH = "data/featured_symbols_metadata.csv"

N_DAYS_YEAR = 365
NUM_WORKERS = 6 # Set based on your 6 performance cores

print("Starting feature calculation process...")

# --- Helper function for Savitzky-Golay filter ---
# MOVED to data_collectors.feature_utils.apply_savgol_filter

# --- Data Loading Functions ---
# MOVED to data_collectors.feature_utils

# --- Metadata Helper Function ---
# MOVED to data_collectors.feature_utils

# --- Feature Calculation Functions (these are called by the worker) ---
# These functions are largely the same as in features.py
# They now take series directly or column names for expressions
def calculate_yoy_features(
    original_series: pl.Series, 
    symbol_name_prefix: str,   
    description_r: str,
    series_start_date_str: str,
    series_end_date_str: str,
    n_days_year: int
) -> Tuple[List[pl.Series], List[Dict[str, Any]]]: 
    """Calculates YoY, YoY4, and YoY5 features using an input Series."""
    feature_series_list = []
    feature_metadata = []

    new_col_yoy_name = f"{symbol_name_prefix}_YoY"
    yoy_series = (((original_series / original_series.shift(n_days_year)) - 1) * 100).alias(new_col_yoy_name)
    feature_series_list.append(yoy_series)
    feature_metadata.append({
        "symbol": new_col_yoy_name, "source": "Calc",
        "description": f"{description_r}\nYear over Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })
    # print(f"  Prepared series for: {new_col_yoy_name}") # Less verbose in parallel worker

    new_col_yoy4_name = f"{symbol_name_prefix}_YoY4"
    yoy4_series = (((original_series / original_series.shift(n_days_year * 4)) - 1) * 100).alias(new_col_yoy4_name)
    feature_series_list.append(yoy4_series)
    feature_metadata.append({
        "symbol": new_col_yoy4_name, "source": "Calc",
        "description": f"{description_r}\n4 Year over 4 Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })

    new_col_yoy5_name = f"{symbol_name_prefix}_YoY5"
    yoy5_series = (((original_series / original_series.shift(n_days_year * 5)) - 1) * 100).alias(new_col_yoy5_name)
    feature_series_list.append(yoy5_series)
    feature_metadata.append({
        "symbol": new_col_yoy5_name, "source": "Calc",
        "description": f"{description_r}\n5 Year over 5 Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })
    
    return feature_series_list, feature_metadata

def calculate_savgol_features(
    original_series: pl.Series,
    symbol_original: str, # Used for naming new columns
    description_r: str,
    label_y_r: str,
    series_start_date_str: str,
    series_end_date_str: str,
    n_days_year: int
) -> Tuple[List[pl.Series], List[Dict[str, Any]]]:
    """Calculates Savitzky-Golay smoothed and derivative features."""
    feature_series_list = []
    feature_metadata = []

    savgol_configs = [
        {"suffix": "_Smooth", "window": n_days_year, "poly": 3, "deriv": 0, "desc_prefix": "Savitsky-Golay Smoothed"},
        {"suffix": "_Smooth_short", "window": 15, "poly": 3, "deriv": 0, "desc_prefix": "Savitsky-Golay Smoothed"},
        {"suffix": "_SmoothDer", "window": 501, "poly": 3, "deriv": 1, "desc_prefix": "Derivative of Savitsky-Golay Smoothed"},
    ]

    for config in savgol_configs:
        new_col_name = f"{symbol_original}{config['suffix']}"
        window = config['window']
        poly = config['poly']

        # Use the imported apply_savgol_filter
        filtered_series = apply_savgol_filter(
            original_series.alias(new_col_name), # Ensure series has the target name for the filter
                window_length=window,
                polyorder=poly,
                deriv=config['deriv']
        )
        
        if filtered_series.is_null().all() and not original_series.is_null().all():
            # print(f"  Skipped/failed in worker: {new_col_name}") # Less verbose
            feature_series_list.append(pl.Series(new_col_name, [None] * len(original_series), dtype=pl.Float64))
            feature_metadata.append({
                "symbol": new_col_name, "source": "Calc (Skipped/Failed)",
                "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r} - SKIPPED/FAILED",
                "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
        else:
            feature_series_list.append(filtered_series)
            feature_metadata.append({
                "symbol": new_col_name, "source": "Calc",
                "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r}",
                "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
            
    return feature_series_list, feature_metadata

def calculate_log_transform_feature(
    original_series: pl.Series, 
    symbol_name_prefix: str,    
    description_r: str,
    label_y_r: str,
    series_start_date_str: str,
    series_end_date_str: str
) -> Tuple[pl.Series, Dict[str, Any]]: 
    """Calculates the log transformed feature using an input Series, ensuring a Series is returned."""
    new_col_log_name = f"{symbol_name_prefix}_Log"

    # Define an element-wise function for safe logarithm
    def safe_log(val):
        if val is not None and val > 0:
            try:
                return math.log(val)
            except (ValueError, TypeError): # Catch math domain error or if val is not a number
                return None
        return None

    # Ensure the series is float for math.log compatibility.
    # If original_series contains non-numeric data that can't be cast, those will become null.
    series_for_log = original_series
    if not original_series.dtype.is_float():
        series_for_log = original_series.cast(pl.Float64, strict=False) # strict=False will turn uncastable to null

    log_transformed_values = series_for_log.map_elements(safe_log, return_dtype=pl.Float64)
    
    log_series = log_transformed_values.interpolate().alias(new_col_log_name)
    
    log_metadata = {
        "symbol": new_col_log_name, "source": "Calc",
        "description": f"Log of {description_r}", "label_y": f"log({label_y_r})",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    }
    return log_series, log_metadata

def calculate_mva_features(
    original_series: pl.Series, 
    symbol_name_prefix: str,   
    description_r: str,
    label_y_r: str,
    series_start_date_str: str,
    series_end_date_str: str,
    n_days_year: int
) -> Tuple[List[pl.Series], List[Dict[str, Any]]]: 
    """Calculates Moving Average features using an input Series."""
    feature_series_list = []
    feature_metadata = []

    mva_configs = {
        "_mva365": n_days_year,
        "_mva200": 200,
        "_mva050": 50
    }

    for suffix, window in mva_configs.items():
        new_col_mva_name = f"{symbol_name_prefix}{suffix}"
        # Check against original series length before attempting rolling mean
        if original_series.drop_nulls().len() >= window :
            mva_series = original_series.rolling_mean(window_size=window, min_samples=1).interpolate().alias(new_col_mva_name)
            feature_series_list.append(mva_series)
            feature_metadata.append({
                "symbol": new_col_mva_name, "source": "Calc",
                "description": f"{description_r} {window} Day MA", "label_y": f"{label_y_r} {window} Day MA",
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
        else:
            # print(f"  Skipped in worker: {new_col_mva_name}") # Less verbose
            feature_series_list.append(pl.Series(new_col_mva_name, [None] * len(original_series), dtype=pl.Float64))
            feature_metadata.append({
                "symbol": new_col_mva_name, "source": "Calc (Skipped)",
                "description": f"{description_r} {window} Day MA - SKIPPED", "label_y": f"{label_y_r} {window} Day MA",
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
            
    return feature_series_list, feature_metadata

# --- Saving Functions ---
# MOVED to data_collectors.feature_utils

# --- Worker Function for Parallel Processing ---
def process_symbol_features(
    symbol_original: str,
    original_series_data: pl.Series,
    df_symbols_meta_global: pl.DataFrame, 
    overall_min_date_global: Any,
    overall_max_date_global: Any,
    n_days_year_global: int
) -> Tuple[str, List[pl.Series], List[Dict[str, Any]]]:
    """
    Processes all features for a single symbol.
    Returns the symbol name, a list of new feature Series, and their metadata.
    """
    # print(f"Processing symbol in worker: {symbol_original}") # Can be too noisy
    
    description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
        symbol_original, 
        df_symbols_meta_global, 
        overall_min_date_global, 
        overall_max_date_global
    )
    
    current_symbol_new_series = []
    current_symbol_new_metadata = []

    yoy_series_list, yoy_meta_list = calculate_yoy_features(
        original_series_data, symbol_original, description_r, 
        series_start_date_str, series_end_date_str, n_days_year_global
    )
    current_symbol_new_series.extend(yoy_series_list)
    current_symbol_new_metadata.extend(yoy_meta_list)

    savgol_series_list, savgol_meta_list = calculate_savgol_features(
        original_series_data, symbol_original, description_r, label_y_r, 
        series_start_date_str, series_end_date_str, n_days_year_global
    )
    current_symbol_new_series.extend(savgol_series_list)
    current_symbol_new_metadata.extend(savgol_meta_list)

    log_series, log_meta_dict = calculate_log_transform_feature(
        original_series_data, symbol_original, description_r, label_y_r, 
        series_start_date_str, series_end_date_str
    )
    current_symbol_new_series.append(log_series)
    current_symbol_new_metadata.append(log_meta_dict)
    
    mva_series_list, mva_meta_list = calculate_mva_features(
        original_series_data, symbol_original, description_r, label_y_r, 
        series_start_date_str, series_end_date_str, n_days_year_global
    )
    current_symbol_new_series.extend(mva_series_list)
    current_symbol_new_metadata.extend(mva_meta_list)
    
    return symbol_original, current_symbol_new_series, current_symbol_new_metadata

# --- Main Orchestration Function ---
def main():
    """Main function to orchestrate the feature calculation process."""
    print("Starting feature calculation process (parallel)...")
    
    df_data_initial = load_and_prepare_data(DATA_PATH)
    df_symbols_meta_global = load_metadata(METADATA_PATH)

    all_new_feature_metadata_collected = []
    
    columns_to_process = [col for col in df_data_initial.columns if col != "date"]
    
    overall_min_date_global = df_data_initial["date"].min() 
    overall_max_date_global = df_data_initial["date"].max()

    df_data_final = df_data_initial.clone() 
    all_new_series_to_add_to_df = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        future_to_symbol = {
            executor.submit(
                process_symbol_features,
                symbol,
                df_data_initial.get_column(symbol).clone(), # Pass a clone of the series
                df_symbols_meta_global, 
                overall_min_date_global,
                overall_max_date_global,
                N_DAYS_YEAR 
            ): symbol for symbol in columns_to_process
        }

        for future in concurrent.futures.as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                _, new_series_for_symbol, new_metadata_for_symbol = future.result()
                all_new_series_to_add_to_df.extend(new_series_for_symbol)
                all_new_feature_metadata_collected.extend(new_metadata_for_symbol)
                # print(f"  Finished processing features for symbol: {symbol}") # Can be too noisy
            except Exception as exc:
                print(f"Symbol {symbol} generated an exception: {exc}")
                import traceback
                traceback.print_exc()

    if all_new_series_to_add_to_df:
        print(f"\nAdding {len(all_new_series_to_add_to_df)} new feature columns to DataFrame...")
        
        valid_series_to_add = []
        expr_found_count = 0
        for i, s_obj in enumerate(all_new_series_to_add_to_df):
            if s_obj is None:
                continue
            
            if isinstance(s_obj, pl.Series):
                if not s_obj.is_empty(): # Check is_empty only if it's a Series
                    valid_series_to_add.append(s_obj)
            elif isinstance(s_obj, pl.Expr):
                expr_found_count += 1
                # It's an Expr, which is unexpected here. Log it.
                # We cannot call .is_empty() on an Expr.
                # For now, we will skip adding such expressions.
                # To fix the root cause, the function creating this Expr needs to be found and corrected
                # to return a pl.Series.
                print(f"Warning: Item {i} (Name hint: {str(s_obj)[:100]}) is a pl.Expr, not pl.Series. Skipping this item.")
            else:
                print(f"Warning: Item {i} (Name hint: {str(s_obj)[:100]}) is of unexpected type {type(s_obj)}. Skipping.")
        
        if expr_found_count > 0:
            print(f"Critical Issue: Found {expr_found_count} pl.Expr objects where pl.Series objects were expected. " +
                  "These expressions were skipped. Review the calculation functions in process_symbol_features.")

        if valid_series_to_add:
            df_data_final = df_data_final.with_columns(valid_series_to_add)
            print(f"Added {len(valid_series_to_add)} valid new feature series to DataFrame.")
        else:
            print("No valid new feature series to add to DataFrame.")


    combine_and_save_metadata(df_symbols_meta_global, all_new_feature_metadata_collected, FEATURED_METADATA_OUTPUT_PATH)
    save_featured_data(df_data_final, FEATURED_DATA_OUTPUT_PATH)

    print("\nFeature calculation process finished.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")