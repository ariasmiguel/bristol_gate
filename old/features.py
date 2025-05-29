import time
import polars as pl
from typing import Tuple, Any, List, Dict
import sys

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

print("Starting feature calculation process...")

# --- Helper function for Savitzky-Golay filter ---
# MOVED to data_collectors.feature_utils.apply_savgol_filter

# --- Data Loading Functions ---
# MOVED to data_collectors.feature_utils

# --- Metadata Helper Function ---
# MOVED to data_collectors.feature_utils

# --- Feature Calculation Functions ---
def calculate_yoy_features(
    symbol_original: str,
    description_r: str,
    series_start_date_str: str,
    series_end_date_str: str,
    n_days_year: int
) -> Tuple[List[pl.Expr], List[Dict[str, Any]]]:
    """Calculates YoY, YoY4, and YoY5 features."""
    feature_exprs = []
    feature_metadata = []

    # YoY
    new_col_yoy = f"{symbol_original}_YoY"
    feature_exprs.append(
        (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year)) - 1) * 100).alias(new_col_yoy)
    )
    feature_metadata.append({
        "symbol": new_col_yoy, "source": "Calc",
        "description": f"{description_r}\nYear over Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })
    print(f"  Prepared expression for: {new_col_yoy}")

    # YoY4
    new_col_yoy4 = f"{symbol_original}_YoY4"
    feature_exprs.append(
        (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year * 4)) - 1) * 100).alias(new_col_yoy4)
    )
    feature_metadata.append({
        "symbol": new_col_yoy4, "source": "Calc",
        "description": f"{description_r}\n4 Year over 4 Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })
    print(f"  Prepared expression for: {new_col_yoy4}")

    # YoY5
    new_col_yoy5 = f"{symbol_original}_YoY5"
    feature_exprs.append(
        (((pl.col(symbol_original) / pl.col(symbol_original).shift(n_days_year * 5)) - 1) * 100).alias(new_col_yoy5)
    )
    feature_metadata.append({
        "symbol": new_col_yoy5, "source": "Calc",
        "description": f"{description_r}\n5 Year over 5 Year", "label_y": "Percent",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    })
    print(f"  Prepared expression for: {new_col_yoy5}")
    
    return feature_exprs, feature_metadata

def calculate_savgol_features(
    original_series: pl.Series,
    symbol_original: str,
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
            original_series.alias(new_col_name), 
            window_length=window,
            polyorder=poly,
            deriv=config['deriv']
        )
        
        # Check if the returned series is all nulls (which apply_savgol_filter might return on failure/skip)
        if filtered_series.is_null().all() and not original_series.is_null().all(): # check if original wasn't all null
            print(f"  Skipped or failed: {new_col_name} (Savitzky-Golay returned all nulls for window {window})")
            feature_series_list.append(pl.Series(new_col_name, [None] * len(original_series), dtype=pl.Float64))
            feature_metadata.append({
                "symbol": new_col_name, "source": "Calc (Skipped/Failed)",
                "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r} - SKIPPED/FAILED",
                "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
        else:
            feature_series_list.append(filtered_series)
            print(f"  Calculated series for: {new_col_name}")
            feature_metadata.append({
                "symbol": new_col_name, "source": "Calc",
                "description": f"{config['desc_prefix']} (p={poly}, n={window}" + (", m=1" if config['deriv'] == 1 else "") + f")\n{description_r}",
                "label_y": f"{label_y_r}/period" if config['deriv'] > 0 else label_y_r,
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
            
    return feature_series_list, feature_metadata

def calculate_log_transform_feature(
    symbol_original: str,
    description_r: str,
    label_y_r: str,
    series_start_date_str: str,
    series_end_date_str: str
) -> Tuple[pl.Expr, Dict[str, Any]]:
    """Calculates the log transformed feature."""
    new_col_log = f"{symbol_original}_Log"
    log_expr = (
        pl.when(pl.col(symbol_original) > 0)
        .then(pl.col(symbol_original).log())
        .otherwise(None)
        .interpolate() 
        .alias(new_col_log)
    )
    log_metadata = {
        "symbol": new_col_log, "source": "Calc",
        "description": f"Log of {description_r}", "label_y": f"log({label_y_r})",
        "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
    }
    print(f"  Prepared expression for: {new_col_log}")
    return log_expr, log_metadata

def calculate_mva_features(
    original_series: pl.Series, 
    symbol_original: str,
    description_r: str,
    label_y_r: str,
    series_start_date_str: str,
    series_end_date_str: str,
    n_days_year: int
) -> Tuple[List[pl.Expr], List[Dict[str, Any]]]:
    """Calculates Moving Average features."""
    feature_exprs = []
    feature_metadata = []

    mva_configs = {
        "_mva365": n_days_year,
        "_mva200": 200,
        "_mva050": 50
    }

    for suffix, window in mva_configs.items():
        new_col_mva = f"{symbol_original}{suffix}"
        if original_series.drop_nulls().len() >= window: # Check against original series length
            feature_exprs.append(
                # Operate on the column name, assuming it exists in the DataFrame being transformed
                pl.col(symbol_original).rolling_mean(window_size=window, min_periods=1).interpolate().alias(new_col_mva)
            )
            feature_metadata.append({
                "symbol": new_col_mva, "source": "Calc",
                "description": f"{description_r} {window} Day MA", "label_y": f"{label_y_r} {window} Day MA",
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
            print(f"  Prepared expression for: {new_col_mva}")
        else:
            print(f"  Skipped: {new_col_mva} (not enough data points for window {window})")
            feature_exprs.append(pl.lit(None, dtype=pl.Float64).alias(new_col_mva))
            feature_metadata.append({
                "symbol": new_col_mva, "source": "Calc (Skipped)",
                "description": f"{description_r} {window} Day MA - SKIPPED", "label_y": f"{label_y_r} {window} Day MA",
                "series_start": series_start_date_str, "series_end": series_end_date_str, "expense_ratio": -1.00
            })
            
    return feature_exprs, feature_metadata

# --- Saving Functions ---
# MOVED to data_collectors.feature_utils

# --- Main Orchestration Function ---
def main():
    """Main function to orchestrate the feature calculation process."""
    print("Starting feature calculation process...")
    
    # Use the new input paths for clarity, these are the outputs of create_aggregate_series.py
    # or features_aggregate.py if it's run before this.
    # This script should ideally run on the MOST processed data available before it.
    # For this refactor, assuming it runs on `final_aggregated_data.csv`
    
    df_data = load_and_prepare_data(DATA_PATH) # Data from create_aggregate_series.py
    df_symbols_meta = load_metadata(METADATA_PATH) # Metadata from create_aggregate_series.py


    all_new_feature_metadata = []
    columns_to_process = [col for col in df_data.columns if col != "date"]

    overall_min_date = df_data["date"].min()
    overall_max_date = df_data["date"].max()

    # Collect all expressions to apply in one go for efficiency, where possible
    all_feature_expressions = []

    for str_symbol_original in columns_to_process:
        print(f"\nProcessing symbol: {str_symbol_original}")

        description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
            str_symbol_original, df_symbols_meta, overall_min_date, overall_max_date
        )
        
        original_series_for_savgol_mva = df_data.get_column(str_symbol_original) 

        # Calculate YoY features (these are expressions)
        yoy_exprs, yoy_meta = calculate_yoy_features(
            str_symbol_original, description_r, series_start_date_str, series_end_date_str, N_DAYS_YEAR
        )
        all_feature_expressions.extend(yoy_exprs)
        all_new_feature_metadata.extend(yoy_meta)

        # Calculate Log Transform feature (this is an expression)
        log_expr, log_meta = calculate_log_transform_feature(
            str_symbol_original, description_r, label_y_r, series_start_date_str, series_end_date_str
        )
        all_feature_expressions.append(log_expr)
        all_new_feature_metadata.append(log_meta)
        
        # Calculate MVA features (these are expressions)
        # Note: calculate_mva_features was changed to take original_series, but its expressions use pl.col()
        # This is fine as they will be applied to df_data which contains str_symbol_original
        mva_exprs, mva_meta = calculate_mva_features(
            original_series_for_savgol_mva, # Pass series for checks, but expr uses pl.col()
            str_symbol_original, 
            description_r, label_y_r, 
            series_start_date_str, series_end_date_str, N_DAYS_YEAR
        )
        all_feature_expressions.extend(mva_exprs)
        all_new_feature_metadata.extend(mva_meta)

    # Apply all collected expressions
    if all_feature_expressions:
        print(f"\nApplying {len(all_feature_expressions)} feature expressions to DataFrame...")
        df_data = df_data.with_columns(all_feature_expressions)
        print("Expressions applied.")

    # Savitzky-Golay features need to be calculated iteratively as they return Series
    # and might depend on columns created by expressions (e.g. if a _Log column was then smoothed)
    # For now, they operate on original_series, which is fine.
    savgol_series_to_add_later = []
    for str_symbol_original in columns_to_process: # Iterate again for SavGol, or integrate better if dependencies are complex
        print(f"\nProcessing Savitzky-Golay for symbol: {str_symbol_original}")
        description_r, label_y_r, series_start_date_str, series_end_date_str, _ = get_symbol_metadata_details(
            str_symbol_original, df_symbols_meta, overall_min_date, overall_max_date
        )
        original_series_for_savgol = df_data.get_column(str_symbol_original) # Get potentially updated series if any expr mod it

        savgol_series_list, savgol_meta = calculate_savgol_features(
            original_series_for_savgol, str_symbol_original, description_r, label_y_r, 
            series_start_date_str, series_end_date_str, N_DAYS_YEAR
        )
        savgol_series_to_add_later.extend(savgol_series_list)
        all_new_feature_metadata.extend(savgol_meta)
        
    if savgol_series_to_add_later:
        print(f"\nAdding {len(savgol_series_to_add_later)} Savitzky-Golay feature series to DataFrame...")
        df_data = df_data.with_columns(savgol_series_to_add_later)
        print("Savitzky-Golay series added.")


    # Save all data and metadata
    combine_and_save_metadata(df_symbols_meta, all_new_feature_metadata, FEATURED_METADATA_OUTPUT_PATH)
    save_featured_data(df_data, FEATURED_DATA_OUTPUT_PATH)

    print("\nFeature calculation process finished.")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    print(f"Total execution time: {end_time - start_time:.2f} seconds")
