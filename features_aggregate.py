import polars as pl
import sys # For sys.exit()
import numpy as np # For Savitzky-Golay
# Removed scipy.signal import, will use from utils

# Import from the new utils file
from data_collectors.feature_utils import apply_savgol_filter

print("Starting features creation process from aggregates...")

# --- Configuration ---
# Input paths (output from create_aggregate_series.py)
AGGREGATED_DATA_PATH = "data/final_aggregated_data.csv"
AGGREGATED_METADATA_PATH = "data/aggregated_symbols_metadata.csv"

# Output paths for this script
FEATURES_DATA_OUTPUT_PATH = "data/final_features_data.csv"
FEATURES_METADATA_OUTPUT_PATH = "data/features_symbols_metadata.csv"

# --- Helper for Savitzky-Golay ---
# MOVED to data_collectors.feature_utils.apply_savgol_filter
# The apply_savgol_filter in feature_utils.py will be used directly.
# Its signature is: apply_savgol_filter(s: pl.Series, window_length: int, polyorder: int, deriv: int = 0)
# The previous local helper was: apply_savgol_filter(series: pl.Series, window_length: int, polyorder: int, deriv: int)
# They are compatible.

# Define the features to create based on the R script
# Each key is the new column name.
# Value is a dictionary:
#   'components': List of column names required for this calculation from the input df
#   'expr_lambda': A lambda function taking the DataFrame, and returning a Polars expression for the new column.
#   'description': Description for metadata,
#   'label_y': Y-axis label for metadata
features_to_create = {
    "TOTLNNSA_plus_WRESBAL": {
        "components": ["TOTLNNSA", "WRESBAL"],
        "expr_lambda": lambda df: pl.col("TOTLNNSA") + pl.col("WRESBAL"),
        "description": "Total Loans Plus All Reserves (TOTLNNSA + WRESBAL)",
        "label_y": "Percent" 
    },
    "TOTLLNSA_plus_WRESBAL": { # Note: TOTLLNSA (double L)
        "components": ["TOTLLNSA", "WRESBAL"],
        "expr_lambda": lambda df: pl.col("TOTLLNSA") + pl.col("WRESBAL"),
        "description": "Total Loans Plus All Reserves (TOTLLNSA + WRESBAL)",
        "label_y": "Percent" 
    },
    "GDP_YoY_to_DGS1": {
        "components": ["GDP_YoY", "DGS1"],
        "expr_lambda": lambda df: pl.col("GDP_YoY") - pl.col("DGS1"),
        "description": "Economic Yield Curve, GDP YoY and 1-Year Tr (GDP_YoY-DGS1)",
        "label_y": "Percent"
    },
    "GDP_YoY_to_TB3MS": {
        "components": ["GDP_YoY", "TB3MS"],
        "expr_lambda": lambda df: pl.col("GDP_YoY") - pl.col("TB3MS"),
        "description": "Economic Yield Curve, GDP YoY and 3-Month Tr (GDP_YoY-TB3MS)",
        "label_y": "Percent"
    },
    "OPHNFB_YoY_to_DGS1": {
        "components": ["OPHNFB_YoY", "DGS1"],
        "expr_lambda": lambda df: pl.col("OPHNFB_YoY") - pl.col("DGS1"),
        "description": "Productivity Yield Curve, Real output YoY and 1-Year Tr (OPHNFB_YoY-DGS1)",
        "label_y": "Percent"
    },
    "^GSPC_open_mva200_norm": {
        "components": ["^GSPC_open", "^GSPC_open_mva200"], # Assuming ^GSPC_open -> GSPC_open
        "expr_lambda": lambda df: 100 * (pl.col("^GSPC_open") / pl.col("^GSPC_open_mva200").replace(0, None)), 
        "description": "S&P 500 normalized by 200 SMA",
        "label_y": "Percent"
    },
    "^GSPC_open_mva050_mva200": {
        "components": ["^GSPC_open_mva050", "^GSPC_open_mva200"],
        "expr_lambda": lambda df: pl.col("^GSPC_open_mva050") - pl.col("^GSPC_open_mva200"),
        "description": "S&P 500 50 SMA - 200 SMA",
        "label_y": "Dollars"
    },
    "^GSPC_open_mva050_mva200_sig": {
        "components": ["^GSPC_open_mva050_mva200"], # Depends on the previously calculated feature
        "expr_lambda": lambda df: (pl.col("^GSPC_open_mva050_mva200") > 0).cast(pl.Int8),
        "description": "Signal S&P 500 50 SMA - 200 SMA (1 if > 0, else 0)",
        "label_y": "-"
    },
    # New features from the second R script part
    "UNRATE_smooth_21": {
        "components": ["UNRATE"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("UNRATE"), window_length=21, polyorder=3, deriv=0),
        "description": "Smoothed Civilian Unemployment Rate U-3 (21-period Savitzky-Golay)",
        "label_y": "Percent"
    },
    "UNRATE_smooth_der2": {
        "components": ["UNRATE"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("UNRATE"), window_length=501, polyorder=3, deriv=2),
        "description": "2nd Derivative of Smoothed U-3 (501-period Savitzky-Golay, p=3, m=2)",
        "label_y": "Percent/period^2"
    },
    "U6RATE_smooth_21": {
        "components": ["U6RATE"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("U6RATE"), window_length=21, polyorder=3, deriv=0),
        "description": "Smoothed Total Unemployed U-6 (21-period Savitzky-Golay)",
        "label_y": "Percent"
    },
    "U6RATE_smooth_der2": { # Corrected symbol name from R's metadata typo
        "components": ["U6RATE"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("U6RATE"), window_length=501, polyorder=3, deriv=2),
        "description": "2nd Derivative of Smoothed U-6 (501-period Savitzky-Golay, p=3, m=2)",
        "label_y": "Percent/period^2"
    },
    "^GSPC_open_Log_smooth_der": { # Assuming GSPC_Open_Log exists
        "components": ["^GSPC_open_Log"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("^GSPC_open_Log"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed Log Scale S&P 500 Open (501-period Savitzky-Golay, p=3, m=1)",
        "label_y": "log-points/period"
    },
    "^GSPC_open_by_GDPDEF_Log_smooth_der": { # Assuming GSPC_Open_by_GDPDEF_Log exists
        "components": ["^GSPC_open_by_GDPDEF_Log"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("^GSPC_open_by_GDPDEF_Log"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed Log S&P 500 Open (Real) by GDP Deflator (501-period Savitzky-Golay, p=3, m=1)",
        "label_y": "log-points/period"
    },
    "^GSPC_open_Log_smooth_der_der": { # Depends on GSPC_Open_Log_SmoothDer
        "components": ["^GSPC_open_Log_smooth_der"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("^GSPC_open_Log_smooth_der"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed ^GSPC_open_Log_smooth_der (effectively 2nd order effect on ^GSPC_open_Log)",
        "label_y": "log-points/period^2"
    },
    "NCBDBIQ027S_Log_Der": { # Assuming NCBDBIQ027S_Log exists
        "components": ["NCBDBIQ027S_Log"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("NCBDBIQ027S_Log"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed Log Nonfinancial Corporate Business Debt (NCBDBIQ027S_Log)",
        "label_y": "log-points/period"
    },
    "BUSLOANS_Log_Der": { # Assuming BUSLOANS_Log exists
        "components": ["BUSLOANS_Log"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("BUSLOANS_Log"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed Log Commercial and Industrial Loans (BUSLOANS_Log)",
        "label_y": "log-points/period"
    },
    "GPDI_Log_Der": { # Assuming GPDI_Log exists
        "components": ["GPDI_Log"],
        "expr_lambda": lambda df: apply_savgol_filter(df.get_column("GPDI_Log"), window_length=501, polyorder=3, deriv=1),
        "description": "Derivative of Smoothed Log Gross Private Domestic Investment (GPDI_Log)",
        "label_y": "log-points/period" # Corrected from R
    },
    "GDPSP500": {
        "components": ["^GSPC_close", "GDP"], # Assuming ^GSPC.Close -> ^GSPC_close
        "expr_lambda": lambda df: (pl.col("^GSPC_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
        "description": "S&P 500 (^GSPC_close) / GDP, interpolated",
        "label_y": "Ratio ($/$)"
    },
    "RLGSP500": {
        "components": ["RLG_close", "GDP"], # Assuming RLG.Close -> RLG_close
        "expr_lambda": lambda df: (pl.col("RLG_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
        "description": "Russell 2000 (RLG_close) / GDP, interpolated",
        "label_y": "Ratio ($/$)"
    },
    "DJISP500": {
        "components": ["DJI_close", "GDP"], # Assuming DJI.Close -> DJI_close
        "expr_lambda": lambda df: (pl.col("DJI_close") / pl.col("GDP").replace(0,None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
        "description": "Dow Jones Industrial Average (DJI_close) / GDP, interpolated",
        "label_y": "Ratio ($/$)"
    },
    "GPDI_by_GDP": {
        "components": ["GPDI", "GDP"],
        "expr_lambda": lambda df: (pl.col("GPDI") / pl.col("GDP").replace(0, None)).interpolate().fill_null(strategy="forward").fill_null(strategy="backward"),
        "description": "Gross Private Domestic Investment/GDP",
        "label_y": "Ratio ($/$)"
    },
    "ret_base": {
        "components": ["^GSPC_close"],
        "expr_lambda": lambda df: pl.col("^GSPC_close").pct_change().fill_null(0),
        "description": "S&P 500 Rate of Change",
        "label_y": "Percent"
    },
    "ret_base_short_TB3MS": {
        "components": ["TB3MS"],
        "expr_lambda": lambda df: (pl.col("TB3MS") / 365).fill_null(0),
        "description": "Daily Return from 3-Month T-Bill (TB3MS/365)",
        "label_y": "Percent"
    },
    "eq_base": {
        "components": ["ret_base"], # Depends on ret_base being created in the same run
        "expr_lambda": lambda df: (1 + pl.col("ret_base")).cum_prod(),
        "description": "Equity Return, 100% long S&P 500",
        "label_y": "$1 Invested"
    },
    "eq_base_short_TB3MS": {
        "components": ["ret_base_short_TB3MS"], # Depends on ret_base_short_TB3MS
        "expr_lambda": lambda df: (
            1 + (
                pl.col("ret_base_short_TB3MS").cum_sum() - 
                pl.col("ret_base_short_TB3MS").cum_sum().filter(pl.col("ret_base_short_TB3MS").cum_sum().is_not_null()).first().fill_null(0)
            )
        ),
        "description": "Cumulative Equity from $1 Invested in 3-Month T-Bill (simple interest basis)",
        "label_y": "$1 Invested"
    }
}

def generate_derived_features(
    input_data_path: str,
    input_metadata_path: str,
    features_definition: dict,
    final_data_output_path: str,
    updated_metadata_output_path: str
) -> bool:
    """
    Loads aggregated data and metadata, creates new derived features
    based on the provided definitions, and saves the resulting data
    and updated metadata to specified paths.

    Args:
        input_data_path: Path to the input aggregated data CSV.
        input_metadata_path: Path to the input aggregated metadata CSV.
        features_definition: Dictionary defining the features to create.
        final_data_output_path: Path to save the final data with new features.
        updated_metadata_output_path: Path to save the updated metadata.

    Returns:
        True if the process completes successfully and files are saved, False otherwise.
    """
    print("Starting derived features creation process...")

    # --- Load Data ---
    print(f"Loading aggregated data from: {input_data_path}")
    try:
        df_data = pl.read_csv(input_data_path)
        if "date" in df_data.columns and df_data["date"].dtype == pl.Utf8:
             df_data = df_data.with_columns(pl.col("date").str.to_datetime()) # Infer format
        elif "date" not in df_data.columns:
            print("Error: 'date' column not found in input data.")
            return False
        print(f"Loaded data shape: {df_data.shape}")
        if df_data.is_empty():
            print("Error: Input data is empty.")
            return False
    except Exception as e:
        print(f"Error loading aggregated data: {e}.")
        return False

    print(f"Loading aggregated metadata from: {input_metadata_path}")
    try:
        df_symbols_meta = pl.read_csv(input_metadata_path)
        print(f"Loaded aggregated metadata shape: {df_symbols_meta.shape}")
    except Exception as e:
        print(f"Error loading aggregated metadata: {e}.")
        return False

    if df_data.is_empty():
        print("Error: Dataframe is empty before determining date range.")
        return False
    
    if df_data["date"].dtype != pl.Datetime:
        try:
            df_data = df_data.with_columns(pl.col("date").str.to_datetime())
        except Exception as e:
            print(f"Error converting date column to datetime: {e}. Trying specific format.")
            try:
                df_data = df_data.with_columns(pl.col("date").str.to_datetime("%Y-%m-%d"))
            except Exception as e_fmt:
                print(f"Error converting date column with specific format: {e_fmt}")
                return False

    series_start_date_for_features = df_data.select(pl.min("date").dt.strftime("%Y-%m-%d")).item()
    series_end_date_for_features = df_data.select(pl.max("date").dt.strftime("%Y-%m-%d")).item()
    print(f"Overall date range for new feature metadata: {series_start_date_for_features} to {series_end_date_for_features}")

    new_metadata_rows = []

    # Store created columns to allow for chained dependencies (e.g. GSPC_Open_Log_SmoothDerDer)
    # This is implicitly handled if expr_lambda operates on the df which is updated in the loop.
    
    processed_columns_df = df_data.clone() # Start with a clone to add new columns

    for new_col_name, feat_details in features_definition.items():
        # Check if this feature has already been processed (e.g. if it was a dependency for an earlier one and added)
        if new_col_name in processed_columns_df.columns:
            print(f"Feature '{new_col_name}' already exists or was created as a dependency. Skipping creation, but adding metadata.")
            # continue # If we continue, metadata won't be added. Let it fall through for metadata.
        else: # Only attempt creation if not already present
            print(f"Attempting to create feature series: {new_col_name}")
            
            missing_components = [comp for comp in feat_details["components"] if comp not in processed_columns_df.columns]
            if missing_components:
                print(f"Error: Missing component columns for '{new_col_name}': {missing_components}.")
                print(f"Available columns: {processed_columns_df.columns}")
                print(f"Skipping feature series '{new_col_name}' due to missing components.")
                continue 
                
            try:
                # Pass the current state of processed_columns_df to the lambda
                series_expr_or_series = feat_details["expr_lambda"](processed_columns_df)
                
                if isinstance(series_expr_or_series, pl.Expr):
                    processed_columns_df = processed_columns_df.with_columns(series_expr_or_series.alias(new_col_name))
                elif isinstance(series_expr_or_series, pl.Series):
                    processed_columns_df = processed_columns_df.with_columns(series_expr_or_series.alias(new_col_name))
                else:
                    print(f"Error: expr_lambda for '{new_col_name}' did not return a Polars Expression or Series.")
                    continue
                print(f"Successfully created: {new_col_name}")
                
            except Exception as e:
                print(f"Error calculating or adding column '{new_col_name}': {e}.")
                print(f"Skipping feature series '{new_col_name}' due to calculation error.")
                continue 
        
        # Add metadata regardless of whether it was newly created or pre-existing (if we didn't 'continue' above)
        # Ensure we don't add duplicate metadata if script is run multiple times on outputs.
        # This check should ideally be more robust, e.g. by checking existing metadata file.
        # For now, assume it's a fresh run or metadata gets overwritten.
        new_row = {
            "symbol": new_col_name,
            "description": feat_details["description"],
            "label_y": feat_details["label_y"],
            "series_start": series_start_date_for_features,
            "series_end": series_end_date_for_features
        }
        # Adjust source based on R script logic
        if feat_details.get("string.source") == "Ratio" or \
           ("GPDI_by_GDP" == new_col_name) or \
           ("SP500" in new_col_name and "/" in feat_details.get("description","")): # Heuristic for ratios
             new_row["source"] = "Ratio"
        elif new_col_name in ["ret_base", "ret_base_short_TB3MS", "eq_base", "eq_base_short_TB3MS"] or "Smooth" in new_col_name : # Specific R "Calc" items
            new_row["source"] = "Calc"
        else: # Default for other calculated features if not specified
            new_row["source"] = "CalcFeat"


        if "expense_ratio" in df_symbols_meta.columns:
            new_row["expense_ratio"] = -1.00
        
        new_metadata_rows.append(new_row)

    df_data = processed_columns_df # Update df_data with all new columns

    if new_metadata_rows:
        schema_for_new_meta = {}
        if not df_symbols_meta.is_empty():
            schema_for_new_meta = {name: dtype for name, dtype in df_symbols_meta.schema.items()}
        else: 
            schema_for_new_meta = {
                "symbol": pl.Utf8, "source": pl.Utf8, "description": pl.Utf8,
                "label_y": pl.Utf8, "series_start": pl.Utf8, "series_end": pl.Utf8,
                "expense_ratio": pl.Float64
            }

        df_new_meta = pl.DataFrame(new_metadata_rows)

        for col_name, expected_dtype in schema_for_new_meta.items():
            if col_name not in df_new_meta.columns:
                # Create the column with nulls of the expected type if it's missing in new_metadata_rows' direct creation
                df_new_meta = df_new_meta.with_columns(pl.lit(None, dtype=expected_dtype).alias(col_name))
            elif df_new_meta[col_name].dtype != expected_dtype:
                try:
                    if expected_dtype == pl.Float64 and df_new_meta[col_name].dtype.is_integer():
                        df_new_meta = df_new_meta.with_columns(pl.col(col_name).cast(pl.Float64))
                    elif expected_dtype == pl.Utf8 and df_new_meta[col_name].dtype != pl.Utf8:
                         df_new_meta = df_new_meta.with_columns(pl.col(col_name).cast(pl.Utf8))
                except Exception as cast_e:
                    print(f"Warning: Could not cast column {col_name} in new metadata to {expected_dtype}. Error: {cast_e}")
        
        if not df_symbols_meta.is_empty():
            # Ensure df_new_meta has same columns in same order as df_symbols_meta before concat
            select_cols = []
            for existing_col_name in df_symbols_meta.columns:
                if existing_col_name not in df_new_meta.columns:
                     # This case should be handled by the loop above which adds missing columns
                     print(f"Critical: Column {existing_col_name} from existing metadata schema not found or created in new metadata. Appending with nulls.")
                     df_new_meta = df_new_meta.with_columns(pl.lit(None, dtype=df_symbols_meta[existing_col_name].dtype).alias(existing_col_name))
                select_cols.append(existing_col_name)
            df_new_meta = df_new_meta.select(select_cols)


        if df_symbols_meta.is_empty():
            # If original metadata was empty, new metadata becomes the current metadata
            # Ensure schema consistency based on first new_metadata_row if available
            if new_metadata_rows:
                 base_schema = {k: pl.Utf8 for k in new_metadata_rows[0].keys()} # Default to Utf8
                 if "expense_ratio" in base_schema : base_schema["expense_ratio"] = pl.Float64
                 df_symbols_meta = pl.DataFrame(new_metadata_rows, schema=base_schema)
            # else df_symbols_meta remains empty
        else:
            df_symbols_meta = pl.concat([df_symbols_meta, df_new_meta], how="diagonal_relaxed") 
        
        print(f"Appended {len(new_metadata_rows)} new rows to metadata. New metadata shape: {df_symbols_meta.shape}")

    print(f"Saving final data (with features) to: {final_data_output_path}")
    try:
        df_data.write_csv(final_data_output_path)
        print(f"Final features data saved. Shape: {df_data.shape}")
    except Exception as e:
        print(f"Error saving final features data: {e}")
        return False 

    print(f"Saving updated features metadata to: {updated_metadata_output_path}")
    try:
        df_symbols_meta.write_csv(updated_metadata_output_path)
        print(f"Updated features metadata saved. Shape: {df_symbols_meta.shape}")
    except Exception as e:
        print(f"Error saving updated features metadata: {e}")
        return False 

    print("Derived features creation process finished successfully.")
    return True

if __name__ == "__main__":
    print("Running derived features creation script...")
    
    success = generate_derived_features(
        input_data_path=AGGREGATED_DATA_PATH,
        input_metadata_path=AGGREGATED_METADATA_PATH,
        features_definition=features_to_create, # features_to_create is now extended
        final_data_output_path=FEATURES_DATA_OUTPUT_PATH,
        updated_metadata_output_path=FEATURES_METADATA_OUTPUT_PATH
    )

    if success:
        print("Script completed successfully.")
    else:
        print("Script encountered errors. Please check the logs.")
        sys.exit(1)
