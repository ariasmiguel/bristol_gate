import polars as pl
import sys
import datetime
import numpy as np
# Removed scipy.signal import

# Import from the new utils file
from data_collectors.feature_utils import apply_savgol_filter

print("Starting recession feature creation process...")

# --- Configuration ---
INPUT_DATA_PATH = "data/final_features_data.csv"
INPUT_METADATA_PATH = "data/features_symbols_metadata.csv"
OUTPUT_DATA_PATH = "data/recession_features_data.csv"
OUTPUT_METADATA_PATH = "data/recession_features_metadata.csv"

# --- Helper for Savitzky-Golay ---
# MOVED to data_collectors.feature_utils.apply_savgol_filter
# The apply_savgol_filter in feature_utils.py will be used directly.
# Its signature is: apply_savgol_filter(s: pl.Series, window_length: int, polyorder: int, deriv: int = 0)
# The previous local helper was compatible.

def add_recession_features(
    input_data_path: str,
    input_metadata_path: str,
    output_data_path: str,
    output_metadata_path: str
) -> bool:
    """
    Adds recession-based features (RecInit, RecInit_Smooth) to the dataset.
    """
    try:
        # --- Load Data ---
        print(f"Loading data from: {input_data_path}")
        df_data = pl.read_csv(input_data_path)
        if "date" in df_data.columns and df_data["date"].dtype == pl.Utf8:
            df_data = df_data.with_columns(pl.col("date").str.to_datetime().cast(pl.Date))
        elif "date" in df_data.columns and df_data["date"].dtype == pl.Datetime:
            df_data = df_data.with_columns(pl.col("date").cast(pl.Date))
        elif "date" not in df_data.columns:
            print("Error: 'date' column not found in input data.")
            return False
        
        if "USREC" not in df_data.columns:
            print("Error: 'USREC' column not found. This script requires NBER recession data.")
            return False

        df_data = df_data.sort("date")
        # Ensure USREC is integer type for diff calculation
        if df_data["USREC"].dtype != pl.Int8 and df_data["USREC"].dtype != pl.Int16 and df_data["USREC"].dtype != pl.Int32 and df_data["USREC"].dtype != pl.Int64 :
            df_data = df_data.with_columns(pl.col("USREC").cast(pl.Int8, strict=False))


        print(f"Loading metadata from: {input_metadata_path}")
        df_symbols = pl.read_csv(input_metadata_path)

        # --- Identify Recession Start/End Dates ---
        df_data = df_data.with_columns(pl.col("USREC").diff().alias("USREC_diff"))
        
        dt_start_dates = df_data.filter(pl.col("USREC_diff") == 1).select("date").to_series()
        dt_end_dates = df_data.filter(pl.col("USREC_diff") == -1).select("date").to_series()

        if dt_start_dates.is_empty():
            print("No recession start dates found (USREC_diff == 1). No recession features will be generated.")
            df_data = df_data.drop("USREC_diff" if "USREC_diff" in df_data.columns else []) # drop if exists
            df_data.write_csv(output_data_path)
            df_symbols.write_csv(output_metadata_path)
            print("Original data and metadata saved.")
            return True 

        # --- Calculate Initiation Period Dates ---
        dt_init_start = dt_start_dates.dt.month_start().dt.offset_by("-1y")
        dt_init_end = dt_start_dates.dt.month_start().dt.offset_by("-1mo")
        
        # --- Adjust for NBER Data Starting with a Trough ---
        if not dt_end_dates.is_empty() and not dt_start_dates.is_empty() and dt_end_dates[0] < dt_start_dates[0]:
            dt_end_dates = dt_end_dates[1:]

        # --- Handle Mid-Recession Scenario ---
        if len(dt_end_dates) < len(dt_start_dates):
            # Append system's current date as Polars Date
            current_system_date = datetime.date.today()
            # Create a Polars Series with this date to ensure type compatibility for append
            today_polars_date_series = pl.Series([current_system_date]).cast(pl.Date)
            dt_end_dates = dt_end_dates.append(today_polars_date_series)

        
        # --- Create dfRecession ---
        max_len = min(len(dt_init_start), len(dt_init_end), len(dt_start_dates), len(dt_end_dates))

        if max_len == 0: # If any list became empty making max_len 0
            print("Warning: Not enough aligned recession period dates to form df_recession. Skipping RecInit generation.")
            df_recession = pl.DataFrame() # Empty DataFrame
        else:
            df_recession = pl.DataFrame({
                "initStart": dt_init_start[:max_len],
                "initEnd": dt_init_end[:max_len],
                "start": dt_start_dates[:max_len],
                "end": dt_end_dates[:max_len] 
            })
        
            min_overall_date = df_data["date"].min()
            if min_overall_date is not None and not df_recession.is_empty():
                df_recession = df_recession.filter(pl.col("start") >= min_overall_date)

        # --- Initialize and Populate RecInit ---
        df_data = df_data.with_columns(pl.lit(0).cast(pl.Int8).alias("RecInit"))
        if not df_recession.is_empty():
            for row in df_recession.iter_rows(named=True):
                init_s, init_e = row["initStart"], row["initEnd"]
                if init_s is not None and init_e is not None: 
                    df_data = df_data.with_columns(
                        pl.when((pl.col("date") > init_s) & (pl.col("date") < init_e))
                        .then(pl.lit(1).cast(pl.Int8))
                        .otherwise(pl.col("RecInit"))
                        .alias("RecInit")
                    )
        
        # --- Populate RecInit_Smooth (Day counter within each RecInit window) ---
        block_starts = (pl.col("RecInit") == 1) & (pl.col("RecInit").shift(1).fill_null(0) == 0)
        window_id_col = block_starts.cum_sum().alias("window_id")
        df_data = df_data.with_columns(window_id_col)
        
        df_data = df_data.with_columns(
            pl.when(pl.col("RecInit") == 1)
            .then(pl.col("RecInit").cum_sum().over("window_id")) 
            .otherwise(0)
            .cast(pl.Float64) 
            .alias("RecInit_Smooth")
        )
        
        # Clean up helper columns (ensure they exist before dropping)
        cols_to_drop = [col for col in ["window_id", "USREC_diff"] if col in df_data.columns]
        if cols_to_drop:
            df_data = df_data.drop(cols_to_drop)


        # --- Final Smoothing and Processing of RecInit_Smooth ---
        rec_init_smooth_series = df_data.get_column("RecInit_Smooth") # Use get_column
        filtered_smooth_series = apply_savgol_filter(rec_init_smooth_series, window_length=201, polyorder=3, deriv=0)
        df_data = df_data.with_columns(filtered_smooth_series.alias("RecInit_Smooth"))

        df_data = df_data.with_columns(
            pl.when(pl.col("RecInit_Smooth") < 0).then(0.0).otherwise(pl.col("RecInit_Smooth")).alias("RecInit_Smooth")
        )
        
        max_smooth_val = df_data.select(pl.col("RecInit_Smooth").max()).item()
        if max_smooth_val is not None and max_smooth_val > 0:
            df_data = df_data.with_columns((pl.col("RecInit_Smooth") / max_smooth_val).alias("RecInit_Smooth"))
        else: 
            df_data = df_data.with_columns(pl.lit(0.0).cast(pl.Float64).alias("RecInit_Smooth"))


        jitter_amount = 0.01
        # Generate noise Series with the same length as the DataFrame
        noise_array = (np.random.rand(len(df_data)) * 2 - 1) * jitter_amount
        noise_series = pl.Series("noise", noise_array)
        df_data = df_data.with_columns((pl.col("RecInit_Smooth") + noise_series).alias("RecInit_Smooth"))

        
        # --- Update Metadata ---
        usrec_col = df_data.get_column("USREC") # Use get_column
        usrec_series_dates = df_data.filter(usrec_col.is_not_null()).select("date")
        
        series_start_meta = "N/A"
        series_end_meta = "N/A"

        if not usrec_series_dates.is_empty():
            min_date_val = usrec_series_dates["date"].min()
            max_date_val = usrec_series_dates["date"].max()
            if min_date_val is not None:
                series_start_meta = min_date_val.strftime("%Y-%m-%d")
            if max_date_val is not None:
                series_end_meta = max_date_val.strftime("%Y-%m-%d")


        new_symbols_data = [
            {
                "symbol": "RecInit", "source": "Calc",
                "description": "1 for Recession Initiation Period, 0 For All Else",
                "label_y": "(-)", "expense_ratio": -1.00,
                "series_start": series_start_meta, "series_end": series_end_meta
            },
            {
                "symbol": "RecInit_Smooth", "source": "Calc",
                "description": "Smoothed indicator for Recession Initiation Period (0-1 scale, jittered)",
                "label_y": "(-)", "expense_ratio": -1.00,
                "series_start": series_start_meta, "series_end": series_end_meta
            }
        ]
        
        # Define a schema that includes all potential columns from df_symbols and new_symbols_data
        # to ensure compatibility during DataFrame creation and concatenation.
        # Start with existing metadata schema
        meta_schema = df_symbols.schema.copy()
        # Add keys from new_symbols_data if not present, defaulting to Utf8 or specific types
        for key in new_symbols_data[0].keys():
            if key not in meta_schema:
                if key == "expense_ratio": meta_schema[key] = pl.Float64
                else: meta_schema[key] = pl.Utf8
        
        df_new_symbols = pl.DataFrame(new_symbols_data, schema=meta_schema, strict=False)


        # Ensure df_new_symbols has the same columns and order as df_symbols for concat
        if not df_symbols.is_empty():
            select_cols_for_new = []
            for col_name_orig in df_symbols.columns:
                select_cols_for_new.append(col_name_orig)
                if col_name_orig not in df_new_symbols.columns:
                    df_new_symbols = df_new_symbols.with_columns(
                        pl.lit(None, dtype=df_symbols[col_name_orig].dtype).alias(col_name_orig)
                    )
            df_new_symbols = df_new_symbols.select(select_cols_for_new)
            df_symbols = pl.concat([df_symbols, df_new_symbols], how="diagonal_relaxed")
        else: # If original metadata was empty
            df_symbols = df_new_symbols

        
        # --- Save Outputs ---
        print(f"Saving data with recession features to: {output_data_path}")
        df_data.write_csv(output_data_path)
        print(f"Saving updated metadata to: {output_metadata_path}")
        df_symbols.write_csv(output_metadata_path)

        print("Recession feature creation process finished successfully.")
        return True

    except Exception as e:
        print(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = add_recession_features(
        INPUT_DATA_PATH,
        INPUT_METADATA_PATH,
        OUTPUT_DATA_PATH,
        OUTPUT_METADATA_PATH
    )
    if success:
        print("Script completed successfully.")
    else:
        print("Script encountered errors.")
        sys.exit(1)
