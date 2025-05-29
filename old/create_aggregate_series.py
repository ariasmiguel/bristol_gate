import polars as pl
import sys # For sys.exit()

print("Starting aggregate series creation process...")

# --- Configuration ---\n
INTERPOLATED_DATA_PATH = "data/interpolated_flat_cols_filtered_wide_data.csv"
ORIGINAL_SYMBOLS_PATH = "data/symbols.csv"
AGGREGATED_METADATA_OUTPUT_PATH = "data/aggregated_symbols_metadata.csv"
FINAL_DATA_OUTPUT_PATH = "data/final_aggregated_data.csv"

# Define the initial subset of aggregations to implement
# Each key is the new column name.
# Value is a dictionary:
#   'components': List of column names required for this calculation from the input df
#   'expr_lambda': A lambda function taking the DataFrame, and returning a Polars expression for the new column.
#   'description': Description for metadata,
#   'label_y': Y-axis label for metadata
#
# Note on expr_lambda: Lambdas should return a single Polars expression.
# e.g., (pl.col("A") + pl.col("B")) / 2
# not df.select(...) unless it's selecting a single expression.
aggregations_to_create = {
    "RSALESAGG": {
        "components": ["RRSFS", "RSALES"],
        "expr_lambda": lambda df: (pl.col("RRSFS") + pl.col("RSALES")) / 2,
        "description": "Real Retail and Food Services Sales (Mean of RRSFS and RSALES)",
        "label_y": "Millions of Dollars"
    },
    "BUSLOANS_minus_BUSLOANSNSA": { # Adjusted name for Polars compatibility
        "components": ["BUSLOANS", "BUSLOANSNSA"],
        "expr_lambda": lambda df: pl.col("BUSLOANS") - pl.col("BUSLOANSNSA"),
        "description": "Business Loans (Monthly) SA - NSA",
        "label_y": "Billions of U.S. Dollars"
    },
    "BUSLOANS_minus_BUSLOANSNSA_by_GDP": {
        "components": ["BUSLOANS_minus_BUSLOANSNSA", "GDP"], # Depends on the previous one
        "expr_lambda": lambda df: (pl.col("BUSLOANS_minus_BUSLOANSNSA") / pl.col("GDP")) * 100,
        "description": "Business Loans (Monthly) SA - NSA divided by GDP",
        "label_y": "Percent"
    },
    "BUSLOANS_by_GDP": {
        "components": ["BUSLOANS", "GDP"],
        "expr_lambda": lambda df: (pl.col("BUSLOANS") / pl.col("GDP")) * 100,
        "description": "Business Loans (Monthly, SA) Normalized by GDP",
        "label_y": "Percent"
    },
    "BUSLOANS_INTEREST": {
        "components": ["BUSLOANS", "DGS10"],
        "expr_lambda": lambda df: (pl.col("BUSLOANS") * pl.col("DGS10")) / 100,
        "description": "Business Loans (Monthly, SA) Adjusted Interest Burdens (using DGS10)",
        "label_y": "Calculated Billions of U.S. Dollars"
    },
    "BUSLOANS_INTEREST_by_GDP": {
        "components": ["BUSLOANS_INTEREST", "GDP"], # Depends on BUSLOANS_INTEREST
        "expr_lambda": lambda df: (pl.col("BUSLOANS_INTEREST") / pl.col("GDP")) * 100,
        "description": "Business Loans (Monthly, SA) Adjusted Interest Burden Divided by GDP",
        "label_y": "Percent"
    },
    "BUSLOANSNSA_by_GDP": {
        "components": ["BUSLOANSNSA", "GDP"],
        "expr_lambda": lambda df: (pl.col("BUSLOANSNSA") / pl.col("GDP")) * 100,
        "description": "Business Loans (Monthly, NSA) Normalized by GDP",
        "label_y": "Percent"
    },
    "TOTCI_by_GDP": {
        "components": ["TOTCI", "GDP"],
        "expr_lambda": lambda df: (pl.col("TOTCI") / pl.col("GDP")) * 100,
        "description": "Commercial and Industrial Loans (Weekly, SA) Normalized by GDP", # Description adjusted to reflect typical TOTCI meaning
        "label_y": "Percent"
    },
    "TOTCINSA_by_GDP": {
        "components": ["TOTCINSA", "GDP"],
        "expr_lambda": lambda df: (pl.col("TOTCINSA") / pl.col("GDP")) * 100,
        "description": "Commercial and Industrial Loans (Weekly, NSA) Normalized by GDP", # Description adjusted
        "label_y": "Percent"
    },
    "TOTCINSA_INTEREST": {
        "components": ["TOTCINSA", "DGS10"],
        "expr_lambda": lambda df: (pl.col("TOTCINSA") * pl.col("DGS10")) / 100,
        "description": "Commercial and Industrial Loans (Weekly, NSA) Adjusted Interest Burdens (using DGS10)", # Description adjusted
        "label_y": "Calculated Billions of U.S. Dollars"
    },
    "TOTCINSA_INTEREST_by_GDP": {
        "components": ["TOTCINSA_INTEREST", "GDP"], # Depends on TOTCINSA_INTEREST
        "expr_lambda": lambda df: (pl.col("TOTCINSA_INTEREST") / pl.col("GDP")) * 100,
        "description": "Commercial and Industrial Loans (Weekly, NSA) Adjusted Interest Burden Divided by GDP", # Description adjusted
        "label_y": "Percent"
    },
    "W875RX1_by_GDP": {
        "components": ["W875RX1", "GDP"],
        "expr_lambda": lambda df: (pl.col("W875RX1") / pl.col("GDP")) * 100,
        "description": "Real Personal Income Normalized by GDP",
        "label_y": "Percent"
    },
    "A065RC1A027NBEA_by_GDP": {
        "components": ["A065RC1A027NBEA", "GDP"],
        "expr_lambda": lambda df: (pl.col("A065RC1A027NBEA") / pl.col("GDP")) * 100,
        "description": "Personal Income (NSA) Normalized by GDP",
        "label_y": "Percent"
    },
    "PI_by_GDP": {
        "components": ["PI", "GDP"],
        "expr_lambda": lambda df: (pl.col("PI") / pl.col("GDP")) * 100,
        "description": "Personal Income (SA) Normalized by GDP",
        "label_y": "Percent"
    },
    "A053RC1Q027SBEA_by_GDP": {
        "components": ["A053RC1Q027SBEA", "GDP"],
        "expr_lambda": lambda df: (pl.col("A053RC1Q027SBEA") / pl.col("GDP")) * 100,
        "description": "National income: Corporate profits before tax (without IVA and CCAdj) Normalized by GDP",
        "label_y": "Percent"
    },
    "CPROFIT_by_GDP": {
        "components": ["CPROFIT", "GDP"],
        "expr_lambda": lambda df: (pl.col("CPROFIT") / pl.col("GDP")) * 100,
        "description": "National income: Corporate profits before tax (with IVA and CCAdj) Normalized by GDP",
        "label_y": "Percent"
    },
    "CONSUMERNSA_by_GDP": {
        "components": ["CONSUMERNSA", "GDP"],
        "expr_lambda": lambda df: (pl.col("CONSUMERNSA") / pl.col("GDP")) * 100,
        "description": "Consumer Loans (Not Seasonally Adjusted) divided by GDP",
        "label_y": "Percent"
    },
    "RREACBM027NBOG_by_GDP": {
        "components": ["RREACBM027NBOG", "GDP"],
        "expr_lambda": lambda df: (pl.col("RREACBM027NBOG") / pl.col("GDP")) * 100,
        "description": "Residential Real Estate Loans (Monthly, NSA) divided by GDP",
        "label_y": "Percent"
    },
    "RREACBM027SBOG_by_GDP": {
        "components": ["RREACBM027SBOG", "GDP"],
        "expr_lambda": lambda df: (pl.col("RREACBM027SBOG") / pl.col("GDP")) * 100,
        "description": "Residential Real Estate Loans (Monthly, SA) divided by GDP",
        "label_y": "Percent"
    },
    "RREACBW027SBOG_by_GDP": {
        "components": ["RREACBW027SBOG", "GDP"],
        "expr_lambda": lambda df: (pl.col("RREACBW027SBOG") / pl.col("GDP")) * 100,
        "description": "Residential Real Estate Loans (Weekly, SA) divided by GDP",
        "label_y": "Percent"
    },
    "RREACBW027NBOG_by_GDP": {
        "components": ["RREACBW027NBOG", "GDP"],
        "expr_lambda": lambda df: (pl.col("RREACBW027NBOG") / pl.col("GDP")) * 100,
        "description": "Residential Real Estate Loans (Weekly, NSA) divided by GDP",
        "label_y": "Percent"
    },
    "UMDMNO_by_GDP": {
        "components": ["UMDMNO", "GDP"],
        "expr_lambda": lambda df: ((pl.col("UMDMNO") / 1000) / pl.col("GDP")) * 100,
        "description": "Value of Manufacturers' New Orders for All Durable Goods Industries (Monthly, NSA) divided by GDP", # Adjusted description
        "label_y": "Percent"
    },
    "DGORDER_by_GDP": {
        "components": ["DGORDER", "GDP"],
        "expr_lambda": lambda df: ((pl.col("DGORDER") / 1000) / pl.col("GDP")) * 100,
        "description": "Manufacturers' New Orders: Durable Goods (Monthly, SA) divided by GDP", # Adjusted description
        "label_y": "Percent"
    },
    "ASHMA_by_GDP": {
        "components": ["ASHMA", "GDP"],
        "expr_lambda": lambda df: ((pl.col("ASHMA") / 1000) / pl.col("GDP")) * 100,
        "description": "All Sectors; Home Mortgages; Asset, Level (Quarterly, NSA) divided by GDP", # Adjusted description
        "label_y": "Percent"
    },
    "ASHMA_INTEREST": {
        "components": ["ASHMA", "MORTGAGE30US"],
        "expr_lambda": lambda df: ((pl.col("ASHMA") / 1000) * pl.col("MORTGAGE30US")) / 100,
        "description": "All Sectors; Home Mortgages; Asset, Level (Quarterly, NSA) 30-Year Fixed Interest Burdens", # Adjusted description
        "label_y": "Calculated Billions of U.S. Dollars" # Original R script says Billions
    },
    "ASHMA_INTEREST_by_GDP": {
        "components": ["ASHMA_INTEREST", "GDP"], # Depends on ASHMA_INTEREST
        "expr_lambda": lambda df: (pl.col("ASHMA_INTEREST") / pl.col("GDP")) * 100,
        "description": "All Sectors; Home Mortgages; Asset, Level (Quarterly, NSA) 30-Year Fixed Interest Burdens Divided by GDP", # Adjusted description
        "label_y": "Percent"
    },
    "CONSUMERNSA_INTEREST": {
        "components": ["CONSUMERNSA", "TERMCBPER24NS"],
        "expr_lambda": lambda df: (pl.col("CONSUMERNSA") * pl.col("TERMCBPER24NS")) / 100,
        "description": "Consumer Loans (Not Seasonally Adjusted) Interest Burdens (using TERMCBPER24NS)",
        "label_y": "Calculated Billions of U.S. Dollars" # R script label: "Billions of U.S. Dollars"
    },
    "CONSUMERNSA_INTEREST_by_GDP": {
        "components": ["CONSUMERNSA_INTEREST", "GDP"], # Depends on CONSUMERNSA_INTEREST
        "expr_lambda": lambda df: (pl.col("CONSUMERNSA_INTEREST") / pl.col("GDP")) * 100,
        "description": "Consumer Loans (Not Seasonally Adjusted) Interest Burden Divided by GDP",
        "label_y": "Percent"
    },
    "TOTLNNSA": {
        "components": ["BUSLOANS", "REALLNNSA", "CONSUMERNSA"],
        "expr_lambda": lambda df: pl.col("BUSLOANS") + pl.col("REALLNNSA") + pl.col("CONSUMERNSA"),
        "description": "Total Loans, Not Seasonally Adjusted (BUSLOANS + REALLNNSA + CONSUMERNSA)",
        "label_y": "Billions of U.S. Dollars"
    },
    "TOTLNNSA_by_GDP": {
        "components": ["TOTLNNSA", "GDP"], # Depends on TOTLNNSA
        "expr_lambda": lambda df: (pl.col("TOTLNNSA") / pl.col("GDP")) * 100,
        "description": "Total Loans, Not Seasonally Adjusted, divided by GDP",
        "label_y": "Percent"
    },
    "TOTLNNSA_INTEREST": {
        "components": ["TOTLNNSA", "DGS10"], # Depends on TOTLNNSA
        "expr_lambda": lambda df: (pl.col("TOTLNNSA") * pl.col("DGS10")) / 100,
        "description": "Total Loans, Not Seasonally Adjusted, Interest Burdens (using DGS10)",
        "label_y": "Calculated Billions of U.S. Dollars" # R script label: "Billions of U.S. Dollars"
    },
    "TOTLNNSA_INTEREST_by_GDP": {
        "components": ["TOTLNNSA_INTEREST", "GDP"], # Depends on TOTLNNSA_INTEREST
        "expr_lambda": lambda df: (pl.col("TOTLNNSA_INTEREST") / pl.col("GDP")) * 100,
        "description": "Total Loans, Not Seasonally Adjusted, Interest Burden Divided by GDP",
        "label_y": "Percent"
    },
    "WRESBAL_by_GDP": {
        "components": ["WRESBAL", "GDP"],
        "expr_lambda": lambda df: (pl.col("WRESBAL") / pl.col("GDP")) * 100,
        "description": "Reserve Balances with Federal Reserve Banks Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "EXCSRESNW_by_GDP": {
        "components": ["EXCSRESNW", "GDP"],
        "expr_lambda": lambda df: ((pl.col("EXCSRESNW") / 1000) / pl.col("GDP")) * 100,
        "description": "Excess Reserves of Depository Institutions Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "WLRRAL_by_GDP": {
        "components": ["WLRRAL", "GDP"],
        "expr_lambda": lambda df: ((pl.col("WLRRAL") / 1000) / pl.col("GDP")) * 100,
        "description": "Liabilities and Capital: Liabilities: Reverse Repurchase Agreements: Wednesday Level (NSA) Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "SOFR99_minus_SOFR1": {
        "components": ["SOFR99", "SOFR1"],
        "expr_lambda": lambda df: pl.col("SOFR99") - pl.col("SOFR1"),
        "description": "Secured Overnight Financing Rate: 99th Percentile - 1st Percentile",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "EXPCH_minus_IMPCH": {
        "components": ["EXPCH", "IMPCH"],
        "expr_lambda": lambda df: (pl.col("EXPCH") / 1000) - (pl.col("IMPCH") / 1000),
        "description": "U.S. Exports to China (FAS Basis) - U.S. Imports from China (Customs Basis)", # Adjusted for typical series names
        "label_y": "Billions of U.S. Dollars" # R script uses "Billions of dollars"
    },
    "EXPMX_minus_IMPMX": {
        "components": ["EXPMX", "IMPMX"],
        "expr_lambda": lambda df: (pl.col("EXPMX") / 1000) - (pl.col("IMPMX") / 1000),
        "description": "U.S. Exports to Mexico (FAS Basis) - U.S. Imports from Mexico (Customs Basis)", # Adjusted for typical series names
        "label_y": "Billions of U.S. Dollars" # R script uses "Billions of dollars"
    },
    "SRPSABSNNCB_by_GDP": {
        "components": ["SRPSABSNNCB", "GDP"],
        "expr_lambda": lambda df: (pl.col("SRPSABSNNCB") / pl.col("GDP")) * 100,
        "description": "Nonfinancial corporate business; security repurchase agreements; asset, Level (NSA) Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "ASTLL_by_GDP": {
        "components": ["ASTLL", "GDP"],
        "expr_lambda": lambda df: ((pl.col("ASTLL") / 1000) / pl.col("GDP")) * 100,
        "description": "All sectors; total loans; liability, Level (NSA) Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "ASFMA_by_GDP": {
        "components": ["ASFMA", "GDP"],
        "expr_lambda": lambda df: ((pl.col("ASFMA") / 1000) / pl.col("GDP")) * 100,
        "description": "All sectors; farm mortgages; asset, Level (NSA) Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "ASFMA_by_ASTLL": {
        "components": ["ASFMA", "ASTLL"],
        "expr_lambda": lambda df: ((pl.col("ASFMA") / 1000) / (pl.col("ASTLL") / 1000)) * 100,
        "description": "All sectors; farm mortgages divided by total loans; asset, Level (NSA)", # Adjusted desc.
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "ASFMA_INTEREST": {
        "components": ["ASFMA", "MORTGAGE30US"],
        "expr_lambda": lambda df: ((pl.col("ASFMA") / 1000) * pl.col("MORTGAGE30US")) / 100,
        "description": "All sectors; farm mortgages; asset, Level (NSA) 30-Year Fixed Interest Burdens", # Adjusted desc.
        "label_y": "Calculated Billions of U.S. Dollars" # R script label: "Billions of U.S. Dollars"
    },
    "ASFMA_INTEREST_by_GDP": {
        "components": ["ASFMA_INTEREST", "GDP"], # Depends on ASFMA_INTEREST
        "expr_lambda": lambda df: (pl.col("ASFMA_INTEREST") / pl.col("GDP")) * 100,
        "description": "All sectors; farm mortgages; asset, Level (NSA) Interest Burden Divided by GDP", # Adjusted desc.
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "FARMINCOME_by_GDP": {
        "components": ["USDA_NET_FARM_INCOME", "GDP"],
        "expr_lambda": lambda df: (pl.col("USDA_NET_FARM_INCOME") / pl.col("GDP")) * 100,
        "description": "Farm Income (Annual, NSA) Divided by GDP",
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "BOGMBASE_by_GDP": {
        "components": ["BOGMBASE", "GDP"],
        "expr_lambda": lambda df: ((pl.col("BOGMBASE") / 1000) / pl.col("GDP")) * 100,
        "description": "Monetary Base; Total (BOGMBASE) Divided by GDP", # Adjusted Description
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "WALCL_by_GDP": {
        "components": ["WALCL", "GDP"],
        "expr_lambda": lambda df: ((pl.col("WALCL") / 1000) / pl.col("GDP")) * 100,
        "description": "Assets: Total Assets: Total Assets (Less Eliminations from Consolidation): Wednesday Level (WALCL) Divided by GDP", # Adjusted Description for clarity
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "ECBASSETS_by_EUNNGDP": {
        "components": ["ECBASSETS", "EUNNGDP"],
        "expr_lambda": lambda df: ((pl.col("ECBASSETS") / 1000) / (pl.col("EUNNGDP") / 1000)) * 100,
        "description": "Central Bank Assets for Euro Area Divided by Gross Domestic Product for Euro Area", # Adjusted Description
        "label_y": "Percent" # R script uses "PERCENT"
    },
    "DGS30_to_DGS10": {
        "components": ["DGS30", "DGS10"],
        "expr_lambda": lambda df: pl.col("DGS30") - pl.col("DGS10"),
        "description": "Yield Curve: 30-Year Treasury Constant Maturity Minus 10-Year Treasury Constant Maturity",
        "label_y": "Percent"
    },
    "DGS10_to_DGS1": {
        "components": ["DGS10", "DGS1"],
        "expr_lambda": lambda df: pl.col("DGS10") - pl.col("DGS1"),
        "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 1-Year Treasury Constant Maturity",
        "label_y": "Percent"
    },
    "DGS10_to_DGS2": {
        "components": ["DGS10", "DGS2"],
        "expr_lambda": lambda df: pl.col("DGS10") - pl.col("DGS2"),
        "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity",
        "label_y": "Percent"
    },
    "DGS10_to_TB3MS": {
        "components": ["DGS10", "TB3MS"],
        "expr_lambda": lambda df: pl.col("DGS10") - pl.col("TB3MS"),
        "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 3-Month Treasury Bill Secondary Market Rate",
        "label_y": "Percent"
    },
    "DGS10_to_DTB3": {
        "components": ["DGS10", "DTB3"],
        "expr_lambda": lambda df: pl.col("DGS10") - pl.col("DTB3"),
        "description": "Yield Curve: 10-Year Treasury Constant Maturity Minus 3-Month Treasury Bill (Daily)", # Assuming DTB3 is daily version
        "label_y": "Percent"
    },
    "AAA_div_DGS10": { # Renamed from DGS10ByAAA for clarity of calculation
        "components": ["AAA", "DGS10"],
        "expr_lambda": lambda df: pl.col("AAA") / pl.col("DGS10"),
        "description": "Moody's Seasoned Aaa Corporate Bond Yield Relative to 10-Year Treasury Constant Maturity (AAA/DGS10)", # Adjusted Description
        "label_y": "Ratio" # R script uses "-"
    },
    "LNU03000000_by_POPTHM": {
        "components": ["LNU03000000", "POPTHM"],
        "expr_lambda": lambda df: (pl.col("LNU03000000") / pl.col("POPTHM")) * 100,
        "description": "Unemployment Level (NSA) / Population",
        "label_y": "%"
    },
    "UNEMPLOY_by_POPTHM": {
        "components": ["UNEMPLOY", "POPTHM"],
        "expr_lambda": lambda df: (pl.col("UNEMPLOY") / pl.col("POPTHM")) * 100,
        "description": "Unemployment Level (SA) / Population", # Adjusted Description
        "label_y": "%"
    },
    "NPPTTL_by_POPTHM": {
        "components": ["NPPTTL", "POPTHM"],
        "expr_lambda": lambda df: (pl.col("NPPTTL") / pl.col("POPTHM")) * 100,
        "description": "ADP National Employment Report: Total Nonfarm Private Employment / Population", # Adjusted Description
        "label_y": "%"
    },
    "U6_to_U3": { # Symbol kept as in R script
        "components": ["U6RATE", "UNRATE"],
        "expr_lambda": lambda df: pl.col("U6RATE") - pl.col("UNRATE"),
        "description": "U-6 Unemployment Rate Minus U-3 Unemployment Rate (U6RATE - UNRATE)", # Adjusted Description
        "label_y": "%"
    },
    "DCOILBRENTEU_by_PPIACO": {
        "components": ["DCOILBRENTEU", "PPIACO"],
        "expr_lambda": lambda df: pl.col("DCOILBRENTEU") / pl.col("PPIACO"),
        "description": "Crude Oil Brent Price Normalized by Producer Price Index: All Commodities",
        "label_y": "$/bbl/Index"
    },
    "DCOILWTICO_by_PPIACO": {
        "components": ["DCOILWTICO", "PPIACO"],
        "expr_lambda": lambda df: pl.col("DCOILWTICO") / pl.col("PPIACO"),
        "description": "Crude Oil WTI Price Normalized by Producer Price Index: All Commodities",
        "label_y": "$/bbl/Index"
    },
    "GDP_by_GDPDEF": {
        "components": ["GDP", "GDPDEF"],
        "expr_lambda": lambda df: pl.col("GDP") / pl.col("GDPDEF"),
        "description": "Nominal GDP Normalized by GDP Deflator",
        "label_y": "Ratio"
    },
    "GSG_Close_by_GDPDEF": {
        "components": ["GSG_close", "GDPDEF"],
        "expr_lambda": lambda df: pl.col("GSG_close") / pl.col("GDPDEF"),
        "description": "GSCI Commodity-Indexed Trust (GSG Close) Normalized by GDP Deflator",
        "label_y": "Ratio"
    },
    "GSG_Close_by_GSPC_Close": {
        "components": ["GSG_close", "^GSPC_close"],
        "expr_lambda": lambda df: pl.col("GSG_close") / pl.col("^GSPC_close"),
        "description": "GSCI Commodity-Indexed Trust (GSG Close) Normalized by S&P 500 Close (GSPC Close)",
        "label_y": "Ratio"
    },
    "GDP_by_POPTHM": { # Formerly GDPBYPOPTHM
        "components": ["GDP", "POPTHM"],
        "expr_lambda": lambda df: (pl.col("GDP") * 1_000_000) / pl.col("POPTHM"),
        "description": "GDP per Capita",
        "label_y": "$/person"
    },
    "GDP_by_CPIAUCSL": { # Formerly GDPBYCPIAUCSL
        "components": ["GDP", "CPIAUCSL"],
        "expr_lambda": lambda df: pl.col("GDP") / (pl.col("CPIAUCSL") / 100),
        "description": "GDP Deflated by CPI (CPIAUCSL)",
        "label_y": "Billions of Constant Dollars"
    },
    "GDP_by_CPIAUCSL_by_POPTHM": { # Formerly GDPBYCPIAUCSLBYPOPTHM
        "components": ["GDP_by_CPIAUCSL", "POPTHM"], # Depends on GDP_by_CPIAUCSL
        "expr_lambda": lambda df: (pl.col("GDP_by_CPIAUCSL") * 1_000_000) / pl.col("POPTHM"),
        "description": "GDP Deflated by CPI, per Capita",
        "label_y": "Constant $/Person"
    },
    "GSPC_Close_by_MDY_Close": { # Formerly GSPC_CloseBYMDY_Close
        "components": ["^GSPC_close", "MDY_close"],
        "expr_lambda": lambda df: pl.col("^GSPC_close") / pl.col("MDY_close"),
        "description": "S&P 500 Close Normalized by S&P MidCap 400 Close",
        "label_y": "Ratio"
    },
    "QQQ_Close_by_MDY_Close": { # Formerly QQQ_CloseBYMDY_Close
        "components": ["QQQ_close", "MDY_close"],
        "expr_lambda": lambda df: pl.col("QQQ_close") / pl.col("MDY_close"),
        "description": "Nasdaq 100 Close (QQQ) Normalized by S&P MidCap 400 Close",
        "label_y": "Ratio"
    },
    "GSPC_DailySwing": {
        "components": ["^GSPC_high", "^GSPC_low", "^GSPC_open"],
        "expr_lambda": lambda df: (pl.col("^GSPC_high") - pl.col("^GSPC_low")) / pl.col("^GSPC_open").replace(0, None), # Avoid division by zero
        "description": "S&P 500 (GSPC) Daily Swing: (High - Low) / Open",
        "label_y": "Ratio"
    },
    "GSPC_Open_by_GDPDEF": {
        "components": ["^GSPC_open", "GDPDEF"],
        "expr_lambda": lambda df: pl.col("^GSPC_open") / (pl.col("GDPDEF") / 100),
        "description": "S&P 500 (GSPC) Open Deflated by GDP Deflator",
        "label_y": "Constant Dollars"
    },
    "GSPC_Close_by_GDPDEF": {
        "components": ["^GSPC_close", "GDPDEF"],
        "expr_lambda": lambda df: pl.col("^GSPC_close") / (pl.col("GDPDEF") / 100),
        "description": "S&P 500 (GSPC) Close Deflated by GDP Deflator",
        "label_y": "Constant Dollars"
    },
    "HNFSUSNSA_minus_HSN1FNSA": {
        "components": ["HNFSUSNSA", "HSN1FNSA"],
        "expr_lambda": lambda df: pl.col("HNFSUSNSA") - pl.col("HSN1FNSA"),
        "description": "New Privately-Owned Houses For Sale (NSA) - New Privately-Owned Houses Sold (NSA)",
        "label_y": "Thousands of Units"
    },
    "MSPUS_times_HOUST": {
        "components": ["MSPUS", "HOUST"],
        # MSPUS (Median Sales Price) in $, HOUST (Housing Starts) in Thousands of Units.
        # Result in Millions of Dollars: (Price * Units_Thousands * 1000) / 1,000,000 = Price * Units_Thousands / 1000
        # R script has /1000000. If HOUST is just units (not thousands), then R is Price * Units / 1M.
        # Assuming HOUST is Thousands of Units (common for FRED), then Price * (HOUST * 1000) / 1000000 = Price * HOUST / 1000
        "expr_lambda": lambda df: (pl.col("MSPUS") * pl.col("HOUST")) / 1000, # Adjusted based on typical units
        "description": "Median Sales Price of New Houses Sold times Housing Starts (Value of New Construction Started)",
        "label_y": "Millions of Dollars"
    },
    "HOUST_div_POPTHM": { # Using _div_ as per R script name
        "components": ["HOUST", "POPTHM"],
        # HOUST (Thousands of Units, SAAR), POPTHM (Thousands of Persons)
        # Ratio of starts per person (or per 1000 persons if units aren't adjusted)
        "expr_lambda": lambda df: pl.col("HOUST") / pl.col("POPTHM"), # Results in Starts per 1000 people if HOUST is K and POPTHM is K
        "description": "Housing Starts per Capita (Thousands of Units SAAR / Thousands of Persons)",
        "label_y": "Starts per 1000 Persons" # R script: "Starts per person", adjusted for clarity of units
    },
    "MSPUS_times_HNFSUSNSA": {
        "components": ["MSPUS", "HNFSUSNSA"],
        # MSPUS ($), HNFSUSNSA (Thousands of Units) -> Price * Units_Thousands / 1000 for Millions of $
        "expr_lambda": lambda df: (pl.col("MSPUS") * pl.col("HNFSUSNSA")) / 1000, # Adjusted based on typical units
        "description": "Median Sales Price of New Houses Sold times New Privately-Owned Houses For Sale (Value of Homes For Sale)",
        "label_y": "Millions of Dollars"
    },
    "MSPUS_times_HSN1FNSA_plus_EXHOSLUSM495S": {
        "components": ["MSPUS", "HSN1FNSA", "EXHOSLUSM495S"],
        # MSPUS ($)
        # HSN1FNSA (New Houses Sold, Thousands of Units, NSA)
        # EXHOSLUSM495S (Existing Home Sales, Millions of Units, SAAR - needs care if mixing NSA and SA)
        # R: (MSPUS * (HSN1FNSA*1000 + EXHOSLUSM495S))/1e9
        # This implies EXHOSLUSM495S in R was treated as individual units, not millions.
        # If EXHOSLUSM495S is in Millions of units (e.g. 5.0 for 5 million), then (EXHOSLUSM495S * 1e6) for units.
        # Let's assume EXHOSLUSM495S from data is in # of units, not millions.
        # Then (Price * (NewSold_K*1000 + ExistingSold_Units)) / 1e9 for Billions of $
        "expr_lambda": lambda df: (pl.col("MSPUS") * (pl.col("HSN1FNSA") * 1000 + pl.col("EXHOSLUSM495S"))) / 1_000_000_000,
        "description": "Value of New and Existing Home Sales (MSPUS * (New Sold Units + Existing Sold Units))",
        "label_y": "Billions of Dollars"
    },
    "MSPUS_times_HSN1FNSA_plus_EXHOSLUSM495S_by_GDP": {
        "components": ["MSPUS_times_HSN1FNSA_plus_EXHOSLUSM495S", "GDP"], # Depends on the previous
        "expr_lambda": lambda df: (pl.col("MSPUS_times_HSN1FNSA_plus_EXHOSLUSM495S") / pl.col("GDP")) * 100,
        "description": "Value of New and Existing Home Sales, as a Percentage of GDP",
        "label_y": "Percent"
    },
    # "CHRISCMEHG1_by_PPIACO": {
    #     "components": ["CHRISCMEHG1", "PPIACO"],
    #     "expr_lambda": lambda df: pl.col("CHRISCMEHG1") / pl.col("PPIACO"),
    #     "description": "Copper Price (CHRISCMEHG1) Normalized by Producer Price Index: All Commodities (PPIACO)",
    #     "label_y": "$/lb/Index"
    # },
    # "CHRISCMEHG1_by_CPIAUCSL": {
    #     "components": ["CHRISCMEHG1", "CPIAUCSL"],
    #     "expr_lambda": lambda df: pl.col("CHRISCMEHG1") / (pl.col("CPIAUCSL") / 100), # Assuming CPIAUCSL needs to be scaled (e.g. 1982-84=100)
    #     "description": "Copper Price (CHRISCMEHG1) Normalized by Consumer Price Index: All Urban Consumers (CPIAUCSL)",
    #     "label_y": "$/lb/Constant Dollars" # Or $/lb/Index
    # },
    # "LBMAGOLDUSDPM_by_PPIACO": {
    #     "components": ["LBMAGOLDUSDPM", "PPIACO"],
    #     "expr_lambda": lambda df: pl.col("LBMAGOLDUSDPM") / pl.col("PPIACO"),
    #     "description": "Gold Fixing Price P.M. (LBMAGOLDUSDPM) Normalized by Producer Price Index: All Commodities (PPIACO)",
    #     "label_y": "$/t oz/Index"
    # },
    # "LBMAGOLDUSDPM_by_CPIAUCSL": {
    #     "components": ["LBMAGOLDUSDPM", "CPIAUCSL"],
    #     "expr_lambda": lambda df: pl.col("LBMAGOLDUSDPM") / (pl.col("CPIAUCSL") / 100), # Assuming CPIAUCSL needs to be scaled
    #     "description": "Gold Fixing Price P.M. (LBMAGOLDUSDPM) Normalized by Consumer Price Index: All Urban Consumers (CPIAUCSL)",
    #     "label_y": "$/t oz/Constant Dollars" # Or $/t oz/Index
    # },
    # "LBMAGOLDUSDPM_by_GDP": {
    #     "components": ["LBMAGOLDUSDPM", "GDP"],
    #     "expr_lambda": lambda df: pl.col("LBMAGOLDUSDPM") / pl.col("GDP"), # GDP in Billions, Gold in $/oz. Ratio will be small.
    #     "description": "Gold Fixing Price P.M. (LBMAGOLDUSDPM) Normalized by Nominal GDP",
    #     "label_y": "Ratio ($/oz per Billion $ GDP)" # Or just "Ratio"
    # }
}

def generate_aggregate_series(
    interpolated_data_path: str,
    original_symbols_path: str,
    aggregations_definition: dict,
    final_data_output_path: str,
    aggregated_metadata_output_path: str
) -> bool:
    """
    Loads interpolated wide-format data and original symbols metadata,
    creates new aggregate series based on the provided definitions,
    and saves the resulting data and updated metadata to specified paths.

    Args:
        interpolated_data_path: Path to the input interpolated data CSV.
        original_symbols_path: Path to the input original symbols metadata CSV.
        aggregations_definition: Dictionary defining the aggregations to create.
        final_data_output_path: Path to save the final data with aggregates.
        aggregated_metadata_output_path: Path to save the updated metadata.

    Returns:
        True if the process completes successfully and files are saved, False otherwise.
    """
    print("Starting aggregate series creation process...")

    # --- Load Data ---
    print(f"Loading interpolated data from: {interpolated_data_path}")
    try:
        df_data = pl.read_csv(interpolated_data_path)
        df_data = df_data.with_columns(pl.col("date").str.to_datetime("%Y-%m-%d"))
        print(f"Loaded data shape: {df_data.shape}")
        if df_data.is_empty():
            print("Error: Input data is empty.")
            return False
    except Exception as e:
        print(f"Error loading interpolated data: {e}.")
        return False

    print(f"Loading original symbols metadata from: {original_symbols_path}")
    try:
        df_symbols_meta_orig = pl.read_csv(original_symbols_path)
        rename_map = {
            current_name: new_name 
            for current_name, new_name in {
                "string.symbol": "symbol",
                "string.source": "source",
                "string.description": "description",
                "string.label.y": "label_y",
                "date.series.start": "series_start",
                "date.series.end": "series_end",
                "float.expense.ratio": "expense_ratio" 
            }.items() if current_name in df_symbols_meta_orig.columns
        }
        df_symbols_meta = df_symbols_meta_orig.rename(rename_map)
        
        desired_meta_cols = ["symbol", "source", "description", "label_y", "series_start", "series_end", "expense_ratio"]
        final_meta_cols = [col for col in desired_meta_cols if col in df_symbols_meta.columns]
        df_symbols_meta = df_symbols_meta.select(final_meta_cols)
        print(f"Loaded and processed original metadata shape: {df_symbols_meta.shape}")
    except Exception as e:
        print(f"Error loading or processing original symbols metadata: {e}.")
        return False

    # --- Determine Overall Date Range for New Metadata ---
    if df_data.is_empty(): 
        print("Error: Dataframe is empty before determining date range.")
        return False
    series_start_date_for_aggregates = df_data.select(pl.min("date").dt.strftime("%Y-%m-%d")).item()
    series_end_date_for_aggregates = df_data.select(pl.max("date").dt.strftime("%Y-%m-%d")).item()
    print(f"Overall date range for new aggregate metadata: {series_start_date_for_aggregates} to {series_end_date_for_aggregates}")

    # --- Create Aggregate Series and Update Metadata ---
    new_metadata_rows = []

    for new_col_name, agg_details in aggregations_definition.items():
        print(f"Attempting to create aggregate series: {new_col_name}")
        
        missing_components = [comp for comp in agg_details["components"] if comp not in df_data.columns]
        if missing_components:
            print(f"Error: Missing component columns for '{new_col_name}': {missing_components}.")
            print(f"Skipping aggregate series '{new_col_name}' due to missing components.")
            continue 
            
        try:
            series_expr = agg_details["expr_lambda"](df_data) 
            df_data = df_data.with_columns(series_expr.alias(new_col_name))
            print(f"Successfully created: {new_col_name}")
            
            new_row = {
                "symbol": new_col_name,
                "source": "Calc",
                "description": agg_details["description"],
                "label_y": agg_details["label_y"],
                "series_start": series_start_date_for_aggregates,
                "series_end": series_end_date_for_aggregates
            }
            if "expense_ratio" in df_symbols_meta.columns:
                new_row["expense_ratio"] = -1.00 
            new_metadata_rows.append(new_row)
            
        except Exception as e:
            print(f"Error calculating or adding column '{new_col_name}': {e}.")
            print(f"Skipping aggregate series '{new_col_name}' due to calculation error.")
            continue 


    if new_metadata_rows:
        schema_for_new_meta = {name: dtype for name, dtype in df_symbols_meta.schema.items()}
        df_new_meta = pl.DataFrame(new_metadata_rows, schema_overrides=schema_for_new_meta)
        
        for col_name in df_symbols_meta.columns:
            if col_name not in df_new_meta.columns and col_name in new_metadata_rows[0]:
                 pass 
            elif col_name not in df_new_meta.columns:
                 if df_symbols_meta[col_name].dtype == pl.Float64:
                    df_new_meta = df_new_meta.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
                 else:
                    df_new_meta = df_new_meta.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col_name))
        
        df_symbols_meta = pl.concat([df_symbols_meta, df_new_meta], how="diagonal")
        print(f"Appended {len(new_metadata_rows)} new rows to metadata. New metadata shape: {df_symbols_meta.shape}")

    # --- Save Outputs ---
    print(f"Saving final data (with aggregates) to: {final_data_output_path}")
    try:
        df_data.write_csv(final_data_output_path)
        print(f"Final data saved. Shape: {df_data.shape}")
    except Exception as e:
        print(f"Error saving final data: {e}")
        return False 

    print(f"Saving updated symbols metadata to: {aggregated_metadata_output_path}")
    try:
        df_symbols_meta.write_csv(aggregated_metadata_output_path)
        print(f"Updated metadata saved. Shape: {df_symbols_meta.shape}")
    except Exception as e:
        print(f"Error saving updated metadata: {e}")
        return False 

    print("Aggregate series creation process finished successfully.")
    return True


if __name__ == "__main__":
    print("Running aggregate series creation script...")
    
    success = generate_aggregate_series(
        interpolated_data_path=INTERPOLATED_DATA_PATH,
        original_symbols_path=ORIGINAL_SYMBOLS_PATH,
        aggregations_definition=aggregations_to_create,
        final_data_output_path=FINAL_DATA_OUTPUT_PATH,
        aggregated_metadata_output_path=AGGREGATED_METADATA_OUTPUT_PATH
    )

    if success:
        print("Script completed successfully.")
    else:
        print("Script encountered errors. Please check the logs.")
        sys.exit(1)
