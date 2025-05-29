# Bristol Gate - DuckDB Data Pipeline

A comprehensive financial and economic data pipeline using **DuckDB** and **Parquet** for optimal performance and advanced feature engineering.

## 🚀 **Getting Started - Quick Setup**

### **1. Clone the Repository**
```bash
git clone https://github.com/ariasmiguel/bristol_gate.git
cd bristol_gate
```

### **2. Set Up Python Environment**
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### **3. Configure API Keys**
```bash
# Copy the environment template
cp env.example .env

# Edit .env file and add your API keys:
# FRED_API_KEY=your_actual_fred_api_key
# EIA_TOKEN=your_actual_eia_api_key
```

**Get your API keys:**
- **FRED API Key**: [Register here](https://fred.stlouisfed.org/docs/api/api_key.html) (Free)
- **EIA API Key**: [Register here](https://www.eia.gov/opendata/register.php) (Free)

### **4. Initialize Database**
```bash
python setup_duckdb.py --load-symbols
```

### **5. Run the Pipeline**
```bash
# Step 1: Collect data (starts with small sample for testing)
python run_data_collection.py --sources yahoo,fred

# Step 2: Create aggregated dataset
python run_aggregate_series.py

# Step 3: Generate ML-ready features
python run_features_pipeline.py
```

### **6. Access Your Data**
```python
import pandas as pd

# Load the final ML-ready dataset
df = pd.read_parquet('data/silver/featured_data.parquet')
print(f"Dataset shape: {df.shape}")
print(f"Date range: {df.index.min()} to {df.index.max()}")
print(f"Available features: {list(df.columns)[:10]}...")  # First 10 features
```

**🎉 That's it!** You now have a dataset with 500+ engineered features ready for machine learning!

---

## 🛠️ **Troubleshooting Getting Started**

### **Common Issues & Solutions**

**❌ "Database not found" error:**
```bash
# Make sure you ran the database setup step
python setup_duckdb.py --load-symbols
```

**❌ "API key not found" or "401 Unauthorized":**
```bash
# Check your .env file has the correct format (no quotes needed):
FRED_API_KEY=abcd1234your_actual_key_here
EIA_TOKEN=your_actual_eia_token_here

# Make sure .env is in the root directory (same level as run_data_collection.py)
ls -la .env
```

**❌ "No module named 'src_pipeline'":**
```bash
# Make sure you're in the bristol_gate directory and venv is activated
cd bristol_gate
source venv/bin/activate  # or venv\Scripts\activate on Windows
python run_data_collection.py --sources yahoo
```

**❌ "ChromeDriver not found" (for web scraping sources):**
```bash
# Install ChromeDriver (macOS with Homebrew)
brew install chromedriver

# Or download manually and add to PATH
# https://chromedriver.chromium.org/downloads
```

**❌ Want to test without API keys first?**
```bash
# Test with sources that don't require API keys
python run_data_collection.py --sources yahoo,baker
```

**❌ Pipeline runs but no data collected:**
```bash
# Check if symbols were loaded properly
python -c "import duckdb; con = duckdb.connect('bristol_gate.duckdb'); print('Symbols:', con.execute('SELECT COUNT(*) FROM symbols').fetchone()[0])"

# Should show: Symbols: 6548 (or similar)
```

### **🆕 Getting Help**

- **Check the logs**: Most errors are clearly explained in the console output
- **Start small**: Use `--sources yahoo` for your first test run
- **Use verbose mode**: Add `--verbose` to see detailed progress
- **Issues on GitHub**: [Report bugs or ask questions](https://github.com/ariasmiguel/bristol_gate/issues)

## 📄 **License**

**Bristol Gate is licensed under Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.**

✅ **You CAN:**
- Use for personal projects and research
- Modify and adapt the code
- Share with others (with attribution)
- Use for academic research and education

❌ **You CANNOT:**
- Use for commercial purposes or profit
- Sell the software or derivatives
- Use in commercial products/services
- Use for commercial consulting or research

For commercial licensing inquiries, please contact the author.

---

## 🚀 Quick Setup

### Prerequisites

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables (add your API keys)
cp .env.example .env
# Edit .env with your FRED_API_KEY and EIA_TOKEN
```

## 📋 Four-Step Pipeline

### Step 1: Initialize the Database

Create and set up your DuckDB database with all required tables.

```bash
# Initialize database and load symbol definitions
python setup_duckdb.py --load-symbols
```

**What this does:**

- Creates `bristol_gate.duckdb` database file
- Sets up staging tables for all data sources  
- Loads symbol reference data from `data/symbols.csv`
- Creates bronze layer directory structure

### Step 2: Collect Data

Fetch data from all sources with intelligent incremental loading.

```bash
# Incremental collection (recommended for daily use)
python run_data_collection.py

# Full refresh (when needed)
python run_data_collection.py --full-refresh

# 🆕 Source filtering for testing/development
python run_data_collection.py --sources yahoo,fred,baker
python run_data_collection.py --sources yahoo --incremental
python run_data_collection.py --sources fred,eia --full-refresh
```

**What this does:**

- Fetches data from Yahoo Finance, FRED, EIA, Baker Hughes, FINRA, S&P 500, USDA
- **NEW**: `--sources` parameter allows selective data collection for faster testing
- Only downloads new data since last run (incremental mode)
- Stores data in DuckDB tables and Parquet bronze layer
- Respects API rate limits automatically
- Built on refactored `BaseDataFetcher` architecture for consistent error handling

### Step 3: Create Enhanced Dataset

Transform raw data into analysis-ready dataset with interpolation and aggregate features.

```bash
# Create interpolated and aggregated dataset
python run_aggregate_series.py

# Custom date range
python run_aggregate_series.py --start-date 2020-01-01

# Test mode (skip saving)
python run_aggregate_series.py --skip-save
```

**What this does:**

- Extracts data from DuckDB staging tables using optimized SQL
- Transforms to wide format with daily interpolation
- Creates 50+ aggregate/computed series (ratios, differences, normalized values)
- Updates DuckDB symbols table with new calculated symbols
- Saves enhanced dataset to `data/silver/final_aggregated_data.parquet`

### Step 4: Generate Features

Transform aggregated data into ML-ready features with comprehensive feature engineering.

```bash
# Generate all features (basic + domain-specific)
python run_features_pipeline.py

# Run from DuckDB with parallel processing (production)
python run_features_pipeline.py --full --verbose

# Sequential processing for debugging
python run_features_pipeline.py --sequential

# Skip domain features for faster processing
python run_features_pipeline.py --no-domain-features
```

**What this does:**

- **Basic Features**: YoY changes, log transforms, moving averages, Savitzky-Golay smoothing
- **Domain Features**: Economic yield curves, technical indicators, ratio analysis, return calculations
- Creates 500+ engineered features from ~100 base series
- Automatic timestamped outputs with audit trail
- Updates DuckDB symbols table with all feature metadata
- Saves final dataset to `data/silver/featured_data.parquet`

## 📊 Data Sources

| Source | Data Type | API Key Required |
|--------|-----------|------------------|
| Yahoo Finance | Stock prices (OHLCV) | No |
| FRED | Economic indicators | Yes |
| EIA | Energy data | Yes |
| Baker Hughes | Rig count data | No |
| FINRA | Margin statistics | No |
| S&P 500 | Earnings & estimates | No |
| USDA | Agricultural data | No |

## 🎁 **What You Get**

Running this pipeline gives you a **comprehensive financial dataset** ready for machine learning:

### **📊 Final Dataset Highlights**
- **🗓️ Time Range**: 25+ years of daily data (1999-present)
- **📈 Base Series**: 100+ economic and financial indicators
- **🔧 Engineered Features**: 500+ ML-ready features
- **📏 Dataset Size**: ~50-100 MB Parquet file
- **⚡ Load Time**: 2-3 seconds vs hours for CSV

### **🧮 Feature Categories**
- **📊 Basic Features**: YoY changes, log transforms, moving averages
- **📈 Technical Indicators**: S&P 500 signals, trend analysis
- **💰 Economic Ratios**: Market cap to GDP, yield curves
- **🔄 Lagged Features**: 1, 4, and 5-year historical comparisons

### **💡 Real-World Applications**
- **📈 Market Prediction**: Stock price forecasting models
- **📊 Economic Analysis**: Recession prediction, trend analysis  
- **🏦 Risk Management**: Portfolio optimization, correlation analysis
- **🔬 Research**: Academic studies, quantitative analysis

### **Example Features You'll Get**
```python
['GDP_YoY', 'DGS10_Log', 'UNRATE_mva200', '^GSPC_close_YoY', 
 'GDP_YoY_to_DGS1', 'CPI_Smooth', 'PAYEMS_normalized', ...]
```

## 🎁 Complete Workflow

### **🚀 First Time Setup** 
*(See [Getting Started](#-getting-started---quick-setup) above for detailed instructions)*

```bash
# Clone and setup
git clone https://github.com/ariasmiguel/bristol_gate.git
cd bristol_gate
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp env.example .env  # Edit with your API keys

# Initialize database
python setup_duckdb.py --load-symbols
```

### **📊 Daily/Regular Data Updates**

```bash
# Quick update with all sources (recommended for production)
python run_data_collection.py

# Or target specific sources for faster testing
python run_data_collection.py --sources yahoo,fred

# Create enhanced dataset for analysis
python run_aggregate_series.py

# Generate ML-ready features (500+ features)
python run_features_pipeline.py
```

### **🎯 Your Results**

After running the complete workflow:
- **Raw data**: Stored in `bristol_gate.duckdb` and `data/bronze/` (Parquet files)
- **Enhanced data**: `data/silver/final_aggregated_data_YYYYMMDD_HHMMSS.parquet`
- **ML-ready features**: `data/silver/featured_data_YYYYMMDD_HHMMSS.parquet` ← **This is what you want!**

```python
import pandas as pd
df = pd.read_parquet('data/silver/featured_data.parquet')  # Latest file
print(f"🎉 Ready for ML: {df.shape[0]:,} rows × {df.shape[1]:,} features")
```

## 📁 Output Structure

- **`bristol_gate.duckdb`** - Main analytical database with all metadata
- **`data/bronze/`** - Raw Parquet files by source (audit trail)  
- **`data/silver/`** - Analysis-ready datasets
  - `final_aggregated_data_YYYYMMDD_HHMMSS.parquet` - Interpolated base data
  - `featured_data_YYYYMMDD_HHMMSS.parquet` - Full feature set for ML

## 🧮 Feature Engineering

The pipeline creates comprehensive features across multiple categories:

### Basic Features (for each series)
- **Year-over-Year**: 1, 4, and 5-year changes
- **Log Transforms**: Natural logarithm with interpolation
- **Moving Averages**: 50, 200, and 365-day periods
- **Smoothing**: Savitzky-Golay filters with derivatives

### Domain-Specific Features
- **Economic Indicators**: GDP yield curves, productivity measures
- **Technical Analysis**: S&P 500 normalizations, moving average signals  
- **Market Ratios**: Stock indices to GDP ratios
- **Return Analysis**: Equity and T-bill return calculations

**Example**: From `GDP` → `GDP_YoY`, `GDP_Log`, `GDP_mva365`, `GDP_Smooth`, `GDP_YoY_to_DGS1`, etc.

## 🚀 Advanced Options

### Data Collection Options

```bash
python run_data_collection.py --incremental    # Default: only new data
python run_data_collection.py --full-refresh   # Reload all historical data

# 🆕 Source filtering for targeted collection
python run_data_collection.py --sources yahoo,fred              # Only Yahoo & FRED
python run_data_collection.py --sources baker,finra,sp500       # Only web scraped sources  
python run_data_collection.py --sources eia,usda --full-refresh # Energy & agriculture refresh

# Available sources: yahoo, fred, eia, baker, finra, sp500, usda
```

### Aggregate Series Options

```bash
python run_aggregate_series.py --start-date 2010-01-01  # Custom date range
python run_aggregate_series.py --method staged          # Use pandas pivot (debugging)
python run_aggregate_series.py --skip-save              # Test mode, don't save
```

### Feature Pipeline Options

```bash
python run_features_pipeline.py --full                  # Run complete pipeline from DuckDB
python run_features_pipeline.py --silver                # Load from silver layer (faster)
python run_features_pipeline.py --sequential            # Sequential processing (debugging)
python run_features_pipeline.py --workers 8             # Tune parallel workers
python run_features_pipeline.py --no-domain-features    # Skip domain features
python run_features_pipeline.py --verbose               # Detailed timing information
python run_features_pipeline.py --no-timestamp          # Exact output path
```

### Database Setup Options

```bash
python setup_duckdb.py                          # Database only
python setup_duckdb.py --load-symbols           # Database + symbols
python setup_duckdb.py --symbols-file custom.csv  # Custom symbols file
```

## 🎨 Medallion Architecture

This pipeline follows the **medallion architecture**:

- **🥉 Bronze Layer** (`data/bronze/`): Raw data from sources, stored as Parquet
- **🥈 Silver Layer** (`data/silver/`): Cleaned, interpolated, and enhanced data
  - Base aggregated data (100+ series)
  - Featured data (500+ engineered features)
- **🥇 Gold Layer**: Ready for your specific analytics and ML models

## 🏗️ **Refactored Architecture** 

**The pipeline has been completely refactored with a modular, maintainable design:**

### **Core Utility Classes**

- **`BaseDataFetcher`**: Common base class for all data sources
  - Standardized logging, error handling, and data validation
  - Consistent API rate limiting and retry logic
  - Unified data format standardization

- **`SymbolProcessor`**: Centralized symbol management
  - Eliminates code duplication across 7 fetch modules
  - Standardized symbol preparation and validation
  - Consistent column naming and data structure

- **`DateUtils`**: Comprehensive date/time utilities
  - Standardized timestamp generation for file naming
  - Consistent datetime formatting across all modules
  - Quarter-end date calculations and business day logic

### **Specialized Utility Classes**

- **`WebScrapingUtils`**: Selenium-based web scraping
  - Reusable browser management and page interaction
  - Standardized wait conditions and error handling
  - Used by Baker Hughes, FINRA, and S&P 500 fetchers

- **`FileDownloadUtils`**: File download management
  - Intelligent download directory handling
  - Wait for completion logic with timeout protection
  - Cleanup and file validation

- **`ExcelProcessingUtils`**: Robust Excel file handling
  - Multiple engine fallbacks (openpyxl → xlrd → calamine)
  - Automatic date column detection and conversion
  - Support for both .xlsx and .xlsb formats

- **`DataTransformUtils`**: Data transformation operations
  - Standardized melt operations with configurable parameters
  - Consistent data reshaping patterns
  - Reusable transformation logic

### **Refactoring Benefits Achieved**

✅ **Eliminated 200-300 lines of duplicated code**  
✅ **Removed exact function duplicates** (validate_dataframe)  
✅ **Standardized error handling** across all modules  
✅ **Consistent logging format** and data transformation patterns  
✅ **Better testability** through shared utility classes  
✅ **Easier maintenance** - changes in one place affect all users  
✅ **Faster development** - new data sources use existing utilities  
✅ **Improved reliability** - centralized, battle-tested functionality  

### **Integration Success**

**All major pipeline modules now use the refactored utilities:**
- `data_collection.py` - Uses SymbolProcessor for standardized symbol handling
- `aggregate_series.py` - Uses DateUtils for consistent timestamp generation  
- All fetch modules (`fetch_*.py`) - Inherit from BaseDataFetcher
- Main scripts - Use DateUtils for consistent datetime formatting

**Testing Results**: 100% success rate across all data sources with zero errors 🎉

## 💡 Key Benefits

✅ **No intermediate CSV files** - Everything stored efficiently in Parquet

✅ **Incremental loading** - Only fetch new data, respecting API limits

✅ **Column-oriented storage** - 3-5x smaller files, 10-50x faster loading

✅ **Comprehensive feature engineering** - 500+ ML-ready features automatically generated

✅ **Parallel processing** - Multi-threaded feature calculation for speed

✅ **Audit trail** - Timestamped files with complete metadata tracking

✅ **Local processing** - No cloud dependencies, runs entirely on your machine

✅ **Automatic interpolation** - Daily frequency with smart gap filling

✅ **Domain expertise built-in** - Economic and financial features from research

### **🆕 Refactoring Benefits**

✅ **Modular architecture** - Reusable utility classes eliminate code duplication

✅ **Source filtering** - Test and develop with specific data sources using `--sources`

✅ **Zero code duplication** - Eliminated 200-300 lines of repeated code

✅ **Standardized error handling** - Consistent logging and failure recovery

✅ **Enhanced maintainability** - Centralized utilities make updates easier

✅ **Better testing** - Modular design enables targeted testing and debugging

✅ **Robust web scraping** - Selenium-based utilities with intelligent wait conditions

✅ **Multi-format Excel support** - Fallback engines handle .xlsx, .xlsb files reliably

✅ **Consistent timestamps** - Standardized file naming and audit trails

## 📈 Performance

**Typical pipeline performance on modern hardware:**

- **Data Collection**: 2-5 minutes (incremental)
- **Aggregation**: 30-60 seconds for 25+ years of data
- **Feature Generation**: 1-3 minutes for 500+ features (parallel)
- **Final Dataset**: ~50-100 MB Parquet file

## 🔄 Automation

For production use, create a scheduled workflow:

```bash
#!/bin/bash
# daily_update.sh

# Update data
python run_data_collection.py

# Create features if new data was collected
if [ $? -eq 0 ]; then
    python run_aggregate_series.py
    python run_features_pipeline.py --verbose
    echo "Pipeline completed: $(date)"
fi
```

---

**Ready to start?** Run these four commands:

```bash
python setup_duckdb.py --load-symbols
python run_data_collection.py  
python run_aggregate_series.py
python run_features_pipeline.py
```

🎉 **Done!** Your ML-ready dataset with 500+ features is in `data/silver/featured_data.parquet`

**Load your data:**
```python
import pandas as pd
df = pd.read_parquet('data/silver/featured_data.parquet')
print(f"Dataset shape: {df.shape}")
print(f"Date range: {df.index.min()} to {df.index.max()}")
```