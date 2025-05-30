# Bristol Gate - DuckDB Data Pipeline

A comprehensive financial and economic data pipeline using **DuckDB** and **Parquet** for optimal performance and advanced feature engineering.

## 🚀 **Getting Started - Quick Setup**

### **1. Clone the Repository**
```bash
git clone https://github.com/ariasmiguel/bristol_gate.git
cd bristol_gate
```

### **2. Set Up Python Environment & Install Package**
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the Bristol Gate Pipeline package
pip install -e .

# This installs bristol-gate-pipeline package with all dependencies
# The package contains src_pipeline modules for data collection, 
# aggregation, and feature engineering
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

### **4. Run the Complete Pipeline**

**Option A: Using Bash Scripts (Recommended)**
```bash
# First-time setup (includes database initialization and full data collection)
./scripts/initial_setup.sh

# Daily updates (incremental data collection and feature regeneration)
./scripts/incremental_update.sh
```

**Option B: Step-by-Step Manual Execution**
```bash
# Step 1: Initialize database
python scripts/setup_duckdb.py --load-symbols

# Step 2: Collect data (starts with small sample for testing)
python scripts/run_data_collection.py --sources yahoo,fred

# Step 3: Create aggregated dataset
python scripts/run_aggregate_series.py

# Step 4: Generate ML-ready features
python scripts/run_features_pipeline.py
```

**Option C: Docker (Easy Deployment)**
```bash
# Clone the repository and cd into it
git clone https://github.com/ariasmiguel/bristol_gate.git
cd bristol_gate

# Copy environment template and add your API keys
cp env.example .env
# nano .env  <-- Edit here

# Run initial setup (one-time)
docker-compose up bristol-gate-setup

# Run the application (includes incremental update and keeps container alive)
docker-compose up bristol-gate

# To run scheduled updates, bring up the scheduler as well (or all services)
# docker-compose up bristol-scheduler
# docker-compose up # Starts all services defined
```

### **5. Access Your Data**
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
python scripts/setup_duckdb.py --load-symbols
```

**❌ "API key not found" or "401 Unauthorized":**
```bash
# Check your .env file has the correct format (no quotes needed):
FRED_API_KEY=abcd1234your_actual_key_here
EIA_TOKEN=your_actual_eia_token_here

# Make sure .env is in the root directory (same level as pyproject.toml)
ls -la .env
```

**❌ "No module named 'src_pipeline'":**
```bash
# Make sure you installed the package properly
pip install -e .

# Verify the package is installed
pip show bristol-gate-pipeline

# Test the import
python -c "import src_pipeline; print('✅ Package installed correctly')"
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
python scripts/run_data_collection.py --sources yahoo,baker
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

## 🚀 Package Installation

Bristol Gate Pipeline is organized as a proper Python package for easy installation and reuse.

### **Package Structure**

The `bristol-gate-pipeline` package contains:
- **Core pipeline modules**: Data collection, aggregation, feature engineering
- **Data fetchers**: Yahoo Finance, FRED, EIA, Baker Hughes, FINRA, S&P 500, USDA
- **Utilities**: Web scraping, file processing, transformations
- **All dependencies**: Automatically installs pandas, duckdb, yfinance, etc.

### **Installation Options**

```bash
# Development installation (recommended - allows package modifications)
pip install -e .

# Or regular installation
pip install .

# Install with development tools
pip install -e ".[dev]"
```

### **Using the Package**

```python
# Import the pipeline modules
from src_pipeline.pipelines.data_collection import DataCollectionPipeline
from src_pipeline.core.config_manager import ConfigurationManager
from src_pipeline.fetchers.fetch_yahoo import YahooFinanceFetcher

# Use anywhere in your Python code
config = ConfigurationManager.create_default()
pipeline = DataCollectionPipeline(config)
```

**Scripts remain separate** and use the installed package:
- Located in `scripts/` directory
- Import from `src_pipeline` package
- Can be run from any directory after package installation

---

## 📋 Four-Step Pipeline

### Step 1: Initialize the Database

Create and set up your DuckDB database with all required tables.

```bash
# Initialize database and load symbol definitions
python scripts/setup_duckdb.py --load-symbols
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
python scripts/run_data_collection.py

# Full refresh (when needed)
python scripts/run_data_collection.py --full-refresh

# 🆕 Source filtering for testing/development
python scripts/run_data_collection.py --sources yahoo,fred,baker
python scripts/run_data_collection.py --sources yahoo --incremental
python scripts/run_data_collection.py --sources fred,eia --full-refresh
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
python scripts/run_aggregate_series.py

# Custom date range
python scripts/run_aggregate_series.py --start-date 2020-01-01

# Test mode (skip saving)
python scripts/run_aggregate_series.py --skip-save
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
python scripts/run_features_pipeline.py

# Run from DuckDB with parallel processing (production)
python scripts/run_features_pipeline.py --full --verbose

# Sequential processing for debugging
python scripts/run_features_pipeline.py --sequential

# Skip domain features for faster processing
python scripts/run_features_pipeline.py --no-domain-features
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
- **🗓️ Time Range**: 1950-01-01 to 2025-05-29 (Over 75 years of daily data!)
- **📈 Base Series**: ~250 initial economic and financial indicators from 8 sources.
- **🔧 Engineered Features**: Over 6,700 ML-ready features/symbols in the `featured_data` table.
- **📏 Dataset Size**: ~700-800 MB for the primary timestamped `featured_data_YYYYMMDD_HHMMSS.parquet` file in the silver layer.
- **⚡ Database Records**: Over 181 million rows in the `featured_data` DuckDB table (long format).

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

## 🐳 Docker Support

Bristol Gate now supports Docker for easy deployment and consistent environments:

```bash
# Quick start with Docker Compose
# (Ensure you've cloned, cd'd into the repo, and configured .env)

# Initial setup (downloads all data, sets up database - run once):
docker-compose up bristol-gate-setup

# Main application (runs incremental update, then keeps container alive for exec/logs):
docker-compose up bristol-gate

# For scheduled daily updates (runs cron daemon):
# docker-compose up bristol-scheduler

# To run all services including scheduler and monitor:
# docker-compose up
```

**Key Docker Features:**
- 🚀 **Automated Pipeline**: `bristol-gate-setup` for initial full run, `bristol-gate` for ongoing use.
- 📊 **Scheduled Updates**: `bristol-scheduler` for daily data refreshes via cron.
- 🔒 **Production Ready**: Multi-stage builds, non-root user, health checks.
- 💾 **Persistent Data**: Volumes for DuckDB, Parquet files, and logs.
- 🌐 **Multi-platform**: Supports AMD64/ARM64 builds.

See [DOCKER.md](DOCKER.md) for complete documentation on Docker services, commands, monitoring, and more.

---

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
python scripts/setup_duckdb.py --load-symbols
```

### **📊 Daily/Regular Data Updates**

```bash
# Quick update with all sources (recommended for production)
python scripts/run_data_collection.py

# Or target specific sources for faster testing
python scripts/run_data_collection.py --sources yahoo,fred

# Create enhanced dataset for analysis
python scripts/run_aggregate_series.py

# Generate ML-ready features (500+ features)
python scripts/run_features_pipeline.py
```

### **🎯 Your Results**

After running the complete workflow:
- **Raw data**: Stored in `bristol_gate.duckdb` (staging tables like `stg_yahoo`, `stg_fred`, etc.) and `data/bronze/` (source-specific Parquet files).
- **Aggregated Base Data**: `data/silver/final_aggregated_data_YYYYMMDD_HHMMSS.parquet` (interpolated base data before extensive feature engineering).
- **ML-ready features**: `data/silver/featured_data_YYYYMMDD_HHMMSS.parquet` and in the `featured_data` table in `bristol_gate.duckdb`. ← **This is what you want!**

```python
import pandas as pd
import duckdb

# Option 1: Load from the latest Parquet file (wide format, features as columns)
# Note: You might need to find the exact timestamped filename, e.g., using: ls -t data/silver/featured_data_*.parquet | head -n 1
df_parquet = pd.read_parquet('data/silver/featured_data_20250530_103517.parquet') 
print(f"🎉 Parquet Ready for ML: {df_parquet.shape[0]:,} rows × {df_parquet.shape[1]:,} features (columns)")

# Option 2: Query from DuckDB (long format)
conn = duckdb.connect('bristol_gate.duckdb')
featured_data_count = conn.execute('SELECT COUNT(*) FROM featured_data').fetchone()[0]
featured_symbols_count = conn.execute('SELECT COUNT(DISTINCT symbol) FROM featured_data').fetchone()[0]
print(f"🎉 DuckDB Ready for ML: {featured_data_count:,} total rows, {featured_symbols_count:,} unique features/symbols in long format")
conn.close()
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
python scripts/run_data_collection.py --incremental    # Default: only new data
python scripts/run_data_collection.py --full-refresh   # Reload all historical data

# 🆕 Source filtering for targeted collection
python scripts/run_data_collection.py --sources yahoo,fred              # Only Yahoo & FRED
python scripts/run_data_collection.py --sources baker,finra,sp500       # Only web scraped sources  
python scripts/run_data_collection.py --sources eia,usda --full-refresh # Energy & agriculture refresh

# Available sources: yahoo, fred, eia, baker, finra, sp500, usda
```

### Aggregate Series Options

```bash
python scripts/run_aggregate_series.py --start-date 2010-01-01  # Custom date range
python scripts/run_aggregate_series.py --method staged          # Use pandas pivot (debugging)
python scripts/run_aggregate_series.py --skip-save              # Test mode, don't save
```

## 🗂️ Organized Module Structure

Bristol Gate features a clean, organized codebase with logical module separation:

```
src_pipeline/
├── pipelines/          # Main orchestrator modules
│   ├── data_collection.py      # Data collection pipeline
│   ├── aggregate_series.py     # Data aggregation
│   └── unified_pipeline.py     # Feature engineering pipeline
├── core/               # Core infrastructure
│   ├── base_fetcher.py         # Common fetcher base class
│   ├── utils.py                # Core utilities & database ops
│   ├── duckdb_functions.py     # DuckDB operations
│   ├── date_utils.py           # Date/time utilities
│   └── symbol_processor.py     # Symbol standardization
├── fetchers/           # Data source fetchers  
│   ├── fetch_yahoo.py          # Yahoo Finance (API)
│   ├── fetch_fred.py           # Federal Reserve (API)
│   ├── fetch_eia.py            # Energy Information Admin (API)
│   └── fetch_*.py              # Web scraping fetchers
├── utils/              # Specialized utilities
│   ├── web_scraping_utils.py   # Selenium operations
│   ├── excel_processing_utils.py # Excel file handling
│   └── transform_utils.py      # Data transformations
└── features/           # Feature engineering
    ├── feature_utils.py        # Feature calculation functions
    └── interpolate_data.py     # Data interpolation
```

**Benefits:**
- 🎯 **Clear separation of concerns** - Each module has a specific purpose
- 📦 **Easy imports** - `from src_pipeline.pipelines import DataCollectionPipeline`  
- 🔧 **Maintainable** - Related functionality grouped together
- 🧪 **Testable** - Isolated modules for targeted testing
- 📚 **Self-documenting** - Structure shows system architecture

---