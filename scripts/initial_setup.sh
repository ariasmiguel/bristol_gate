#!/bin/bash
# Bristol Gate - Initial Setup and Full Data Load
# This script performs the complete initial setup including database initialization
# and full historical data collection from all sources.

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[$(date '+%Y-%m-%d %H:%M:%S')] ✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}[$(date '+%Y-%m-%d %H:%M:%S')] ⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}[$(date '+%Y-%m-%d %H:%M:%S')] ❌ $1${NC}"
}

# Banner
echo "🚀 Bristol Gate - Initial Setup & Full Data Load"
echo "=================================================="
echo ""

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    print_error "Not in Bristol Gate root directory. Please run from the root directory."
    exit 1
fi

# Check if package is installed
print_status "Checking package installation..."
if ! pip show bristol-gate-pipeline > /dev/null 2>&1; then
    print_warning "Package not installed. Installing bristol-gate-pipeline..."
    pip install -e .
    print_success "Package installed successfully"
else
    print_success "Package already installed"
fi

# Check for required environment variables
print_status "Checking environment configuration..."
if [ ! -f ".env" ]; then
    print_warning "No .env file found. Copying from .env.example..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        print_warning "⚠️  Please edit .env file with your API keys before continuing!"
        print_warning "Required: FRED_API_KEY, EIA_TOKEN"
        read -p "Press Enter when you've configured your API keys..."
    else
        print_error ".env.example not found. Please create .env with your API keys."
        exit 1
    fi
fi

# Create necessary directories
print_status "Creating data directories..."
mkdir -p data/bronze data/silver data/gold logs downloads
print_success "Data directories created"

# Step 1: Initialize Database
print_status "Step 1: Initializing DuckDB database with symbols..."
if python scripts/setup_duckdb.py --load-symbols; then
    print_success "Database initialized and symbols loaded"
else
    print_error "Database initialization failed"
    exit 1
fi

# Step 2: Full Data Collection
print_status "Step 2: Starting full data collection from all sources..."
print_status "This may take 10-30 minutes depending on API rate limits..."

if python scripts/run_data_collection.py --full-refresh; then
    print_success "Full data collection completed"
else
    print_error "Data collection failed"
    exit 1
fi

# Step 3: Create Aggregated Dataset
print_status "Step 3: Creating aggregated and interpolated dataset..."
if python scripts/run_aggregate_series.py; then
    print_success "Aggregated dataset created"
else
    print_error "Aggregate series creation failed"
    exit 1
fi

# Step 4: Generate Features
print_status "Step 4: Generating ML-ready features..."
if python scripts/run_features_pipeline.py --full; then
    print_success "Feature engineering completed"
else
    print_error "Feature engineering failed"
    exit 1
fi

# Summary
echo ""
echo "🎉 INITIAL SETUP COMPLETE!"
echo "=========================="
print_success "Database: bristol_gate.duckdb created and populated"
print_success "Bronze layer: Raw data stored in data/bronze/*.parquet"
print_success "Silver layer: ML-ready features in data/silver/featured_data*.parquet"

echo ""
echo "📊 Quick Stats:"
# Get latest featured data file
LATEST_FILE=$(ls -t data/silver/featured_data_*.parquet 2>/dev/null | head -1)
if [ -n "$LATEST_FILE" ]; then
    SIZE=$(ls -lh "$LATEST_FILE" | awk '{print $5}')
    echo "   • Latest dataset: $(basename "$LATEST_FILE") ($SIZE)"
    
    # Get dataset info using Python
    python -c "
import pandas as pd
try:
    df = pd.read_parquet('$LATEST_FILE')
    print(f'   • Dataset shape: {df.shape[0]:,} rows × {df.shape[1]:,} features')
    print(f'   • Date range: {df.index.min().date()} to {df.index.max().date()}')
    print(f'   • Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB')
except Exception as e:
    print(f'   • Could not read dataset: {e}')
    "
fi

echo ""
echo "💡 Next Steps:"
echo "   • Use scripts/incremental_update.sh for daily updates"
echo "   • Load data: pd.read_parquet('data/silver/featured_data.parquet')"
echo "   • Query with DuckDB: SELECT * FROM 'data/silver/featured_data*.parquet'"
echo ""
print_success "Bristol Gate is ready for machine learning! 🎯" 