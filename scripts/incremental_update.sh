#!/bin/bash
# Bristol Gate - Incremental Data Update
# This script performs daily incremental updates to collect new data,
# refresh aggregated datasets, and regenerate features.

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

# Parse command line arguments
SOURCES_FILTER=""
SKIP_FEATURES=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --sources)
            SOURCES_FILTER="$2"
            shift 2
            ;;
        --skip-features)
            SKIP_FEATURES=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            echo "Bristol Gate - Incremental Update Script"
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --sources SOURCES    Comma-separated list of sources (e.g., yahoo,fred,baker)"
            echo "  --skip-features      Skip feature generation step"
            echo "  --verbose           Enable verbose logging"
            echo "  --help              Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                              # Update all sources"
            echo "  $0 --sources yahoo,fred         # Update only Yahoo and FRED"
            echo "  $0 --skip-features              # Update data but skip feature generation"
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Banner
echo "🔄 Bristol Gate - Incremental Data Update"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    print_error "Not in Bristol Gate root directory. Please run from the root directory."
    exit 1
fi

# Check if database exists
if [ ! -f "bristol_gate.duckdb" ]; then
    print_error "Database not found. Please run scripts/initial_setup.sh first."
    exit 1
fi

# Check if package is installed
if ! pip show bristol-gate-pipeline > /dev/null 2>&1; then
    print_error "bristol-gate-pipeline not installed. Please run scripts/initial_setup.sh first."
    exit 1
fi

# Check environment file
if [ ! -f ".env" ]; then
    print_warning "No .env file found. Using environment variables if available."
fi

# Build data collection command
COLLECTION_CMD="python scripts/run_data_collection.py --incremental"
if [ -n "$SOURCES_FILTER" ]; then
    COLLECTION_CMD="$COLLECTION_CMD --sources $SOURCES_FILTER"
    print_status "Filtering sources: $SOURCES_FILTER"
fi

# Step 1: Incremental Data Collection
print_status "Step 1: Collecting new data (incremental mode)..."
START_TIME=$(date +%s)

if [ "$VERBOSE" = true ]; then
    echo "Command: $COLLECTION_CMD"
fi

if $COLLECTION_CMD; then
    COLLECTION_TIME=$(($(date +%s) - START_TIME))
    print_success "Data collection completed in ${COLLECTION_TIME}s"
else
    print_error "Data collection failed"
    exit 1
fi

# Step 2: Update Aggregated Dataset
print_status "Step 2: Updating aggregated dataset..."
AGGREGATE_START=$(date +%s)

if python scripts/run_aggregate_series.py; then
    AGGREGATE_TIME=$(($(date +%s) - AGGREGATE_START))
    print_success "Aggregated dataset updated in ${AGGREGATE_TIME}s"
else
    print_error "Aggregate series update failed"
    exit 1
fi

# Step 3: Regenerate Features (optional)
if [ "$SKIP_FEATURES" = false ]; then
    print_status "Step 3: Regenerating ML features..."
    FEATURES_START=$(date +%s)
    
    FEATURES_CMD="python scripts/run_features_pipeline.py"
    if [ "$VERBOSE" = true ]; then
        FEATURES_CMD="$FEATURES_CMD --verbose"
    fi
    
    if $FEATURES_CMD; then
        FEATURES_TIME=$(($(date +%s) - FEATURES_START))
        print_success "Feature generation completed in ${FEATURES_TIME}s"
    else
        print_error "Feature generation failed"
        exit 1
    fi
else
    print_warning "Skipping feature generation as requested"
    FEATURES_TIME=0
fi

# Summary
TOTAL_TIME=$(($(date +%s) - START_TIME))
echo ""
echo "🎉 INCREMENTAL UPDATE COMPLETE!"
echo "==============================="
print_success "Total runtime: ${TOTAL_TIME}s"
echo "   • Data collection: ${COLLECTION_TIME}s"
echo "   • Aggregation: ${AGGREGATE_TIME}s"
if [ "$SKIP_FEATURES" = false ]; then
    echo "   • Feature generation: ${FEATURES_TIME}s"
fi

echo ""
echo "📊 Updated Files:"
# Show latest files
LATEST_BRONZE=$(find data/bronze -name "*.parquet" -type f -exec ls -lt {} + 2>/dev/null | head -1 | awk '{print $NF}' || echo "None")
LATEST_SILVER=$(ls -t data/silver/featured_data_*.parquet 2>/dev/null | head -1)

if [ "$LATEST_BRONZE" != "None" ] && [ -n "$LATEST_BRONZE" ]; then
    echo "   • Latest bronze: $(basename "$LATEST_BRONZE")"
fi

if [ -n "$LATEST_SILVER" ]; then
    SIZE=$(ls -lh "$LATEST_SILVER" | awk '{print $5}')
    echo "   • Latest features: $(basename "$LATEST_SILVER") ($SIZE)"
    
    # Quick dataset stats
    python -c "
import pandas as pd
try:
    df = pd.read_parquet('$LATEST_SILVER')
    print(f'   • Dataset shape: {df.shape[0]:,} rows × {df.shape[1]:,} features')
    print(f'   • Latest date: {df.index.max().date()}')
except Exception as e:
    print(f'   • Could not read dataset: {e}')
    " 2>/dev/null || echo "   • Dataset info unavailable"
fi

echo ""
echo "💡 Next Steps:"
echo "   • Load data: pd.read_parquet('data/silver/featured_data.parquet')"
echo "   • Schedule: Run this script daily via cron"
echo "   • Monitor: Check logs for any collection issues"
echo ""
print_success "Bristol Gate data is up to date! 📈" 