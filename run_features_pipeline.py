#!/usr/bin/env python3
"""
Feature Pipeline Runner - Bristol Gate

Simple script to run the unified feature pipeline with common configurations
"""

import argparse
import sys
from pathlib import Path
import time
import logging
from datetime import datetime

# Add src_pipeline to path
sys.path.insert(0, str(Path(__file__).parent))

from src_pipeline.unified_pipeline import run_silver_pipeline, run_full_pipeline
from src_pipeline.date_utils import DateUtils

def setup_logging(verbose: bool = False):
    """Setup logging configuration"""
    level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

def format_duration(seconds: float) -> str:
    """Format duration in a human-readable way"""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = seconds // 60
        secs = seconds % 60
        return f"{int(minutes)}m {secs:.1f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{int(hours)}h {int(minutes)}m {secs:.1f}s"

def find_latest_silver_file() -> str:
    """
    Find the latest final_aggregated_data file in silver directory
    
    Priority order:
    1. Latest timestamped file (final_aggregated_data_YYYYMMDD_HHMMSS.parquet)
    2. Generic file (final_aggregated_data.parquet)
    3. Any file containing "final_aggregated_data"
    """
    silver_dir = Path("data/silver")
    
    if not silver_dir.exists():
        return None
    
    # Look for timestamped files first
    timestamped_pattern = "final_aggregated_data_*.parquet"
    timestamped_files = list(silver_dir.glob(timestamped_pattern))
    
    if timestamped_files:
        # Sort by filename (which includes timestamp) and return the latest
        # Files are named like: final_aggregated_data_20250528_162008.parquet
        latest_file = max(timestamped_files, key=lambda p: p.name)
        print(f"📂 Found latest timestamped file: {latest_file.name}")
        return str(latest_file)
    
    # Fall back to generic file
    generic_file = silver_dir / "final_aggregated_data.parquet"
    if generic_file.exists():
        print(f"📂 Found generic file: {generic_file.name}")
        return str(generic_file)
    
    # Last resort: any file containing "final_aggregated_data"
    pattern = "*final_aggregated_data*.parquet"
    all_matching_files = list(silver_dir.glob(pattern))
    
    if all_matching_files:
        latest_file = max(all_matching_files, key=lambda p: p.stat().st_mtime)
        print(f"📂 Found matching file: {latest_file.name}")
        return str(latest_file)
    
    return None

def main():
    parser = argparse.ArgumentParser(
        description='Feature Pipeline Runner - Bristol Gate',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Common Usage Patterns:

# Quick development run (automatically finds latest silver file, auto-timestamped output)
python run_features_pipeline.py

# Complete production run (from DuckDB, parallel features)  
python run_features_pipeline.py --full --start-date 1990-01-01

# Sequential processing for debugging
python run_features_pipeline.py --sequential

# Skip domain features for faster processing
python run_features_pipeline.py --no-domain-features

# Verbose logging with detailed timing
python run_features_pipeline.py --verbose

# Custom output path (automatically timestamped by pipeline)
python run_features_pipeline.py --output data/silver/my_features.parquet

# Specify exact file
python run_features_pipeline.py --input data/silver/final_aggregated_data_20250528_162008.parquet

Performance Tips:
• Script automatically finds latest timestamped silver file
• Output files are automatically timestamped by the unified pipeline
• Use --full for first run to populate silver layer
• Use --sequential for debugging feature calculation issues
• Use --workers to tune parallel performance
• Use --verbose for detailed timing information
• Use --no-domain-features to skip domain-specific features for faster processing
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--full', action='store_true',
                           help='Run complete pipeline from DuckDB (slower, comprehensive)')
    mode_group.add_argument('--silver', action='store_true', default=True,
                           help='Load from silver layer (faster, default)')
    
    # Processing options
    parser.add_argument('--sequential', action='store_true',
                       help='Use sequential processing instead of parallel')
    parser.add_argument('--workers', type=int, default=6,
                       help='Number of parallel workers (default: 6)')
    
    # Data paths
    parser.add_argument('--input', default=None,
                       help='Specific input data path (if not provided, auto-detects latest)')
    parser.add_argument('--output', default='data/silver/featured_data.parquet',
                       help='Output featured data path (automatically timestamped by pipeline)')
    parser.add_argument('--start-date', default='1950-01-01',
                       help='Start date for data (full mode only)')
    
    # Development options
    parser.add_argument('--save-intermediates', action='store_true',
                       help='Save intermediate files (full mode only)')
    parser.add_argument('--db-path', default='bristol_gate.duckdb',
                       help='DuckDB database path (full mode only)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging with detailed timing')
    parser.add_argument('--no-domain-features', action='store_true',
                       help='Skip domain-specific derived features (faster processing)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging(args.verbose)
    
    # Record overall start time
    overall_start_time = time.time()
    start_datetime = datetime.now()
    
    print("🚀 Bristol Gate Feature Pipeline Runner")
    print("=" * 50)
    print(f"⏰ Started at: {DateUtils.format_datetime(start_datetime)}")
    print(f"📅 Output will be automatically timestamped by pipeline")
    
    logger.info("🚀 Starting Bristol Gate Feature Pipeline")
    logger.info(f"Mode: {'FULL' if args.full else 'SILVER'}")
    logger.info(f"Processing: {'Sequential' if args.sequential else f'Parallel ({args.workers} workers)'}")
    logger.info(f"Verbose logging: {args.verbose}")
    logger.info(f"Domain features: {not args.no_domain_features}")
    logger.info(f"Base output path: {args.output}")
    
    try:
        if args.full:
            print("📊 Running FULL pipeline from DuckDB staging tables...")
            print(f"   • Database: {args.db_path}")
            print(f"   • Start date: {args.start_date}")
            print(f"   • Processing: {'Sequential' if args.sequential else f'Parallel ({args.workers} workers)'}")
            print(f"   • Save intermediates: {args.save_intermediates}")
            print(f"   • Domain features: {not args.no_domain_features}")
            print(f"   • Output format: Parquet (automatically timestamped)")
            print()
            
            logger.info("📊 Starting FULL pipeline execution")
            pipeline_start = time.time()
            
            result = run_full_pipeline(
                db_path=args.db_path,
                start_date=args.start_date,
                parallel=not args.sequential,
                num_workers=args.workers,
                save_intermediates=args.save_intermediates,
                output_path=args.output,
                create_domain_features=not args.no_domain_features
            )
            
            pipeline_duration = time.time() - pipeline_start
            logger.info(f"✅ FULL pipeline completed in {format_duration(pipeline_duration)}")
            
        else:
            print("⚡ Running SILVER pipeline from existing files...")
            logger.info("⚡ Starting SILVER pipeline execution")
            
            # File discovery phase
            discovery_start = time.time()
            if args.input:
                input_path = args.input
                print(f"⚡ Using specified input file...")
                logger.info(f"Using specified input file: {input_path}")
            else:
                print(f"🔍 Auto-detecting latest silver file...")
                logger.info("Auto-detecting latest silver file...")
                input_path = find_latest_silver_file()
                if input_path:
                    print(f"⚡ Auto-detected input file...")
                    logger.info(f"Auto-detected file: {input_path}")
                else:
                    error_msg = "❌ No silver files found in data/silver/"
                    print(error_msg)
                    logger.error(error_msg)
                    print("\n💡 Available options:")
                    print("   1. Run full pipeline first: python run_features_pipeline.py --full")
                    print("   2. Check if files exist in data/silver/ directory")
                    print("   3. Specify exact file: python run_features_pipeline.py --input path/to/file.parquet")
                    return 1
            
            discovery_duration = time.time() - discovery_start
            logger.debug(f"File discovery took {format_duration(discovery_duration)}")
            
            print(f"   • Input file: {Path(input_path).name}")
            print(f"   • Processing: {'Sequential' if args.sequential else f'Parallel ({args.workers} workers)'}")
            print(f"   • Domain features: {not args.no_domain_features}")
            print(f"   • Output format: Parquet (automatically timestamped)")
            print()
            
            # File validation phase
            validation_start = time.time()
            silver_path = Path(input_path)
            if not silver_path.exists():
                error_msg = f"❌ Silver file not found: {input_path}"
                print(error_msg)
                logger.error(error_msg)
                print("\n💡 Try running with --full to create the silver layer first:")
                print(f"   python run_features_pipeline.py --full --start-date 2020-01-01")
                return 1
            
            # Show file info
            file_size_mb = silver_path.stat().st_size / (1024 * 1024)
            file_mod_time = datetime.fromtimestamp(silver_path.stat().st_mtime)
            print(f"📁 Input file size: {file_size_mb:.1f} MB")
            print(f"📅 File modified: {DateUtils.format_datetime(file_mod_time)}")
            
            validation_duration = time.time() - validation_start
            logger.info(f"Input file validation: {file_size_mb:.1f} MB, modified {DateUtils.format_datetime(file_mod_time)}")
            logger.debug(f"File validation took {format_duration(validation_duration)}")
            
            # Pipeline execution phase
            pipeline_start = time.time()
            logger.info("🚀 Starting SILVER pipeline processing...")
            
            result = run_silver_pipeline(
                silver_data_path=input_path,
                parallel=not args.sequential,
                num_workers=args.workers,
                output_path=args.output,
                create_domain_features=not args.no_domain_features
            )
            
            pipeline_duration = time.time() - pipeline_start
            logger.info(f"✅ SILVER pipeline completed in {format_duration(pipeline_duration)}")
        
        # Results processing and reporting
        if not result.empty:
            end_datetime = datetime.now()
            overall_duration = time.time() - overall_start_time
            
            print("\n" + "=" * 50)
            print("🎉 SUCCESS! Feature pipeline completed!")
            print(f"⏰ Completed at: {DateUtils.format_datetime(end_datetime)}")
            print(f"⌛ Total runtime: {format_duration(overall_duration)}")
            print(f"📊 Final dataset shape: {result.shape}")
            print(f"📅 Date range: {DateUtils.format_date_only(result.index.min())} to {DateUtils.format_date_only(result.index.max())}")
            
            logger.info(f"🎉 Pipeline SUCCESS - Total runtime: {format_duration(overall_duration)}")
            logger.info(f"📊 Final dataset shape: {result.shape}")
            
            # Output file reporting - look for actual timestamped files
            output_dir = Path(args.output).parent
            base_name = Path(args.output).stem
            
            # Find the timestamped file that was just created
            timestamped_pattern = f"{base_name}_*.parquet"
            timestamped_files = list(output_dir.glob(timestamped_pattern))
            
            if timestamped_files:
                # Get the most recent timestamped file
                latest_timestamped = max(timestamped_files, key=lambda p: p.stat().st_mtime)
                output_size_mb = latest_timestamped.stat().st_size / (1024 * 1024)
                output_mod_time = datetime.fromtimestamp(latest_timestamped.stat().st_mtime)
                print(f"💾 Timestamped output: {latest_timestamped.name}")
                print(f"📦 Output file size: {output_size_mb:.1f} MB")
                print(f"📅 Output created: {DateUtils.format_datetime(output_mod_time)}")
                
                logger.info(f"💾 Output file: {output_size_mb:.1f} MB at {latest_timestamped}")
            
            # Feature analysis
            analysis_start = time.time()
            feature_cols = [col for col in result.columns if any(suffix in col for suffix in ['_YoY', '_Log', '_mva', '_Smooth'])]
            original_cols = [col for col in result.columns if not any(suffix in col for suffix in ['_YoY', '_Log', '_mva', '_Smooth'])]
            
            if feature_cols:
                print(f"🧮 Generated {len(feature_cols)} feature columns from {len(original_cols)} original columns")
                print(f"   Sample features: {', '.join(feature_cols[:5])}")
                if len(feature_cols) > 5:
                    print(f"   ... and {len(feature_cols) - 5} more")
                
                logger.info(f"🧮 Feature generation: {len(feature_cols)} features from {len(original_cols)} original columns")
                
                # Feature type breakdown
                yoy_features = [col for col in feature_cols if '_YoY' in col]
                log_features = [col for col in feature_cols if '_Log' in col]
                mva_features = [col for col in feature_cols if '_mva' in col]
                smooth_features = [col for col in feature_cols if '_Smooth' in col]
                
                if args.verbose:
                    print(f"   • YoY features: {len(yoy_features)}")
                    print(f"   • Log features: {len(log_features)}")
                    print(f"   • MVA features: {len(mva_features)}")
                    print(f"   • Smoothing features: {len(smooth_features)}")
                
                logger.debug(f"Feature breakdown - YoY: {len(yoy_features)}, Log: {len(log_features)}, MVA: {len(mva_features)}, Smooth: {len(smooth_features)}")
            
            analysis_duration = time.time() - analysis_start
            logger.debug(f"Feature analysis took {format_duration(analysis_duration)}")
            
            print("\n💡 Next steps:")
            
            # Show how to load the files
            if timestamped_files:
                latest_timestamped = max(timestamped_files, key=lambda p: p.stat().st_mtime)
                print(f"   • Load data: pd.read_parquet('{latest_timestamped}', engine='pyarrow')")
                print(f"   • Use in DuckDB: SELECT * FROM '{latest_timestamped}'")
            
            print(f"   • Check nulls: df.isnull().sum().sum()")
            print(f"   • Analyze features: df.describe()")
            
            # Performance summary for verbose mode
            if args.verbose:
                print(f"\n⏱️  Performance Summary:")
                print(f"   • Total runtime: {format_duration(overall_duration)}")
                if 'pipeline_duration' in locals():
                    print(f"   • Pipeline execution: {format_duration(pipeline_duration)} ({pipeline_duration/overall_duration*100:.1f}%)")
                throughput_rows_per_sec = result.shape[0] / overall_duration if overall_duration > 0 else 0
                throughput_cols_per_sec = result.shape[1] / overall_duration if overall_duration > 0 else 0
                print(f"   • Throughput: {throughput_rows_per_sec:.0f} rows/sec, {throughput_cols_per_sec:.0f} cols/sec")
                
                logger.info(f"⏱️  Performance - {throughput_rows_per_sec:.0f} rows/sec, {throughput_cols_per_sec:.0f} cols/sec")
            
        else:
            error_msg = "❌ Pipeline failed or returned empty data"
            print(f"\n{error_msg}")
            logger.error(error_msg)
            return 1
            
    except KeyboardInterrupt:
        interrupt_msg = "⚠️ Pipeline interrupted by user"
        print(f"\n{interrupt_msg}")
        logger.warning(interrupt_msg)
        return 1
    except Exception as e:
        error_msg = f"❌ Error running pipeline: {e}"
        print(f"\n{error_msg}")
        logger.error(error_msg, exc_info=args.verbose)
        import traceback
        traceback.print_exc()
        return 1
    
    final_duration = time.time() - overall_start_time
    logger.info(f"🏁 Script completed successfully in {format_duration(final_duration)}")
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 