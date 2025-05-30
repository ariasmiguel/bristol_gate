-- DuckDB Database Setup Script for Bristol Gate Data Pipeline
-- Creates staging tables for each data source

-- Note: We're already connected to bristol_gate.duckdb, so no ATTACH needed

-- =====================================================
-- Symbols Reference Table
-- Contains metadata for all symbols across data sources
-- =====================================================
CREATE TABLE IF NOT EXISTS symbols
(
    symbol VARCHAR,
    source VARCHAR,
    description VARCHAR,
    unit VARCHAR,
    expense_ratio DOUBLE
);

-- =====================================================
-- FRED (Federal Reserve Economic Data) Staging Table
-- Schema: date, series_id, value
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_fred
(
    date DATE,
    series_id VARCHAR,
    value DOUBLE
);

-- =====================================================
-- Yahoo Finance Staging Table
-- Schema: date, symbol, open, high, low, close, adj_close, volume
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_yahoo
(
    date DATE,
    symbol VARCHAR,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    -- adj_close DOUBLE,
    volume BIGINT
);

-- =====================================================
-- EIA (Energy Information Administration) Staging Table
-- Schema: date, series_id, value
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_eia
(
    date DATE,
    series_id VARCHAR,
    value DOUBLE
);

-- =====================================================
-- Baker Hughes Rig Count Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Baker Hughes data contains various rig count metrics by region/type
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_baker
(
    date DATE,
    symbol VARCHAR,
    metric VARCHAR,
    value DOUBLE
);

-- =====================================================
-- FINRA Margin Statistics Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains margin debt, free credit cash, and free credit margin data
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_finra
(
    date DATE,
    symbol VARCHAR,
    metric VARCHAR,
    value DOUBLE
);

-- =====================================================
-- S&P 500 Earnings and Estimates Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains quarterly earnings data and forward estimates
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_sp500
(
    date DATE,
    symbol VARCHAR,
    metric VARCHAR,
    value DOUBLE
);

-- =====================================================
-- USDA Economic Research Service Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains agricultural economic indicators like net farm income
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_usda
(
    date DATE,
    symbol VARCHAR,
    metric VARCHAR,
    value DOUBLE
);

-- =====================================================
-- OCC (Options Clearing Corporation) Staging Table
-- Schema: date, symbol, metric, value (standard format)
-- Note: Contains daily options and futures volume data
-- Metrics include: Options_Equity_Volume, Options_Index_Volume, Options_Debt_Volume,
--                  Futures_Total_Volume, Total_Volume, Futures_Equity_Volume, Futures_Index_Volume
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_occ
(
    date DATE,
    symbol VARCHAR,
    metric VARCHAR,
    value DOUBLE
);

-- =====================================================
-- Featured Data Table
-- Schema: date, symbol, value (final standardized format)
-- Note: Contains processed and featured data ready for ML/analysis
-- This table stores the output of the feature engineering pipeline
-- in a consistent long format for easy querying and analysis
-- =====================================================
CREATE TABLE IF NOT EXISTS featured_data
(
    date DATE,
    symbol VARCHAR,
    value DOUBLE
);

-- =====================================================
-- Verify table creation
-- =====================================================
SHOW TABLES;

-- =====================================================
-- Display table schemas for verification
-- =====================================================
DESCRIBE symbols;
DESCRIBE stg_fred;
DESCRIBE stg_yahoo;
DESCRIBE stg_eia;
DESCRIBE stg_baker;
DESCRIBE stg_finra;
DESCRIBE stg_sp500;
DESCRIBE stg_usda;
DESCRIBE stg_occ;
DESCRIBE featured_data;

-- =====================================================
-- Sanity check - count rows in symbols table
-- =====================================================
SELECT COUNT(*) as symbol_count FROM symbols; 