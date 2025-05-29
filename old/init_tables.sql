-- ClickHouse Database Setup Script for Bristol Gate Data Pipeline
-- Creates database and staging tables for each data source

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS bristol_gate;

-- Use the bristol_gate database
USE bristol_gate;

-- =====================================================
-- Symbols Reference Table
-- Contains metadata for all symbols across data sources
-- =====================================================
CREATE TABLE IF NOT EXISTS symbols
(
    symbol String,
    source String,
    description String,
    unit String
)
ENGINE = MergeTree()
ORDER BY (source, symbol)
SETTINGS index_granularity = 8192;

-- =====================================================
-- FRED (Federal Reserve Economic Data) Staging Table
-- Schema: date, series_id, value
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_fred
(
    date Date,
    series_id String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (series_id, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- Yahoo Finance Staging Table
-- Schema: date, symbol, open, high, low, close, adj_close, volume
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_yahoo
(
    date Date,
    symbol String,
    open Nullable(Float64),
    high Nullable(Float64),
    low Nullable(Float64),
    close Nullable(Float64),
    adj_close Nullable(Float64),
    volume Nullable(UInt64)
)
ENGINE = MergeTree()
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- EIA (Energy Information Administration) Staging Table
-- Schema: date, series_id, value
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_eia
(
    date Date,
    series_id String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (series_id, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- Baker Hughes Rig Count Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Baker Hughes data contains various rig count metrics by region/type
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_baker
(
    date Date,
    symbol String,
    metric String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- FINRA Margin Statistics Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains margin debt, free credit cash, and free credit margin data
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_finra
(
    date Date,
    symbol String,
    metric String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- S&P 500 Earnings and Estimates Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains quarterly earnings data and forward estimates
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_sp500
(
    date Date,
    symbol String,
    metric String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- USDA Economic Research Service Staging Table
-- Schema: date, symbol, metric, value (currently melted format)
-- Note: Contains agricultural economic indicators like net farm income
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_usda
(
    date Date,
    symbol String,
    metric String,
    value Float64
)
ENGINE = MergeTree()
ORDER BY (symbol, date)
SETTINGS index_granularity = 8192;

-- =====================================================
-- Verify table creation
-- =====================================================
SHOW TABLES FROM bristol_gate;

-- =====================================================
-- Display table schemas for verification
-- =====================================================
DESCRIBE TABLE bristol_gate.symbols;
DESCRIBE TABLE bristol_gate.stg_fred;
DESCRIBE TABLE bristol_gate.stg_yahoo;
DESCRIBE TABLE bristol_gate.stg_eia;
DESCRIBE TABLE bristol_gate.stg_baker;
DESCRIBE TABLE bristol_gate.stg_finra;
DESCRIBE TABLE bristol_gate.stg_sp500;
DESCRIBE TABLE bristol_gate.stg_usda;
