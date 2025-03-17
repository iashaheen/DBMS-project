-- Unified Economic Data Analysis Schema
CREATE DATABASE IF NOT EXISTS economic_data;
USE economic_data;

-- Reference Tables for Location/Area Management
DROP TABLE IF EXISTS state_income;
DROP TABLE IF EXISTS regional_income;
DROP TABLE IF EXISTS state_food_sales;
DROP TABLE IF EXISTS cpi_values;
DROP TABLE IF EXISTS food_prices;
DROP TABLE IF EXISTS cpi_categories;
DROP TABLE IF EXISTS food_categories;
DROP TABLE IF EXISTS time_periods;
DROP TABLE IF EXISTS regions;

-- Now create tables
CREATE TABLE regions (
    region_id INT AUTO_INCREMENT PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL UNIQUE,
    region_type ENUM('state', 'division', 'region') NOT NULL
);

-- Time Period Reference
CREATE TABLE time_periods (
    period_id INT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL,
    month TINYINT,  -- NULL for yearly data
    period_type ENUM('monthly', 'yearly') NOT NULL,
    UNIQUE KEY year_month_unique (year, month)
);

-- Food Categories
CREATE TABLE food_categories (
    item_code VARCHAR(20) PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL
);

-- CPI Categories
CREATE TABLE cpi_categories (
    item_code VARCHAR(20) PRIMARY KEY,
    item_name VARCHAR(255) NOT NULL
);

-- Food Price Data
CREATE TABLE food_prices (
    region_id INT,
    item_code VARCHAR(20),
    period_id INT,
    price DECIMAL(10,2),
    PRIMARY KEY (region_id, item_code, period_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (item_code) REFERENCES food_categories(item_code),
    FOREIGN KEY (period_id) REFERENCES time_periods(period_id)
);

-- CPI Data
CREATE TABLE cpi_values (
    region_id INT,
    item_code VARCHAR(20),
    period_id INT,
    value DECIMAL(10,2),
    base_period VARCHAR(50),
    base_value DECIMAL(10,2),
    PRIMARY KEY (region_id, item_code, period_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (item_code) REFERENCES cpi_categories(item_code),
    FOREIGN KEY (period_id) REFERENCES time_periods(period_id)
);

-- State Food Sales
CREATE TABLE state_food_sales (
    region_id INT,
    period_id INT,
    total_sales_million DECIMAL(12,2),
    PRIMARY KEY (region_id, period_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (period_id) REFERENCES time_periods(period_id)
);

-- Regional Income Statistics
CREATE TABLE regional_income (
    region_id INT,
    period_id INT,
    households_thousands INT,
    median_income_current DECIMAL(12,2),
    median_income_2023 DECIMAL(12,2),
    mean_income_current DECIMAL(12,2),
    mean_income_2023 DECIMAL(12,2),
    PRIMARY KEY (region_id, period_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (period_id) REFERENCES time_periods(period_id)
);

-- State Income Statistics
CREATE TABLE state_income (
    region_id INT,
    period_id INT,
    median_income_current DECIMAL(12,2),
    median_income_2023 DECIMAL(12,2),
    standard_error_current DECIMAL(10,2),
    standard_error_2023 DECIMAL(10,2),
    PRIMARY KEY (region_id, period_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (period_id) REFERENCES time_periods(period_id)
);