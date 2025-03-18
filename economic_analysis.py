import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Optional
import calendar

def connect_to_db():
    """Establish database connection"""
    return mysql.connector.connect(
        host="localhost",
        user="econ_user",  # Update with your MySQL username
        password="econ_pass",   # Update with your MySQL password
        database="economic_data"
    )

def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame"""
    conn = connect_to_db()
    try:
        return pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

# 1. Static Query: Regional Income Inequality Analysis
def analyze_income_inequality():
    """
    Analyzes income inequality by comparing median and mean incomes across regions
    Returns data showing the gap between mean and median income (2023 dollars)
    """
    query = """
    SELECT 
        r.region_name,
        ri.period_id,
        tp.year,
        ri.median_income_2023,
        ri.mean_income_2023,
        (ri.mean_income_2023 - ri.median_income_2023) as income_gap
    FROM regional_income ri
    JOIN regions r ON ri.region_id = r.region_id
    JOIN time_periods tp ON ri.period_id = tp.period_id
    WHERE r.region_type = 'region'
    ORDER BY tp.year DESC, income_gap DESC;
    """
    return execute_query(query)

# 2. Interactive Query: Food Price Trends by Item
def analyze_food_price_trends(item_name: str):
    """
    Analyzes price trends for a specific food item across all regions over time
    """
    query = """
    SELECT 
        r.region_name,
        tp.year,
        tp.month,
        fp.price,
        fc.item_name
    FROM food_prices fp
    JOIN regions r ON fp.region_id = r.region_id
    JOIN time_periods tp ON fp.period_id = tp.period_id
    JOIN food_categories fc ON fp.item_code = fc.item_code
    WHERE fc.item_name LIKE %s AND
    tp.period_type = 'monthly'
    ORDER BY tp.year, tp.month;
    """
    return execute_query(query, (f"%{item_name}%",))

# 3. Static Query: State Food Sales Rankings
def get_state_food_sales_rankings(year: int = 2023):
    """
    Ranks states by their total food sales for the specified year
    """
    query = """
    SELECT 
        r.region_name as state,
        tp.year,
        sfs.total_sales_million
    FROM state_food_sales sfs
    JOIN regions r ON sfs.region_id = r.region_id
    JOIN time_periods tp ON sfs.period_id = tp.period_id
    WHERE tp.year = %s
    AND r.region_type = 'state'
    ORDER BY sfs.total_sales_million DESC;
    """
    return execute_query(query, (year,))

# 4. Interactive Query: CPI Analysis by Category and Region
def analyze_cpi_by_category(region_name: str, year: int):
    """
    Analyzes CPI values for different categories in a specific region and year
    Handles both state and region level filtering
    """
    query = """
    SELECT 
        cc.item_name as category,
        cv.value as cpi_value,
        cv.base_period,
        cv.base_value
    FROM cpi_values cv
    JOIN regions r ON cv.region_id = r.region_id
    JOIN time_periods tp ON cv.period_id = tp.period_id
    JOIN cpi_categories cc ON cv.item_code = cc.item_code
    WHERE r.region_name = %s
    AND r.region_type IN ('state', 'region', 'division')
    AND tp.year = %s
    ORDER BY cv.value DESC;
    """
    return execute_query(query, (region_name, year))

# 5. Static Query: Income Growth Analysis
def analyze_income_growth():
    """
    Calculates income growth rates across regions over the available time period
    """
    query = """
    WITH income_by_year AS (
        SELECT 
            r.region_name,
            tp.year,
            ri.median_income_2023
        FROM regional_income ri
        JOIN regions r ON ri.region_id = r.region_id
        JOIN time_periods tp ON ri.period_id = tp.period_id
        WHERE r.region_type = 'region'
    )
    SELECT 
        i1.region_name,
        i1.year as start_year,
        i2.year as end_year,
        i1.median_income_2023 as start_income,
        i2.median_income_2023 as end_income,
        ((i2.median_income_2023 - i1.median_income_2023) / i1.median_income_2023 * 100) as growth_rate
    FROM income_by_year i1
    JOIN income_by_year i2 ON i1.region_name = i2.region_name
    WHERE i1.year = (SELECT MIN(year) FROM income_by_year)
    AND i2.year = (SELECT MAX(year) FROM income_by_year)
    ORDER BY growth_rate DESC;
    """
    return execute_query(query)

# 6. Interactive Query: State Income Comparison
def compare_state_incomes(state1: str, state2: str):
    """
    Compares income statistics between two states over time
    """
    query = """
    SELECT 
        r.region_name as state,
        tp.year,
        si.median_income_2023,
        si.standard_error_2023
    FROM state_income si
    JOIN regions r ON si.region_id = r.region_id
    JOIN time_periods tp ON si.period_id = tp.period_id
    WHERE r.region_name IN (%s, %s)
    ORDER BY tp.year, r.region_name;
    """
    return execute_query(query, (state1, state2))

# 7. Static Query: Monthly Food Price Volatility
def analyze_food_price_volatility():
    """
    Calculates price volatility for food items over the most recent year
    """
    query = """
    WITH monthly_stats AS (
        SELECT 
            fc.item_name,
            tp.year,
            tp.month,
            AVG(fp.price) as avg_price,
            STDDEV(fp.price) as price_std
        FROM food_prices fp
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        WHERE tp.year = (SELECT MAX(year) FROM time_periods)
        GROUP BY fc.item_name, tp.year, tp.month
    )
    SELECT 
        item_name,
        AVG(price_std) as avg_volatility
    FROM monthly_stats
    GROUP BY item_name
    ORDER BY avg_volatility DESC
    LIMIT 10;
    """
    return execute_query(query)

# 8. Interactive Query: Regional CPI Trends
def analyze_regional_cpi_trends(item_code: str):
    """
    Analyzes CPI trends for a specific category across regions
    """
    query = """
    SELECT 
        r.region_name,
        tp.year,
        tp.month,
        cv.value as cpi_value
    FROM cpi_values cv
    JOIN regions r ON cv.region_id = r.region_id
    JOIN time_periods tp ON cv.period_id = tp.period_id
    WHERE cv.item_code = %s
    AND r.region_type = 'region'
    ORDER BY tp.year, tp.month, r.region_name;
    """
    return execute_query(query, (item_code,))

# 9. Static Query: Income-Sales Correlation
def analyze_income_sales_correlation():
    """
    Analyzes correlation between median income and food sales by state
    """
    query = """
    SELECT 
        r.region_name as state,
        si.median_income_2023,
        sfs.total_sales_million
    FROM state_income si
    JOIN state_food_sales sfs ON si.region_id = sfs.region_id 
        AND si.period_id = sfs.period_id
    JOIN regions r ON si.region_id = r.region_id
    JOIN time_periods tp ON si.period_id = tp.period_id
    WHERE tp.year = (SELECT MAX(year) FROM time_periods)
    AND r.region_type = 'state'
    ORDER BY si.median_income_2023 DESC;
    """
    return execute_query(query)

# 10. Interactive Query: Monthly Price Analysis
def analyze_monthly_prices(year: int, month: int):
    """
    Analyzes food prices across all categories for a specific month
    """
    query = """
    SELECT 
        fc.item_name,
        r.region_name,
        fp.price
    FROM food_prices fp
    JOIN food_categories fc ON fp.item_code = fc.item_code
    JOIN regions r ON fp.region_id = r.region_id
    JOIN time_periods tp ON fp.period_id = tp.period_id
    WHERE tp.year = %s AND tp.month = %s
    ORDER BY r.region_name, fp.price DESC;
    """
    return execute_query(query, (year, month))

# 11. Static Query: Regional Income Distribution
def analyze_regional_income_distribution():
    """
    Analyzes the distribution of income across regions for the most recent year
    """
    query = """
    SELECT 
        r.region_name,
        ri.households_thousands,
        ri.median_income_2023,
        ri.mean_income_2023
    FROM regional_income ri
    JOIN regions r ON ri.region_id = r.region_id
    JOIN time_periods tp ON ri.period_id = tp.period_id
    WHERE tp.year = (SELECT MAX(year) FROM time_periods)
    AND r.region_type = 'region'
    ORDER BY ri.median_income_2023 DESC;
    """
    return execute_query(query)

# 12. Interactive Query: Year-over-Year CPI Change
def analyze_yoy_cpi_change(region_name: str, category_code: str):
    """
    Calculates year-over-year CPI changes for a specific region and category
    """
    query = """
    WITH cpi_by_year AS (
        SELECT 
            tp.year,
            AVG(cv.value) as avg_cpi
        FROM cpi_values cv
        JOIN regions r ON cv.region_id = r.region_id
        JOIN time_periods tp ON cv.period_id = tp.period_id
        WHERE r.region_name = %s
        AND cv.item_code = %s
        GROUP BY tp.year
    )
    SELECT 
        curr.year,
        curr.avg_cpi,
        prev.avg_cpi as prev_year_cpi,
        ((curr.avg_cpi - prev.avg_cpi) / prev.avg_cpi * 100) as yoy_change
    FROM cpi_by_year curr
    LEFT JOIN cpi_by_year prev ON curr.year = prev.year + 1
    ORDER BY curr.year;
    """
    return execute_query(query, (region_name, category_code))

# 13. Static Query: Food Price Range Analysis
def analyze_price_ranges():
    """
    Analyzes the price ranges (min, max, avg) for food items across all regions
    """
    query = """
    SELECT 
        fc.item_name,
        MIN(fp.price) as min_price,
        MAX(fp.price) as max_price,
        AVG(fp.price) as avg_price,
        (MAX(fp.price) - MIN(fp.price)) as price_range
    FROM food_prices fp
    JOIN food_categories fc ON fp.item_code = fc.item_code
    JOIN time_periods tp ON fp.period_id = tp.period_id
    WHERE tp.year = (SELECT MAX(year) FROM time_periods)
    GROUP BY fc.item_name
    ORDER BY price_range DESC;
    """
    return execute_query(query)

# 14. Interactive Query: State Income Percentile
def get_state_income_percentile(state_name: str):
    """
    Calculates the percentile ranking of a state's median income
    """
    query = """
    WITH state_rankings AS (
        SELECT 
            r.region_name,
            si.median_income_2023,
            PERCENT_RANK() OVER (ORDER BY si.median_income_2023) as percentile
        FROM state_income si
        JOIN regions r ON si.region_id = r.region_id
        JOIN time_periods tp ON si.period_id = tp.period_id
        WHERE tp.year = (SELECT MAX(year) FROM time_periods)
        AND r.region_type = 'state'
    )
    SELECT *
    FROM state_rankings
    WHERE region_name = %s;
    """
    return execute_query(query, (state_name,))

# 15. Static Query: Seasonal Price Patterns
def analyze_seasonal_patterns():
    """
    Analyzes seasonal patterns in food prices
    """
    query = """
    SELECT 
        fc.item_name,
        tp.month,
        AVG(fp.price) as avg_price
    FROM food_prices fp
    JOIN food_categories fc ON fp.item_code = fc.item_code
    JOIN time_periods tp ON fp.period_id = tp.period_id
    WHERE tp.month IS NOT NULL
    GROUP BY fc.item_name, tp.month
    ORDER BY fc.item_name, tp.month;
    """
    return execute_query(query)

# Example usage of visualization for some queries
def plot_income_inequality(df: pd.DataFrame):
    plt.figure(figsize=(12, 6))
    for region in df['region_name'].unique():
        region_data = df[df['region_name'] == region]
        plt.plot(region_data['year'], region_data['income_gap'], label=region, marker='o')
    plt.title('Income Inequality Gap by Region Over Time')
    plt.xlabel('Year')
    plt.ylabel('Income Gap (Mean - Median) in 2023 Dollars')
    plt.legend()
    plt.grid(True)
    plt.show()

def plot_food_price_trends(df: pd.DataFrame):
    plt.figure(figsize=(12, 6))
    for region in df['region_name'].unique():
        region_data = df[df['region_name'] == region]
        plt.plot(pd.to_datetime(region_data['year'].astype(str) + '-' + region_data['month'].astype(str) + '-01'),
                region_data['price'], label=region)
    plt.title(f'Price Trends for {df["item_name"].iloc[0]}')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()

# Add more visualization functions as needed