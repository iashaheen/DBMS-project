import pandas as pd
from typing import Optional
import calendar
import logging
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import matplotlib.pyplot as plt

logger = logging.getLogger(__name__)

def get_db_engine() -> Engine:
    """Create and return SQLAlchemy engine"""
    try:
        connection_string = "mysql+pymysql://root:@localhost/economic_data"
        engine = create_engine(connection_string)
        return engine
    except Exception as err:
        logger.error(f"Database connection error: {err}")
        raise

def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame"""
    try:
        engine = get_db_engine()
        if params:
            # Log the query and parameters for debugging
            logger.debug(f"Executing query with params: {query}, {params}")
            return pd.read_sql_query(query, engine, params=params)
        else:
            logger.debug(f"Executing query: {query}")
            return pd.read_sql_query(query, engine)
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        raise

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
    WITH price_validation AS (
        SELECT 
            r.region_name,
            tp.year,
            tp.month,
            fp.price,
            fc.item_name,
            AVG(fp.price) OVER (
                PARTITION BY r.region_name 
                ORDER BY tp.year, tp.month
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) as rolling_avg,
            ABS(fp.price - AVG(fp.price) OVER (
                PARTITION BY r.region_name 
                ORDER BY tp.year, tp.month
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            )) / NULLIF(AVG(fp.price) OVER (
                PARTITION BY r.region_name 
                ORDER BY tp.year, tp.month
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ), 0) as price_deviation
        FROM food_prices fp
        JOIN regions r ON fp.region_id = r.region_id
        JOIN time_periods tp ON fp.period_id = tp.period_id
        JOIN food_categories fc ON fp.item_code = fc.item_code
        WHERE fc.item_name = %s
        AND tp.period_type = 'monthly'
    )
    SELECT 
        region_name,
        year,
        month,
        CASE 
            WHEN price_deviation > 0.5 THEN rolling_avg  -- Replace outliers with rolling average
            ELSE price
        END as price,
        item_name,
        price as original_price,
        rolling_avg,
        price_deviation
    FROM price_validation
    ORDER BY year, month;
    """
    try:
        logger.debug(f"Analyzing food price trends for: {item_name}")
        df = execute_query(query, (item_name,))
        logger.debug(f"Retrieved {len(df)} records for {item_name}")
        
        # Log data quality metrics
        if not df.empty:
            outliers = df[df['price'] != df['original_price']].shape[0]
            logger.info(f"Found and corrected {outliers} price outliers for {item_name}")
            coverage = df.groupby('region_name').size().describe()
            logger.info(f"Data coverage statistics:\n{coverage}")
        
        return df
    except Exception as e:
        logger.error(f"Error in analyze_food_price_trends: {e}")
        raise

# 3. Static Query: State Food Sales Rankings
def get_state_food_sales_rankings():
    """
    Ranks states by their total food sales for the most recent year
    """
    query = """
    SELECT 
        r.region_name as state,
        tp.year,
        sfs.total_sales_million
    FROM state_food_sales sfs
    JOIN regions r ON sfs.region_id = r.region_id
    JOIN time_periods tp ON sfs.period_id = tp.period_id
    WHERE tp.year = 2023
    AND r.region_type = 'state'
    ORDER BY sfs.total_sales_million DESC;
    """
    return execute_query(query)

# 4. Interactive Query: CPI Analysis by Category and Region
def analyze_cpi_by_category(region_name: str, year: int):
    """
    Analyzes CPI values for different categories in a specific region and year
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
    Analyzes seasonal patterns in food prices with improved aggregation and seasonality metrics
    """
    query = """
    WITH monthly_avg AS (
        SELECT 
            fc.item_name,
            tp.month,
            AVG(fp.price) as avg_price,
            MIN(fp.price) as min_price,
            MAX(fp.price) as max_price,
            COUNT(DISTINCT r.region_id) as num_regions,
            STDDEV(fp.price) as price_std
        FROM food_prices fp
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        JOIN regions r ON fp.region_id = r.region_id
        WHERE tp.month IS NOT NULL
        AND tp.period_type = 'monthly'
        GROUP BY fc.item_name, tp.month
    ),
    yearly_stats AS (
        SELECT 
            item_name,
            AVG(avg_price) as yearly_avg,
            STDDEV(avg_price) as yearly_std
        FROM monthly_avg
        GROUP BY item_name
    )
    SELECT 
        ma.item_name,
        ma.month,
        ma.avg_price,
        ma.min_price,
        ma.max_price,
        ma.num_regions,
        ma.price_std,
        ys.yearly_avg,
        ((ma.avg_price - ys.yearly_avg) / ys.yearly_avg * 100) as seasonal_index,
        (ma.price_std / ma.avg_price * 100) as monthly_cv
    FROM monthly_avg ma
    JOIN yearly_stats ys ON ma.item_name = ys.item_name
    ORDER BY ma.item_name, ma.month;
    """
    return execute_query(query)

# 16. Geographic Price Distribution
def analyze_geographic_price_distribution(item_name: str, year: int, month: Optional[int] = None):
    """
    Analyzes price distribution across states for geographical visualization
    """
    query = """
    WITH latest_prices AS (
        SELECT 
            r.region_name,
            UPPER(SUBSTRING(r.region_name, 1, 2)) as state_code,
            r.region_type,
            fc.item_name,
            fp.price,
            tp.year,
            tp.month,
            AVG(fp.price) OVER (PARTITION BY r.region_type) as avg_price,
            (fp.price - AVG(fp.price) OVER (PARTITION BY r.region_type)) / 
            NULLIF(AVG(fp.price) OVER (PARTITION BY r.region_type), 0) * 100 as price_diff_pct
        FROM food_prices fp
        JOIN regions r ON fp.region_id = r.region_id
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        WHERE r.region_type = 'state'
        AND fc.item_name = %s
        AND tp.year = %s
        AND tp.period_type = 'monthly'
        {}
    )
    SELECT 
        region_name,
        state_code,
        item_name,
        price,
        year,
        month,
        avg_price as national_avg,
        COALESCE(price_diff_pct, 0) as price_diff_pct
    FROM latest_prices
    WHERE price IS NOT NULL
    ORDER BY price DESC;
    """
    
    if month:
        month_condition = "AND tp.month = %s"
        query = query.format(month_condition)
        return execute_query(query, (item_name, year, month))
    else:
        query = query.format("")
        return execute_query(query, (item_name, year))

# 17. Regional Price Disparities
def analyze_regional_price_disparities():
    """
    Analyzes price differences between regions for common food items
    """
    query = """
    WITH avg_prices AS (
        SELECT 
            r.region_name,
            fc.item_name,
            AVG(fp.price) as avg_price
        FROM food_prices fp
        JOIN regions r ON fp.region_id = r.region_id
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        WHERE tp.year = (SELECT MAX(year) FROM time_periods)
        AND r.region_type = 'region'
        GROUP BY r.region_name, fc.item_name
    ),
    price_stats AS (
        SELECT 
            item_name,
            MAX(avg_price) - MIN(avg_price) as price_gap,
            AVG(avg_price) as national_avg,
            STDDEV(avg_price) as price_std
        FROM avg_prices
        GROUP BY item_name
    )
    SELECT 
        item_name,
        price_gap,
        national_avg,
        price_std,
        (price_std / national_avg) as coefficient_of_variation
    FROM price_stats
    ORDER BY coefficient_of_variation DESC
    LIMIT 15;
    """
    return execute_query(query)

# 18. Income-Adjusted Food Prices
def analyze_income_adjusted_prices(item_name: str):
    """
    Analyzes food prices adjusted by median income across states
    """
    query = """
    WITH latest_data AS (
        SELECT 
            r.region_name,
            fc.item_name,
            fp.price,
            si.median_income_2023,
            (fp.price / si.median_income_2023 * 50000) as adjusted_price
        FROM food_prices fp
        JOIN regions r ON fp.region_id = r.region_id
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        JOIN state_income si ON r.region_id = si.region_id 
            AND tp.period_id = si.period_id
        WHERE fc.item_name = %s
        AND tp.year = (SELECT MAX(year) FROM time_periods)
        AND r.region_type = 'state'
    )
    SELECT 
        region_name,
        price as nominal_price,
        median_income_2023,
        adjusted_price,
        (adjusted_price - price) as price_difference
    FROM latest_data
    ORDER BY adjusted_price DESC;
    """
    return execute_query(query, (item_name,))

# 19. Price Trend Correlation
def analyze_price_trend_correlation():
    """
    Analyzes correlation between different food items' price trends
    """
    query = """
    WITH monthly_prices AS (
        SELECT 
            fc.item_name,
            tp.year,
            tp.month,
            AVG(fp.price) as avg_price
        FROM food_prices fp
        JOIN food_categories fc ON fp.item_code = fc.item_code
        JOIN time_periods tp ON fp.period_id = tp.period_id
        GROUP BY fc.item_name, tp.year, tp.month
    ),
    price_changes AS (
        SELECT 
            a.item_name,
            a.year,
            a.month,
            ((a.avg_price - b.avg_price) / b.avg_price) as price_change
        FROM monthly_prices a
        JOIN monthly_prices b ON a.item_name = b.item_name
            AND ((a.year = b.year AND a.month = b.month + 1)
                OR (a.year = b.year + 1 AND a.month = 1 AND b.month = 12))
    )
    SELECT 
        p1.item_name as item1,
        p2.item_name as item2,
        CORR(p1.price_change, p2.price_change) as correlation
    FROM price_changes p1
    JOIN price_changes p2 ON p1.year = p2.year 
        AND p1.month = p2.month
        AND p1.item_name < p2.item_name
    GROUP BY p1.item_name, p2.item_name
    HAVING ABS(correlation) > 0.7
    ORDER BY ABS(correlation) DESC;
    """
    return execute_query(query)

# 20. Urban-Rural Price Comparison
def analyze_urban_rural_prices():
    """
    Compares food prices between urban and rural areas
    """
    query = """
    WITH state_urbanization AS (
        SELECT 
            r.region_id,
            r.region_name,
            CASE 
                WHEN sfs.total_sales_million > (
                    SELECT AVG(total_sales_million) 
                    FROM state_food_sales sfs2
                    JOIN time_periods tp2 ON sfs2.period_id = tp2.period_id
                    WHERE tp2.year = (SELECT MAX(year) FROM time_periods)
                ) THEN 'Urban'
                ELSE 'Rural'
            END as area_type
        FROM regions r
        JOIN state_food_sales sfs ON r.region_id = sfs.region_id
        JOIN time_periods tp ON sfs.period_id = tp.period_id
        WHERE r.region_type = 'state'
        AND tp.year = (SELECT MAX(year) FROM time_periods)
    )
    SELECT 
        fc.item_name,
        su.area_type,
        COUNT(DISTINCT su.region_id) as num_states,
        AVG(fp.price) as avg_price,
        MIN(fp.price) as min_price,
        MAX(fp.price) as max_price,
        STDDEV(fp.price) as price_std
    FROM food_prices fp
    JOIN food_categories fc ON fp.item_code = fc.item_code
    JOIN time_periods tp ON fp.period_id = tp.period_id
    JOIN state_urbanization su ON fp.region_id = su.region_id
    WHERE tp.year = (SELECT MAX(year) FROM time_periods)
    AND tp.period_type = 'monthly'
    AND tp.month = (
        SELECT MAX(month) 
        FROM time_periods 
        WHERE year = (SELECT MAX(year) FROM time_periods)
    )
    GROUP BY fc.item_name, su.area_type
    ORDER BY fc.item_name, su.area_type;
    """
    return execute_query(query)

def get_state_food_sales_timeseries():
    """
    Gets food sales data for all states over time for visualization
    """
    query = """
    SELECT 
        r.region_name,
        UPPER(SUBSTRING(r.region_name, 1, 2)) as state_code,
        tp.year,
        sfs.total_sales_million
    FROM state_food_sales sfs
    JOIN regions r ON sfs.region_id = r.region_id
    JOIN time_periods tp ON sfs.period_id = tp.period_id
    WHERE r.region_type = 'state'
    ORDER BY tp.year, r.region_name;
    """
    return execute_query(query)

# Example visualization functions for plotting
def plot_income_inequality(df: pd.DataFrame):
    """Plot income inequality trends"""
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
    """Plot food price trends over time"""
    plt.figure(figsize=(12, 6))
    for region in df['region_name'].unique():
        region_data = df[df['region_name'] == region]
        plt.plot(
            pd.to_datetime(region_data['year'].astype(str) + '-' + 
                         region_data['month'].astype(str) + '-01'),
            region_data['price'],
            label=region
        )
    plt.title(f'Price Trends for {df["item_name"].iloc[0]}')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.show()