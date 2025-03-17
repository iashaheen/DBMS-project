import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
from typing import Dict, List
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseETL:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.connection_config = {
            'host': host,
            'user': user,
            'password': password,
            'database': database
        }
        self.connection = None
        self.region_map = {}  # Cache for region IDs
        self.period_map = {}  # Cache for period IDs
        self.state_map = {}  # Cache for normalized state names
        
    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.connection_config)
            logger.info("Successfully connected to MySQL database")
        except Error as e:
            logger.error(f"Error connecting to MySQL database: {e}")
            raise

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")

    def _extract_states_from_area_name(self, area_name: str) -> list[str]:
        """Extract state codes from area name like 'City1-City2, ST1-ST2' or 'City, ST'"""
        # Handle special cases first
        if area_name in ['Urban Hawaii', 'Urban Alaska']:
            return [area_name.split()[1]]
            
        # Find everything after the comma
        parts = area_name.split(',')
        if len(parts) < 2:  # No state codes found
            return []
            
        # Get the state codes part and split on dash or space
        state_codes = parts[1].strip()
        return [code.strip() for code in state_codes.split('-')]

    def _normalize_state_name(self, state_code: str) -> str:
        """Convert state code to full state name"""
        state_map = {
            'AK': 'Alaska', 'AL': 'Alabama', 'AR': 'Arkansas', 'AZ': 'Arizona',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DC': 'District of Columbia',
            'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii',
            'IA': 'Iowa', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana',
            'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'MA': 'Massachusetts',
            'MD': 'Maryland', 'ME': 'Maine', 'MI': 'Michigan', 'MN': 'Minnesota',
            'MO': 'Missouri', 'MS': 'Mississippi', 'MT': 'Montana', 'NC': 'North Carolina',
            'ND': 'North Dakota', 'NE': 'Nebraska', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
            'NM': 'New Mexico', 'NV': 'Nevada', 'NY': 'New York', 'OH': 'Ohio',
            'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island',
            'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas',
            'UT': 'Utah', 'VA': 'Virginia', 'VT': 'Vermont', 'WA': 'Washington',
            'WI': 'Wisconsin', 'WV': 'West Virginia', 'WY': 'Wyoming'
        }
        return state_map.get(state_code, state_code)

    def _get_or_create_region(self, region_name: str, region_type: str) -> list[int]:
        """Get region IDs from cache or create new region entries. Returns list of region IDs."""
        # For city entries, extract and create state regions
        if ',' in region_name and region_type != 'division':
            state_codes = self._extract_states_from_area_name(region_name)
            region_ids = []
            for state_code in state_codes:
                state_name = self._normalize_state_name(state_code)
                if state_name in self.region_map:
                    region_ids.append(self.region_map[state_name])
                    continue
                    
                cursor = self.connection.cursor()
                cursor.execute(
                    "INSERT INTO regions (region_name, region_type) VALUES (%s, 'state') ON DUPLICATE KEY UPDATE region_id=LAST_INSERT_ID(region_id)",
                    (state_name,)
                )
                region_id = cursor.lastrowid
                self.region_map[state_name] = region_id
                region_ids.append(region_id)
            return region_ids
                
        # For non-city entries, behave as before
        if region_name in self.region_map:
            return [self.region_map[region_name]]
            
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO regions (region_name, region_type) VALUES (%s, %s) ON DUPLICATE KEY UPDATE region_id=LAST_INSERT_ID(region_id)",
            (region_name, region_type)
        )
        region_id = cursor.lastrowid
        self.region_map[region_name] = region_id
        return [region_id]

    def _get_or_create_period(self, year: int, month: int = None) -> int:
        """Get period ID from cache or create new period entry"""
        period_key = (year, month)
        if period_key in self.period_map:
            return self.period_map[period_key]
        
        period_type = 'monthly' if month else 'yearly'
        cursor = self.connection.cursor()
        cursor.execute(
            "INSERT INTO time_periods (year, month, period_type) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE period_id=LAST_INSERT_ID(period_id)",
            (year, month, period_type)
        )
        period_id = cursor.lastrowid
        self.period_map[period_key] = period_id
        return period_id

    def load_food_categories(self):
        """Load food categories from CSV"""
        df = pd.read_csv('data/food_prices_items.csv')
        cursor = self.connection.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO food_categories (item_code, item_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE item_name=%s",
                (row['Item_code'], row['Item_name'], row['Item_name'])
            )
        self.connection.commit()

    def load_cpi_categories(self):
        """Load CPI categories from CSV"""
        df = pd.read_csv('data/cpi_basket.csv')
        cursor = self.connection.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO cpi_categories (item_code, item_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE item_name=%s",
                (row['Item_code'], row['Item_name'], row['Item_name'])
            )
        self.connection.commit()

    def load_food_prices(self):
        """Load food prices data with both monthly and yearly aggregates"""
        series_df = pd.read_csv('data/food_prices_series.csv')
        metadata_df = pd.read_csv('data/food_prices_metadata.csv')
        areas_df = pd.read_csv('data/food_prices_area.csv')
        
        cursor = self.connection.cursor()
        
        # Group series by Series_id, Year for yearly aggregation
        yearly_aggs = series_df.groupby(['Series_id', 'Year'])['Value'].mean().reset_index()
        
        # Create a mapping to track values per state for averaging
        state_values = {}  # (state, period_id, item_code) -> [values]
        
        # Process each series
        for _, series in series_df.iterrows():
            metadata = metadata_df[metadata_df['Series_id'] == series['Series_id']].iloc[0]
            area = areas_df[areas_df['Area_code'] == metadata['Area_code']].iloc[0]
            
            # Get all region IDs (could be multiple for cities spanning states)
            region_ids = self._get_or_create_region(area['Area_name'], 'region')
            
            # Create period IDs
            monthly_period_id = self._get_or_create_period(
                series['Year'], 
                int(series['Period'].replace('M', '')))
            yearly_period_id = self._get_or_create_period(series['Year'])
            
            # Get yearly aggregate
            yearly_agg = yearly_aggs[
                (yearly_aggs['Series_id'] == series['Series_id']) & 
                (yearly_aggs['Year'] == series['Year'])
            ].iloc[0]
            
            # For each region (state), store values for later averaging
            for region_id in region_ids:
                key = (region_id, monthly_period_id, metadata['Item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append(series['Value'])
                
                key = (region_id, yearly_period_id, metadata['Item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append(yearly_agg['Value'])
        
        # Insert averaged values
        for (region_id, period_id, item_code), values in state_values.items():
            avg_value = sum(values) / len(values)
            cursor.execute("""
                INSERT INTO food_prices (series_id, region_id, item_code, period_id, price)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE price=%s
            """, (f"DERIVED_{region_id}_{period_id}", region_id, item_code, 
                  period_id, avg_value, avg_value))
            
        self.connection.commit()

    def load_cpi_data(self):
        """Load CPI data with both monthly and yearly aggregates"""
        series_df = pd.read_csv('data/cpi_series.csv')
        metadata_df = pd.read_csv('data/cpi_metadata.csv')
        areas_df = pd.read_csv('data/cpi_area.csv')
        
        cursor = self.connection.cursor()
        
        # Group series by Series_id, Year for yearly aggregation
        yearly_aggs = series_df.groupby(['Series_id', 'Year'])['Value'].mean().reset_index()
        
        # Create a mapping to track values per state for averaging
        state_values = {}  # (state, period_id, item_code) -> [(value, base_period, base_value)]
        
        # Process each series
        for _, series in series_df.iterrows():
            metadata = metadata_df[metadata_df['Series_id'] == series['Series_id']].iloc[0]
            area = areas_df[areas_df['Area_code'] == metadata['Area_code']].iloc[0]
            
            # Split base_period into period and value
            base_period_parts = metadata['Base_period'].split('=')
            base_period = base_period_parts[0].strip()
            base_value = float(base_period_parts[1].strip())
            
            # Get all region IDs (could be multiple for cities spanning states)
            region_ids = self._get_or_create_region(area['Area_name'], 'region')
            
            # Create period IDs
            monthly_period_id = self._get_or_create_period(
                series['Year'], 
                int(series['Period'].replace('M', '')))
            yearly_period_id = self._get_or_create_period(series['Year'])
            
            # Get yearly aggregate
            yearly_agg = yearly_aggs[
                (yearly_aggs['Series_id'] == series['Series_id']) & 
                (yearly_aggs['Year'] == series['Year'])
            ].iloc[0]
            
            # For each region (state), store values for later averaging
            for region_id in region_ids:
                key = (region_id, monthly_period_id, metadata['Item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append((series['Value'], base_period, base_value))
                
                key = (region_id, yearly_period_id, metadata['Item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append((yearly_agg['Value'], base_period, base_value))
        
        # Insert averaged values
        for (region_id, period_id, item_code), value_tuples in state_values.items():
            avg_value = sum(v[0] for v in value_tuples) / len(value_tuples)
            # Use the most common base period and value
            base_periods = {}
            for _, bp, bv in value_tuples:
                if (bp, bv) not in base_periods:
                    base_periods[(bp, bv)] = 0
                base_periods[(bp, bv)] += 1
            base_period, base_value = max(base_periods.items(), key=lambda x: x[1])[0]
            
            cursor.execute("""
                INSERT INTO cpi_values (series_id, region_id, item_code, period_id, value,
                                      base_period, base_value)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    value=%s, base_period=%s, base_value=%s
            """, (f"DERIVED_{region_id}_{period_id}", region_id, item_code, 
                  period_id, avg_value, base_period, base_value,
                  avg_value, base_period, base_value))
            
        self.connection.commit()

    def load_state_food_sales(self):
        """Load state food sales data"""
        df = pd.read_csv('data/state_sales_no_taxes_tips.csv')
        cursor = self.connection.cursor()
        
        for _, row in df.iterrows():
            # State names here are already in the desired format
            region_id = self._get_or_create_region(row['State'], 'state')
            period_id = self._get_or_create_period(row['Year'])
            
            cursor.execute("""
                INSERT INTO state_food_sales (region_id, period_id, total_sales_million)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE total_sales_million=%s
            """, (region_id, period_id, row['Total_sales_million'], row['Total_sales_million']))
            
        self.connection.commit()

    def load_regional_income(self):
        """Load regional income data"""
        df = pd.read_csv('data/income_by_region.csv')
        cursor = self.connection.cursor()
        
        for _, row in df.iterrows():
            region_id = self._get_or_create_region(row['Region'], 'region')
            period_id = self._get_or_create_period(row['Year'])
            
            cursor.execute("""
                INSERT INTO regional_income 
                (region_id, period_id, households_thousands, median_income_current, 
                median_income_2023, mean_income_current, mean_income_2023)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                households_thousands=%s, median_income_current=%s, 
                median_income_2023=%s, mean_income_current=%s, mean_income_2023=%s
            """, (region_id, period_id, row['Number_thousands'], 
                  row['Median_income_Current_dollars'], row['Median_income_2023_dollars'],
                  row['Mean_income_Current_dollars'], row['Mean_income_2023_dollars'],
                  row['Number_thousands'], row['Median_income_Current_dollars'],
                  row['Median_income_2023_dollars'], row['Mean_income_Current_dollars'],
                  row['Mean_income_2023_dollars']))
            
        self.connection.commit()

    def load_state_income(self):
        """Load state income data"""
        current_df = pd.read_csv('data/income_by_state_current_dollars.csv')
        adjusted_df = pd.read_csv('data/income_by_state_2023_dollars.csv')
        cursor = self.connection.cursor()
        
        # Process each state
        for _, row in current_df.iterrows():
            # State names here are already in the desired format
            state = row['State']
            region_id = self._get_or_create_region(state, 'state')
            adjusted_row = adjusted_df[adjusted_df['State'] == state].iloc[0]
            
            # Process each year's data
            for year in range(1984, 2024):
                if f"{year}_Median_income" in row:
                    period_id = self._get_or_create_period(year)
                    
                    cursor.execute("""
                        INSERT INTO state_income 
                        (region_id, period_id, median_income_current, median_income_2023,
                        standard_error_current, standard_error_2023)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                        median_income_current=%s, median_income_2023=%s,
                        standard_error_current=%s, standard_error_2023=%s
                    """, (region_id, period_id, 
                          row[f"{year}_Median_income"], adjusted_row[f"{year}_Median_income"],
                          row[f"{year}_Standard_error"], adjusted_row[f"{year}_Standard_error"],
                          row[f"{year}_Median_income"], adjusted_row[f"{year}_Median_income"],
                          row[f"{year}_Standard_error"], adjusted_row[f"{year}_Standard_error"]))
                    
        self.connection.commit()

    def execute_etl(self):
        """Execute the full ETL process"""
        try:
            self.connect()
            logger.info("Starting ETL process...")
            
            # Load reference data first
            self.load_food_categories()
            self.load_cpi_categories()
            
            # Load main data
            self.load_food_prices()
            self.load_cpi_data()
            self.load_state_food_sales()
            self.load_regional_income()
            self.load_state_income()
            
            logger.info("ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"Error during ETL process: {e}")
            raise
        finally:
            self.close()

if __name__ == "__main__":
    # Load database configuration from environment variables
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'economic_data')
    }
    
    etl = DatabaseETL(**db_config)
    etl.execute_etl()