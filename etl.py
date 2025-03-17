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
        # Handle special cases - map Urban Alaska/Hawaii directly to state names
        if area_name == 'Urban Alaska':
            return ['Alaska']
        if area_name == 'Urban Hawaii':
            return ['Hawaii']
            
        # Find everything after the comma
        parts = area_name.split(',')
        if len(parts) < 2:  # No state codes found
            # For special region names without state codes, return empty list
            # These will be handled as regions rather than states
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
        if ',' in region_name or region_name in ['Urban Alaska', 'Urban Hawaii']:
            state_codes = self._extract_states_from_area_name(region_name)
            if state_codes:  # Only process if we found valid state codes
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
                
        # For non-city entries or when no state codes found, create/get as a region
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

    def _parse_period(self, period_str: str) -> int:
        """Convert period string to month number"""
        if period_str.startswith('M'):
            return int(period_str.replace('M', ''))
        elif period_str.startswith('S'):
            # Convert semester to middle month (S01 -> 6, S02 -> 12)
            return int(period_str.replace('S', '')) * 6
        else:
            raise ValueError(f"Unknown period format: {period_str}")

    def load_food_categories(self):
        """Load food categories from CSV"""
        df = pd.read_csv('data/food_prices_items.csv')
        cursor = self.connection.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO food_categories (item_code, item_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE item_name=%s",
                (row['item_code'], row['item_name'], row['item_name'])
            )
        self.connection.commit()

    def load_cpi_categories(self):
        """Load CPI categories from CSV"""
        df = pd.read_csv('data/cpi_basket.csv')
        cursor = self.connection.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO cpi_categories (item_code, item_name) VALUES (%s, %s) ON DUPLICATE KEY UPDATE item_name=%s",
                (row['item_code'], row['item_name'], row['item_name'])
            )
        self.connection.commit()

    def load_food_prices(self):
        """Load food prices data with both monthly and yearly aggregates"""
        # Read CSVs and clean column names
        series_df = pd.read_csv('data/food_prices_series.csv', low_memory=False)
        series_df.columns = series_df.columns.str.strip()
        # Convert value column to float
        series_df['value'] = pd.to_numeric(series_df['value'], errors='coerce')
        
        metadata_df = pd.read_csv('data/food_prices_metadata.csv')
        metadata_df.columns = metadata_df.columns.str.strip()
        
        areas_df = pd.read_csv('data/food_prices_area.csv')
        areas_df.columns = areas_df.columns.str.strip()
        
        cursor = self.connection.cursor()
        
        # Create a mapping to track values per state for averaging
        state_values = {}  # (state, period_id, item_code) -> [values]
        
        # Process each series
        for _, series in series_df.iterrows():
            if pd.isna(series['value']):  # Skip rows with invalid values
                continue
                
            metadata = metadata_df[metadata_df['series_id'] == series['series_id']].iloc[0]
            area = areas_df[areas_df['area_code'] == metadata['area_code']].iloc[0]
            
            # Get all region IDs (could be multiple for cities spanning states)
            region_ids = self._get_or_create_region(area['area_name'], 'region')
            
            # Create period IDs
            monthly_period_id = self._get_or_create_period(
                int(series['year']), 
                int(series['period'].replace('M', '')))
            yearly_period_id = self._get_or_create_period(int(series['year']))
            
            # For each region (state), store values for later averaging
            for region_id in region_ids:
                key = (region_id, monthly_period_id, metadata['item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append(float(series['value']))
                
                key = (region_id, yearly_period_id, metadata['item_code'])
                if key not in state_values:
                    state_values[key] = []
                state_values[key].append(float(series['value']))  # Use the same value for yearly
        
        # Insert averaged values
        for (region_id, period_id, item_code), values in state_values.items():
            if values:  # Only process if we have valid values
                avg_value = sum(values) / len(values)
                cursor.execute("""
                    INSERT INTO food_prices (region_id, item_code, period_id, price)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE price=%s
                """, (region_id, item_code, period_id, avg_value, avg_value))
        
        self.connection.commit()

    def load_cpi_data(self):
        """Load CPI data with both monthly and yearly aggregates"""
        # Read CSVs and clean column names
        series_df = pd.read_csv('data/cpi_series.csv')
        series_df.columns = series_df.columns.str.strip()
        # Convert value column to float
        series_df['value'] = pd.to_numeric(series_df['value'], errors='coerce')
        
        metadata_df = pd.read_csv('data/cpi_metadata.csv')
        metadata_df.columns = metadata_df.columns.str.strip()
        
        areas_df = pd.read_csv('data/cpi_area.csv')
        areas_df.columns = areas_df.columns.str.strip()
        
        cursor = self.connection.cursor()
        
        # Create a mapping to track values per state for averaging
        state_values = {}  # (state, period_id, item_code) -> [(value, base_period, base_value)]
        
        # Process each series
        for _, series in series_df.iterrows():
            if pd.isna(series['value']):  # Skip rows with invalid values
                continue
                
            metadata = metadata_df[metadata_df['series_id'] == series['series_id']].iloc[0]
            area = areas_df[areas_df['area_code'] == metadata['area_code']].iloc[0]
            
            # Split base_period into period and value
            base_period_parts = metadata['base_period'].split('=')
            base_period = base_period_parts[0].strip()
            base_value = float(base_period_parts[1].strip())
            
            # Get all region IDs (could be multiple for cities spanning states)
            region_ids = self._get_or_create_region(area['area_name'], 'region')
            
            try:
                # Create period IDs
                month = self._parse_period(series['period'])
                monthly_period_id = self._get_or_create_period(
                    int(series['year']), month)
                yearly_period_id = self._get_or_create_period(int(series['year']))
                
                # For each region (state), store values for later averaging
                for region_id in region_ids:
                    key = (region_id, monthly_period_id, metadata['item_code'])
                    if key not in state_values:
                        state_values[key] = []
                    state_values[key].append((float(series['value']), base_period, base_value))
                    
                    key = (region_id, yearly_period_id, metadata['item_code'])
                    if key not in state_values:
                        state_values[key] = []
                    state_values[key].append((float(series['value']), base_period, base_value))
            except ValueError as e:
                logger.warning(f"Skipping invalid period format: {e}")
                continue
        
        # Insert averaged values
        for (region_id, period_id, item_code), value_tuples in state_values.items():
            if value_tuples:  # Only process if we have valid values
                avg_value = sum(v[0] for v in value_tuples) / len(value_tuples)
                # Use the most common base period and value
                base_periods = {}
                for _, bp, bv in value_tuples:
                    if (bp, bv) not in base_periods:
                        base_periods[(bp, bv)] = 0
                    base_periods[(bp, bv)] += 1
                base_period, base_value = max(base_periods.items(), key=lambda x: x[1])[0]
                
                cursor.execute("""
                    INSERT INTO cpi_values (region_id, item_code, period_id, value,
                                          base_period, base_value)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        value=%s, base_period=%s, base_value=%s
                """, (region_id, item_code, period_id, 
                      avg_value, base_period, base_value,
                      avg_value, base_period, base_value))
                
        self.connection.commit()

    def load_state_food_sales(self):
        """Load state food sales data"""
        df = pd.read_csv('data/state_sales_no_taxes_tips.csv')
        cursor = self.connection.cursor()
        
        for _, row in df.iterrows():
            # State names here are already in the desired format
            region_ids = self._get_or_create_region(row['State'], 'state')
            region_id = region_ids[0]  # Take first region ID since states map to single regions
            period_id = self._get_or_create_period(row['Year'])
            
            # Convert sales value to proper decimal format
            sales_value = float(str(row['Total_sales_million']).replace(',', ''))
            
            cursor.execute("""
                INSERT INTO state_food_sales (region_id, period_id, total_sales_million)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE total_sales_million=%s
            """, (region_id, period_id, sales_value, sales_value))
            
        self.connection.commit()

    def load_regional_income(self):
        """Load regional income data"""
        df = pd.read_csv('data/income_by_region.csv')
        cursor = self.connection.cursor()
        
        for _, row in df.iterrows():
            region_ids = self._get_or_create_region(row['Region'], 'region')
            region_id = region_ids[0]  # Take first region ID
            
            # Extract just the year number before the parentheses
            year = int(str(row['Year']).split()[0])
            period_id = self._get_or_create_period(year)
            
            # Clean number formatting
            households = float(str(row['Number_thousands']).replace(',', ''))
            median_current = float(str(row['Median_income_Current_dollars']).replace(',', ''))
            median_2023 = float(str(row['Median_income_2023_dollars']).replace(',', ''))
            mean_current = float(str(row['Mean_income_Current_dollars']).replace(',', ''))
            mean_2023 = float(str(row['Mean_income_2023_dollars']).replace(',', ''))
            
            cursor.execute("""
                INSERT INTO regional_income 
                (region_id, period_id, households_thousands, median_income_current, 
                median_income_2023, mean_income_current, mean_income_2023)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                households_thousands=%s, median_income_current=%s, 
                median_income_2023=%s, mean_income_current=%s, mean_income_2023=%s
            """, (region_id, period_id, households, 
                  median_current, median_2023,
                  mean_current, mean_2023,
                  households, median_current,
                  median_2023, mean_current, mean_2023))
            
        self.connection.commit()

    def load_state_income(self):
        """Load state income data"""
        current_df = pd.read_csv('data/income_by_state_current_dollars.csv')
        adjusted_df = pd.read_csv('data/income_by_state_2023_dollars.csv')
        cursor = self.connection.cursor()
        
        # Process each state
        for _, row in current_df.iterrows():
            state = row['State']
            region_ids = self._get_or_create_region(state, 'state')
            region_id = region_ids[0]
            adjusted_row = adjusted_df[adjusted_df['State'] == state].iloc[0]
            
            # Process each year's data
            for year in range(1984, 2024):
                # Handle different column name formats
                year_current = f"{year}_Median_income"  # Format in current dollars file
                year_adj = f"{year} Median income"      # Format in 2023 dollars file
                error_current = f"{year}_Standard_error"
                error_adj = f"{year} Standard error"
                
                # Skip if the columns don't exist
                if year_current not in row.index or year_adj not in adjusted_row.index:
                    # logger.debug(f"Skipping {state} {year}: column not found")
                    continue
                    
                if pd.isna(row[year_current]) or pd.isna(adjusted_row[year_adj]):
                    # logger.debug(f"Skipping {state} {year}: NaN value")
                    continue
                    
                # Clean numeric values by removing quotes and commas
                try:
                    current_val = str(row[year_current]).replace(',', '').replace('"', '')
                    adj_val = str(adjusted_row[year_adj]).replace(',', '').replace('"', '')
                    current_err = str(row[error_current]).replace(',', '').replace('"', '')
                    adj_err = str(adjusted_row[error_adj]).replace(',', '').replace('"', '')
                    
                    # logger.debug(f"Processing {state} {year}: {current_val}, {adj_val}")
                    
                    median_current = float(current_val)
                    median_2023 = float(adj_val)
                    error_current = float(current_err)
                    error_2023 = float(adj_err)
                    
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
                          median_current, median_2023,
                          error_current, error_2023,
                          median_current, median_2023,
                          error_current, error_2023))

                    # logger.debug(f"Inserted data for {state} {year}")
                    
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid data for {state} in {year}: {e}")
                    continue
                    
        self.connection.commit()

    def execute_etl(self):
        """Execute the full ETL process"""
        try:
            logger.info("Connecting to the Database...")
            self.connect()
            logger.info("Starting ETL process...")
            
            # Load reference data first
            self.load_food_categories()
            logger.info("Loaded food categories")
            self.load_cpi_categories()
            logger.info("Loaded CPI categories")
            
            # Load main data
            self.load_food_prices()
            logger.info("Loaded food prices")
            self.load_cpi_data()
            logger.info("Loaded CPI data")
            self.load_state_food_sales()
            logger.info("Loaded state food sales")
            self.load_regional_income()
            logger.info("Loaded regional income")
            self.load_state_income()
            logger.info("Loaded state income")
            
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
        'user': os.getenv('DB_USER', 'econ_user'),
        'password': os.getenv('DB_PASSWORD', 'econ_pass'),
        'database': os.getenv('DB_NAME', 'economic_data')
    }
    
    etl = DatabaseETL(**db_config)
    etl.execute_etl()