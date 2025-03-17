# Economic Data Analysis System

## Schema Design Documentation

The database schema has been designed following 3rd Normal Form (3NF) principles to eliminate data redundancy and ensure data integrity. Here are the key design decisions:

### 1. Unified Location/Region Management
- `regions` table serves as a central reference for all geographic entities
- Handles different geographic hierarchies (state, division, region) using a type field
- Enables consistent joining across different datasets regardless of original geographic notation

### 2. Temporal Data Management
- `time_periods` table normalizes all temporal references
- Supports both monthly and yearly data granularity
- Enables efficient time-based queries and aggregations across datasets
- Facilitates joining datasets with different time granularities

### 3. Category Classifications
- Separate `food_categories` and `cpi_categories` tables maintain distinct classification systems
- Preserves original category codes while enabling cross-category analysis

### 4. Core Data Tables
- `food_prices`: Monthly food price data by item and region
- `cpi_values`: Monthly CPI data with base period reference
- `state_food_sales`: Annual food sales data by state
- `regional_income`: Regional income statistics with multiple metrics
- `state_income`: State-level income data with error margins

## Setup Instructions

1. Create MySQL Database:
```sql
CREATE DATABASE economic_data;
```

2. Set up environment variables:
```bash
export DB_HOST=localhost
export DB_USER=your_username
export DB_PASSWORD=your_password
export DB_NAME=economic_data
```

3. Install Python dependencies:
```bash
pip install pandas mysql-connector-python
```

4. Create database schema:
```bash
mysql -u $DB_USER -p$DB_PASSWORD $DB_NAME < schema.sql
```

5. Run ETL process:
```bash
python etl.py
```

## Schema Advantages

1. **Data Integrity**
   - Foreign key constraints ensure referential integrity
   - Normalized design prevents data anomalies

2. **Query Flexibility**
   - Efficient joins across different datasets
   - Support for various temporal aggregations
   - Geographic analysis at multiple levels

3. **Data Consistency**
   - Unified handling of geographic entities
   - Standardized time period representation
   - Consistent category management

4. **Extensibility**
   - New data sources can be easily integrated
   - Additional metrics can be added without schema changes
   - Supports future geographic or temporal expansions