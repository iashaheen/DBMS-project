# Economic Analysis Dashboard

A comprehensive data analysis platform that processes and visualizes various economic indicators including Consumer Price Index (CPI), food prices, income statistics, and state-level sales data.

## Features

- **Interactive Dashboard**: Built with Streamlit for real-time data exploration and visualization
- **Multiple Analysis Categories**:
  - Income Inequality Analysis
  - Food Price Trends
  - State Food Sales Rankings
  - CPI Analysis
  - Income Growth Analysis
  - State Income Comparison
  - Food Price Volatility
  - Seasonal Patterns
  - Price vs CPI Comparison
  - State Sales vs Income Analysis
- **Custom SQL Query Interface**: Direct database access for custom analysis
- **Data Visualization**: Interactive charts and maps using Plotly

## Prerequisites

- Python 3.x
- MySQL Server
- Required Python packages:
  ```
  streamlit
  pandas
  plotly
  mysql-connector-python
  ```

## Project Structure

- `dashboard.py`: Main Streamlit application with the user interface
- `economic_analysis.py`: Core analysis functions and database queries
- `etl.py`: ETL (Extract, Transform, Load) pipeline for data processing
- `schema.sql`: Database schema and table definitions

## Database Schema

The project uses a MySQL database with the following main tables:

- **Regions**: Geographic location reference data
- **Time Periods**: Temporal reference data
- **Food Categories & Prices**: Food item classifications and price data
- **CPI Data**: Consumer Price Index information
- **Sales & Income Data**: State-level sales and income statistics

## Setup Instructions

1. **Database Setup**
   ```bash
   mysql -u root -p < schema.sql
   ```

2. **Environment Setup**
   ```bash
   # Create a virtual environment (optional but recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install required packages
   pip install streamlit pandas plotly mysql-connector-python
   ```

3. **Database Configuration**
   - Update database credentials in `economic_analysis.py`:
     ```python
     host="localhost"
     user="econ_user"
     password="econ_pass"
     database="economic_data"
     ```

4. **Data Loading**
   ```bash
   python etl.py
   ```

5. **Running the Dashboard**
   ```bash
   streamlit run dashboard.py
   ```

## Data Sources

The project uses data from multiple sources, stored in the `data/` directory:
- CPI data (cpi_*.csv files)
- Food prices data (food_prices_*.csv files)
- Income statistics (income_*.csv files)
- State sales data (state_sales_*.csv files)

## Features in Detail

### Income Analysis
- Regional and state-level income comparisons
- Income inequality metrics
- Historical trends and growth analysis

### Price Analysis
- Food price trends and volatility
- Seasonal pattern detection
- CPI correlations

### Sales Analysis
- State-level food sales rankings
- Sales vs. income correlations
- Regional comparisons

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License

[Add your license information here]