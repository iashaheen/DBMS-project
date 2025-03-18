import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from economic_analysis import (
    analyze_income_inequality,
    analyze_food_price_trends,
    get_state_food_sales_rankings,
    analyze_cpi_by_category,
    analyze_income_growth,
    compare_state_incomes,
    analyze_food_price_volatility,
    analyze_regional_cpi_trends,
    analyze_income_sales_correlation,
    analyze_monthly_prices,
    analyze_regional_income_distribution,
    analyze_yoy_cpi_change,
    get_state_income_percentile,
    analyze_seasonal_patterns,
    execute_query,
    analyze_price_vs_cpi,
    analyze_avg_food_price_vs_cpi,
    analyze_state_sales_vs_income
)

# Add state name to code mapping
STATE_TO_CODE = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR', 'California': 'CA',
    'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE', 'Florida': 'FL', 'Georgia': 'GA',
    'Hawaii': 'HI', 'Idaho': 'ID', 'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA',
    'Kansas': 'KS', 'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS', 'Missouri': 'MO',
    'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
    'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', 'Ohio': 'OH',
    'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC',
    'South Dakota': 'SD', 'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY',
    'District of Columbia': 'DC'
}

# Set page config
st.set_page_config(
    page_title="Economic Analysis Dashboard",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("Economic Analysis Dashboard")

# Sidebar for navigation
analysis_type = st.sidebar.selectbox(
    "Select Analysis Type",
    [
        "Income Inequality Analysis",
        "Food Price Trends",
        "State Food Sales Rankings",
        "CPI Analysis",
        "Income Growth Analysis",
        "State Income Comparison",
        "Food Price Volatility",
        "Seasonal Patterns",
        "Price vs CPI Comparison",
        "State Sales vs Income Analysis"  # Added new option
    ]
)

# Helper function to get list of states
@st.cache_data
def get_states():
    query = """
    SELECT DISTINCT region_name 
    FROM regions 
    WHERE region_type = 'state' 
    ORDER BY region_name;
    """
    return execute_query(query)['region_name'].tolist()

# Helper function to get food items
@st.cache_data
def get_food_items():
    query = """
    SELECT DISTINCT item_name 
    FROM food_categories 
    ORDER BY item_name;
    """
    return execute_query(query)['item_name'].tolist()

# Helper function to get available years for state food sales
@st.cache_data
def get_state_food_sales_years():
    query = """
    SELECT MIN(year) as min_year, MAX(year) as max_year
    FROM time_periods tp
    JOIN state_food_sales sfs ON tp.period_id = sfs.period_id;
    """
    return execute_query(query).iloc[0]

# Helper function to get CPI categories
@st.cache_data
def get_cpi_categories():
    query = """
    SELECT DISTINCT item_name, item_code 
    FROM cpi_categories 
    ORDER BY item_name;
    """
    return execute_query(query)

# Helper function to get regions
@st.cache_data
def get_regions():
    query = """
    SELECT DISTINCT region_name 
    FROM regions 
    WHERE region_type IN ('division', 'region')
    ORDER BY region_name;
    """
    return execute_query(query)['region_name'].tolist()

# Main content based on selection
if analysis_type == "Income Inequality Analysis":
    st.header("Regional Income Inequality Analysis")
    
    df = analyze_income_inequality()
    
    fig = px.line(df, 
                  x='year', 
                  y='income_gap', 
                  color='region_name',
                  title='Income Inequality Gap by Region Over Time',
                  labels={'income_gap': 'Income Gap (Mean - Median) in 2023 Dollars',
                         'year': 'Year',
                         'region_name': 'Region'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(df)

elif analysis_type == "Food Price Trends":
    st.header("Food Price Trends Analysis")
    
    food_items = get_food_items()
    selected_item = st.selectbox("Select Food Item", food_items)
    
    df = analyze_food_price_trends(selected_item)
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')
    
    fig = px.line(df,
                  x='date',
                  y='price',
                  color='region_name',
                  title=f'Price Trends for {selected_item}',
                  labels={'price': 'Price',
                         'date': 'Date',
                         'region_name': 'Region'})
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "State Food Sales Rankings":
    st.header("State Food Sales Rankings")
    
    # Get year range from database
    year_range = get_state_food_sales_years()
    selected_year = st.slider("Select Year", 
                            min_value=int(year_range['min_year']), 
                            max_value=int(year_range['max_year']), 
                            value=int(year_range['max_year']))
    
    df = get_state_food_sales_rankings(selected_year)
    
    # Add state codes to the dataframe
    df['state_code'] = df['state'].map(STATE_TO_CODE)
    
    # Create choropleth map
    fig_map = go.Figure(data=go.Choropleth(
        locations=df['state_code'],  # Using state codes (e.g., 'CA', 'NY')
        z=df['total_sales_million'],  # Data to be color-coded
        locationmode='USA-states',  # Set of locations match entries in `locations`
        colorscale='Cividis',
        colorbar_title="Sales (Million $)"
    ))

    fig_map.update_layout(
        title=f'State Food Sales Heat Map ({selected_year})',
        geo_scope='usa',  # Limit map scope to USA
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        geo=dict(
            bgcolor='rgba(0,0,0,0)',    # Transparent geo background
            lakecolor='rgba(0,0,0,0)',  # Transparent lakes
            landcolor='rgba(0,0,0,0)',  # Transparent land
            subunitcolor='gray'         # State boundaries color
        )
    )
    st.plotly_chart(fig_map, use_container_width=True, transparent=True)
    
    # Original bar chart
    fig = px.bar(df,
                 x='state',
                 y='total_sales_million',
                 title=f'State Food Sales Rankings ({selected_year})',
                 labels={'total_sales_million': 'Total Sales (Million $)',
                        'state': 'State'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(df)

elif analysis_type == "CPI Analysis":
    st.header("CPI Analysis by Category and Region")
    
    col1, col2 = st.columns(2)
    with col1:
        filter_type = st.selectbox("Filter by", ["State", "Region"])
    
    with col2:
        if filter_type == "State":
            locations = get_states()
        else:
            locations = get_regions()
        selected_location = st.selectbox(f"Select {filter_type}", locations)
    
    selected_year = st.selectbox("Select Year", range(2023, 2019, -1))
    
    df = analyze_cpi_by_category(selected_location, selected_year)
    
    fig = px.bar(df,
                 x='category',
                 y='cpi_value',
                 title=f'CPI Values by Category in {selected_location} ({selected_year})',
                 labels={'cpi_value': 'CPI Value',
                        'category': 'Category'},
                 height=600)  # Increased height
    fig.update_layout(
        xaxis_tickangle=-45,
        margin=dict(b=150)  # Increased bottom margin to prevent label cutoff
    )
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "Income Growth Analysis":
    st.header("Regional Income Growth Analysis")
    
    df = analyze_income_growth()
    
    fig = px.bar(df,
                 x='region_name',
                 y='growth_rate',
                 title='Income Growth Rate by Region',
                 labels={'growth_rate': 'Growth Rate (%)',
                        'region_name': 'Region'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(df)

elif analysis_type == "State Income Comparison":
    st.header("State Income Comparison")
    
    states = get_states()
    col1, col2 = st.columns(2)
    with col1:
        state1 = st.selectbox("Select First State", states)
    with col2:
        state2 = st.selectbox("Select Second State", [s for s in states if s != state1])
    
    df = compare_state_incomes(state1, state2)
    
    fig = px.line(df,
                  x='year',
                  y='median_income_2023',
                  color='state',
                  title='Median Income Comparison',
                  labels={'median_income_2023': 'Median Income (2023 $)',
                         'year': 'Year'})
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "Food Price Volatility":
    st.header("Food Price Volatility Analysis")
    
    df = analyze_food_price_volatility()
    
    fig = px.bar(df,
                 x='item_name',
                 y='avg_volatility',
                 title='Food Price Volatility',
                 labels={'avg_volatility': 'Average Volatility',
                        'item_name': 'Food Item'},
                 height=700)  # Increased height
    fig.update_layout(
        xaxis_tickangle=-45,
        margin=dict(b=150)  # Increased bottom margin to prevent label cutoff
    )
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "Seasonal Patterns":
    st.header("Seasonal Price Patterns")
    
    df = analyze_seasonal_patterns()
    food_items = df['item_name'].unique()
    selected_item = st.selectbox("Select Food Item", food_items)
    
    item_df = df[df['item_name'] == selected_item]
    
    fig = px.line(item_df,
                  x='month',
                  y='avg_price',
                  title=f'Seasonal Price Pattern for {selected_item}',
                  labels={'avg_price': 'Average Price',
                         'month': 'Month'})
    fig.update_xaxes(ticktext=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
                     tickvals=list(range(1, 13)))
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "Price vs CPI Comparison":
    st.header("Price vs CPI Comparison Analysis")
    
    # Get inputs
    col1, col2 = st.columns(2)
    with col1:
        food_items = get_food_items()
        selected_food = st.selectbox("Select Food Item", food_items)
    
    with col2:
        cpi_categories_df = get_cpi_categories()
        selected_cpi = st.selectbox("Select CPI Category", cpi_categories_df['item_name'].tolist())
    
    # Get data for specific food item vs CPI
    df_specific = analyze_price_vs_cpi(selected_food, selected_cpi)
    df_specific['date'] = pd.to_datetime(df_specific['year'].astype(str) + '-' + df_specific['month'].astype(str) + '-01')
    
    # Create first plot
    fig1 = go.Figure()
    
    # Add food price line
    fig1.add_trace(go.Scatter(
        x=df_specific['date'],
        y=df_specific['avg_price'],
        name=f'{selected_food} Price',
        line=dict(color='blue')
    ))
    
    # Add CPI line on secondary y-axis
    fig1.add_trace(go.Scatter(
        x=df_specific['date'],
        y=df_specific['cpi_value'],
        name=f'{selected_cpi} CPI',
        yaxis='y2',
        line=dict(color='red')
    ))
    
    fig1.update_layout(
        title=f'{selected_food} Price vs {selected_cpi} CPI',
        yaxis=dict(title='Price ($)', side='left', showgrid=False),
        yaxis2=dict(title='CPI Value', side='right', overlaying='y', showgrid=False),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    st.plotly_chart(fig1, use_container_width=True)
    
    # Get data for average food prices vs CPI
    df_avg = analyze_avg_food_price_vs_cpi(selected_cpi)  # Now using the selected CPI instead of hardcoded value
    df_avg['date'] = pd.to_datetime(df_avg['year'].astype(str) + '-' + df_avg['month'].astype(str) + '-01')
    
    # Create second plot
    fig2 = go.Figure()
    
    # Add average food price line
    fig2.add_trace(go.Scatter(
        x=df_avg['date'],
        y=df_avg['avg_price'],
        name='Average Food Price',
        line=dict(color='green')
    ))
    
    # Add CPI line on secondary y-axis
    fig2.add_trace(go.Scatter(
        x=df_avg['date'],
        y=df_avg['cpi_value'],
        name=f'{selected_cpi} CPI',  # Updated to use selected CPI name
        yaxis='y2',
        line=dict(color='red')
    ))
    
    fig2.update_layout(
        title=f'Average Food Price vs {selected_cpi} CPI',  # Updated title to reflect selected CPI
        yaxis=dict(title='Price ($)', side='left', showgrid=False),
        yaxis2=dict(title='CPI Value', side='right', overlaying='y', showgrid=False),
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    st.plotly_chart(fig2, use_container_width=True)

elif analysis_type == "State Sales vs Income Analysis":
    st.header("State Food Sales vs Income Analysis")
    
    # Get input
    states = get_states()
    selected_state = st.selectbox("Select State", states)
    
    # Get data
    df = analyze_state_sales_vs_income(selected_state)
    
    # Create figure with secondary y-axis
    fig = go.Figure()
    
    # Add food sales line
    fig.add_trace(go.Scatter(
        x=df['year'],
        y=df['total_sales_million'],
        name='Food Sales',
        line=dict(color='blue')
    ))
    
    # Add income line on secondary y-axis
    fig.add_trace(go.Scatter(
        x=df['year'],
        y=df['median_income_2023'],
        name='Median Income',
        yaxis='y2',
        line=dict(color='green')
    ))
    
    fig.update_layout(
        title=f'Food Sales vs Median Income in {selected_state}',
        yaxis=dict(title='Total Sales (Million $)', side='left', showgrid=False),
        yaxis2=dict(title='Median Income (2023 $)', side='right', overlaying='y', showgrid=False),
        hovermode='x unified',
        xaxis=dict(title='Year'),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show correlation coefficient
    correlation = df['total_sales_million'].corr(df['median_income_2023'])
    st.write(f"Correlation coefficient between food sales and median income: {correlation:.3f}")
    
    # Show the data table
    st.dataframe(df)

# Add a footer with data source information
st.sidebar.markdown("---")
st.sidebar.markdown("Data source: Economic Database")
st.sidebar.markdown("Last updated: 2023")