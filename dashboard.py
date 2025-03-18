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
    analyze_price_ranges,
    get_state_income_percentile,
    analyze_seasonal_patterns,
    execute_query
)

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
        "Price Range Analysis",
        "Seasonal Patterns"
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

# Helper function to get CPI categories
@st.cache_data
def get_cpi_categories():
    query = """
    SELECT DISTINCT item_name, item_code 
    FROM cpi_categories 
    ORDER BY item_name;
    """
    return execute_query(query)

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
    
    df = get_state_food_sales_rankings()
    
    fig = px.bar(df,
                 x='state',
                 y='total_sales_million',
                 title='State Food Sales Rankings (2023)',
                 labels={'total_sales_million': 'Total Sales (Million $)',
                        'state': 'State'})
    st.plotly_chart(fig, use_container_width=True)
    
    st.dataframe(df)

elif analysis_type == "CPI Analysis":
    st.header("CPI Analysis by Category and Region")
    
    states = get_states()
    selected_state = st.selectbox("Select State", states)
    selected_year = st.selectbox("Select Year", range(2023, 2019, -1))
    
    df = analyze_cpi_by_category(selected_state, selected_year)
    
    fig = px.bar(df,
                 x='category',
                 y='cpi_value',
                 title=f'CPI Values by Category in {selected_state} ({selected_year})',
                 labels={'cpi_value': 'CPI Value',
                        'category': 'Category'})
    fig.update_layout(xaxis_tickangle=-45)
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
                        'item_name': 'Food Item'})
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

elif analysis_type == "Price Range Analysis":
    st.header("Food Price Range Analysis")
    
    df = analyze_price_ranges()
    
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name='Price Range',
        x=df['item_name'],
        y=df['price_range'],
        marker_color='lightblue'
    ))
    fig.add_trace(go.Scatter(
        name='Average Price',
        x=df['item_name'],
        y=df['avg_price'],
        mode='markers',
        marker=dict(color='red', size=8)
    ))
    fig.update_layout(
        title='Price Ranges and Averages by Food Item',
        xaxis_tickangle=-45,
        barmode='overlay'
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

# Add a footer with data source information
st.sidebar.markdown("---")
st.sidebar.markdown("Data source: Economic Database")
st.sidebar.markdown("Last updated: 2023")