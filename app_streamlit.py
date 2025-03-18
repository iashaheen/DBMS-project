import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import folium
from streamlit_folium import folium_static
import json
import geopandas as gpd
from economic_analysis import *

# Page configuration
st.set_page_config(
    page_title="Economic Data Analysis",
    page_icon="📊",
    layout="wide"
)

# Database connection
@st.cache_resource
def get_database_connection():
    return create_engine("mysql+pymysql://root:@localhost/economic_data")

# Data loading functions with caching
@st.cache_data
def load_state_data():
    query = """
    SELECT r.region_name as state, 
           si.median_income_2023,
           sfs.total_sales_million
    FROM state_income si
    JOIN regions r ON si.region_id = r.region_id
    JOIN state_food_sales sfs ON si.region_id = sfs.region_id
    WHERE r.region_type = 'state'
    AND si.period_id = (SELECT MAX(period_id) FROM state_income)
    """
    return pd.read_sql(query, get_database_connection())

@st.cache_data
def load_food_items():
    query = "SELECT DISTINCT item_name FROM food_categories ORDER BY item_name"
    return pd.read_sql(query, get_database_connection())

@st.cache_data
def load_regions():
    query = "SELECT DISTINCT region_name FROM regions WHERE region_type = 'state' ORDER BY region_name"
    return pd.read_sql(query, get_database_connection())

# Sidebar for navigation
st.sidebar.title("Navigation")
page = st.sidebar.selectbox(
    "Choose a page",
    ["Home", "Income Analysis", "Food Prices", "CPI Analysis", "Custom Query"]
)

# Home page
if page == "Home":
    st.title("Economic Data Analysis Dashboard")
    st.write("Welcome to the Economic Data Analysis Dashboard. This interactive tool allows you to explore various economic indicators across the United States.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Quick Stats")
        stats_query = """
        SELECT 
            COUNT(DISTINCT r.region_name) as num_states,
            FORMAT(AVG(si.median_income_2023), 2) as avg_income,
            FORMAT(SUM(sfs.total_sales_million), 2) as total_sales
        FROM regions r
        JOIN state_income si ON r.region_id = si.region_id
        JOIN state_food_sales sfs ON r.region_id = sfs.region_id
        WHERE r.region_type = 'state'
        """
        stats = pd.read_sql(stats_query, get_database_connection())
        st.metric("Number of States", stats['num_states'].iloc[0])
        st.metric("Average State Median Income", f"${stats['avg_income'].iloc[0]}")
        st.metric("Total Food Sales (Millions)", f"${stats['total_sales'].iloc[0]}M")
    
    with col2:
        st.subheader("Latest Income Distribution")
        income_dist = load_state_data()
        fig = px.box(income_dist, y='median_income_2023', title="State Income Distribution")
        st.plotly_chart(fig, use_container_width=True)

elif page == "Income Analysis":
    st.title("Income Analysis")
    
    tab1, tab2 = st.tabs(["State Comparison", "Regional Analysis"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            state1 = st.selectbox("Select first state", load_regions()['region_name'])
        with col2:
            state2 = st.selectbox("Select second state", load_regions()['region_name'])
        
        if state1 and state2:
            comparison_data = compare_state_incomes(state1, state2)
            fig = px.line(comparison_data, 
                         x='year', 
                         y='median_income_2023',
                         color='state',
                         title=f"Income Comparison: {state1} vs {state2}")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        regional_data = analyze_income_inequality()
        fig = px.line(regional_data,
                     x='year',
                     y='income_gap',
                     color='region_name',
                     title='Income Inequality Gap by Region')
        st.plotly_chart(fig, use_container_width=True)

elif page == "Food Prices":
    st.title("Food Price Analysis")
    
    food_item = st.selectbox("Select Food Item", load_food_items()['item_name'])
    
    if food_item:
        price_data = analyze_food_price_trends(food_item)
        
        # Create date column for time series
        price_data['date'] = pd.to_datetime(
            price_data['year'].astype(str) + '-' + 
            price_data['month'].astype(str) + '-01'
        )
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            fig = px.line(price_data,
                         x='date',
                         y='price',
                         color='region_name',
                         title=f'Price Trends for {food_item}')
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Price Statistics")
            stats = price_data.groupby('region_name')['price'].agg(['mean', 'min', 'max']).round(2)
            st.dataframe(stats)

elif page == "CPI Analysis":
    st.title("CPI Analysis")
    
    region = st.selectbox("Select Region", load_regions()['region_name'])
    year = st.slider("Select Year", 2015, 2023, 2023)
    
    if region and year:
        cpi_data = analyze_cpi_by_category(region, year)
        
        fig = px.bar(cpi_data,
                    x='category',
                    y='cpi_value',
                    title=f'CPI Values by Category in {region} ({year})')
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

elif page == "Custom Query":
    st.title("Custom SQL Query")
    
    # Create a code editor-like text area
    query = st.text_area(
        "Enter your SQL query:",
        height=200,
        help="Write your SQL query here. Use proper MySQL syntax.",
        placeholder="SELECT * FROM regions LIMIT 5;"
    )
    
    # Add syntax highlighting and a dark theme
    st.markdown("""
        <style>
        .stTextArea textarea {
            font-family: 'Courier New', Courier, monospace;
            background-color: #1e1e1e;
            color: #d4d4d4;
        }
        </style>
    """, unsafe_allow_html=True)
    
    if st.button("Run Query"):
        try:
            if query:
                results = pd.read_sql(query, get_database_connection())
                st.write("Query Results:")
                st.dataframe(results)
                
                # Show download button if there are results
                if not results.empty:
                    st.download_button(
                        label="Download results as CSV",
                        data=results.to_csv(index=False).encode('utf-8'),
                        file_name="query_results.csv",
                        mime="text/csv"
                    )
        except Exception as e:
            st.error(f"Error executing query: {str(e)}")

# Footer
st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.info(
    "This dashboard provides interactive visualizations and analysis tools "
    "for exploring economic data across the United States."
)