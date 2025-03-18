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
    ["Home", "Income Analysis", "Food Prices", "CPI Analysis", "Analysis Insights", "Custom Query"]
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

    st.markdown("---")
    st.subheader("State Food Sales Over Time")
    
    # Get sales data
    sales_data = get_state_food_sales_timeseries()
    if not sales_data.empty:
        years = sorted(sales_data['year'].unique())
        selected_year = st.slider("Select Year", min_value=min(years), max_value=max(years), value=max(years))
        
        # Filter data for selected year
        year_data = sales_data[sales_data['year'] == selected_year]
        
        st.markdown("""
            This map visualizes the distribution of food sales across US states. 
            Darker blue shades indicate higher sales volumes, helping identify key economic centers 
            and regional patterns in the food industry. Use the year slider to explore historical trends.
        """)
        
        # Create choropleth map
        fig = px.choropleth(
            year_data,
            locations='state_code',
            locationmode='USA-states',
            color='total_sales_million',
            scope='usa',
            color_continuous_scale=[[0, '#e6f3ff'], 
                                  [0.2, '#a6d3ff'],
                                  [0.4, '#66b3ff'],
                                  [0.6, '#3399ff'],
                                  [0.8, '#0080ff'],
                                  [1, '#004d99']],  # Custom blue gradient
            title=f'State Food Sales in {selected_year}',
            labels={'total_sales_million': 'Food Sales (Millions USD)'},
            custom_data=['region_name', 'total_sales_million']
        )
        
        # Customize hover template
        fig.update_traces(
            hovertemplate="<br>".join([
                "<b>%{customdata[0]}</b>",
                "Sales: $%{customdata[1]:.1f}M",
                "<extra></extra>"
            ])
        )
        
        # Update layout with dark theme compatible colors
        fig.update_layout(
            coloraxis_colorbar=dict(
                title="Sales (Millions USD)",
                tickformat="$.0f",
                title_font_color='white',
                tickfont_color='white',
            ),
            margin=dict(r=0, l=0, t=30, b=0),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=600,
            geo=dict(
                bgcolor='rgba(0,0,0,0)',
                lakecolor='#1e2f4d',
                landcolor='#0d1117',
                subunitcolor='#1f2937',
                showlakes=True,
                showland=True,
                showsubunits=True
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Add summary metrics below the map
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "Total Sales",
                f"${year_data['total_sales_million'].sum():.0f}M",
                f"{((year_data['total_sales_million'].sum() / sales_data[sales_data['year'] == selected_year-1]['total_sales_million'].sum() - 1) * 100):.1f}% vs prev year" if selected_year > min(years) else None
            )
        
        with col2:
            st.metric(
                "Average per State",
                f"${year_data['total_sales_million'].mean():.0f}M"
            )
        
        with col3:
            st.metric(
                "Sales Range",
                f"${year_data['total_sales_million'].max() - year_data['total_sales_million'].min():.0f}M"
            )

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

elif page == "Analysis Insights":
    st.title("Analysis Insights")
    
    tab1, tab2, tab3, tab4 = st.tabs([
        "Income-Sales Correlation", 
        "Regional Price Analysis", 
        "Seasonal Patterns",
        "Urban vs Rural Comparison"
    ])
    
    with tab1:
        st.subheader("Income and Food Sales Relationship")
        st.markdown("""
            This analysis explores the relationship between median income and food sales across states,
            helping identify economic patterns and market opportunities.
        """)
        
        correlation_data = analyze_income_sales_correlation()
        
        # Create scatter plot
        fig = px.scatter(
            correlation_data,
            x='median_income_2023',
            y='total_sales_million',
            text='state',
            trendline="ols",
            title='State Median Income vs Food Sales',
            labels={
                'median_income_2023': 'Median Income (2023 USD)',
                'total_sales_million': 'Food Sales (Millions USD)'
            }
        )
        fig.update_traces(
            textposition='top center',
            marker=dict(size=10)
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Calculate correlation coefficient
        correlation = correlation_data['median_income_2023'].corr(correlation_data['total_sales_million'])
        st.metric("Correlation Coefficient", f"{correlation:.3f}")
        
        # Add interpretation
        if correlation > 0.7:
            st.markdown("**Strong positive correlation** indicates that states with higher median incomes tend to have significantly higher food sales.")
        elif correlation > 0.4:
            st.markdown("**Moderate positive correlation** suggests some relationship between income levels and food sales, though other factors likely play important roles.")
        else:
            st.markdown("**Weak correlation** indicates that food sales may be more influenced by factors other than median income.")
    
    with tab2:
        st.subheader("Regional Price Disparities")
        st.markdown("""
            Analyze price variations across regions for different food items.
            This helps identify regional market inefficiencies and pricing patterns.
        """)
        
        disparities = analyze_regional_price_disparities()
        
        # Create bar chart
        fig = px.bar(
            disparities,
            x='item_name',
            y='coefficient_of_variation',
            title='Price Variation by Food Item',
            labels={
                'coefficient_of_variation': 'Coefficient of Variation (%)',
                'item_name': 'Food Item'
            }
        )
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show detailed statistics
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### Highest Price Variations")
            st.dataframe(
                disparities.head(5)[['item_name', 'coefficient_of_variation', 'price_gap']]
                .round(2)
                .style.background_gradient(subset=['coefficient_of_variation'], cmap='Blues')
            )
        
        with col2:
            st.markdown("### Price Range Analysis")
            st.dataframe(
                disparities[['item_name', 'national_avg', 'price_gap']]
                .sort_values('price_gap', ascending=False)
                .head(5)
                .round(2)
                .style.background_gradient(subset=['price_gap'], cmap='Blues')
            )
    
    with tab3:
        st.subheader("Seasonal Price Patterns")
        st.markdown("""
            Explore how food prices vary seasonally throughout the year.
            Understanding these patterns can help with price forecasting and planning.
        """)
        
        seasonal_data = analyze_seasonal_patterns()
        
        # Let user select an item
        selected_item = st.selectbox(
            "Select Food Item",
            pd.unique(seasonal_data['item_name']),
            key='seasonal_analysis'
        )
        
        if selected_item:
            item_data = seasonal_data[seasonal_data['item_name'] == selected_item]
            
            # Create seasonal pattern visualization
            fig = go.Figure()
            
            # Add price range area
            fig.add_trace(go.Scatter(
                x=item_data['month'],
                y=item_data['max_price'],
                fill=None,
                mode='lines',
                line_color='rgba(0,100,255,0.2)',
                name='Price Range'
            ))
            
            fig.add_trace(go.Scatter(
                x=item_data['month'],
                y=item_data['min_price'],
                fill='tonexty',
                mode='lines',
                line_color='rgba(0,100,255,0.2)',
                name='Price Range'
            ))
            
            # Add average price line
            fig.add_trace(go.Scatter(
                x=item_data['month'],
                y=item_data['avg_price'],
                mode='lines+markers',
                name='Average Price',
                line=dict(color='rgb(0,100,255)', width=2)
            ))
            
            fig.update_layout(
                title=f'Seasonal Price Pattern: {selected_item}',
                xaxis_title='Month',
                yaxis_title='Price (USD)',
                xaxis=dict(
                    tickmode='array',
                    ticktext=[calendar.month_abbr[m] for m in range(1, 13)],
                    tickvals=list(range(1, 13))
                )
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show seasonal insights
            col1, col2 = st.columns(2)
            with col1:
                peak_month = item_data.loc[item_data['seasonal_index'].idxmax()]
                st.markdown(f"""
                    ### Peak Season
                    - Month: {calendar.month_name[int(peak_month['month'])]}
                    - Average Price: ${peak_month['avg_price']:.2f}
                    - Seasonal Index: {peak_month['seasonal_index']:.1f}%
                """)
            
            with col2:
                trough_month = item_data.loc[item_data['seasonal_index'].idxmin()]
                st.markdown(f"""
                    ### Low Season
                    - Month: {calendar.month_name[int(trough_month['month'])]}
                    - Average Price: ${trough_month['avg_price']:.2f}
                    - Seasonal Index: {trough_month['seasonal_index']:.1f}%
                """)
    
    with tab4:
        st.subheader("Urban vs Rural Price Comparison")
        st.markdown("""
            Compare food prices between urban and rural areas to understand
            geographic price distributions and market accessibility.
        """)
        
        urban_rural = analyze_urban_rural_prices()
        
        # Let user select an item
        selected_item = st.selectbox(
            "Select Food Item",
            pd.unique(urban_rural['item_name']),
            key='urban_rural'
        )
        
        if selected_item:
            item_data = urban_rural[urban_rural['item_name'] == selected_item]
            
            # Create comparison visualization
            fig = go.Figure()
            
            for area in ['Urban', 'Rural']:
                area_data = item_data[item_data['area_type'] == area]
                fig.add_box(
                    y=[area_data['min_price'].iloc[0], 
                       area_data['avg_price'].iloc[0],
                       area_data['max_price'].iloc[0]],
                    name=area,
                    boxpoints='all',
                    jitter=0.3,
                    pointpos=-1.8
                )
            
            fig.update_layout(
                title=f'Price Distribution: {selected_item}',
                yaxis_title='Price (USD)',
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Show statistics
            col1, col2 = st.columns(2)
            with col1:
                st.dataframe(
                    item_data[['area_type', 'num_states', 'avg_price', 'price_std']]
                    .round(2)
                    .style.background_gradient(subset=['avg_price'], cmap='Blues')
                )
            
            with col2:
                price_diff = (
                    item_data[item_data['area_type'] == 'Urban']['avg_price'].iloc[0] -
                    item_data[item_data['area_type'] == 'Rural']['avg_price'].iloc[0]
                )
                price_diff_pct = (price_diff / item_data[item_data['area_type'] == 'Rural']['avg_price'].iloc[0]) * 100
                
                st.markdown(f"""
                    ### Price Gap Analysis
                    - Absolute Difference: ${abs(price_diff):.2f}
                    - Percentage Difference: {price_diff_pct:+.1f}%
                    - {'Urban prices are higher' if price_diff > 0 else 'Rural prices are higher'}
                """)

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