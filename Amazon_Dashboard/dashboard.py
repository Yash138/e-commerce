import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Amazon_Scraper.helpers.postgres_handler import PostgresDBHandler
from Amazon_Scraper.settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT
import streamlit as st
import pandas as pd
import plotly.express as px

TOP_SALES_QUERY = """
    SELECT category, 
        SUM(total_sales) AS total_sales
    FROM curated.vw_amz__category_performance
    GROUP BY category
    ORDER BY total_sales DESC
    LIMIT 10;
"""

TOP_VOLUME_QUERY = """
    SELECT category, 
        SUM(total_volume) AS total_volume
    FROM curated.vw_amz__category_performance
    GROUP BY category
    ORDER BY total_volume DESC
    LIMIT 10;
"""

@st.cache_data
def load_data(query):
    # It's good practice to ensure the handler is connected before use inside a cached function
    # or pass a connected handler
    # with PostgresDBHandler(
    #     POSTGRES_HOST,
    #     POSTGRES_DATABASE,
    #     POSTGRES_USERNAME,
    #     POSTGRES_PASSWORD,
    #     POSTGRES_PORT
    # ) as handler: # Using 'with' ensures proper connection closing
    #     with st.spinner("Loading Data..."):
    #         df = pd.DataFrame(handler.read(query=query))
    postgres_handler =  PostgresDBHandler(
            POSTGRES_HOST,
            POSTGRES_DATABASE,
            POSTGRES_USERNAME,
            POSTGRES_PASSWORD,
            POSTGRES_PORT
        )
    postgres_handler.connect()
    with st.spinner("Loading Data..."):
        df = pd.DataFrame(postgres_handler.read(query=query))
    postgres_handler.close()
    return df

st.set_page_config(layout="wide") # Optional: Use wide layout for more space
st.title("Amazon Category Performance Dashboard")
st.divider()

# --- Top 10 Categories by Sales ---
st.subheader("Top 10 Categories by Sales")
top_sales_df = load_data(query=TOP_SALES_QUERY)

if not top_sales_df.empty: # Check if DataFrame is not empty before proceeding
    if st.checkbox("Show Raw Data", value=False, key="top_sales"):
        st.subheader("Raw Data")
        st.dataframe(top_sales_df.style.format({
            'total_sales': '₹{:,.2f}'
        }))

    # Create Bar Chart using Plotly Express
    # The key to correct ordering is to explicitly set category_orders
    fig_sales = px.bar(
        top_sales_df,
        x='category',
        y='total_sales',
        title='Top 10 Categories by Total Sales',
        labels={'total_sales': 'Total Sales (₹)', 'category': 'Category'},
        color_discrete_sequence=px.colors.qualitative.Plotly, # Optional: good color palette
        category_orders={"category": top_sales_df['category'].tolist()} # IMP: Maintain order from DataFrame
    )
    fig_sales.update_layout(xaxis={'categoryorder':'array', 'categoryarray': top_sales_df['category'].tolist()}) # Alternative/complementary for ordering
    fig_sales.update_xaxes(tickangle=45) # Rotate x-axis labels if categories are long
    st.plotly_chart(fig_sales, use_container_width=True)
else:
    st.info("No sales data available for the selected period.")

st.divider()

# --- Top 10 Categories by Volume ---
st.subheader("Top 10 Categories by Volume")
top_volume_df = load_data(query=TOP_VOLUME_QUERY)

if not top_volume_df.empty: # Check if DataFrame is not empty
    if st.checkbox("Show Raw Data", value=False, key="top_volume"):
        st.subheader("Raw Data")
        st.dataframe(top_volume_df.style.format({
            'total_volume': '{:,.0f} units'
        }))

    # Create Bar Chart using Plotly Express for Volume
    fig_volume = px.bar(
        top_volume_df,
        x='category',
        y='total_volume',
        title='Top 10 Categories by Total Volume',
        labels={'total_volume': 'Total Units', 'category': 'Category'},
        color_discrete_sequence=px.colors.qualitative.Alphabet, # Another optional color palette
        category_orders={"category": top_volume_df['category'].tolist()} # IMP: Maintain order from DataFrame
    )
    fig_volume.update_layout(xaxis={'categoryorder':'array', 'categoryarray': top_volume_df['category'].tolist()})
    fig_volume.update_xaxes(tickangle=45)
    st.plotly_chart(fig_volume, use_container_width=True)
else:
    st.info("No volume data available for the selected period.")

# No need to close the handler here if using 'with' statement in load_data