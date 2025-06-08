import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Amazon_Scraper.helpers.postgres_handler import PostgresDBHandler
from Amazon_Scraper.settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT

TOP_CATEGORY_SALES_QUERY = """
    SELECT coalesce(ac.category_name, kpi_lcdp.category) as category,
        SUM(kpi_lcdp.total_sales) AS total_sales
    FROM curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    GROUP BY kpi_lcdp.category, ac.category_name
    ORDER BY total_sales DESC
    LIMIT 10
"""

TOP_CATEGORY_VOLUME_QUERY = """
    SELECT coalesce(ac.category_name, kpi_lcdp.category) as category,
        SUM(kpi_lcdp.total_volume) AS total_volume
    FROM curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    GROUP BY kpi_lcdp.category, ac.category_name
    ORDER BY total_volume DESC
    LIMIT 10;
"""

TOP_LOWEST_CATEGORY_SALES_QUERY = """
    select 
        coalesce(ac.category_name, kpi_lcdp.category) as category, 
        coalesce(achfn.sub_category_name, kpi_lcdp.lowest_category) as lowest_category, 
        kpi_lcdp.total_sales
    from curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    left join transformed.vw_amz__category_hierarchy_flattened_names achfn
        on kpi_lcdp.category = achfn.category and kpi_lcdp.lowest_category = achfn.sub_category
    where total_sales is not null
    order by total_sales desc
    limit 10;
"""

TOP_LOWEST_CATEGORY_VOLUME_QUERY = """
    select 
        coalesce(ac.category_name, kpi_lcdp.category) as category, 
        coalesce(achfn.sub_category_name, kpi_lcdp.lowest_category) as lowest_category, 
        kpi_lcdp.total_volume 
    from curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    left join transformed.vw_amz__category_hierarchy_flattened_names achfn
        on kpi_lcdp.category = achfn.category and kpi_lcdp.lowest_category = achfn.sub_category
    where total_sales is not null
    order by total_volume desc
    limit 10;
"""

TOP_PRODUCT_SALES_QUERY = """
    select category, lowest_category, product_name, last_month_sale*coalesce(sell_price, sell_mrp) as total_sales, asin
    from curated.vw_amz_kpi__product_daily_performance
    where last_month_sale is not null and sell_price is not null
    order by total_sales desc
    limit 10
"""

TOP_PRODUCT_VOLUME_QUERY = """
    select category, lowest_category, product_name, last_month_sale as total_volume, asin
    from curated.vw_amz_kpi__product_daily_performance
    where last_month_sale is not null
    order by total_volume desc
    limit 10
"""

BELOW_AVG_RATING_CATEGORY_QUERY = """
    select 
        coalesce(ac.category_name, kpi_lcdp.category) as category, 
        round(avg(kpi_lcdp.avg_rating),2) as category_avg_rating, 
        count(*) as lc_count
    from curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    where kpi_lcdp.avg_rating is not null
    group by kpi_lcdp.category, ac.category_name
    order by category_avg_rating;
"""

BELOW_AVG_RATING_LC_QUERY = """
    select 
        coalesce(ac.category_name, kpi_lcdp.category) as category, 
        coalesce(achfn.sub_category_name, kpi_lcdp.lowest_category) as lowest_category, 
        kpi_lcdp.avg_rating,
        kpi_lcdp.total_volume
    from curated.vw_amz_kpi__lowest_category_daily_performance kpi_lcdp
    LEFT JOIN transformed.amz__category ac
        on kpi_lcdp.category = ac.category_code
    left join transformed.vw_amz__category_hierarchy_flattened_names achfn
        on kpi_lcdp.category = achfn.category and kpi_lcdp.lowest_category = achfn.sub_category
    where kpi_lcdp.avg_rating is not null and kpi_lcdp.total_volume > 0
    order by avg_rating 
    limit 10
"""

GLOBAL_AVG_RATING_QUERY = """
    select round(avg(avg_rating),2) as global_avg_rating
    from curated.vw_amz_kpi__lowest_category_daily_performance;
"""

@st.cache_data
def load_data(query):
    # It's good practice to ensure the handler is connected before use inside a cached function
    # or pass a connected handler
    with PostgresDBHandler(
        POSTGRES_HOST,
        POSTGRES_DATABASE,
        POSTGRES_USERNAME,
        POSTGRES_PASSWORD,
        POSTGRES_PORT
    ) as handler: # Using 'with' ensures proper connection closing
        with st.spinner("Loading Data..."):
            df = pd.DataFrame(handler.read(query=query))
    return df

st.set_page_config(layout="wide", page_title="Amazon Market Insights")
st.title("Market Pulse & Current Leaders")
# st.markdown(f"#### Data as of: **{get_latest_scrape_date()}**")
st.divider()


# --- Helper function to draw bar charts ---
def draw_bar_chart(df, x_col, y_col, title, x_label, y_label, format_string=None, color_palette=px.colors.qualitative.Plotly, hover_data_list=None):
    if df.empty:
        st.info(f"No data available for {title}.")
        return

    # Ensure the x_col (category names) are ordered as per the DataFrame's sort
    # This is crucial for Plotly to respect the SQL ORDER BY
    category_order = df[x_col].tolist()

    fig = px.bar(
        df,
        x=x_col,
        y=y_col,
        title=title,
        labels={x_col: x_label, y_col: y_label},
        color_discrete_sequence=color_palette,
        category_orders={x_col: category_order}, # Explicitly set category order
        hover_data=hover_data_list
    )
    fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': category_order}) # Redundant but robust
    fig.update_xaxes(tickangle=45) # Rotate x-axis labels for readability

    # Optional: Apply specific format to y-axis if provided
    if format_string:
        fig.update_yaxes(tickformat=format_string)

    st.plotly_chart(fig, use_container_width=True)


# --- Section: Top High-Level Categories ---
st.header("Overall Category Performance ")

col1, col2 = st.columns(2)
with col1:
    # st.subheader("Top 10 By Sales")
    high_level_sales_df = load_data(query=TOP_CATEGORY_SALES_QUERY)
    draw_bar_chart(
        high_level_sales_df,
        x_col='category',
        y_col='total_sales',
        title='Top 10 High-Level Categories by Sales',
        x_label='High-Level Category',
        y_label='Total Sales (₹)',
        format_string=None #"~s" # Shorthand for SI prefix (e.g., K, M, G)
    )
    if st.checkbox("Show Raw Data", value=False, key="high_level_sales_raw"):
        st.dataframe(high_level_sales_df.style.format({'total_sales': '₹{:,.2f}'}))

with col2:
    # st.subheader("Top 10 By Volume")
    high_level_volume_df = load_data(query=TOP_CATEGORY_VOLUME_QUERY)
    draw_bar_chart(
        high_level_volume_df,
        x_col='category',
        y_col='total_volume',
        title='Top 10 High-Level Categories by Volume',
        x_label='High-Level Category',
        y_label='Total Volume',
        format_string=None #"{:,.0f} units"
    )
    if st.checkbox("Show Raw Data", value=False, key="high_level_volume_raw"):
        st.dataframe(high_level_volume_df.style.format({'total_volume': '{:,.0f} units'}))

st.divider()

# --- Section: Top Lowest Categories ---
st.header("Lowest Category Performance")
col3, col4 = st.columns(2) # Create two new columns

with col3:
    # st.subheader("Top 10 By Sales")
    lowest_category_sales_df = load_data(query=TOP_LOWEST_CATEGORY_SALES_QUERY)
    draw_bar_chart(
        lowest_category_sales_df,
        x_col='lowest_category',
        y_col='total_sales',
        title='Top 10 Lowest Categories by Sales',
        x_label='Lowest Category',
        y_label='Total Sales (₹)',
        format_string=None, #"~s", # Shorthand for SI prefix (e.g., K, M, G)
        color_palette=px.colors.qualitative.Dark24,
        hover_data_list=['category']
    )
    if st.checkbox("Show Raw Data", value=False, key="lowest_category_sales_raw"):
        st.dataframe(lowest_category_sales_df.style.format({'total_sales': '₹{:,.2f}'}))

with col4:
    # st.subheader("Top 10 By Volume")
    lowest_category_volume_df = load_data(query=TOP_LOWEST_CATEGORY_VOLUME_QUERY)
    draw_bar_chart(
        lowest_category_volume_df,
        x_col='lowest_category',
        y_col='total_volume',
        title='Top 10 Lowest Categories by Volume',
        x_label='Lowest Category',
        y_label='Total Units',
        format_string=",", # Thousand separator
        color_palette=px.colors.qualitative.Set2,
        hover_data_list=['category']
    )
    if st.checkbox("Show Raw Data", value=False, key="lowest_category_volume_raw"):
        st.dataframe(lowest_category_volume_df.style.format({'total_volume': '{:,.0f} units'}))

st.divider()

# --- Section: Top Products ---
st.header("Product Performance")
col5, col6 = st.columns(2) # Create two more columns

with col5:
    product_sales_df = load_data(query=TOP_PRODUCT_SALES_QUERY)
    draw_bar_chart(
        product_sales_df,
        x_col='asin', # Use product_name for x-axis
        y_col='total_sales',
        title='Top 10 Products by Sales',
        x_label='ASIN',
        y_label='Total Sales (₹)',
        format_string="~s",
        color_palette=px.colors.qualitative.T10,
        hover_data_list=['product_name', 'category', 'lowest_category']
    )
    if st.checkbox("Show Raw Data", value=False, key="product_sales_raw"):
        st.dataframe(product_sales_df.style.format({'total_sales': '₹{:,.2f}'}))

with col6:
    product_volume_df = load_data(query=TOP_PRODUCT_VOLUME_QUERY)
    draw_bar_chart(
        product_volume_df,
        x_col='asin', # Use product_name for x-axis
        y_col='total_volume',
        title='Top 10 Products by Volume',
        x_label='ASIN',
        y_label='Total Units',
        format_string=",",
        color_palette=px.colors.qualitative.G10,
        hover_data_list=['product_name', 'category', 'lowest_category']
    )
    if st.checkbox("Show Raw Data", value=False, key="product_volume_raw"):
        st.dataframe(product_volume_df.style.format({'total_volume': '{:,.0f} units'}))

st.divider()

# --- Section: Categories with Below Average Rating ---
st.header("Market Gaps & Opportunities")
below_avg_rating_df = load_data(query=BELOW_AVG_RATING_CATEGORY_QUERY)

if not below_avg_rating_df.empty:
    # Get the global average rating for context (optional, but good for display)
    overall_avg_rating = load_data(query=GLOBAL_AVG_RATING_QUERY)
    if not overall_avg_rating.empty:
        st.info(f"The Global Average Rating for Categories is: **{overall_avg_rating['global_avg_rating'].iloc[0]:.2f}**")

    # A horizontal bar chart is often good for showing individual items against a threshold
    fig_rating = px.bar(
        below_avg_rating_df,
        x='category_avg_rating', # Rating on x-axis (horizontal bar)
        y='category',       # Category on y-axis
        orientation='h',    # Make it horizontal
        title='Categories Below Overall Average Rating',
        labels={'category_avg_rating': 'Average Rating', 'category': 'Category'},
        color='category_avg_rating', # Color bars by their rating value
        color_continuous_scale=px.colors.sequential.Aggrnyl, # Red-Yellow-Green scale
        category_orders={"category": below_avg_rating_df['category'].tolist()} # Maintain order
    )
    fig_rating.update_layout(yaxis={'categoryorder':'total ascending'}) # Sort y-axis by value ascending
    fig_rating.update_xaxes(range=[0, 5], dtick=0.5) # Set fixed range for rating (0-5)
    st.plotly_chart(fig_rating, use_container_width=True)

    if st.checkbox("Show Raw Data", value=False, key="below_avg_rating_raw"):
        st.dataframe(below_avg_rating_df.style.format({'category_avg_rating': '{:.2f}'}))
else:
    st.info("No lowest categories found with below-average ratings (or no data).")

st.divider()

# BELOW_AVG_RATING_LC_QUERY
below_avg_rating_df_lc = load_data(query=BELOW_AVG_RATING_LC_QUERY)
if not below_avg_rating_df_lc.empty:
    # A horizontal bar chart is often good for showing individual items against a threshold
    fig_rating = px.bar(
        below_avg_rating_df_lc,
        x='avg_rating', # Rating on x-axis (horizontal bar)
        y='lowest_category',       # Category on y-axis
        orientation='h',    # Make it horizontal
        title='Lowest Categories Below Overall Average Rating',
        labels={'avg_rating': 'Average Rating', 'lowest_category': 'Lowest Category'},
        color='avg_rating', # Color bars by their rating value
        color_continuous_scale=px.colors.sequential.Aggrnyl, # Red-Yellow-Green scale
        category_orders={"lowest_category": below_avg_rating_df_lc['lowest_category'].tolist()}, # Maintain order
        hover_data=['category', 'total_volume']
    )
    fig_rating.update_layout(yaxis={'categoryorder':'total ascending'}) # Sort y-axis by value ascending
    fig_rating.update_xaxes(range=[0, 5], dtick=0.5) # Set fixed range for rating (0-5)
    st.plotly_chart(fig_rating, use_container_width=True)

    if st.checkbox("Show Raw Data", value=False, key="below_avg_rating_lc_raw"):
        st.dataframe(below_avg_rating_df_lc.style.format({'avg_rating': '{:.2f}'}))
else:
    st.info("No lowest categories found with below-average ratings (or no data).")

st.divider()