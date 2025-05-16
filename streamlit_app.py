# Import python packages
from snowflake.snowpark.context import get_active_session
import streamlit as st
from datetime import date
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, count, sum as snow_sum
import pandas as pd

# Get the active Snowpark session
session = get_active_session()

# App Title
st.title("🎥 Movie Booking Dashboard")

# Sidebar Filters
st.sidebar.header("Filters")

# Date Range Filter
date_range = st.sidebar.date_input(
    "Select Booking Date Range:",
    [date(2024, 1, 1), date(2024, 12, 31)]
)

# Booking Status Filter
status_options = ["All", "BOOKED", "CANCELLED", "COMPLETED"]
selected_status = st.sidebar.selectbox("Select Booking Status:", status_options)

# Load Data
raw_data_df = session.table("raw_movie_bookings")

# Apply Date Range Filter
if len(date_range) == 2 and date_range[0] and date_range[1]:
    start_date, end_date = date_range
    filtered_data = raw_data_df.filter(
        (col("booking_date") >= start_date) & (col("booking_date") <= end_date)
    )
else:
    filtered_data = raw_data_df

# Apply Status Filter
if selected_status != "All":
    filtered_data = filtered_data.filter(col("status") == selected_status)

# Convert to Pandas DataFrame
filtered_data_pandas = filtered_data.to_pandas()

# Ensure column names are lowercase for Pandas consistency
filtered_data_pandas.columns = [col.lower() for col in filtered_data_pandas.columns]

# Show Filtered Data
st.subheader("Filtered Data")
st.dataframe(filtered_data_pandas)

# Summary Statistics
if not filtered_data_pandas.empty:
    st.subheader("Summary Statistics")
    total_bookings = len(filtered_data_pandas)
    total_revenue = filtered_data_pandas["ticket_price"].sum()
    total_tickets = filtered_data_pandas["ticket_count"].sum()

    st.metric("Total Bookings", total_bookings)
    st.metric("Total Revenue ($)", f"${total_revenue:,.2f}")
    st.metric("Total Tickets Sold", total_tickets)

    # Additional Insights
    st.subheader("Additional Insights")

    # Revenue by Status
    revenue_by_status = (
        filtered_data_pandas.groupby("status")["ticket_price"].sum().reset_index()
    )
    st.bar_chart(revenue_by_status.set_index("status"))

    # Bookings by Movie
    bookings_by_movie = (
        filtered_data_pandas.groupby("movie_id")["booking_id"].count().reset_index()
    )
    bookings_by_movie.columns = ["Movie ID", "Number of Bookings"]
    st.bar_chart(bookings_by_movie.set_index("Movie ID"))

    # Tickets Sold by Status
    tickets_by_status = (
        filtered_data_pandas.groupby("status")["ticket_count"].sum().reset_index()
    )
    tickets_by_status.columns = ["Status", "Tickets Sold"]
    st.bar_chart(tickets_by_status.set_index("Status"))

else:
    st.warning("No data available for the selected filters.")

# Advanced Visualization
if not filtered_data_pandas.empty:
    st.subheader("Detailed Analysis")
    
    # Revenue by Movie
    revenue_by_movie = (
        filtered_data_pandas.groupby("movie_id")["ticket_price"].sum().reset_index()
    )
    revenue_by_movie.columns = ["Movie ID", "Total Revenue"]
    st.dataframe(revenue_by_movie)
    st.bar_chart(revenue_by_movie.set_index("Movie ID"))

    # Booking Status Distribution
    status_distribution = (
        filtered_data_pandas.groupby("status")["booking_id"].count().reset_index()
    )
    status_distribution.columns = ["Booking Status", "Count"]
    st.dataframe(status_distribution)
    st.bar_chart(status_distribution.set_index("Booking Status"))

# Footer
st.markdown("---")
st.markdown("**Powered by Snowflake & Streamlit** 🚀")