"""
This is a streamlit app that allows you to visualize the relationship 
between COVID-19 cases and flight volumes in the United States over time.

Run the app with:
streamlit run streamlit_app.py -- --data_path <path_to_data_folder>
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import argparse
import sys

def load_data(data_path: str):
    """
    Load and prepare the data for the dashboard.

    Args:
        data_path (str): The path to the data file
    
    Returns:
        tuple: Contains two dataframes:
            - df_US: DataFrame with US COVID cases and flight data
            - df_end: DataFrame with global flight data
    """
    covid_global = pd.read_csv(f'{data_path}/covid_data/time_series_covid19_confirmed_global.csv')  # Update path
    df_end = pd.read_csv(f'{data_path}/plane_data_results/end_country_us_flight_count.csv')  # Update path

    df_covid_day = covid_global.drop(columns=['Province/State', 'Lat', 'Long']).melt(id_vars=['Country/Region'], var_name='Date', value_name='Confirmed')
    df_covid_day['Date'] = pd.to_datetime(df_covid_day['Date'], format='%m/%d/%y')
    df_covid_month = df_covid_day.groupby(['Country/Region', pd.Grouper(key='Date', freq='ME')]).sum().reset_index().rename(columns={'Date': 'month'})
    df_covid_month['month'] = df_covid_month['month'].dt.strftime('%Y%m').astype(int)
    df_end['month'] = df_end['month'].astype(int)
    
    df_US = df_covid_month[df_covid_month['Country/Region'] == 'US'].merge(
        df_end[df_end['end_country'] == 'US'], 
        on='month', 
        how='left'
    ).rename(
        columns={'Confirmed': 'cases', 'Country/Region': 'country'}
    ).drop(columns=['end_country'])
    
    return df_US, df_end

def create_time_series_plot(df_US):
    """
    Create a dual-line plot showing COVID cases and flight volume over time.
    
    Args:
        df_US (pd.DataFrame): DataFrame containing US COVID and flight data
        
    Returns:
        go.Figure: Plotly figure object containing the time series plot
    """
    fig = go.Figure()

    # Add COVID cases line with improved formatting
    fig.add_trace(
        go.Scatter(
            x=df_US['month'], 
            y=df_US['cases'], 
            name='COVID Cases',
            line=dict(color='#FF4B4B', width=3)
        )
    )

    # Add flight volume line with improved formatting
    fig.add_trace(
        go.Scatter(
            x=df_US['month'], 
            y=df_US['flights'], 
            name='Flight Volume',
            line=dict(color='#1F77B4', width=3)
        )
    )

    # Update layout with improved styling
    fig.update_layout(
        xaxis=dict(
            title='Month',
            tickangle=45
        ),
        yaxis=dict(title='Count'),
        title='US COVID Cases and Flight Volume Trends',
        hovermode='x unified',
        plot_bgcolor='white',
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01
        )
    )
    
    return fig

def create_choropleth_map(df_end):
    """
    Create a choropleth map showing global flight volumes.
    
    Args:
        df_end (pd.DataFrame): DataFrame containing global flight data
        
    Returns:
        go.Figure: Plotly figure object containing the choropleth map
    """
    fig_map = px.choropleth(
        df_end,
        locations='end_country',
        locationmode='country names',
        color='flights',
        hover_name='end_country',
        color_continuous_scale='Viridis',
        title='Flight Volume by Destination Country'
    )
    
    fig_map.update_layout(
        title_x=0.5,
        geo=dict(showframe=False, showcoastlines=True),
        width=800,
        height=500
    )
    
    return fig_map

def calculate_correlation(df_US):
    """
    Calculate the correlation between COVID cases and flight volume.
    
    Args:
        df_US (pd.DataFrame): DataFrame containing US COVID and flight data
        
    Returns:
        float: Correlation coefficient
    """
    return df_US['cases'].corr(df_US['flights'])

def main(data_path: str):
    """
    Main function to run the Streamlit dashboard.
    Sets up the layout and calls other functions to create visualizations.

    Args:
        data_path (str): The path to the data file
    """
    # Set up the page
    st.set_page_config(
        page_title="COVID-19 and Flight Volume Analysis",
        page_icon="✈️",
        layout="wide"
    )
    
    # Load data
    df_US, df_end = load_data(data_path)
    
    # Main title
    st.title('COVID-19 Cases and Flight Volume Analysis')
    
    # Create two tabs
    tab1, tab2 = st.tabs(['US Time Series', 'Global Flight Volume'])
    
    with tab1:
        st.header('US COVID Cases vs Flight Volume Over Time')
        
        # Create and display time series plot
        fig = create_time_series_plot(df_US)
        st.plotly_chart(fig, use_container_width=True)
        
        # Display correlation
        correlation = calculate_correlation(df_US)
        st.write(f"Correlation coefficient between cases and flights: {correlation:.2f}")
        
        # Add explanatory text
        st.markdown("""
        ### About this Visualization
        This chart shows the relationship between COVID-19 cases and flight volumes in the United States over time:
        - The red line represents the number of COVID-19 cases
        - The blue line represents the flight volume
        - Both metrics are plotted on the same scale to show relative changes
        """)
    
    with tab2:
        st.header('Global Flight Volume by Country')
        
        # Create and display choropleth map
        fig_map = create_choropleth_map(df_end)
        st.plotly_chart(fig_map)
    
    # Add data source information
    st.markdown("""
    **Data Sources:**
    - COVID-19 case data from Johns Hopkins CSSE
    - Flight volume data from international aviation records
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='COVID-19 and Flight Volume Analysis Dashboard')
    parser.add_argument('--data_path', type=str, default='./data', required=False, help='Path to the data directory')
    
    args = parser.parse_args(sys.argv[1:])
    
    main(args.data_path)