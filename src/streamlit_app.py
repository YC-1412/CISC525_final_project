"""
This is a streamlit app that allows you to visualize the relationship 
between COVID-19 cases and flight volumes in the United States over time.

Run the app with:
streamlit run ./src/streamlit_app.py -- --data_path <path_to_data_folder>
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
    df_covid_month['month'] = df_covid_month['month'].dt.strftime('%Y-%m')
    df_end['month'] = pd.to_datetime(df_end['month'].astype(str), format='%Y%m').dt.strftime('%Y-%m')
    
    df_US = df_covid_month[df_covid_month['Country/Region'] == 'US'].merge(
        df_end[df_end['end_country'] == 'US'], 
        on='month', 
        how='left'
    ).rename(
        columns={'Confirmed': 'cases', 'Country/Region': 'country'}
    ).drop(columns=['end_country'])
    
    return df_US, df_end, df_covid_month

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
            line=dict(color='#FF4B4B', width=3),
            yaxis='y'
        )
    )

    # Add flight volume line with improved formatting
    fig.add_trace(
        go.Scatter(
            x=df_US['month'], 
            y=df_US['flights'], 
            name='Flight Volume',
            line=dict(color='#1F77B4', width=3),
            yaxis='y2'
        )
    )

    # Update layout with improved styling and secondary y-axis
    fig.update_layout(
        xaxis=dict(
            title='Month',
            tickangle=45
        ),
        yaxis=dict(
            title='COVID Cases',
            titlefont=dict(color='#FF4B4B'),
            tickfont=dict(color='#FF4B4B')
        ),
        yaxis2=dict(
            title='Flight Volume',
            titlefont=dict(color='#1F77B4'),
            tickfont=dict(color='#1F77B4'),
            anchor='x',
            overlaying='y',
            side='right',
            showgrid=False
        ),
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

def create_choropleth_maps(df_end, df_covid, start_date, end_date):
    """
    Create side-by-side choropleth maps showing global flight volumes and COVID cases.
    
    Args:
        df_end (pd.DataFrame): DataFrame containing global flight data
        df_covid (pd.DataFrame): DataFrame containing global COVID data
        start_date (str): Start date for filtering
        end_date (str): End date for filtering
        
    Returns:
        go.Figure: Plotly figure object containing the choropleth maps
    """
    # Filter flight data based on date range
    df_flights_filtered = df_end[
        (df_end['month'] >= start_date) & 
        (df_end['month'] <= end_date)
    ].groupby('end_country')['flights'].sum().reset_index()
    
    # Filter and prepare COVID data
    df_covid_filtered = df_covid[
        (df_covid['month'] >= start_date) & 
        (df_covid['month'] <= end_date)
    ].groupby('Country/Region')['Confirmed'].max().reset_index()
    
    # Create subplot figure with two separate subplots
    fig = go.Figure()
    
    # Add flight volume map
    fig.add_trace(
        go.Choropleth(
            locations=df_flights_filtered['end_country'],
            z=df_flights_filtered['flights'],
            locationmode='country names',
            colorscale='Viridis',
            name='Flight Volume',
            colorbar=dict(
                title='Flights',
                x=0.46
            ),
            geo='geo'
        )
    )
    
    # Add COVID cases map
    fig.add_trace(
        go.Choropleth(
            locations=df_covid_filtered['Country/Region'],
            z=df_covid_filtered['Confirmed'],
            locationmode='country names',
            colorscale='Reds',
            name='COVID Cases',
            colorbar=dict(
                title='Cases',
                x=0.98
            ),
            geo='geo2'
        )
    )
    
    # Update layout with two separate geo subplots
    fig.update_layout(
        geo=dict(
            scope='world',
            showframe=False,
            showcoastlines=True,
            projection_type='equirectangular',
            domain=dict(x=[0, 0.48], y=[0, 1])
        ),
        geo2=dict(
            scope='world',
            showframe=False,
            showcoastlines=True,
            projection_type='equirectangular',
            domain=dict(x=[0.52, 1], y=[0, 1])
        ),
        width=1200,
        height=500,
        autosize=False,
        annotations=[
            dict(
                text=f'Flight Volume by Destination Country ({start_date} to {end_date})',
                showarrow=False,
                x=0.05,
                y=1.1,
                xref='paper',
                yref='paper',
                font=dict(size=14)
            ),
            dict(
                text=f'COVID Cases by Country ({start_date} to {end_date})',
                showarrow=False,
                x=0.725,
                y=1.1,
                xref='paper',
                yref='paper',
                font=dict(size=14)
            )
        ]
    )
    
    return fig

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
    df_US, df_end, df_covid_month = load_data(data_path)
    
    # Main title
    st.title('COVID-19 Cases and Flight Volume Analysis')
    
    # Create two tabs
    tab1, tab2 = st.tabs(['US Time Series', 'Global Comparison'])
    
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
        st.header('Global Flight Volume and COVID Cases by Country')
        
        # Add timeline selector
        all_months = sorted(df_end['month'].unique())
        start_idx, end_idx = st.select_slider(
            'Select Date Range',
            options=range(len(all_months)),
            value=(0, len(all_months)-1),
            format_func=lambda x: all_months[x]
        )
        
        start_date = all_months[start_idx]
        end_date = all_months[end_idx]
            
        # Create and display choropleth maps
        fig_maps = create_choropleth_maps(df_end, df_covid_month, start_date, end_date)
        st.plotly_chart(fig_maps, use_container_width=True)
    
    # Add data source information
    st.markdown("""
    **Data Sources:**
    - [COVID-19 case data from Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19)
    - [Flight volume data from international aviation records](https://doi.org/10.5281/zenodo.7923702)
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='COVID-19 and Flight Volume Analysis Dashboard')
    parser.add_argument('--data_path', type=str, default='./data', required=False, help='Path to the data directory')
    
    args = parser.parse_args(sys.argv[1:])
    
    main(args.data_path)
