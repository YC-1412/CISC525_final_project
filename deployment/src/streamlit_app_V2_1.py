"""
This is a streamlit app that allows you to visualize the relationship 
between COVID-19 cases and flight volumes in the United States over time.

Deployed on GitHub and load data from GCS.

View the app at: https://cisc525-proj2025-spring-v2-1.streamlit.app/
"""

import streamlit as st
import pandas as pd
import numpy as np
import glob
import plotly.express as px
import plotly.graph_objects as go
import argparse
import sys
import os
# import gcsfs
from st_files_connection import FilesConnection


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
    PROJECT_ID = os.environ['PROJECT_ID']
    BUCKET_NAME = os.environ['BUCKET_NAME']
    conn = st.connection("gcs", type=FilesConnection)
    
    # data_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
    fs = conn.fs  # Get the underlying fsspec filesystem
    covid_files = fs.glob(f'gs://{BUCKET_NAME}/final_project/data/processed_data/covid_*_all.csv')
    covid_files_US = fs.glob(f'gs://{BUCKET_NAME}/final_project/data/processed_data/covid_*_US.csv')
    flight_files = fs.glob(f'gs://{BUCKET_NAME}/final_project/data/processed_data/flight_*_US.csv')

    stats = ['Confirmed_cumulative', 'Deaths_cumulative', 'Recovered_cumulative', 'Active_cumulative']
    key = ['year_month', 'Country_Region']
    df_covid_month = pd.concat([pd.read_csv(fs.open(file)) for file in covid_files], ignore_index=True)
    df_covid_month_US = pd.concat([pd.read_csv(fs.open(file)) for file in covid_files_US], ignore_index=True)
    # combine same month and country
    df_covid_month[stats] = df_covid_month[stats].fillna(0)
    df_covid_month = df_covid_month.groupby(key)[stats].sum().reset_index()
    df_covid_month_US = df_covid_month_US.groupby(key)[stats].sum().reset_index()
    # recalculate monthly change
    df_covid_month = df_covid_month.sort_values(by=key)
    for stat in stats:
        base_name = stat.replace('_cumulative', '')
        df_covid_month[f'{base_name}_monthly_new'] = df_covid_month.groupby('Country_Region')[stat].diff()
    df_covid_month_US = df_covid_month_US.sort_values(by=key)
    _ = [f'{i.replace("_cumulative", "_monthly_new")}' for i in stats]
    df_covid_month_US[_] = df_covid_month_US.groupby('Country_Region')[stats].diff()
    
    key = ['year_month', 'destination_country', 'destination_country_code', 'origin_country', 'origin_country_code']
    df_end2 = pd.concat([pd.read_csv(fs.open(file)) for file in flight_files], ignore_index=True)
    df_end2['flight_count'] = df_end2['flight_count'].fillna(0)
    df_end2 = df_end2.groupby(key)['flight_count'].sum().reset_index()
    df_end = df_end2.groupby(key[:3])['flight_count'].sum().reset_index()

    df_covid_month = df_covid_month.rename(columns={'Country_Region': 'country', 'year_month': 'month'})
    df_covid_month_US = df_covid_month_US.rename(columns={'Country_Region': 'country', 'year_month': 'month'})
    df_end2 = df_end2.rename(columns={'destination_country': 'end_country', 'year_month': 'month'})
    df_end = df_end.rename(columns={'destination_country': 'end_country', 'year_month': 'month'})
    df_end = df_end.drop(columns=['destination_country_code'])
    
    df_US = df_covid_month_US.merge(
        df_end, 
        on='month', 
        how='left'
    ).rename(
        columns={'Confirmed': 'cases', 'Country/Region': 'country'}
    ).drop(columns=['end_country'])

    # pivot the dataframe to have one column for each stat
    _ = stats + [f'{stat.replace("_cumulative", "_monthly_new")}' for stat in stats]
    df_covid_month = df_covid_month.melt(id_vars=['country', 'month'], value_vars=_, var_name='stat', value_name='value')
    df_covid_month_US = df_covid_month_US.melt(id_vars=['country', 'month'], value_vars=_, var_name='stat', value_name='value')
    # df_covid_month = df_covid_month.pivot_table(index=['country', 'month'], columns='stat', values='value').reset_index()
    
    return df_US, df_end, df_covid_month, df_covid_month_US, df_end2

# df_US, df_end, df_covid_month, df_covid_month_US, df_end2 = load_data('')

def create_time_series_plot(df_US, selected_stat, start_date, end_date):
    """
    Create a dual-line plot showing selected COVID stat and flight volume over time.
    
    Args:
        df_US (pd.DataFrame): DataFrame containing US COVID and flight data
        selected_stat (str): Selected COVID statistic to display
        start_date (str): Start date for filtering
        end_date (str): End date for filtering
        
    Returns:
        go.Figure: Plotly figure object containing the time series plot
    """
    # Filter data based on date range
    df_US_filtered = df_US[
        (df_US['month'] >= start_date) & 
        (df_US['month'] <= end_date)
    ]

    fig = go.Figure()

    # Add COVID stat line with improved formatting
    fig.add_trace(
        go.Scatter(
            x=df_US_filtered['month'], 
            y=df_US_filtered[selected_stat], 
            name=selected_stat.replace('_', ' ').title(),
            line=dict(color='#FF4B4B', width=3),
            yaxis='y'
        )
    )

    # Add flight volume line with improved formatting
    fig.add_trace(
        go.Scatter(
            x=df_US_filtered['month'], 
            y=df_US_filtered['flight_count'], 
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
            title=selected_stat.replace('_', ' ').title(),
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
        title=f'US {selected_stat.replace("_", " ").title()} and Flight Volume Trends',
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

def create_choropleth_maps(df_end, df_covid_month, start_date, end_date):
    """
    Create side-by-side choropleth maps showing global flight volumes and COVID cases.
    
    Args:
        df_end (pd.DataFrame): DataFrame containing global flight data
        df_covid_month (pd.DataFrame): DataFrame containing global COVID data
        start_date (str): Start date for filtering
        end_date (str): End date for filtering
        
    Returns:
        go.Figure: Plotly figure object containing the choropleth maps
    """
    # Get the maximum values across all time periods for consistent scale
    max_flights = df_end.groupby('origin_country')['flight_count'].sum().max()
    max_cases = df_covid_month[df_covid_month['stat'] == 'Confirmed_monthly_new']['value'].max()
    
    # Filter flight data based on date range and group by origin country
    df_flights_filtered = df_end[
        (df_end['month'] >= start_date) & 
        (df_end['month'] <= end_date)
    ].groupby('origin_country')['flight_count'].sum().reset_index()
    
    # Filter and prepare COVID data
    df_covid_filtered = df_covid_month[
        (df_covid_month['month'] >= start_date) & 
        (df_covid_month['month'] <= end_date) &
        (df_covid_month['stat'] == 'Confirmed_monthly_new')
    ].groupby('country')['value'].max().reset_index()
    
    # Create subplot figure with two separate subplots
    fig = go.Figure()
    
    # Add flight volume map with fixed scale
    fig.add_trace(
        go.Choropleth(
            locations=df_flights_filtered['origin_country'],
            z=df_flights_filtered['flight_count'],
            locationmode='country names',
            colorscale='Viridis',
            name='Flight Volume',
            zmin=0,
            zmax=max_flights,
            colorbar=dict(
                title='Flights',
                x=0.46,
            ),
            geo='geo'
        )
    )
    
    # Add COVID cases map with fixed scale
    fig.add_trace(
        go.Choropleth(
            locations=df_covid_filtered['country'],
            z=df_covid_filtered['value'],
            locationmode='country names',
            colorscale='Reds',
            name='COVID Cases',
            zmin=0,
            zmax=max_cases,
            colorbar=dict(
                title='Cases',
                x=0.98,
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
            domain=dict(x=[0, 0.46], y=[0, 1])
        ),
        geo2=dict(
            scope='world',
            showframe=False,
            showcoastlines=True,
            projection_type='equirectangular',
            domain=dict(x=[0.52, 0.98], y=[0, 1])
        ),
        width=1200,
        height=500,
        autosize=False,
        annotations=[
            dict(
                text=f'Flight Volume into US by Origin Country ({start_date} to {end_date})',
                showarrow=False,
                x=0.05,
                y=1.1,
                xref='paper',
                yref='paper',
                font=dict(size=14)
            ),
            dict(
                text=f'COVID Cases (monthly new) by Country ({start_date} to {end_date})',
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

def calculate_correlation(df_US, selected_stat, start_date, end_date):
    """
    Calculate the correlation between selected COVID stat and flight volume.
    
    Args:
        df_US (pd.DataFrame): DataFrame containing US COVID and flight data
        selected_stat (str): Selected COVID statistic
        start_date (str): Start date for filtering
        end_date (str): End date for filtering
        
    Returns:
        float: Correlation coefficient
    """
    df_filtered = df_US[
        (df_US['month'] >= start_date) & 
        (df_US['month'] <= end_date)
    ]
    return df_filtered[selected_stat].corr(df_filtered['flight_count'])

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
    df_US, df_end, df_covid_month, df_covid_month_US, df_end2 = load_data(data_path)
    
    # Main title
    st.title('COVID-19 Cases and Flight Volume Analysis')
    
    # Create two tabs
    tab1, tab2 = st.tabs(['US Time Series', 'Global Comparison'])
    
    with tab1:
        st.header('US COVID Stats vs Flight Volume Over Time')
        
        # Add dropdown for stat selection
        stats = ['Confirmed_monthly_new', 'Deaths_monthly_new', 'Recovered_monthly_new', 'Active_monthly_new',
                 'Confirmed_cumulative', 'Deaths_cumulative', 'Recovered_cumulative', 'Active_cumulative']
        selected_stat = st.selectbox(
            'Select COVID Statistic',
            stats,
            format_func=lambda x: x.replace('_', ' ').title()
        )

        # Add timeline selector for tab 1
        all_months = sorted(df_US['month'].unique())
        _travel_ban_time = '2020-08'
        try:
            default_end_idx = all_months.index(_travel_ban_time)
        except ValueError:
            default_end_idx = len(all_months) - 1

        start_idx, end_idx = st.select_slider(
            'Select Date Range',
            options=range(len(all_months)),
            value=(0, default_end_idx),
            format_func=lambda x: all_months[x]
        )
        
        start_date = all_months[start_idx]
        end_date = all_months[end_idx]
        
        # Create and display time series plot
        fig = create_time_series_plot(df_US, selected_stat, start_date, end_date)
        st.plotly_chart(fig, use_container_width=True)
        
        # Display correlation
        correlation = calculate_correlation(df_US, selected_stat, start_date, end_date)
        st.write(f"Correlation coefficient between {selected_stat.replace('_', ' ').lower()} and flights: {correlation:.2f}")
        
        # Add explanatory text
        st.markdown(f"""
        ### About this Visualization
        This chart shows the relationship between COVID-19 {selected_stat.replace('_', ' ').lower()} and flight volumes in the United States over time:
        - The red line represents the {selected_stat.replace('_', ' ').lower()}
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
        fig_maps = create_choropleth_maps(df_end2, df_covid_month, start_date, end_date)
        st.plotly_chart(fig_maps, use_container_width=True)
    
    # Add data source information
    st.markdown("""
    **Data Sources:**
    - [COVID-19 case data from Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19)
    - [Flight volume data from international aviation records](https://doi.org/10.5281/zenodo.7923702)
    - [Mapping data: airports to country](https://github.com/mborsetti/airportsdata/blob/main/airportsdata/airports.csv)
    - [Mapping data: country to country code](https://gist.github.com/radcliff/f09c0f88344a7fcef373#file-wikipedia-iso-country-codes-csv)
    """)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='COVID-19 and Flight Volume Analysis Dashboard')
    parser.add_argument('--data_path', type=str, default='./data', required=False, help='Path to the data directory')
    
    args = parser.parse_args(sys.argv[1:])
    
    main(args.data_path)