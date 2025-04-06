"""
This is a combination of processing_flight.ipynb and processing_covid.ipynb

## Processing COVID cases
The input files are daily cumulative COVID Confirmed, Deaths, Recovered, and Active case count by country, province and administration area. This script load in the daily file, aggregate it to the country-month level, producing both cumulative and newly COVID Confirmed, Deaths, Recovered, and Active case count.
- Data comes from [COVID-19 case data from Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19).
- Data cleaning
    - There are some daily files that does not come with the consistent schema, e.g. missing `Admin2` and `Province_State`, and have `Country/Region` instead of `Country_Region`. Those are a small portion of the data and hard to identify because all daily file has the same name. We just skip those files in this analysis.
    - There are 487 (0.011%) records with abnormal dates, e.g. 2682-01-01, or `Country_Region` name as numbers. Those records are dropped.
    - The daily file could have records not on that date. For example, a 01-01-2021 file can have records for 2020 or Apr 2021, maybe because it is the last updated date.

## Processing flight volume data
The input files are daily flight information about flight origin and destination by airport. This script load in the data, map airport to country, and aggregate flight volume to the country-month level. It can either aggregate by origin or desination country.
- Data comes from https://zenodo.org/records/7923702
    - The monthly file could have records not in that month. For example, a Jan 2021 file can have records for 2020, reason unknown.
- `airports.csv` comes from [https://github.com/mborsetti/airportsdata](https://github.com/mborsetti/airportsdata/blob/main/airportsdata/airports.csv)
- `countries.csv` comes from [radcliff/wikipedia-iso-country-codes.csv](https://gist.github.com/radcliff/f09c0f88344a7fcef373#file-wikipedia-iso-country-codes-csv)

Run the script with:
python src/process_data.py --year_month 202101 --country US

"""
# start a spark session
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
import argparse
import sys
# hide the warning
# Initialize Spark session before setting log level

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


###########################################################
# COVID related data processing
###########################################################

def load_covid_data(spark: SparkSession, data_path: str, batch_date: str) -> DataFrame:
    """
    Load the covid data for a given batch date. batch_date is in the format of MM-DD-YYYY
    """
    schema = StructType([
        StructField("FIPS", StringType(), True),
        StructField("Admin2", StringType(), True),
        StructField("Province_State", StringType(), True),
        StructField("Country_Region", StringType(), True),
        StructField("Last_Update", TimestampType(), True),
        StructField("Lat", FloatType(), True),
        StructField("Long_", FloatType(), True),
        StructField("Confirmed", IntegerType(), True),
        StructField("Deaths", IntegerType(), True),
        StructField("Recovered", IntegerType(), True),
        StructField("Active", IntegerType(), True),
        StructField("Combined_Key", StringType(), True),
        StructField("Incident_Rate", FloatType(), True),
        StructField("Case_Fatality_Ratio", FloatType(), True),
    ]
    )
    try:
        df_covid = spark.read.csv(f"{data_path}/{batch_date}.csv", header=True, schema=schema, mode='DROPMALFORMED')
    except Exception as e:
        logger.error(f"Error loading covid data for {batch_date}: {e} \nSkip this batch date")
        return None
    return df_covid


def process_covid_daily_data(df: DataFrame, country: list[str] = None) -> DataFrame:
    """
    Process the daily data for given country.

    Args:
        df: DataFrame, the daily data
        country: list[str], the country to process

    Returns:
        DataFrame, the daily data for the given country
    """
    locations = ['Admin2', 'Province_State', 'Country_Region']
    stats = ['Confirmed', 'Deaths', 'Recovered', 'Active']

    if country is None:
        df_daily = df
    else:
        df_daily = df.filter(df['Country_Region'].isin(country))

    df_daily = df_daily.withColumn('date', F.date_format(F.col('Last_Update'), 'yyyy-MM-dd'))
    df_daily = df_daily[['date'] + locations + stats]

    # drop rows with missing date
    
    _count = df_daily.select(
        F.count(F.when(F.isnull('date') | (F.year('date') >= 2024) | (F.year('date') < 2020), 1)).alias('date Missing Abnormal'),
        (F.count(F.when(F.isnull('date') | (F.year('date') >= 2024) | (F.year('date') < 2020), 1)) / F.count('*') * 100).alias('date Missing Abnormal %')
    )
    _details = df_daily.filter(
        F.isnull('date') | (F.year('date') >= 2024) | (F.year('date') < 2020)
    )
    with pd.option_context('display.max_columns', 1000, 'display.width', 1000, ):
        logger.info(f"{'='*10} date is missing or abnormal: {'='*10}"
                    f"\n{_count.toPandas()}"
                    f"\n{_details.toPandas().head(10)}")
    
    logger.info(f"Dropping rows with missing or abnormal date")
    df_daily = df_daily.na.drop(subset=['date']).filter((F.year('date') >= 2020) & (F.year('date') <= 2024))

    # aggregate by date and country
    df_daily = df_daily.groupBy('date', 'Country_Region').agg(
        *[F.sum(c).alias(c) for c in stats]
    ).sort(['Country_Region', 'date'])

    # get daily change from the previous day for Confirmed, Deaths, Recovered, Active
    for stat in stats:
        df_daily = df_daily.withColumn(f'{stat}_daily_new', 
                            F.col(stat) - F.lag(stat).over(Window.partitionBy().orderBy('date'))) \
                 .withColumnRenamed(stat, f'{stat}_cumulative')
    
    with pd.option_context('display.max_columns', 1000, 'display.width', 1000, ):
        logger.info(f"{'='*10} Aggregate by date: {'='*10}"
                    f"\n{df_daily.toPandas().head(5)}")

    # check missing
    with pd.option_context('display.max_columns', 1000, 'display.width', 1000, ):
        logger.info(f"{'='*10} Count na values: {'='*10}"
                    f"\n{df_daily.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df_daily.columns]).toPandas()}")
    return df_daily


def process_covid_monthly_data(df: DataFrame) -> DataFrame:
    """
    Process the monthly data.

    Args:
        df: DataFrame, the daily data

    Returns:
        DataFrame, the monthly data
    """
    locations = ['Admin2', 'Province_State', 'Country_Region']
    stats = ['Confirmed', 'Deaths', 'Recovered', 'Active']
    df_monthly = df.withColumn('year_month', F.date_format(F.col('date'), 'yyyy-MM'))
    df_monthly = df_monthly.groupBy(['year_month', 'Country_Region']).agg(
        *[F.max(c + '_cumulative').alias(c + '_cumulative') for c in stats],
        *[F.sum(c + '_daily_new').alias(c + '_monthly_new') for c in stats]
    ).sort(['Country_Region', 'year_month'])
    return df_monthly


def save_covid_data(df: DataFrame, data_path: str, filename: str):
    df.toPandas().to_csv(f'{data_path}/{filename}.csv', index=False)


###########################################################
# Flight related data processing
###########################################################

def load_airports(spark: SparkSession, data_path: str):
    airports = spark.read.csv(f'{data_path}/airports.csv', header=True, inferSchema=True)
    return airports

def load_countries(spark: SparkSession, data_path: str):
    countries = spark.read.csv(f'{data_path}/countries.csv', header=True, inferSchema=True) \
    .withColumnRenamed('English short name lower case', 'country') \
    .withColumnRenamed('Alpha-2 code', 'country_code') \
    .select('country', 'country_code')
    return countries

def load_flight_data(spark: SparkSession, data_path: str, batch_date: str):
    logger.info(f"Loading flight data for {batch_date}...")
    batch_date = batch_date[:6]   # only use year and month
    try:
        df_flight = spark.read.csv(f'{data_path}/flightlist_{batch_date}*_{batch_date}*.csv.gz', header=True, inferSchema=True).select('day', 'origin', 'destination')
    except Exception as e:
        logger.error(f"Error loading flight data for {batch_date}: {e} \nSkip this batch date")
        return None
    return df_flight


def map_flight_data(df_flight: DataFrame, airports: DataFrame, countries: DataFrame) -> DataFrame:
    """
    Map the flight data to country and icao

    Args:
        df_flight: DataFrame, the flight data
        airports: DataFrame, the airports data

    Returns:
        df_flight_mapped: DataFrame, the flight data with country and icao
    """
    airports = airports.select('icao', 'country')
    df_flight_mapped = df_flight.join(airports, df_flight['origin'] == airports['icao'], 'left')\
        .withColumnRenamed('country', 'origin_country').withColumnRenamed('icao', 'origin_icao') \
    .join(airports, df_flight['destination'] == airports['icao'], 'left') \
    .withColumnRenamed('country', 'destination_country').withColumnRenamed('icao', 'destination_icao') \
    .select('day', 'origin_country', 'destination_country', 'origin_icao', 'destination_icao')
    df_flight_mapped = df_flight_mapped.withColumn('day', F.to_date('day')) \
        .withColumn('year_month', F.date_format('day', 'yyyy-MM')) \
        .select('day', 'year_month', 'origin_country', 'destination_country', 'origin_icao', 'destination_icao')
    # map country abbreviation to country name
    df_flight_mapped = df_flight_mapped.join(countries, df_flight_mapped['origin_country'] == countries['country_code'], 'left') \
        .drop('country_code') \
        .withColumnRenamed('origin_country', 'origin_country_code').withColumnRenamed('country', 'origin_country')
    df_flight_mapped = df_flight_mapped.join(countries, df_flight_mapped['destination_country'] == countries['country_code'], 'left') \
        .drop('country_code') \
        .withColumnRenamed('destination_country', 'destination_country_code').withColumnRenamed('country', 'destination_country')
    

    logger.info(f"Count of missing country or date"
                f"\n{df_flight_mapped.select([F.count(F.when(F.isnull(c), c)).alias(c) for c in df_flight_mapped.columns]).toPandas()}")
    # drop rows with missing country or date
    df_flight_mapped = df_flight_mapped.filter(F.col('origin_country').isNotNull() & F.col('destination_country').isNotNull() & F.col('day').isNotNull())

    return df_flight_mapped

def process_flight_data(df: DataFrame, direction: str = 'origin', granular: str = 'day', country: list[str] = None) -> DataFrame:
    """
    Process the daily or monthly data
    Args:
        df: DataFrame, the flight data
        direction: str, the direction of the flight
        granular: str, the granularity of the data
        country: list[str], the country to filter the data
    Returns:
        df_agg: DataFrame, the aggregated data
    """
    assert direction in ['origin', 'destination']
    assert granular in ['day', 'year_month']
    opposite_direction = 'origin' if direction == 'destination' else 'destination'
    if country:
        df = df.filter(F.col(f'{direction}_country').isin(country))
    # exclude domestic flight
    df = df.filter(F.col('origin_country_code') != F.col('destination_country_code'))
    # aggrgate by country and month or day
    df_agg = df.groupBy(f'{granular}', 'origin_country', 'origin_country_code', 'destination_country', 'destination_country_code').agg(
        F.count('*').alias(f'flight_count')
    ).sort(f'{direction}_country_code', f'{opposite_direction}_country_code', f'{granular}')
    return df_agg

def save_flight_data(df: DataFrame, data_path: str, filename: str):
    df.toPandas().to_csv(f'{data_path}/{filename}.csv', index=False)


def main(data_path: str, covid_folder: str, flight_folder: str, save_folder: str, 
         year_month: str, country: list[str] = None):
    """
    Main function to process the data
    """
    spark = SparkSession.builder.appName("COVID-19 and Flight Volume Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    covid_year_month = f'{year_month[4:] if len(year_month) > 4 else "*"}-*-{year_month[:4] if len(year_month) >= 4 else "*"}'
    
    covid_data_path = f'{data_path}/{covid_folder}'
    flight_data_path = f'{data_path}/{flight_folder}'
    covid_country = country.copy()
    flight_country = [i if i != 'US' else 'United States' for i in country]

    df_covid = load_covid_data(spark, covid_data_path, covid_year_month)
    if df_covid is not None:
        covid_daily = process_covid_daily_data(df_covid, None)   # need all country instead of covid_country
        covid_monthly = process_covid_monthly_data(covid_daily)
    else:
        covid_daily = None
        covid_monthly = None
    
    airports = load_airports(spark, data_path)
    countries = load_countries(spark, data_path)
    df_flight = load_flight_data(spark, flight_data_path, year_month)
    if df_flight is not None:
        df_flight_mapped = map_flight_data(df_flight, airports, countries)
        df_flight_monthly = process_flight_data(df_flight_mapped, 'destination', 'year_month', flight_country)
    else:
        df_flight_mapped = None
        df_flight_monthly = None

    if covid_monthly is not None:
        save_covid_data(covid_monthly, f'{data_path}/{save_folder}', f'covid_{year_month}_all')
        if country:
            covid_monthly_country = covid_monthly.filter(F.col('Country_Region').isin(country))
            save_covid_data(covid_monthly_country, f'{data_path}/{save_folder}', f'covid_{year_month}_{"__".join(country).replace(" ", "_").replace("*", "all")}')

    if df_flight_monthly is not None:
        save_flight_data(df_flight_monthly, f'{data_path}/{save_folder}', f'flight_{year_month}_{"__".join(country).replace(" ", "_").replace("*", "all")}')
    
    spark.stop()
    return df_flight_mapped, covid_monthly, covid_monthly_country


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='Process COVID-19 and flight volume data')
    parser.add_argument('--year_month', type=str, default='202101',
                        help='Year and month to process (format: YYYYMM or *)')
    parser.add_argument('--country', type=str, nargs='*', default=None,
                        help='Optional list of countries to filter data')
    parser.add_argument('--data_path', type=str, default='./data',
                        help='Path to data directory')
    parser.add_argument('--save_folder', type=str, default='processed_data',
                        help='Path to save processed data directory')
    parser.add_argument('--covid_folder', type=str, default='csse_covid_19_daily_reports',
                        help='Path to COVID-19 data directory')
    parser.add_argument('--flight_folder', type=str, default='flight_volume_raw',
                        help='Path to flight volume data directory')
    parser.add_argument('--log_level', type=str, default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Set the logging level')
    

    args = parser.parse_args(sys.argv[1:])
    data_path = args.data_path
    save_folder = args.save_folder
    covid_folder = args.covid_folder
    flight_folder = args.flight_folder
    year_month = args.year_month
    country = args.country

    # change the logger level
    logging.basicConfig(level=getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))
    
    main(data_path=data_path, 
         save_folder=save_folder, 
         covid_folder=covid_folder, 
         flight_folder=flight_folder, 
         year_month=year_month, 
         country=country)
