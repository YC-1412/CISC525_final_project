# Instruction
## Streamlit Dashboard
- [V2](https://cisc525-proj2025-spring-v2.streamlit.app/) (read data from GitHub)
- [V2.1](https://cisc525-proj2025-spring-v2-1.streamlit.app/) (read data from GCP). This one will fail after May 2025 when this couse ends and the cluster and bucket are deleted.
    - For next time deployment, refer to the README under the deployment folder or the [wiki page](https://github.com/YC-1412/CISC525_final_project/wiki).
## If you want to run Streamlit app locally
Clone the entire repo including the data, and run the below command in terminal
- Option 1
    ```
    chmod +x scripts/streamlit_app.sh
    cripts/streamlit_app.sh
    ```
- Option 2
    ```
    streamlit run ./src/streamlit_app.py -- --data_path ./data
    ```

## Data Source
- Plane data
    - [Crowdsourced air traffic data from The OpenSky Network 2020](https://doi.org/10.5281/zenodo.7923702)
- COVID data
    - [csse_covid_19_daily_reports](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)
    - [time_series_covid19_confirmed_global.csv](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv)
    - [time_series_covid19_confirmed_US.csv](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv)
- Mapping
    - `airports.csv` comes from [https://github.com/mborsetti/airportsdata](https://github.com/mborsetti/airportsdata/blob/main/airportsdata/airports.csv)
    - `countries.csv` comes from [radcliff/wikipedia-iso-country-codes.csv](https://gist.github.com/radcliff/f09c0f88344a7fcef373#file-wikipedia-iso-country-codes-csv)

## For batch data analysis
- Clone this repository
- Save the COVID data to data/csse_covid_19_daily_reports.
    - There is a 2022-01-01.csv file for testing.
- Save the flight data to data/flight_volume_raw.
    - There is a 20220101_20220101 file for testing.
- Modify the time param in scripts/process_data.sh to process the data in batches. Supported time param is either year (YYYY) or yearmonth (YYYYMM). 
- Result will be in the processed_data folder.
- Upload the new files in processed_data to GitHub.
- Streamlit dashboard should updated automatically.


# How it works
**Processing COVID cases (PySpark)**

Process global daily COVID cases to monthly COVID cases. Aggregate from province/administration area level to national level, and calculate daily change.

The input files are daily cumulative COVID Confirmed, Deaths, Recovered, and Active case count by country, province and administration area. This script load in the daily file, aggregate it to the country-month level, producing both cumulative and newly COVID Confirmed, Deaths, Recovered, and Active case count.
- Data comes from [COVID-19 case data from Johns Hopkins CSSE](https://github.com/CSSEGISandData/COVID-19).
- Data cleaning
    - There are some daily files that does not come with the consistent schema, e.g. missing `Admin2` and `Province_State`, and have `Country/Region` instead of `Country_Region`. Those are a small portion of the data and hard to identify because all daily file has the same name. We just skip those files in this analysis.
    - There are 487 (0.011%) records with abnormal dates, e.g. 2682-01-01, or `Country_Region` name as numbers. Those records are dropped.
    - The daily file could have records not on that date. For example, a 01-01-2021 file can have records for 2020 or Apr 2021, maybe because it is the last updated date.

**Processing flight volume data (PySpark)**

Map airport names to nations, and aggreate flight records to daily level for each country.

The input files are daily flight information about flight origin and destination by airport. This script load in the data, map airport to country, and aggregate flight volume to the country-month level. It can either aggregate by origin or desination country.
- Data comes from https://zenodo.org/records/7923702
    - The monthly file could have records not in that month. For example, a Jan 2021 file can have records for 2020, reason unknown.
- `airports.csv` comes from [https://github.com/mborsetti/airportsdata](https://github.com/mborsetti/airportsdata/blob/main/airportsdata/airports.csv)
- `countries.csv` comes from [radcliff/wikipedia-iso-country-codes.csv](https://gist.github.com/radcliff/f09c0f88344a7fcef373#file-wikipedia-iso-country-codes-csv)

**Dashboard**

The processed data is saved to data/processed_data folder. A dashboard is deployed on Streamlit to read in the data and render the dashboard using src/streamlit_app_V2.py.


# Todo
- Support batch process on GCP. 
    - Need to move the code to GCP and schedule a job.
    - Need to set up the data connection from GCP to streamlit. Consider using the Cloud Storage for data storage.
    - (Optional) Use a database to do this.
- Support real-time process on GCP.
    - Need to update process_data.sh to support realtime data process.
- Consider doing some predictive modeling on the data.

# Team members
- Matthew Kane
- Shengjie Fu
- Rupa Shravya Gajula
- Yingyu Cao
- Ramesh Anusha Katta

# Acknowledge
- Dong E, Du H, Gardner L. An interactive web-based dashboard to track COVID-19 in real time. Lancet Inf Dis. 20(5):533-534. doi: 10.1016/S1473-3099(20)30120-1
- Xavier Olive, Martin Strohmeier, & Jannis Lübbe. (2023). Crowdsourced air traffic data from The OpenSky Network 2020 (v23.00) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.7923702