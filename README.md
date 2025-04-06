## Instruction
### Streamlit Dashboard
- [V2](https://cisc525-proj2025-spring-v2.streamlit.app/)
### To run Streamlit app locally
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
    - Xavier Olive, Martin Strohmeier, & Jannis Lübbe. (2023). Crowdsourced air traffic data from The OpenSky Network 2020 (v23.00) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.7923702
- COVID data
    - Dong E, Du H, Gardner L. An interactive web-based dashboard to track COVID-19 in real time. Lancet Inf Dis. 20(5):533-534. doi: 10.1016/S1473-3099(20)30120-1
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

# Todo
- Support batch process on GCP. 
    - Need to move the code to GCP and schedule a job.
    - Need to set up the data connection from GCP to streamlit. Consider using the Cloud Storage for data storage.
    - (Optional) Use a database to do this.
- Support real-time process on GCP.
    - Need to update process_data.sh to support realtime data process.
- Consider doing some predictive modeling on the data.
