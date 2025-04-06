## Instruction
### Streamlit Dashboard
- [V2](https://appappv2py-lyuz5i6avwj6iurz5udpkp.streamlit.app/)
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