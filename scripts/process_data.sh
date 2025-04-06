#!/bin/bash

# Process US COVID and flight data
python src/process_data.py --year_month 2020 --country US
python src/process_data.py --year_month 2021 --country US
python src/process_data.py --year_month 2022 --country US
python src/process_data.py --year_month 2023 --country US

# # Navigate to the directory containing your script
# cd scripts

# # Make the script executable
# chmod +x process_data.sh

# # Or if you're in the project root directory:
# chmod +x scripts/process_data.sh