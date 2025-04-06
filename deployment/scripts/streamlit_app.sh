#!/bin/bash

# Navigate to the project root directory
cd ..

# Run the Streamlit app
streamlit run ./src/streamlit_app_V2.py -- --data_path ./data 

# # Navigate to the directory containing your script
# cd scripts

# # Make the script executable
# chmod +x streamlit_app.sh

# # Or if you're in the project root directory:
# chmod +x scripts/streamlit_app.sh