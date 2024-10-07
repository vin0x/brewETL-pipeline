import requests
import os
import json

# openbrewery API URL: https://api.openbrewerydb.org/breweries

def extract_data():
    url = 'https://api.openbrewerydb.org/breweries'
    response = requests.get(url)
    response.raise_for_status()  

    data = response.json()
    os.makedirs('/opt/airflow/data/bronze', exist_ok=True)
    with open('/opt/airflow/data/bronze/breweries_raw.json', 'w') as raw_data_file:
        json.dump(data, raw_data_file)
