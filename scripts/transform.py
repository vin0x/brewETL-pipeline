import os
import json
import pandas as pd

silver_path = '/opt/airflow/data/silver'

def transform_data():

    # loading rawdata from bronze layer
    with open('/opt/airflow/data/bronze/breweries_raw.json', 'r') as rawdata:
        data = json.load(rawdata)

    df = pd.DataFrame(data)

    # simple data cleaning
    df.drop_duplicates(subset='id', inplace=True)

    df.fillna({
        'state': 'Unknown', 
        'city': 'Unknown',
        'longitude': 0.0,
        'latitude': 0.0,
        'phone': 'Unknown',
        'website_url': 'Unknown'}, 
        inplace=True)

    df['id'] = df['id'].astype(str)
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0.0)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0.0)

    # create .parquet file and .csv
    os.makedirs(silver_path, exist_ok=True)
    df.to_parquet(os.path.join(silver_path, 'breweries_data_state.parquet'), index=False, partition_cols=['state'])
    df.to_parquet(os.path.join(silver_path, 'breweries_data_country.parquet'), index=False, partition_cols=['country'])
    df.to_csv(os.path.join(silver_path, 'breweries_df.csv'), index=False)