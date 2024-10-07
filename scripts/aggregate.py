import os
import pandas as pd
import sqlite3

def aggregate_data():
    silver_path = '/opt/airflow/data/silver'
    gold_path = '/opt/airflow/data/gold'
    os.makedirs(gold_path, exist_ok=True)

    # SQLite database connection
    conn = sqlite3.connect('/opt/airflow/data/breweries_aggregated.db')

    # creating a df by state, saving in .csv and loading to SQLite.
    parquet_file = os.path.join(silver_path, 'breweries_data_country.parquet')
    df_state = pd.read_parquet(parquet_file)
    
    state_df = df_state.groupby(['state', 'brewery_type']).size().reset_index(name='brewery_count')
    state_df = state_df[state_df['brewery_count'] != 0]

    state_df.to_csv(os.path.join(gold_path, 'breweries_aggregated_state.csv'), index=False)
    state_df.to_sql('breweries_aggregated_state', conn, if_exists='replace', index=False)

    

    # creating a df by country, saving in .csv and loading to SQLite.
    parquet_file = os.path.join(silver_path, 'breweries_data_country.parquet')
    df_country = pd.read_parquet(parquet_file)

    country_df = df_country.groupby(['country', 'brewery_type']).size().reset_index(name='brewery_count')
    country_df = country_df[country_df['brewery_count'] != 0]

    country_df.to_csv(os.path.join(gold_path, 'breweries_aggregated_country.csv'), index=False)
    country_df.to_sql('breweries_aggregated_country', conn, if_exists='replace', index=False)

