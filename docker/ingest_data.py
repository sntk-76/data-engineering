
import pandas as pd 
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    parquet_name = 'yellow_tripdata_2021-01.parquet'
    os.system(f'wget {url} -O {parquet_name}')
   
    parquet_file = pd.read_parquet(parquet_name)
    parquet_file.to_csv('yellow_tripdata_2021-01.csv')
    csv_name = 'yellow_tripdata_2021-01.csv'
  
    engin = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engin.connect()

    df_iter  = pd.read_csv(csv_name,iterator=True,chunksize=100000)
    #sql_table = pd.io.sql.get_schema(df,name='yellow_taxi_data',con=engin)
    #print(sql_table)

    df = next(df_iter)
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df.head(n=0).to_sql(name=table_name,con=engin,if_exists='replace')
    df.to_sql(name=table_name,con=engin,if_exists='append')

    while True:
        try :
            begin = time()
            df = next(df_iter)
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df.to_sql(name=table_name,con=engin,if_exists='append')
            end = time()
        
            print(f'tHE NEW CHUNCK WAS ADDED TO THE DATABASE IN : {end-begin} SECONDS')
            
        except StopIteration:

            print("NO MORE DATA TO APPEND \n FIRST TABLE WAS COMPLETELY ADDED")
            break
    
    url_zone_data = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
    file_name = 'zone_data.csv'
    os.system(f'wget {url_zone_data} -O {file_name}')
    zone_df = pd.read_csv(file_name)
    zone_df.head(n=0).to_sql(name='zone_data',con=engin,if_exists='replace')
    zone_df.to_sql(name='zone_data',con=engin,if_exists='append')    
    print("NO MORE DATA TO APPEND \n SECOND TABLE WAS COMPLETELY ADDED")

if __name__ == "__main__" :     

    parser = argparse.ArgumentParser(description='ingest CSV data to the postgres')
    parser.add_argument('--user',help='username for the postgres')
    parser.add_argument('--password',help='password for the postgres')
    parser.add_argument('--host',help='host for the postgres')
    parser.add_argument('--port',help='port for the postgres')
    parser.add_argument('--db',help='database name for the postgres')
    parser.add_argument('--table_name',help='first table name for the postgres')
    parser.add_argument('--url',help='url of the csv file')
    args = parser.parse_args()

    main(args)