import pandas as pd
from sqlalchemy import create_engine
import os


file_name = 'zone_data.csv'
url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'
os.system(f'wget {url} -O {file_name}')
zone_df = pd.read_csv(file_name)
engin = create_engine(url='postgresql://root:root@localhost:5432/ny_taxi_data')
engin.connect()

zone_df.head(n=0).to_sql(name='zone_data',con=engin,if_exists='replace')
zone_df.to_sql(name='zone_data',con=engin,if_exists='append')

