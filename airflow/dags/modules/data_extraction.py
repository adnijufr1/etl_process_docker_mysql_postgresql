from sqlalchemy import create_engine
import requests
import os
import pandas as pd 

url_src = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'


response = requests.get(url_src)
data= response.json()['data']['content']
df = pd.DataFrame(data)

# Dataframe information
print(df.info())

engine_mysql = create_engine('mysql+mysqlconnector://root:mysql@192.168.100.16:3307/raw_data')

df.to_sql(name='covid_jabar', con=engine_mysql,index=False,if_exists='replace')
print('DATA INSERTED TO MYSQL SUCCESSFULLY')