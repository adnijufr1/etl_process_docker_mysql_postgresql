from sqlalchemy.engine import create_engine 
import pandas as pd 
import os

# MySQL connection
mysql_username = 
mysql_password = 
mysql_host = 
mysql_port = 
mysql_database = 

mysql_conn_string = f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
engine_mysql = create_engine(mysql_conn_string)

# PostgreSQL connection
postgres_username = 
postgres_password = 
postgres_host = 
postgres_port = 
postgres_database = 

postgres_conn_string = f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
engine_postgre = create_engine(postgres_conn_string)



# Dim Province

df_province = pd.read_sql("""SELECT kode_prov,nama_prov FROM covid_jabar;
""", con=engine_mysql)

dim_province = df_province[['kode_prov','nama_prov']].copy()
new_column_names_dim_prov = {'kode_prov':'province_id', 'nama_prov':'province_name'}
dim_province.rename(columns = new_column_names_dim_prov, inplace=True) 
dim_province.drop_duplicates(inplace=True)
dim_province.to_sql(name='dim_provice_table', con=engine_postgre, index=False, if_exists='replace')

# Dim District
df_district = pd.read_sql("""SELECT DISTINCT(kode_kab), nama_kab,nama_prov from covid_jabar;
""", con=engine_mysql)

dim_district = df_district.copy()
new_column_names_dim_district = {'kode_kab':'district_id', 'nama_kab':'district_name'}
dim_district.rename(columns=new_column_names_dim_district, inplace=True)
dim_district.to_sql(name='dim_district_table',con=engine_postgre, index=False, if_exists='replace')


print("---SUCCESSFULLY TRASFORMED THE DATA AND LOADED TO POSTGRESQL---")


