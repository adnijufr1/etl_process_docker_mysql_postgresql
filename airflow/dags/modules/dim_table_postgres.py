from sqlalchemy.engine import create_engine 
import pandas as pd 
import os

# MySQL connection
mysql_username = 'root'
mysql_password = 'mysql'
mysql_host = '192.168.100.16'
mysql_port = '3307'
mysql_database = 'raw_data'

mysql_conn_string = f"mysql+mysqlconnector://{mysql_username}:{mysql_password}@{mysql_host}:{mysql_port}/{mysql_database}"
engine_mysql = create_engine(mysql_conn_string)

# PostgreSQL connection
postgres_username = 'postgres'
postgres_password = 'postgres'
postgres_host = '192.168.100.16'
postgres_port = '5435'
postgres_database = 'dwh'

postgres_conn_string = f"postgresql://{postgres_username}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"
engine_postgre = create_engine(postgres_conn_string)



df_status = pd.read_sql("""SELECT * FROM status_class;""", con=engine_mysql)  

dim_status = df_status.copy()
dim_status.to_sql(name='dim_status_table', index=False , con=engine_postgre, if_exists ='replace')


df_province = pd.read_sql("""SELECT kode_prov,nama_prov FROM covid_jabar;
""", con=engine_mysql)

dim_province = df_province[['kode_prov','nama_prov']].copy()
new_column_names_dim_prov = {'kode_prov':'province_id', 'nama_prov':'province_name'}
dim_province.rename(columns = new_column_names_dim_prov, inplace=True) 
dim_province.drop_duplicates(inplace=True)
dim_province.to_sql(name='dim_provice_table', con=engine_postgre, index=False, if_exists='replace')


df_district = pd.read_sql("""SELECT DISTINCT(kode_kab), nama_kab,nama_prov from covid_jabar;
""", con=engine_mysql)

dim_district = df_district.copy()
new_column_names_dim_district = {'kode_kab':'district_id', 'nama_kab':'district_name'}
dim_district.rename(columns=new_column_names_dim_district, inplace=True)
dim_district.to_sql(name='dim_district_table',con=engine_postgre, index=False, if_exists='replace')


print("---SUCCESSFULLY TRASFORMED THE DATA AND LOADED TO POSTGRESQL---")


