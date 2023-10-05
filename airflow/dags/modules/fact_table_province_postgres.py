from sqlalchemy.engine import create_engine 
import pandas as pd 
import psycopg2


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

engine_postgre = psycopg2.connect(dbname=postgres_database, 
user=postgres_username, 
host=postgres_host, 
password=postgres_password, 
port=postgres_port)
cur = engine_postgre.cursor()


raw_fact_prov = pd.read_sql("""
                SELECT
                    kode_prov AS province_id,
                    tanggal AS date, 
                    closecontact_dikarantina,
                    closecontact_discarded,
                    closecontact_meninggal,
                    confirmation_meninggal,
                    confirmation_sembuh,
                    probable_diisolasi,
                    probable_discarded,
                    probable_meninggal,
                    suspect_diisolasi,
                    suspect_discarded,
                    suspect_meninggal
                FROM mysql.covid_jabar
             """,
             con = engine_mysql
            )

raw_fact_prov_postgres = pd.read_sql("""
                                    SELECT 
                                        case_id,
                                        status
                                    FROM dwh.public.dim_status_table;
                                """,
                                con=engine_postgre
                                )

df_melted_prov = raw_fact_prov.melt(    id_vars=['province_id','date'],
                            value_vars= [ 'closecontact_dikarantina', 
                                          'closecontact_discarded',
                                          'closecontact_meninggal', 
                                          'confirmation_meninggal',
                                          'confirmation_sembuh',
                                          'probable_diisolasi', 
                                          'probable_discarded',
                                          'probable_meninggal', 
                                          'suspect_diisolasi', 
                                          'suspect_discarded',
                                          'suspect_meninggal'],
                            var_name = 'status',value_name = 'total')


province_daily = pd.merge(  df_melted_prov,raw_fact_prov_postgres,
                            on='status'
                        )
province_daily= province_daily[['province_id','case_id','date','total']]


province_daily.province_id = province_daily.province_id.astype(str)
province_daily.case_id = province_daily.case_id.astype(int)
province_daily.date = province_daily.date.astype(str)
province_daily.total = province_daily.total.astype(int)



fact_table_province_daily_query = """
CREATE TABLE IF NOT EXISTS dwh.public.fact_province_daily (
    id SERIAL PRIMARY KEY,
    province_id VARCHAR,
    case_id INT,
    date VARCHAR,
    total INT
);
"""
cur.execute("ROLLBACK")
cur.execute(fact_table_province_daily_query)



for i in province_daily.to_records():
    cur.execute(  f'''
                            DELETE FROM 
                                dwh.public.fact_province_daily
                            WHERE 
                                province_id	= '{i[1]}'
                                AND case_id	= '{i[2]}'
                                AND date	= '{i[3]}'
                                AND total   = '{i[4]}';
                            
                            INSERT INTO 
                                dwh.public.fact_province_daily  (province_id,
                                                            case_id,
                                                            date,
                                                            total) 
                            VALUES ('{i[1]}',
                                    '{i[2]}',
                                    '{i[3]}',
                                    '{i[4]}')
                        '''
                    )

cur.close()