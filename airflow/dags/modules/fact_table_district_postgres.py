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


raw_fact_dist = pd.read_sql("""
                SELECT
                    kode_kab AS district_id,
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

raw_fact_dist_postgres = pd.read_sql("""
                                    SELECT 
                                        case_id,
                                        status
                                    FROM dwh.public.dim_status_table;
                                """,
                                con=engine_postgre
                                )

df_melted = raw_fact_dist.melt(    id_vars=['district_id','date'],
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


district_daily = pd.merge(  df_melted,raw_fact_dist_postgres,
                            on='status'
                        )
district_daily = district_daily[['district_id','case_id','date','total']]


district_daily.district_id = district_daily.district_id.astype(str)
district_daily.case_id = district_daily.case_id.astype(int)
district_daily.date = district_daily.date.astype(str)
district_daily.total = district_daily.total.astype(int)


fact_table_district_daily_query = """
CREATE TABLE IF NOT EXISTS dwh.public.fact_district_daily (
    id SERIAL PRIMARY KEY,
    district_id VARCHAR,
    case_id INT,
    date VARCHAR,
    total INT
);
"""
cur.execute("ROLLBACK")
cur.execute(fact_table_district_daily_query)



for i in district_daily.to_records():
    cur.execute(  f'''
                            DELETE FROM 
                                dwh.public.fact_district_daily
                            WHERE 
                                district_id	= '{i[1]}'
                                AND case_id	= '{i[2]}'
                                AND date	= '{i[3]}'
                                AND total   = '{i[4]}';
                            
                            INSERT INTO 
                                dwh.public.fact_district_daily  (district_id,
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