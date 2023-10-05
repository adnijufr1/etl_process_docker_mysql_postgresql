from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime


path = '/opt/airflow/dags/modules'

with DAG(
        dag_id="DAG__Final",
        schedule_interval=None,
        start_date=datetime(2023, 10, 3),
        catchup=False
        ) as dag:

        task1 = BashOperator(
            task_id = "get_data_from_API",
            bash_command = f'python3 {path}/data_extraction.py')

        mysql_query = """
                CREATE TABLE IF NOT EXISTS status_class(
                        case_id INT,
                        status_name varchar(50), 
                        status_detail varchar(50), 
                        status varchar(50));

                INSERT INTO status_class (case_id,status_name, status_detail, status)
                VALUES
                (1,'closecontact', 'dikarantina', 'closecontact_dikarantina'),
                (2,'closecontact', 'discarded', 'closecontact_discarded'),
                (3,'closecontact', 'meninggal', 'closecontact_meninggal'),
                (4,'confirmation', 'meninggal', 'confirmation_meninggal'),
                (5,'confirmation', 'sembuh', 'confirmation_sembuh'),
                (6,'probable', 'diisolasi', 'probable_diisolasi'),
                (7,'probable', 'discarded', 'probable_discarded'),
                (8,'probable', 'discarded', 'probable_discarded'),
                (9,'suspect', 'diisolasi', 'suspect_diisolasi'),
                (10,'suspect', 'diisolasi', 'suspect_diisolasi'),
                (11,'suspect', 'diisolasi', 'suspect_diisolasi');;"""


        mysql_task = MySqlOperator(
                task_id = "Creating_NewColumns_and_Inserting_Values_for_dim_status_table_only",
                mysql_conn_id = 'Mysql',
                sql=mysql_query,
                autocommit=True,
                dag=dag)


        task2 =  BashOperator(
            task_id = "generate_dim_table",
            bash_command = f'python3 {path}/dim_table_postgres.py')

        task3 = BashOperator(
            task_id = "generate_district_daily_table",
            bash_command =  f'python3 {path}/fact_table_district_postgres.py')    

        task4 = BashOperator(
            task_id = "generate_province_daily_table",
            bash_command =  f'python3 {path}/fact_table_province_postgres.py') 
        

        task1 >> mysql_task >> task2 >> [task3, task4]  

