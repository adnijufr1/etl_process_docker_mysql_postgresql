# ETL Process Pipelines Using Docker, Airflow, MySQL, and PostGreSQL

This final project is about Dockerize ETL Pipeline using ETL tools and Airflow that extracts Public API data from PIKOBAR, then load into MySQL (Staging Area) and finally aggregate the data and save into PostgreSQL.

## Project's Steps
1. Create Docker (MySQL, Airflow and PostgreSQL) in your local computer. 
2. Create Docker Database in MySQL and PostgreSQL.
3. First create connection on Airflow in order to extract data from API Endpoint.
4. Create DLL in MySQL.
5. Extract data from API Endpoint then the data will be loaded and staged in MySQL.
6. Create DDL in PostgreSQL for Fact table and Dimension table.
7. Create load data to Dimension table 
8. Create script for aggregate Province Daily save to Province Daily Table 
9. Create script for aggregate District Daily save to District Daily Table 
10. Create DAG with schedule daily basis with task:
    
    a. get_data_from_API.
    
    b. Creating_NewColumns_and_Inserting_Values_for_dim_status_table_only.
    
    c. generate_dim_table.
    
    d. generate_district_daily_table.
    
    e. generate_province_daily_table.

## ETL Architecture Diagram
![ETL Architecture](https://github.com/adnijufr1/etl_process_docker_mysql_postgresql/assets/108950455/a09fb51c-0699-4d26-b722-6510cbde7f24)

## Dag Flow
![flow_dag (1)](https://github.com/adnijufr1/etl_process_docker_mysql_postgresql/assets/108950455/aada2500-9032-4a4f-bede-fb474b0339ca)

## Relational Database Model COVID-19 Jabar
![Relational Database Model Covid19 Jabar ](https://github.com/adnijufr1/etl_process_docker_mysql_postgresql/assets/108950455/9e4fa39c-25e3-4750-af34-d4d8ac96149a)




