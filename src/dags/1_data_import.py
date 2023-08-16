from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import configparser
import pendulum
import vertica_python
import psycopg2
import os
import logging
import io
 
# Чтение параметров подключения для Postgres и Vertica из конфигурации
config = configparser.ConfigParser()
config.read("../../../lessons/dags/config.ini")
 

# Параметры подключения Postgres
postgres_host=config.get('Postgres', 'host')
postgres_port=config.get('Postgres', 'port')
postgres_dbname=config.get('Postgres', 'dbname')
postgres_user=config.get('Postgres', 'user')
postgres_password=config.get('Postgres', 'password')

postgres_conn={
"dbname": postgres_dbname,
"host" : postgres_host,
"port" : postgres_port,
"user" : postgres_user,
"password" : postgres_password}

# Параметры подключения Vertica
vertica_host = config.get('Vertica', 'host')
vertica_port = config.get('Vertica', 'port')
vertica_user = config.get('Vertica', 'user')
vertica_password =  config.get('Vertica', 'password')

vertica_conn_info = {'host': vertica_host,
             'port': vertica_port,
             'user': vertica_user,
             'password': vertica_password,
             'database': '',
             'read_timeout': 600,
             'unicode_error': 'strict',
             'ssl': False,
             'connection_timeout': 30
         
            }

def load_data_postgres_vertica(table: str, operation_ts: str, count_date)->None:
   print(count_date)
   with psycopg2.connect(**postgres_conn) as connect_to_postgresql:
        cur_postrgres = connect_to_postgresql.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(f'''COPY (SELECT distinct * from public.{table} WHERE {operation_ts}::date='{count_date}'::date-1 ORDER BY {operation_ts}) TO STDOUT;''', input)
        cur_postrgres.close()
      
   with vertica_python.connect(**vertica_conn_info) as connection:

        cur_vertica = connection.cursor()  
        cur_vertica.execute(f"""DELETE FROM STV230530__STAGING.{ table } WHERE {operation_ts}::date='{count_date}'::date-1""")
        connection.commit()
        cur_vertica.copy(f'''COPY STV230530__STAGING.{table} FROM STDIN DELIMITER E'\t'  NULL AS 'null'  REJECTED DATA AS TABLE COPY_EX1_rej;''', input.getvalue())
        cur_vertica.connection.commit()
        cur_vertica.close()
    

with DAG('final_project_staging', schedule_interval="@daily", start_date=pendulum.parse('2022-10-01'), catchup=True 
) as dag:

     task1 = PythonOperator(
         task_id='load_transactions',
         python_callable=load_data_postgres_vertica,
         op_kwargs={'table': 'transactions' ,'operation_ts': 'transaction_dt', 'count_date': '{{ ds }}'},
      )
     task2 = PythonOperator(
         task_id='load_currencies',
         python_callable=load_data_postgres_vertica,
         op_kwargs={'table': 'currencies' ,'operation_ts': 'date_update', 'count_date': '{{ ds }}'}, 
      )
  
task1 >> task2 

