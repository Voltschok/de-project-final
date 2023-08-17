from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
import configparser
import pendulum
import vertica_python
import psycopg2
import os

 
# Чтение параметров подключения для Postgres и Vertica из конфигурации
config = configparser.ConfigParser()
config.read("../../../lessons/dags/config.ini")
 

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

def create_table_currency():

     query=  open(f"/lessons/sql/create_table_currencies.sql").read() 
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()
def create_table_transaction():

     query=  open(f"/lessons/sql/create_table_transactions.sql").read() 
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()
       
def create_table_global_metric():

     query=  open(f"/lessons/sql/create_table_global_metrics.sql").read() 
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()

with DAG('init_ddl', schedule_interval=None, start_date=pendulum.parse('2023-08-17'), catchup=False) as dag:
 

     task1 = PythonOperator(
          task_id='create_table_currency',
          python_callable=create_table_currency,
         
      )
     task2 = PythonOperator(
          task_id='create_table_transaction',
          python_callable=create_table_transaction,
         
      )
     task3 = PythonOperator(
          task_id='create_table_global_metric',
          python_callable=create_table_global_metric,
         
      ) 
 
 
task1  >> task2 >> task3
