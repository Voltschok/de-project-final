from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag
import configparser
import pendulum
import vertica_python
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd 
import psycopg2
import os
import logging
import io
 
# Чтение параметров подключения для Postgres и Vertica из конфигурации
config = configparser.ConfigParser()
config.read("../../../lessons/config.ini")
 

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

# Параметры безопасности Vertica
vertica_host = config.get('Vertica', 'host')
vertica_port = config.get('Vertica', 'port')
vertica_user = config.get('Vertica', 'user')
vertica_password =  config.get('Vertica', 'password')

vertica_conn_info = {'host': vertica_host,
             'port': vertica_port,
             'user': vertica_user,
             'password': vertica_password,
             'database': '',
             # 10 minutes timeout on queries
             'read_timeout': 600,
             # default throw error on invalid UTF-8 results
             'unicode_error': 'strict',
             # SSL is disabled by default
             'ssl': False,
             'connection_timeout': 30
             # connection timeout is not enabled by default
            }
print(vertica_conn_info)

#Параметры подключения к S3
key_id= 'YCAJEWXOyY8Bmyk2eJL-hlt2K' #config.get('S3', 'aws_access_key_id')
secret_key='YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA' #config.get('S3', 'aws_secret_access_key')

def load_data_postgres(table: str, operation_ts: str)->None:
   with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(f"SELECT max(update_ts) FROM  STV230530__STAGING.{table}_update")
        last_loaded_dt= (cur_vertica.fetchone())[0].date()   
        print(last_loaded_dt)
        cur_vertica.close()
     
   with psycopg2.connect(**postgres_conn) as connect_to_postgresql:
        cur_postrgres = connect_to_postgresql.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(f'''COPY (SELECT * from public.{table} WHERE {operation_ts} > '{last_loaded_dt}' ORDER BY {operation_ts}) TO STDOUT;''', input)
        cur_postrgres.close()
        print(input.getvalue())
   with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.copy(f'''COPY STV230530__STAGING.{table} FROM STDIN DELIMITER E'\t'  NULL AS 'null'  ABORT ON ERROR;''', input.getvalue())
        cur_vertica.connection.commit()

        cur_vertica.execute(f"""UPDATE STV230530__STAGING.{table}_update SET update_ts=(SELECT max({operation_ts}) FROM STV230530__STAGING.{table})""")
        cur_vertica.connection.commit()
        cur_vertica.close()
    

 

def insert_into_hubs(hub_list):
 
  for hub in hub_list:
     path=f"/lessons/sql/insert_into_{hub}.sql"
     query=  open(path).read() 
     print(query)  
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()
       
def insert_into_links(link_list):
 
  for link in link_list:
     query= open(f"/lessons/sql/insert_into_{link}.sql").read() 
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()  
       
def insert_into_sattelites(sattelite_list):
 
  for sattelite in sattelite_list:
     query= open(f"/lessons/sql/insert_into_{sattelite}.sql").read() 
 
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()

 

with DAG('final_project_staging', schedule_interval=None, start_date=pendulum.parse('2022-10-01'),  
) as dag:
 

   #  task1 = PythonOperator(
   #       task_id='load_transactions',
   #       python_callable=load_data_postgres,
   #       op_kwargs={'table': 'transactions' ,'operation_ts': 'transaction_dt'},
   #   )
   #  task2 = PythonOperator(
   #      task_id='load_currencies',
   #      python_callable=load_data_postgres,
   #       op_kwargs={'table': 'currencies' ,'operation_ts': 'date_update'}, 
   #  )
    task3=PythonOperator(
       task_id='insert_into_hubs',
       python_callable=insert_into_hubs,
       op_kwargs={'hub_list': ['h_currencies', 'h_accounts', 'h_transactions']  },
    )
    task4=PythonOperator(
       task_id='insert_into_links',
       python_callable=insert_into_links,
       op_kwargs={'link_list': ['l_transaction_account', 'l_transaction_currency'] },
    )
    task5=PythonOperator(
       task_id='insert_into_sattelites',
       python_callable=insert_into_sattelites,
       op_kwargs={'sattelite_list': ['s_transactions_amount', 's_transactions_status', 's_transactions_type', 's_transactions_country', 's_currency_exchange_rate'] },
    )
 
 
#task1 >> task2 >> 
task3 >> task4 >> task5



