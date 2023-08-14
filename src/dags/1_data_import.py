from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import configparser
import pendulum
import vertica_python
import boto3
import pandas as pd 
import psycopg2
import os
import logging



#Параметры подключения POSTGRES
postgres_conn={
"host" : "rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net",
"port" : 6432,
"dbname": "db1",
"user" : “student”,
"password" : "d_student_112022"}




# Параметры безопасности Vertica
vertica_host = 'vertica.tgcloudenv.ru'  
vertica_port = '5433' 
vertica_user = 'stv230530' 
vertica_password =  'IjUMUB8AONAHDcT' 

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
 

#Параметры подключения к S3
key_id= 'YCAJEWXOyY8Bmyk2eJL-hlt2K' #config.get('S3', 'aws_access_key_id')
secret_key='YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA' #config.get('S3', 'aws_secret_access_key')

def load_transactions_data_postgres(table: str, operation_ts: str)->None:
    with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(f"SELECT max({operation_ts}) FROM  STV230530__STAGING.{table}_update")
        last_loaded_dt= cur_vertica.fetchone()
        cur_vertica.close()

    with psycopg2.connect(**postgres_conn) as connect_to_postgresql: 
        cursor = connect_to_postgresql.cursor()
        cur_postrgres = conn.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(f'''COPY (SELECT * from public.{table} WHERE {operation_ts} > {last_loaded_dt} ORDER BY {operation_ts}) TO STDOUT;''', input)
        cur_postrgres.close()
      
    with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.copy('''COPY table_1_temp FROM STDIN NULL AS 'null' ''',  input.getvalue())
        cur_vertica.execute(f"""INSERT INTO STV230530__STAGING.{table}_update(key, updates_ts) VALUES ({table}, SELECT max(transaction_dt) FROM STV230530__STAGING.transaction
                              ON CONFLICT (key) DO UPDATE SET update_ts=EXCLUDED.update_ts""")
        cur_vertica.connection.commit()
        cur_vertica.close()
    

def insert_into_hubs(hub_list):
 
  for table in hub_list:
     query= f'sql/insert_into_{table}.sql'
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()
       
def insert_into_links(sattelite_list):
 
  for table in sattelite_list:
     query= f'sql/insert_into_{table}.sql'
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()  
       
def insert_into_sattelites(sattelite_list):
 
  for table in sattelite_list:
     query= f'sql/insert_into_{table}.sql'
     with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(query)
        cur_vertica.connection.commit()
        cur_vertica.close()
       
with DAG('final_project_staging', schedule_interval=@daily, start_date=pendulum.parse('2022-10-01')
) as dag:
 

    task1 = PythonOperator(
         task_id='load_transactions',
         python_callable=load_transactions_data_postgres,
         op_kwargs={'operation_ts': 'transaction_dt'},
     )
    task2 = PythonOperator(
        task_id='load_currencies',
        python_callable=load_currencies_data_postgres,
         op_kwargs={'operation_ts': 'date_update'}, 
    )
 
    task3=PythonOperator(
       task_id='insert_into_hubs',
       python_callable=insert_into_hubs,
       op_kwargs={'hub_list': ['h_currencies', 'h_accounts', 'h_transactions'] },
    )
    task4=PythonOperator(
       task_id='insert_into_links',
       python_callable=insert_into_links,
       op_kwargs={'link_list': ['l_transaction_account', 'l_transaction_currency] },
    )
    task5=PythonOperator(
       task_id='insert_into_sattelites',
       python_callable=insert_into_sattelites,
       op_kwargs={'sattelite_list': ['s_transaction_amount', 's_transaction_status', 's_transaction_type', 's_transaction_country', 's_currency_exchange_rate'] },
    )
 
 
task1 >> task2 >> task3 >> task4 >> task5



