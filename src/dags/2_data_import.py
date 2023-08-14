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

def insert_into_global_metrics():
  with vertica_python.connect(**vertica_conn_info) as connection:
 
     query= f'sql/insert_into_global_metrics.sql'
 
     cur_vertica = connection.cursor()  
     cur_vertica.execute(query)
     cur_vertica.connection.commit()
     cur_vertica.close()

with DAG('final_project_staging', schedule_interval=None, start_date=pendulum.parse('2022-10-01')
) as dag:
 

    task1 = PythonOperator(
         task_id='load_transactions',
         python_callable=load_transactions_data_postgres,
         op_kwargs={'operation_ts': 'transaction_dt'},
     )
 
 
task1  
