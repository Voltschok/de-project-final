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
 

 

# Параметры безопасности Vertica
vertica_host = 'vertica.tgcloudenv.ru'  
vertica_port = '5433' 
vertica_user = 'stv230530' 
vertica_password =  'IjUMUB8AONAHDcT' 

conn_info = {'host': vertica_host,
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

 
 

def load_data(conn, path:str ,  file:str):  
    df_csv = pd.read_csv( path )
    tuple_col=", ".join(list(df_csv.columns) )
    tuple_col_str= ('('+ str(tuple_col)+')')
    print(tuple_col_str)
    with vertica_python.connect(**conn) as connection:
        cur = connection.cursor()
        cur.execute(f"""delete from STV230530__STAGING.{ file }""")
        connection.commit()
        cur.execute(f"""COPY STV230530__STAGING.{ file }{tuple_col_str}
            FROM LOCAL '{ path }' DELIMITER ',' ENFORCELENGTH""" )
        connection.commit()
        cur.close()
        
 
def fetch_s3_file(bucket: str, key: str, bucket_quantity: int):
    # сюда поместить код из скрипта для скачивания файла
    
    for i in range(1, bucket_quantity+1):	
        if key=='transactions_batch_':
            key_boto3=key+str(i)+'.csv'
        else:
            key_boto3=key+'.csv'
        print(key)

        session = boto3.session.Session()
        s3_client = session.client(service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key)
        s3_client.download_file(bucket,
        key_boto3,
        Filename=f'/data/{key_boto3}')

 
with DAG('final_project_staging', schedule_interval=None, start_date=pendulum.parse('2022-10-01')
) as dag:
 

    task1 = PythonOperator(
         task_id='fetch_currencies',
         python_callable=fetch_s3_file,
         op_kwargs={'bucket': 'final-project', 'key': 'currencies_history', 'bucket_quantity':1},
     )
    task2 = PythonOperator(
        task_id='fetch_transactions',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'final-project', 'key': 'transactions_batch_', 'bucket_quantity':10}
    )
 
    task3=PythonOperator(
       task_id='load_currencies',
       python_callable=load_data,
       op_kwargs={'conn': conn_info, 'path':'/data/currencies_history.csv', 'file':'currencies'},
    )
    task4=PythonOperator(
       task_id='load_transactions',
       python_callable=load_data,
       op_kwargs={'conn': conn_info, 'path':'/data/transactions_batch_1.csv', 'file':'transactions'},
    )
 
task1 >> task2 >> task3 >> task4



