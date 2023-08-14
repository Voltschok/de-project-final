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

def load_transactions_data_postgres(table:str)->None:
    with vertica_python.connect(**conn) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.execute(f"SELECT max(update_ts) FROM  STV230530__STAGING.{table}_update")
        last_loaded_dt= cur_vertica.fetchone()
        cur_vertica.close()

    with psycopg2.connect(**postgres_conn) as connect_to_postgresql: 
        cursor = connect_to_postgresql.cursor()
        cur_postrgres = conn.cursor()
        input = io.StringIO()
        cur_postrgres.copy_expert(f'''COPY (SELECT * from public.{table} WHERE {operation_ts} > {last_loaded_dt} ORDER BY {operation_ts}) TO STDOUT;''', input)
        cur_postrgres.close()
      
    with vertica_python.connect(**conn) as connection:
        cur_vertica = connection.cursor()  
        cur_vertica.copy('''COPY table_1_temp FROM STDIN NULL AS 'null' ''',  input.getvalue())
        cur_vertica.execute("""INSERT INTO STV230530__STAGING.transactions_update(key, updates_ts) VALUES ("transactions", SELECT max(transaction_dt) FROM STV230530__STAGING.transaction
                              ON CONFLICT (key) DO UPDATE SET update_ts=EXCLUDED.update_ts""")
        cur_vertica.connection.commit()
        cur_vertica.close()
    
 
# def load_data(conn, path:str ,  file:str):  
#     df_csv = pd.read_csv( path )
#     tuple_col=", ".join(list(df_csv.columns) )
#     tuple_col_str= ('('+ str(tuple_col)+')')
#     print(tuple_col_str)
#     with vertica_python.connect(**conn) as connection:
#         cur = connection.cursor()
#         cur.execute(f"""delete from STV230530__STAGING.{ file }""")
#         connection.commit()
#         cur.execute(f"""COPY STV230530__STAGING.{ file }{tuple_col_str}
#             FROM LOCAL '{ path }' DELIMITER ',' ENFORCELENGTH""" )
#         connection.commit()
#         cur.close()
        
# def get_s3_file_list():
#         s3_file_list=[]
#         session = boto3.session.Session()
#         s3_client = session.client(service_name='s3',
#         endpoint_url='https://storage.yandexcloud.net',
#         aws_access_key_id=key_id,
#         aws_secret_access_key=secret_key)
#         objects = s3_client.list_objects(Bucket='final-project')
#         for object in objects['Contents']:
#             s3_file_list.append(object['Key'])
        
#         return s3_file_list

# def fetch_s3_file(bucket: str, key: str, bucket_quantity: int):
#     # сюда поместить код из скрипта для скачивания файла
#     files = os.listdir('/data/')
#     for i in range(1, bucket_quantity+1):	
#         if key=='transactions_batch_':
#             key_boto3=key+str(i)+'.csv'
#         else:
#             key_boto3=key+'.csv'
#         print(key)
        
#         if key_boto3 not in files:
#             s3_client.download_file(bucket,
#             key_boto3,
#             Filename=f'/data/{key_boto3}')

 
with DAG('final_project_staging', schedule_interval=None, start_date=pendulum.parse('2022-10-01')
) as dag:
 

    task1 = PythonOperator(
         task_id='load_transactions',
         python_callable=load_transactions_data_postgres,
         op_kwargs={'operation_ts': 'transaction_dt'},
     )
    task2 = PythonOperator(
        task_id='load_currencies',
        python_callable=load_currencies_data_postgres,
         op_kwargs={'operation_ts': 'transaction_dt'}, 
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



