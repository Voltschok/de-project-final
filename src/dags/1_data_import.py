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
 

# Чтение параметров подключения для Vertica и Postgres из конфигурации
config = configparser.ConfigParser()

config.read('config.ini')

# Параметры безопасности Vertica
vertica_host = config.get('Vertica', 'host')
vertica_port = config.get('Vertica', 'port')
vertica_user = config.get('Vertica', 'user')
vertica_password = config.get('Vertica', 'password')

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

# Параметры подключения Postgres

#postgres_host=config.get('Postgres', 'host')
#postgres_port=config.get('Postgres', 'port')
#postgres_dbname=config.get('Postgres', 'dbname')
#postgres_user=config.get('Postgres', 'user')
#postgres_password=config.get('Postgres', 'password')

#Параметры подключения к S3
key_id=config.get('S3', 'aws_access_key_id')
secret_key=config.get('S3', 'aws_secret_access_key')

# Модуль 4: Чтение данных из PostgreSQL
# def read_from_postgresql():

#     try:
#         connect_to_postgresql = psycopg2.connect(f"""host={postgres_host} 
#                                                 port={postgres_port} 
#                                                 dbname={postgres_dbname}
#                                                 user={postgres_user}
#                                                 password={postgres_password}""")
#         cursor = connect_to_postgresql.cursor()
#         # Здесь выполняется чтение данных из  PostgreSQL 
#         cursor.execute(""" """) 
                       
#     except psycopg2.Error as e:
#         # Обработка ошибки подключения к PostgreSQL
#         print("Ошибка при подключении к PostgreSQL:", e) 
 

def load_data(conn, path:str ,  file:str):  
    df_csv = pd.read_csv( path )
    tuple_col=", ".join(list(df_csv.columns) )
    tuple_col_str= ('('+ str(tuple_col)+')')
    
    with vertica_python.connect(**conn) as connection:
        cur = connection.cursor()
        cur.execute(f"""delete from STV230530__STAGING.{ file }""")
        connection.commit()
        cur.execute(f"""COPY STV230530__STAGING.{ file }{tuple_col_str}
            FROM LOCAL '{ path }' DELIMITER ',' ENFORCELENGTH""" )
        connection.commit()
        cur.close()
        
 
def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла

    session = boto3.session.Session()
    s3_client = session.client(
    service_name='S3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=key_id,
    aws_secret_access_key=secret_key)
    s3_client.download_file(
    bucket,
    key,
    Filename=f'/data/{key}')

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла

bucket_files = ['currencies_history.csv']

bash_command_tmpl = """
{% for file in params.files %}
head {{ file }}
{% endfor %}
"""



with DAG('final_project_staging', schedule_interval=None, start_date=pendulum.parse('2022-10-01')
) as dag:
 

    task1 = PythonOperator(
         task_id='fetch_currencies',
         python_callable=fetch_s3_file,
         op_kwargs={'bucket': 'final_project', 'key': 'currencies_history.csv'},
     )
#     task2 = PythonOperator(
#         task_id='fetch_dialogs',
#         python_callable=fetch_s3_file,
#         op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
#     )
#     task3 = PythonOperator(
#        task_id='fetch_users',
#        python_callable=fetch_s3_file,
#        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
#     )
#     task4 = PythonOperator(
#         task_id='fetch_group_log',
#         python_callable=fetch_s3_file,
#         op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
#     )
    
#     print_10_lines_of_each = BashOperator(
#         task_id='print_10_lines_of_each',
#         bash_command=bash_command_tmpl,
#         params={'files': [f'/data/{f}' for f in bucket_files]}
#     )
    task5=PythonOperator(
       task_id='load_currencies',
       python_callable=load_data,
       op_kwargs={'conn': 'conn_info', 'path':'/data/currencies_history.csv', 'file':'currencies'},
    )
#     task6=PythonOperator(
#        task_id='load_groups',
#        python_callable=load_data,
#        op_kwargs={'conn': 'conn_info', 'path':'/data/groups.csv', 'file':'groups'},
#     )
#     task7=PythonOperator(
#          task_id='load_dialogs',
#          python_callable=load_data,
#          op_kwargs={'conn': 'conn_info', 'path':'/data/dialogs.csv', 'file':'dialogs' },
#      )

#     task8=PythonOperator(
#        task_id='load_group_log',
#        python_callable=load_data,
#        op_kwargs={'conn': 'conn_info', 'path':'/data/group_log.csv', 'file':'group_log'},
#     )
# ( [task1, task2, task3, task4]
# >> print_10_lines_of_each
# >> task5 
# >> task6
# >> task7
#>> task8)
task1 >> task5
