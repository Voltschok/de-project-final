from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pendulum
import vertica_python
import boto3
import pandas as pd 
 
conn_info = {'host': 'vertica.tgcloudenv.ru',
             'port': '5433',
             'user': 'stv230530',
             'password': 'IjUMUB8AONAHDcT',
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

key_id=Variable.get("AWS_ACCESS_KEY_ID")
secret_key=Variable.get("AWS_SECRET_ACCESS_KEY")

def load_data(conn, path:str ,  file:str):  
    df_csv = pd.read_csv( path )
    if file=='dialogs':
        df_csv = df_csv.rename(columns={'message_type': 'message_group'})
    elif file=='group_log':
    	df_csv = df_csv.rename(columns={'datetime': 'event_dt'})
    tuple_col=", ".join(list(df_csv.columns) )
    tuple_col_str= ('('+ str(tuple_col)+')')
   
    with vertica_python.connect(**conn_info) as connection:
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
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    aws_access_key_id=key_id,
    aws_secret_access_key=secret_key)
    s3_client.download_file(
    bucket,
    key,
    Filename=f'/data/{key}')

# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла

bucket_files = ['groups.csv', 'dialogs.csv', 'users.csv', 'group_log.csv']

bash_command_tmpl = """
{% for file in params.files %}
head {{ file }}
{% endfor %}
"""


#{{ params.files }} for ((i=1; i<3; i++)) {{ params.files[i] }}



with DAG('test2', schedule_interval=None, start_date=pendulum.parse('2022-07-13')
) as dag:
 

    task1 = PythonOperator(
         task_id='fetch_groups',
         python_callable=fetch_s3_file,
         op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )
    task2 = PythonOperator(
        task_id='fetch_dialogs',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )
    task3 = PythonOperator(
       task_id='fetch_users',
       python_callable=fetch_s3_file,
       op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )
    task4 = PythonOperator(
        task_id='fetch_group_log',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )
    
    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )
    task5=PythonOperator(
       task_id='load_users',
       python_callable=load_data,
       op_kwargs={'conn': 'conn_info', 'path':'/data/users.csv', 'file':'users'},
    )
    task6=PythonOperator(
       task_id='load_groups',
       python_callable=load_data,
       op_kwargs={'conn': 'conn_info', 'path':'/data/groups.csv', 'file':'groups'},
    )
    task7=PythonOperator(
         task_id='load_dialogs',
         python_callable=load_data,
         op_kwargs={'conn': 'conn_info', 'path':'/data/dialogs.csv', 'file':'dialogs' },
     )

    task8=PythonOperator(
       task_id='load_group_log',
       python_callable=load_data,
       op_kwargs={'conn': 'conn_info', 'path':'/data/group_log.csv', 'file':'group_log'},
    )
( [task1, task2, task3, task4]
>> print_10_lines_of_each
>> task5 
>> task6
>> task7
>> task8)
 
 
