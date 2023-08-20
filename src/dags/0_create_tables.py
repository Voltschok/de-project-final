from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.python import PythonOperator
import configparser
import pendulum
import vertica_python
import psycopg2
import os
import logging
from pathlib import Path


# Определение пути к текущему скрипту
current_dir = os.path.dirname(os.path.abspath(__file__))

# Формирование пути к конфигурационному файлу с использованием относительного пути
config_file_path = os.path.join(current_dir, "../../../lessons/dags/config.ini")

# Чтение параметров подключения для Postgres и Vertica из конфигурации
config = configparser.ConfigParser()
config.read(config_file_path) 
 

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

# Список SQL-файлов для создания таблиц
sql_files = [
"/lessons/sql/create_table_currencies.sql",
"/lessons/sql/create_table_transactions.sql",
"/lessons/sql/create_table_global_metrics.sql"
]


def create_table(sql_file):
     try:
          query = open(sql_file).read()
          with vertica_python.connect(**vertica_conn_info) as connection:
               cur_vertica = connection.cursor()
               cur_vertica.execute(query)
               cur_vertica.connection.commit()
               cur_vertica.close() 
     except Exception as e:
          logging.error(f"Error in load_data_postgres_vertica for table {Path(sql_file).stem.replace('create_table_','')}: {str(e)}")
          raise 


 
with DAG('init_ddl', schedule_interval=None, start_date=pendulum.parse('2023-08-20'), catchup=False) as dag:

     # Создание задач для каждого SQL-файла
     for i, sql_file in enumerate(sql_files):
          task = PythonOperator(
               task_id=f'create_table_{i + 1}',
               python_callable=lambda file=sql_file: create_table(file),
          )

task  

 
 
