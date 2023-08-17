from airflow import DAG
from airflow.operators.python import PythonOperator
import configparser
import pendulum
import vertica_python

 
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

 

def load_global_metrics(date):

    query = open("/lessons/sql/insert_into_global_metrics.sql").read() 
    f_query =query.format(count_date=date) 
 
    with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()
        cur_vertica.execute(f_query)
       
        cur_vertica.connection.commit()
        cur_vertica.close()  
     
with DAG('final_project_cdm', schedule_interval="@daily", start_date=pendulum.parse('2022-10-01'),
         end_date=pendulum.parse('2022-11-02'),   catchup=True
) as dag:
 

    task1 = PythonOperator(
         task_id='load_global_metrics',
         python_callable=load_global_metrics,
         op_kwargs={"date": "{{ ds }}"},
     )
 
task1  
