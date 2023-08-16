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

 

def load_global_metrics(count_date):
    print('DATA RASCHETA', count_date)  

    with vertica_python.connect(**vertica_conn_info) as connection:
        cur_vertica = connection.cursor()
        cur_vertica.execute(f"""DELETE FROM STV230530__DWH.global_metrics WHERE date_update::date='{count_date}'::date-1""")
        connection.commit()
       
        cur_vertica.execute(f"""
                INSERT INTO STV230530__DWH.global_metrics 
                
                WITH all_currency_transactions AS (
                SELECT t.operation_id,
                    t.currency_code,
                    t.account_number_from,
                    t.amount,
                    t.transaction_dt,
                    t.status,
                    t.transaction_type 
                FROM STV230530__STAGING.transactions AS t
                WHERE
                    t.status='done' AND
                    t.account_number_from>0 AND
                    t.transaction_dt::date='{count_date}'::date-1 AND
                    t.transaction_type !='authorisation'),

                dollar_currency_transactions AS (
                SELECT act.operation_id,
                    act.currency_code as currency_from,
                    act.account_number_from,
                    act.transaction_dt,
                    act.amount,
                    act.status
                FROM all_currency_transactions AS act
                WHERE act.currency_code=420),

                non_dollar_currency_transactions AS (
                SELECT act.operation_id,
                    act.currency_code as currency_from,
                    act.account_number_from,
                    act.transaction_dt,
                    (act.amount*c.currency_with_div) as amount,
                    act.status
                FROM all_currency_transactions AS act
                LEFT JOIN STV230530__STAGING.currencies as c ON act.currency_code=c.currency_code  and c.date_update::date=act.transaction_dt::date
                WHERE act.currency_code!=420),

                all_transactions AS(
                SELECT transaction_dt::date as date_update, operation_id, currency_from, amount, account_number_from FROM dollar_currency_transactions
                UNION ALL 
                SELECT transaction_dt::date as date_update, operation_id, currency_from, amount, account_number_from FROM non_dollar_currency_transactions)
                
                SELECT   
                    date_update,
                    currency_from, 
                    sum(amount) as amount_total, 
                    count(*) as cnt_transactions,  
                    round(sum(amount)/count(distinct account_number_from),2) as avg_transactions_per_account, 
                    count(distinct account_number_from) as cnt_accounts_make_transactions
                FROM all_transactions
                GROUP BY  date_update, currency_from;
                            """)
        cur_vertica.connection.commit()
        cur_vertica.close()  
     
with DAG('final_project_cdm', schedule_interval="@daily", start_date=pendulum.parse('2022-10-01'),  catchup=True
) as dag:
 

    task1 = PythonOperator(
         task_id='load_global_metrics',
         python_callable=load_global_metrics,
         op_kwargs={"count_date": "{{ ds }}"},
     )
 
task1  
