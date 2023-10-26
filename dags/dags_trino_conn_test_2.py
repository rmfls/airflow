from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from operators.fetch_data_from_trino import fetch_data_from_trino


# 쿼리
query = '''
    select
        id,
        date (created + interval '9' hour) as ymd,
        content,
        category_id
    from 
        kn_users_note
'''

columns = ['id', 'ymd', 'content', 'category_id']


with DAG(
    dag_id='dags_trino_conn_test_2',
    description='Read data from trino',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_trino',
        python_callable=fetch_data_from_trino,
        op_kwargs={'query': query, 'columns': columns},
        dag=dag,
    )

    fetch_data_task