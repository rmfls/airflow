from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

from operators.fetch_data_from_trino import fetch_data_from_trino
from operators.data_processing import process_data


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
        task_id='fetch_data_task',
        python_callable=fetch_data_from_trino,
        op_kwargs={'query': query, 'columns': columns},
        dag=dag,
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        op_kwargs={
            'task_id_to_pull': 'fetch_data_task',
            'processing_type': 'uppercase'
        },
        provide_context=True,
        dag=dag,

    )


    fetch_data_task >> process_data_task