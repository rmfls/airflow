from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
import json


with DAG(
    dag_id='dags_trino_conn_test',
    description='Read data from trino',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    # Airflow connection 정보 가져오기
    conn_info = BaseHook.get_connection('trino_conn')

    load_from_trino_task = SimpleHttpOperator(
        task_id='load_from_trino_task',
        method='POST',
        http_conn_id='local_fast_api_conn_id',
        endpoint='/trino_query',
        data=json.dumps({
            'query': '''select 
                            id,
                            date (created + interval '9' hour) as ymd,
                            content,
                            cast(category_id as integer)
                        from kn_users_note''',
            'connection': {
                'host': conn_info.host,
                'port': conn_info.port,
                'user': conn_info.login,
                'http_scheme': 'https',
                'password': conn_info.password,
                'catalog': 'hadoop_doopey',
                'schema': 'kidsnote'
            }
        }),
        headers={'Content-Type': 'application/json'},
        dag=dag
    )

    load_from_trino_task
    

    