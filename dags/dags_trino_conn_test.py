from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
import pendulum
import json

def preprocess_data(**context):
    # XCom을 사용하여 이전 태스크의 결과 가져오기
    trino_data = context['ti'].xcom_pull(task_ids='load_from_trino_task')

    # 여기에 전처리 로직 추가
    processed_data = ... # trino_data를 사용하여 필요한 전처리 수행

    # 전처리된 데이터를 다음 태스크에서 사용할 수 있도록 XCom에 저장
    context['ti'].xcom_push(key='processed_data', value=processed_data)


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
                            category_id
                        from kn_users_note''',
            'connection': {
                'host': conn_info.host,
                'port': conn_info.port,
                'user': conn_info.login,
                'password': conn_info.password,
                'http_scheme': 'https',
                'catalog': 'hadoop_doopey',
                'schema': 'kidsnote'
            }
        }),
        headers={'Content-Type': 'application/json'},
        dag=dag
    )

    load_from_trino_task
    

    