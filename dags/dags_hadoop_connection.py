# Package Import
from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import  SimpleHttpOperator
from airflow.decorators import task
import pendulum
import json
import os

'''
경로 설정을 실제 공용하둡에서 사용하는것과 동일한 형태로 가져가고 싶다.
'''



with DAG(
    dag_id = 'dags_hadoop_connection',
    start_date=pendulum.datetime(2023, 9, 20, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    BASE_PATH = Variable.get('hadoop_base_path')
    PROJECT_NAME = "gcp"
    LOCAL_PATH = f"/Users/green/airflow/files/{PROJECT_NAME}/"
    USER = "green"

    hdfs_put_cmd = SimpleHttpOperator(
        task_id='hdfs_put_cmd',
        method='POST',
        endpoint='/hdfs_cmd',
        http_conn_id='local_fast_api_conn_id', # 운영서버에서 Admin-Connection 등록 후 변경
        data=json.dumps({
            'option': 'put',
            'hadoop_base_path': BASE_PATH,
            'project_name': PROJECT_NAME, # 변수로 설정
            'local_path': LOCAL_PATH,
            'user': USER # 스크립트가 돌아가는 상대경로 (변수 설정)
        }),
        headers={'Content-Type': 'application/json'}
    )

    hdfs_put_cmd