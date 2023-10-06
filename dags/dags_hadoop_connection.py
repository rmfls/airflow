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
하둡 경로 : 베이스 경로 - 유저 이름 - 프로젝트 이름 
구글시트 경로 (로컬 경로) : 베이스 경로 - 프로젝트 이름 
'''



with DAG(
    dag_id = 'dags_hadoop_connection',
    start_date=pendulum.datetime(2023, 9, 20, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    BASE_PATH = Variable.get('hadoop_base_path') # airflow UI에 등록한 값 => /tmp3/kidsnote/kidsnote
    PROJECT_NAME = "gcp"
    LOCAL_PATH = f"/Users/green/airflow/files/{PROJECT_NAME}/"
    USER = "green"

    # 하둡에 유저의 경로가 존재하지 않으면 만들어야 함.
    # 위에서 설정한 변수들을 simplehttp operator에 필수 입력 값으로 받아야함.
    hdfs_put_cmd = SimpleHttpOperator(
        task_id='hdfs_put_cmd',
        method='POST',
        endpoint='/hdfs_cmd',
        http_conn_id='local_fast_api_conn_id', # 운영서버에서 Admin-Connection 등록 후 변경
        data=json.dumps({
            'option': 'put',
            'hadoop_base_path': BASE_PATH,
            'project_name': PROJECT_NAME, 
            'local_path': LOCAL_PATH,
            'user': USER 
        }),
        headers={'Content-Type': 'application/json'}
    )

    hdfs_put_cmd