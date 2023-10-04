# Package Import
from airflow import DAG
from airflow.providers.http.operators.http import  SimpleHttpOperator
from airflow.decorators import task
import pendulum
import json

with DAG(
    dag_id = 'dags_hadoop_connection',
    start_date=pendulum.datetime(2023, 9, 20, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    """ hadoop put 명령 """
    hdfs_mkdir_cmd = SimpleHttpOperator(
        task_id='hdfs_mkdir_cmd',
        method='POST',
        endpoint='/hdfs_cmd',
        http_conn_id='local_fast_api_conn_id', # 운영서버에서 Admin-Connection 등록 후 변경
        data=json.dumps({
            'option': 'mkdir',
            'project_name': 'loading_google_sheet'
        }),
        headers={'Content-Type': 'application/json'}
    )

    hdfs_put_cmd = SimpleHttpOperator(
        task_id='hdfs_put_cmd',
        method='POST',
        endpoint='/hdfs_cmd',
        http_conn_id='local_fast_api_conn_id', # 운영서버에서 Admin-Connection 등록 후 변경
        data=json.dumps({
            'option': 'put',
            'project_name': 'loading_google_sheet',
            'local_path': 'Users/green/airflow/files/gcp/'
        }),
        headers={'Content-Type': 'application/json'}
    )

    hdfs_mkdir_cmd >> hdfs_put_cmd