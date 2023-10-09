from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from airflow.models import XCom
from operators.load_google_sheet import GoogleSheetsHook
from airflow import settings
from pathlib import Path
import logging

default_args = {
    'project_nm': 'gcp'
}

def download_google_sheet(**kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.save_sheets_as_parquet(spreadsheet_name='KN 광고 관리 문서', task_instance=kwargs['ti'])

def generate_hive_task(schema_key, dag):
    # schema_key에서 실제 테이블 이름을 가져옵니다.
    table_name = schema_key.replace("_schema", "")

    hive_data = {
        "option": "create",
        "database_name": "gcp",
        "table_name": table_name,
        # 이전에 XCom에 저장한 키로 스키마 값을 가져옵니다.
        "schema": "{{ ti.xcom_pull(task_ids='read_sheet_task', key='" + schema_key + "') }}",
        "project_name": "gcp"
    }

    return SimpleHttpOperator(
        task_id=f'create_hive_table_{table_name}',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps(hive_data),
        headers={'Content-Type': 'application/json'},
        dag=dag
    )

def log_all_xcom_keys(**kwargs):
    session = settings.Session()
    execution_date = kwargs['execution_date']
    xcom_list = XCom.get_many(task_ids='read_sheet_task', dag_ids=dag.dag_id, execution_date=execution_date, session=session)
    
    schema_key = [xcom.key for xcom in xcom_list]

    for xcom in xcom_list:
        logging.info(f"Key: {xcom.key}, Value: {xcom.value}")
    
    return schema_key
    
    # session.close()

with DAG(
    dag_id='dags_gcp_hdfs_test',
    description='Read data from Google Sheets & put to Hadoop',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False,
    default_args=default_args
) as dag:
    
    read_sheet_task = PythonOperator(
        task_id='read_sheet_task',
        python_callable=download_google_sheet,
        provide_context=True,
        dag=dag
    )

    hdfs_put_cmd = SimpleHttpOperator(
        task_id='hdfs_put_cmd',
        method='POST',
        endpoint='/hdfs_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'put',
            'hadoop_base_path': Variable.get('hadoop_base_path'),
            'project_name': 'gcp',
            'local_path': f"/Users/green/airflow/files/gcp/",
            'user': 'green' 
        }),
        headers={'Content-Type': 'application/json'}
    )

    # XCom에서 스키마 키 목록을 가져옵니다.
    log_xcom_task = PythonOperator(
        task_id='log_all_xcom_keys',
        python_callable=log_all_xcom_keys,
        provide_context=True,
        dag=dag
    )

    # 스키마 키를 반환받는 것을 기대합니다.
    schema_keys = log_xcom_task.output
    hive_tasks = []

    # 각 스키마 키에 대한 SimpleHttpOperator 작업을 동적으로 생성합니다.
    for schema_key in schema_keys:
        hive_task = generate_hive_task(schema_key, dag)
        hive_tasks.append(hive_task)

    read_sheet_task >> hdfs_put_cmd >> log_xcom_task >> hive_tasks