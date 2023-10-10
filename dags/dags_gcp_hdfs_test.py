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

# def log_all_xcom_keys(**kwargs):
#     session = settings.Session()
#     execution_date = kwargs['execution_date']
#     xcom_list = XCom.get_many(task_ids='read_sheet_task', dag_ids=dag.dag_id, execution_date=execution_date, session=session)
    
#     schema_key = [xcom.key for xcom in xcom_list]

#     for xcom in xcom_list:
#         logging.info(f"Key: {xcom.key}, Value: {xcom.value}")

#     session.close()

# def create_hive_table_from_xcom(**kwargs):
#     session = settings.Session()
#     execution_date = kwargs['execution_date']
#     xcom_list = XCom.get_many(task_ids='read_sheet_task', dag_ids=dag.dag_id, execution_date=execution_date, session=session)

#     for xcom in xcom_list:
#         logging.info(f"Key: {xcom.key}, Value: {xcom.value}")

#         schema = ', '.join(xcom.value)

#         hive_create_table_task = SimpleHttpOperator(
#             task_id=f'hive_create_table_task_{xcom.key}',
#             method='POST',
#             endpoint='/hive_cmd',
#             http_conn_id='local_fast_api_conn_id',
#             data=json.dumps({
#                 'option': 'create',
#                 'database_name': 'gcp',
#                 'table_name': f'{xcom.key}',
#                 'schema': schema,
#                 'project_name': 'gcp'
#             }),
#             headers={'Content-Type': 'application/json'},
#             dag=dag
#         )

#         # 이 연산자를 실행하도록 작업 흐름도 설정
#         hive_create_table_task.execute(context=kwargs)

#     session.close()

def create_hive_table_task_for_xcom(xcom, dag):
    schema = xcom.value

    return SimpleHttpOperator(
        task_id=f'hive_create_table_task_{xcom.key}',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'database_name': 'gcp',
            'project_name': 'gcp',
            'table_name': f'{xcom.key}',
            'schema': schema
        }),
        headers={'Content-Type': 'application/json'},
        dag=dag
    )


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

    read_sheet_task >> hdfs_put_cmd 

    session = settings.Session()
    execution_date = pendulum.datetime(2023, 10, 1, tz='Asia/Seoul')
    xcom_list = XCom.get_many(task_ids='read_sheet_task', dag_ids=dag.dag_id, execution_date=execution_date, session=session)

    for xcom in xcom_list:
        hive_create_table_task = create_hive_table_task_for_xcom(xcom, dag)
        hdfs_put_cmd >> hive_create_table_task
    
    session.close()

    


