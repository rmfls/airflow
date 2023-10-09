from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from airflow.models import XCom
from operators.load_google_sheet import GoogleSheetsHook
from pathlib import Path

default_args = {
    'project_nm': 'gcp'
}

def download_google_sheet(**kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.save_sheets_as_parquet(spreadsheet_name='KN 광고 관리 문서', task_instance=kwargs['ti'])

def log_all_xcom_keys(**kwargs):
    ti = kwargs['ti']
    xcom_items = ti.xcom_pull(task_ids=None, key=None, include_prior_dates=True) 
    for item in xcom_items:
        print(item)  # 혹은 logging.info(item) 사용

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

    # XCom에서 모든 키 가져오기
    # all_schema_keys = [xcom.key for xcom in XCom.get_many(dag_ids=dag.dag_id, key="*_schema")]

    log_xcom_task = PythonOperator(
        task_id='log_all_xcom_keys',
        python_callable=log_all_xcom_keys,
        provide_context=True,
        dag=dag
    )

    hdfs_put_cmd >> log_all_xcom_keys
