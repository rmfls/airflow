from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from operators.load_google_sheet import GoogleSheetsHook
from operators.hive_schema import generate_hive_schema_from_parquet
from pathlib import Path

default_args = {
    'project_nm': 'gcp'
}

def download_google_sheet(task_instance, project_nm):
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.save_sheets_as_parquet(spreadsheet_name='KN 광고 관리 문서', task_instance=task_instance)

def extract_schema_from_parquet(**context):
    local_path = f"/Users/green/airflow/files/gcp/"
    parquet_files = [f for f in Path(local_path).glob('*.parquet')]

    if not parquet_files:
        raise Exception('No parquet files found!')
    
    schema = generate_hive_schema_from_parquet(parquet_files[0])
    context['ti'].xcom_push(key='hive_schema', value=schema)


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
        op_args=[default_args['project_nm']],
        provide_context=True
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

    # extract schema from parquet
    extract_schema_task = PythonOperator(
        task_id='extract_schema_task',
        python_callable=extract_schema_from_parquet,
        provide_context=True
    )

    create_hive_table_cmd = SimpleHttpOperator(
        task_id='create_hive_table_cmd',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'database_name': 'gcp',
            'table_name': '',
            'schema': "{{ ti.xcom_pull(task_ids='extract_schema_from_parquet', key='hive_schema')}}",
            'project_name': 'gcp'
        }),
        headers={'Content-Type': 'application/json'}
    )


    read_sheet_task >> hdfs_put_cmd >> extract_schema_task >> create_hive_table_cmd