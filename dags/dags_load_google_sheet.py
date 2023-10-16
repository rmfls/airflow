from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from operators.load_google_sheet_ver_2 import GoogleSheetsHook

default_args = {
    'project_nm': 'gcp'
}

def download_google_sheet(worksheet_name, **kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.load_and_save_google_sheet_as_parquet(spreadsheet_name='KN 광고 관리 문서', worksheet_name=worksheet_name, task_instance=kwargs)

with DAG(
    dag_id='dags_load_google_sheet',
    description='Read data from Google Sheets',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False,
    default_args=default_args
) as dag:
    
    read_sheet_task_1 = PythonOperator(
        task_id='read_sheet_task_1',
        python_callable=download_google_sheet,
        op_kwargs={'worksheet_name': 'Push 관리'},
        provide_context=True,
        dag=dag
    )

    read_sheet_task_1
