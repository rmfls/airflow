from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from operators.load_google_sheet import GoogleSheetsHook

def read_sheet():
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test')
    hook.read_google_sheet("KN 광고 관리 문서")


with DAG(
    dag_id='dags_google_sheet_test_2',
    description='Read data from Google Sheets',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    schedule_interval=None,
    catchup=False
) as dag:
    
    read_sheet_task = PythonOperator(
        task_id='read_sheet_task',
        python_callable=read_sheet
    )

read_sheet_task