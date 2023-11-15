from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable

from airflow.utils.task_group import TaskGroup

import json
import pendulum
from operators.load_google_sheet_ver_2 import GoogleSheetsHook

default_args = {
    'project_nm': 'gcp'
}

# 모든 워크시트 이름 조회
worksheet_names = ['Push 관리', '23년목표및현황', 'PUSH 월별집계', '매출관리', '03_캠페인관리', '제안 관리', '단비웅진', '02_계약관리', '01_ContactList', '시트16', '업종구분', '[기타] 계약번호 구성 체계', '시트28']

def download_google_sheet(worksheet_name, **kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.load_and_save_google_sheet_as_parquet(spreadsheet_name='KN 광고 관리 문서', worksheet_name=worksheet_name, task_instance=kwargs['ti'])

def preprocessing_google_sheet(worksheet_name, **kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.read_and_preprocessing_data(worksheet_name=worksheet_name)

def schema_xcom_push(worksheet_name, **kwargs):
    project_nm = default_args['project_nm']
    hook = GoogleSheetsHook(gcp_conn_id='sheet_conn_id_test', project_nm=project_nm)
    hook.read_and_xcom_push(worksheet_name=worksheet_name, task_instance=kwargs['ti'])

def print_completion_message(task_name, **kwargs):
    print(f"Task {task_name} has been completed!")


with DAG(
    dag_id='dags_load_google_sheet_2',
    description='Read data from Google Sheets',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False,
    default_args=default_args
) as dag:
    
    with TaskGroup("read_sheet_group") as read_sheet_group:
        # 데이터 로드 task
        read_sheet_task_1 = PythonOperator(
            task_id='read_sheet_task_1',
            python_callable=download_google_sheet,
            op_kwargs={'worksheet_name': '01_ContactList'},
            provide_context=True,
            dag=dag
        )

        read_sheet_task_2 = PythonOperator(
            task_id='read_sheet_task_2',
            python_callable=download_google_sheet,
            op_kwargs={'worksheet_name': '02_계약관리'},
            provide_context=True,
            dag=dag
        )

        read_sheet_task_3 = PythonOperator(
            task_id='read_sheet_task_3',
            python_callable=download_google_sheet,
            op_kwargs={'worksheet_name': '03_캠페인관리'},
            provide_context=True,
            dag=dag
        )

        read_sheet_task_4 = PythonOperator(
            task_id='read_sheet_task_4',
            python_callable=download_google_sheet,
            op_kwargs={'worksheet_name': 'Push 관리'},
            provide_context=True,
            dag=dag
        )

        read_sheet_task_5 = PythonOperator(
            task_id='read_sheet_task_5',
            python_callable=download_google_sheet,
            op_kwargs={'worksheet_name': '제안 관리'},
            provide_context=True,
            dag=dag
        )

    with TaskGroup("preprocessing_group") as preprocessing_group:
        # 데이터 전처리 task
        preprocessing_task_1 = PythonOperator(
            task_id='preprocessing_task_1',
            python_callable=preprocessing_google_sheet,
            op_kwargs={'worksheet_name': '01_ContactList'},
            provide_context=True,
            dag=dag
        )

        preprocessing_task_2 = PythonOperator(
            task_id='preprocessing_task_2',
            python_callable=preprocessing_google_sheet,
            op_kwargs={'worksheet_name': '02_계약관리'},
            provide_context=True,
            dag=dag
        )

        preprocessing_task_3 = PythonOperator(
            task_id='preprocessing_task_3',
            python_callable=preprocessing_google_sheet,
            op_kwargs={'worksheet_name': '03_캠페인관리'},
            provide_context=True,
            dag=dag
        )

        preprocessing_task_4 = PythonOperator(
            task_id='preprocessing_task_4',
            python_callable=preprocessing_google_sheet,
            op_kwargs={'worksheet_name': 'Push 관리'},
            provide_context=True,
            dag=dag
        )

        preprocessing_task_5 = PythonOperator(
            task_id='preprocessing_task_5',
            python_callable=preprocessing_google_sheet,
            op_kwargs={'worksheet_name': '제안 관리'},
            provide_context=True,
            dag=dag
        )

    read_sheet_group >> preprocessing_group