from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime
from airflow.models import Variable

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
    dag_id='dags_load_google_sheet',
    description='Read data from Google Sheets',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False,
    default_args=default_args
) as dag:
    
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

    # 데이터 로드 task 완료
    load_complete_task = PythonOperator(
        task_id='load_complete_task',
        python_callable=print_completion_message,
        op_kwargs={'task_name': 'loading_google_sheet_task'},
        provide_context=True,
        dag=dag
    )

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

    # 데이터 전처리 task 완료
    preprocessing_data_task = PythonOperator(
        task_id='preprocessing_data_task',
        python_callable=print_completion_message,
        op_kwargs={'task_name': 'preprocessing_data_task'},
        provide_context=True,
        dag=dag
    )

    # hadoop put task
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

    # hadoop -put task 완료
    hdfs_put_task = PythonOperator(
        task_id='hdfs_put_task',
        python_callable=print_completion_message,
        op_kwargs={'task_name': 'hdfs_put_task'},
        provide_context=True,
        dag=dag
    )

    # 스키마 생성 task
    xcom_push_task_1 = PythonOperator(
        task_id='xcom_push_task_1',
        python_callable=schema_xcom_push,
        op_kwargs={'worksheet_name': '01_ContactList'},
        provide_context=True,
        dag=dag
    )

    xcom_push_task_2 = PythonOperator(
        task_id='xcom_push_task_2',
        python_callable=schema_xcom_push,
        op_kwargs={'worksheet_name': '02_계약관리'},
        provide_context=True,
        dag=dag
    )

    xcom_push_task_3 = PythonOperator(
        task_id='xcom_push_task_3',
        python_callable=schema_xcom_push,
        op_kwargs={'worksheet_name': '03_캠페인관리'},
        provide_context=True,
        dag=dag
    )

    # 스키마 생성 task 완료
    create_schema_task = PythonOperator(
        task_id='create_schema_task',
        python_callable=print_completion_message,
        op_kwargs={'task_name': 'create_schema_task'},
        provide_context=True,
        dag=dag
    )

    # create hive table
    hive_create_cmd_1 = SimpleHttpOperator(
        task_id='hive_create_cmd_1',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'project_name': 'gcp',
            'table_name': '01_contactlist',
            'schema': '{{ ti.xcom_pull(key=\'01_contactlist\') }}'
        }),
        headers={'Content-Type': 'application/json'}
    )

    hive_create_cmd_2 = SimpleHttpOperator(
        task_id='hive_create_cmd_2',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'project_name': 'gcp',
            'table_name': '02_contract_management',
            'schema': '{{ ti.xcom_pull(key=\'02_contract_management\') }}'
        }),
        headers={'Content-Type': 'application/json'}
    )

    hive_create_cmd_3 = SimpleHttpOperator(
        task_id='hive_create_cmd_3',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'project_name': 'gcp',
            'table_name': '03_campaign_management',
            'schema': '{{ ti.xcom_pull(key=\'03_campaign_management\') }}'
        }),
        headers={'Content-Type': 'application/json'}
    )

    # 하이브 테이블 생성 task
    create_hive_table_task = PythonOperator(
        task_id='create_hive_table_task',
        python_callable=print_completion_message,
        op_kwargs={'task_name': 'create_hive_table_task'},
        provide_context=True,
        dag=dag
    )


    read_sheet_task_1 >> preprocessing_task_1
    read_sheet_task_2 >> preprocessing_task_2
    read_sheet_task_3 >> preprocessing_task_3
    
    [preprocessing_task_1, preprocessing_task_2, preprocessing_task_3] >> hdfs_put_cmd

    hdfs_put_cmd >> xcom_push_task_1
    hdfs_put_cmd >> xcom_push_task_2
    hdfs_put_cmd >> xcom_push_task_3

    xcom_push_task_1 >> hive_create_cmd_1
    xcom_push_task_2 >> hive_create_cmd_2
    xcom_push_task_3 >> hive_create_cmd_3