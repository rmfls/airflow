from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum
from plugins.operators.load_google_sheet import GoogleSheetsHook

def read_google_sheet():
    hook = GoogleSheetsHook(gcp_conn_id='google_sheet_conn_id')
    service = hook.get_service()
    # 원하는 연산 수행
    sheet = service.open("KN 광고 관리 문서")

    
    # 특정 시트 읽기 (예: 'Sheet1'라는 이름의 시트)
    # 첫 번째 워크시트를 선택
    worksheet = sheet.get_worksheet(0)
    values = worksheet.get_all_values()
    # sheet = client.open('Your_Spreadsheet_Name').sheet1
    # values = sheet.get_all_values()
    for row in values:
        print(row)

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }

with DAG(
    dag_id='dags_google_sheet_test_2',
    description='Read data from Google Sheets',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    schedule_interval=None,
    catchup=False
) as dag:
    
    read_sheet_task = PythonOperator(
        task_id='read_sheet_task',
        python_callable=read_google_sheet
    )

read_sheet_task