from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import gspread
from oauth2client import ServiceAccountCredentials

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def test_google_sheets_conn():
    # Google Sheets API와 연동하기 위한 인증
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('/path_to_your_service_account_key.json', scope)
    client = gspread.authorize(creds)
    
    # 특정 시트 읽기 (예: 'Sheet1'라는 이름의 시트)
    sheet = client.open('Your_Spreadsheet_Name').sheet1
    values = sheet.get_all_values()
    for row in values:
        print(row)

with DAG('test_google_sheets_dag',
         default_args=default_args,
         description='Test Google Sheets Connection',
         schedule_interval=None,  # set to None, we don't want this to run automatically
         start_date=datetime(2023, 9, 28),
         catchup=False) as dag:

    test_conn_task = PythonOperator(
        task_id='test_google_sheets_conn_task',
        python_callable=test_google_sheets_conn
    )
