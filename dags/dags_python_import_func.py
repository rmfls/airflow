from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from common.common_func import get_sftp

with DAG(
    dag_id = "dags_python_import_func",
    schedule = "30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz = "Asia/Seoul"),
    catchup = False
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id = "task_get_sftp",
        python_callable = get_sftp
    )

    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="msmsm104@naver.com",
        subject="Airflow 성공메일",
        html_content="Airflow 작업이 완료되었습니다. (외부 함수 호출)"
    )

    task_get_sftp >> send_email_task




