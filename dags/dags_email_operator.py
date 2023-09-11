from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="msmsm104@naver.com",
        subject="Airflow 성공메일",
        html_content="Airflow 작업이 완료되었습니다."
    )

    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",
    )

    [t1_orange, t2_avocado] >> send_email_task


