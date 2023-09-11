from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.email import EmailOperator



with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="msmsm104@naver.com",
        subject="Airflow 성공메일",
        html_content="Airflow(decorator) 작업이 완료되었습니다."
    )
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)

    python_task_1 = print_context('task_decorator 실행')
    
    send_email_task