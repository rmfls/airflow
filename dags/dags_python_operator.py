import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

import random


with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"]
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANNANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = "py_t1",
        python_callable = select_fruit
    )

    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="msmsm104@naver.com",
        subject="Airflow 성공메일",
        html_content="Airflow(python) 작업이 완료되었습니다."
    )

    py_t1 >> send_email_task