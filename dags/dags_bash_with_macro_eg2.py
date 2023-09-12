import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow import macros

with DAG(
    dag_id="dags_bash_with_macro_eg2",
    schedule="0 0 * * 6#2",
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
   # START_DATE: 2주전 월요일, END_DATE: 2주전 토요일
    bash_task_2 = BashOperator(
        task_id='bash_task_2',
        env = {'START_DATE': '{{}}', 
               'END_DATE': '{{}}'
               },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )

    bash_task_1