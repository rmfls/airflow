from airflow import DAG
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    # 배치일 기준으로 전월 1일 (8월 1일)
    # 배치일 기준 전월 말일 (8월 30일)
    ## start_date => 한달을 빼고, 일을 1일로 해라 
    ## end_date => 일을 1일로 해라, 하루를 빼라 => 전월 말일
    @task(task_id = 'task_using_macros',
          templates_dict = {'start_date' : '{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
                            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
                            }
        )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'start_date없음'
            end_date = templates_dict.get('end_date') or 'end_date없음'
            print(start_date)
            print(end_date)

    @task(task_id = 'task_direct_calc')
    def get_datetime_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        data_interval_end = kwargs['data_interval_end']
        prev_month_day_first = data_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_day_last = data_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)
        print(prev_month_day_first.strftime('%Y-%m-%d'))
        print(prev_month_day_last.strftime('%Y-%m-%d'))

    get_datetime_macro() >> get_datetime_calc()