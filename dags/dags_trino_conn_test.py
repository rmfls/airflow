from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator

import pendulum



def fetch_data_from_trino():
    from trino.dbapi import connect
    from trino.auth import BasicAuthentication
    import pandas as pd

    # Airflow Connection 에서 정보 가져오기
    conn_info = BaseHook.get_connection('trino_conn')

    conn = connect(
        host=conn_info.host,
        port=conn_info.port,
        user=conn_info.login,
        auth=BasicAuthentication(conn_info.login, conn_info.password),
        http_scheme='https',
        catalog='hadoop_doopey',
        schema=conn_info.schema
    )

    cur = conn.cursor()
    
    query = '''
        select
        id,
        date (created + interval '9' hour) as ymd,
        content,
        category_id
        from
        kn_users_note
    '''
    
    cur.execute(query)
    store_gg = cur.fetchall()
    df = pd.DataFrame(store_gg, columns=['id', 'date', 'content', 'category_id'])

    print(df)



with DAG(
    dag_id='dags_trino_conn_test',
    description='Read data from trino',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_trino,
        dag=dag
    )

    fetch_data_task


    