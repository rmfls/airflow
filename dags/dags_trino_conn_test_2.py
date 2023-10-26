from airflow.hooks.base import BaseHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup



import pendulum
import json


from operators.fetch_data_from_trino import fetch_data_from_trino
from operators.data_processing import process_data
from operators.hive_schema import generate_hive_schema_from_parquet
from common.export_to_parquet import export_to_parquet


# 쿼리
query = '''
    select
        id,
        date (created + interval '9' hour) as ymd,
        content,
        category_id
    from 
        kn_users_note
'''

columns = ['id', 'ymd', 'content', 'category_id']


with DAG(
    dag_id='dags_trino_conn_test_2',
    description='Read data from trino',
    start_date=pendulum.datetime(2023, 10, 1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    fetch_data_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_trino,
        op_kwargs={'query': query, 'columns': columns},
        dag=dag,
    )

    process_data_task = PythonOperator(
        task_id='process_data_task',
        python_callable=process_data,
        op_kwargs={
            'task_id_to_pull': 'fetch_data_task',
            'processing_type': 'uppercase'
        },
        provide_context=True,
        dag=dag,

    )

    save_to_parquet_task = PythonOperator(
        task_id='save_to_parquet_task',
        python_callable=export_to_parquet,
        op_kwargs={
            'task_id_to_pull': 'process_data_task',
            'project_nm': 'pr_morpheme',
        },
        provide_context=True,
        dag=dag
    )

    with TaskGroup("hdfs_cmd_and_xcom_push_group") as hdfs_cmd_and_xcom_push_group:
        hdfs_put_task = SimpleHttpOperator(
            task_id='hdfs_put_task',
            method='POST',
            endpoint='/hdfs_cmd',
            http_conn_id='local_fast_api_conn_id',
            data=json.dumps({
                'option': 'put',
                'hadoop_base_path': Variable.get('hadoop_base_path'),
                'project_name': 'pr_morpheme',
                'local_path': '/Users/green/airflow/file_export/pr_morpheme',
                'user': 'green'
            }),
            headers={'Content-Type': 'application/json'}
        )

        xcom_push_schema_task = PythonOperator(
            task_id='xcom_push_schema_task',
            python_callable=generate_hive_schema_from_parquet,
            op_kwargs={
                'parquet_path': './file_export/pr_morpheme/pr_morpheme/pr_morpheme.parquet'
            },
            provide_context=True,
            dag=dag
        )

    hive_create_cmd = SimpleHttpOperator(
        task_id='hive_create_cmd',
        method='POST',
        endpoint='/hive_cmd',
        http_conn_id='local_fast_api_conn_id',
        data=json.dumps({
            'option': 'create',
            'project_name': 'pr_morpheme',
            'table_name': 'pr_morpheme',
            'schema': '{{ ti.xcom_pull(task_ids=\'xcom_push_schema_task\')}}'
        }),
        headers={'Content-Type': 'application/json'}
    )


    fetch_data_task >> process_data_task >> save_to_parquet_task 
    save_to_parquet_task >> [xcom_push_schema_task, hdfs_put_task] >> hive_create_cmd