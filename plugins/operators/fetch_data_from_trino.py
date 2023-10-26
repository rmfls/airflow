from trino.dbapi import connect
from trino.auth import BasicAuthentication
from airflow.hooks.base import BaseHook
import pandas as pd

def fetch_data_from_trino(query, columns):
    # Airflow Connection에서 정보 가져오기
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
    cur.execute(query)
    store_gg = cur.fetchall()
    df = pd.DataFrame(store_gg, columns=columns)
    
    print(df)

    return df  # xcom으로 데이터 전달