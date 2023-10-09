import pyarrow.parquet as pq
import requests
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import XCom
from airflow import settings
import json


def generate_hive_schema_from_parquet(parquet_path):
    # Parquet 파일의 스키마 읽기
    table = pq.read_table(parquet_path)
    schema = table.schema

    type_mapping = {
        'int8': 'TINYINT',
        'int16': 'SMALLINT',
        'int32': 'INT',
        'int64': 'BIGINT',
        'uint8': 'INT',  # Hive does not support unsigned integers, so casting them.
        'uint16': 'INT',
        'uint32': 'BIGINT',
        'uint64': 'BIGINT',  # Beware: Overflow might happen.
        'float16': 'FLOAT',
        'float32': 'FLOAT',
        'float64': 'DOUBLE',
        'bool': 'BOOLEAN',
        'string': 'STRING',
        'binary': 'BINARY',
        'timestamp[ns]': 'TIMESTAMP',
        'timestamp[us]': 'TIMESTAMP',
        'date32[day]': 'DATE',
        'double': 'DOUBLE',
        'fixed_size_binary': 'BINARY',  # you might need more logic based on the size.
        # Additional types can be added as needed
    }

    columns = []
    for field in schema:
        field_type = str(field.type)

        # 해당 컬럼의 데이터 유형이 알려진 유형인 경우 매핑하여 칼럼 유형 추가
        if field_type in type_mapping:
            columns.append(f"{field.name} {type_mapping[field_type]}")
        else:
            # 알려지지 않은 타입에 대한 처리
            columns.append(f"{field.name} STRING")

    return ', '.join(columns)