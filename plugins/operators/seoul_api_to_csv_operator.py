from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{ var.value.apikey_openapi_seoul_go_kr }}/json/' + dataset_nm
        self.base_dt = base_dt
    
    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        # base_url 마지막에 년도 월 추가가 필요할것으로 보인다.
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000
        
        self.log.info(f'경로 : {self.path}')
        self.log.info(f'출력값 : {not os.path.exists(self.path)}')

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')

        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }
        
        request_url = f'{base_url}/{start_row}/{end_row}'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers=headers)

         # 로그 추가: API 응답 내용과 상태 코드 및 헤더 정보를 출력
        self.log.info(f'API Response: {response.text}')
        self.log.info(f'API Response Status Code: {response.status_code}')
        self.log.info(f'API Response Headers: {response.headers}')
    
        contests = json.loads(response.text)

        key_nm = list(contests.keys())[0]
        row_data = contests.get(key_nm).get('row')
        row_dt = pd.DataFrame(row_data)

        # self.log.info(f'API Response: {response.text}')

        return row_dt