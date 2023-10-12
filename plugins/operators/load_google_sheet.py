from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread
import os
import pandas as pd
from operators.hive_schema import generate_hive_schema_from_parquet


class GoogleSheetsHook(GoogleBaseHook):
    BASE_PATH = '/opt/airflow/files'

    NAME_MAPPING = {
        '단비웅진': 'danbi_woongjin',
        '매출관리': 'sales_management',
        '시트': 'sheet',
        '업종구분': 'business_type',
        '제안 관리': 'proposal_management',
        '계약관리': 'contract_management',
        '캠페인관리': 'campaign_management',
        '목표및현황': 'target_status',
        'PUSH 월별집계': 'monthly_push_aggregation',
        'Push 관리': 'push_management',
        '계약번호 구성 체계': 'contract_number_structure',
        '[기타] ': '',
        '년': 'year'
    }   

    COLUMN_NAME_MAPPING_01 = {
        "회사": "company",
        "유형": "type",
        "부서": "department",
        "담당자": "manager",
        "현재 상태": "current_status",
        "5월프로모션": "may_promotion",
        "비고": "remarks",
        "계약번호": "contract_number"
    }

    def __init__(self, gcp_conn_id='google_cloud_default', project_nm='', *args, **kwargs):
        if not project_nm:
            raise ValueError("The project_nm parameter is required.")
        
        super().__init__(gcp_conn_id=gcp_conn_id, *args, **kwargs)
        self.airflow_file_path = os.path.join(self.BASE_PATH, project_nm)
        # self.gcp_conn_id = gcp_conn_id

    def get_service(self):
        """Google Sheets API 서비스 객체를 반환합니다."""
        credentials = self.get_credentials()
        gc = gspread.authorize(credentials)
        return gc
    
    def read_google_sheet(self, sheet_name):
        service = self.get_service()
        sheet = service.open(sheet_name)

        worksheet = sheet.get_worksheet(0)
        values = worksheet.get_all_values()
        for row in values:
            print(row)
    
    def save_sheets_as_parquet(self, spreadsheet_name, task_instance=None):
        service = self.get_service()
        spreadsheet = service.open(spreadsheet_name)
        # airflow (docker 저장 경로)
        # airflow_file_path = '/opt/airflow/files/gcp'

        if not os.path.exists(self.airflow_file_path):
            os.makedirs(self.airflow_file_path)

        # 모든 워크시트 이름 조회
        worksheet_names = [worksheet.title for worksheet in spreadsheet.worksheets()]
        
        dfs = {}
        for name in worksheet_names:
            worksheet = spreadsheet.worksheet(name)
            
            # 워크시트 이름에 따른 데이터 전처리
            # 현재 작업된 워크시트 목록
            # 01_ContactList
            # 02_계약관리
            df = self.data_preprocessing_1(name, worksheet)

            dfs[name] = df
            self.rename_duplicated_columns(df)

            # 매핑 딕셔너리를 사용해 영문 파일명을 가져옴. 
            english_name = self.convert_filename(name)
            en_directory_path = os.path.join(self.airflow_file_path, english_name)

            if not os.path.exists(en_directory_path):
                os.makedirs(en_directory_path)

            # parquet 파일로 저장
            path = os.path.join(en_directory_path, f'{english_name}.parquet')  # 상대경로
            df.to_parquet(path, index=False)
            print(f"파일 생성 : {english_name}.parquet")

            # parquet의 스키마 정보를 추출하여 xcom_push로 저장
            if task_instance:
                schema = generate_hive_schema_from_parquet(path)
                task_instance.xcom_push(key=f"{english_name}_schema", value=schema)
                print(f"스키마 생성 : {english_name}")

    @staticmethod
    def convert_filename(korean_name):
        english_name = korean_name.split('.')[0]
        for kor, eng in GoogleSheetsHook.NAME_MAPPING.items():
            english_name = english_name.replace(kor, eng)
        return english_name.lower()

    @staticmethod
    def rename_duplicated_columns(df):
        cols = pd.Series(df.columns)

        # 공백 컬럼 이름을 "Unnamed"로 바꾸기
        cols = cols.replace("", "Unnamed")

        for dup in cols[cols.duplicated()].unique():
            cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in
                                                            range(sum(cols == dup))]

        df.columns = cols

    def data_preprocessing_1(self, name, worksheet):
        if name in ['01_ContactList']:
            columns = worksheet.get('B4:O4')[0]
            values = worksheet.get('B5:O')[0:]
            df = pd.DataFrame(values, columns=columns)

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_01[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_01}
            df.rename(columns=to_rename, inplace=True)

            # no 컬럼을 int로 변환
            for col in ['no']:
                if col in df.columns:
                    df[col] = df[col].astype(int)
        else:
            data = worksheet.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])

        return df




