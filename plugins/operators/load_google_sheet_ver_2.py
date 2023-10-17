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
        "계약번호": "contract_number",
        "f/u": "f_u"
    }

    def __init__(self, gcp_conn_id='google_cloud_default', project_nm='', *args, **kwargs):
        if not project_nm:
            raise ValueError("The project_nm parameter is required.")
        
        super().__init__(gcp_conn_id=gcp_conn_id, *args, **kwargs)
        self.airflow_file_path = os.path.join(self.BASE_PATH, project_nm)

    def get_service(self):
        """Google Sheets API 서비스 객체를 반환합니다."""
        credentials = self.get_credentials()
        gc = gspread.authorize(credentials)
        return gc
    
    def load_and_save_google_sheet_as_parquet(self, spreadsheet_name, worksheet_name, task_instance=None):
        # 워크시트 로드
        service = self.get_service()
        spreadsheet = service.open(spreadsheet_name)
        worksheet = spreadsheet.worksheet(worksheet_name)

        # data 가져오기
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])

        # 중복된 컬럼 제거
        self.rename_duplicated_columns(df)

        # 저장 경로 설정
        en_worksheet_name = self.convert_filename(worksheet_name)
        en_directory_path = os.path.join(self.airflow_file_path, en_worksheet_name)
        save_parquet_path = os.path.join(en_directory_path, f"{en_worksheet_name}.parquet")

        # airflow (docker 저장 경로)
        # airflow_file_path = '/opt/airflow/files/gcp'
        if not os.path.exists(self.airflow_file_path):
            os.makedirs(self.airflow_file_path)
        if not os.path.exists(en_directory_path):
            os.makedirs(en_directory_path)
        
        # parquet 파일로 저장
        df.to_parquet(save_parquet_path, index=False)
        print(f"파일 생성: {en_worksheet_name}.parquet")
    
    def read_and_preprocessing_data(self, worksheet_name):
        en_worksheet_name = self.convert_filename(worksheet_name)
        save_parquet_path = os.path.join(self.airflow_file_path, en_worksheet_name, f"{en_worksheet_name}.parquet")

        df = pd.read_parquet(save_parquet_path)

        if worksheet_name == '01_ContactList':
            # 데이터 전처리

            df = df.iloc[:, 1:]
            df.columns = df.iloc[2]
            df = df.iloc[3:].reset_index(drop=True)
            df.columns.name = None

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_01[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_01}
            df.rename(columns=to_rename, inplace=True)

            # no 컬럼을 int로 변환
            for col in ['no']:
                if col in df.columns:
                    df[col] = df[col].astype(int)

            print(f"{worksheet_name} 워크시트 전처리 완료")
        elif worksheet_name == '02_계약관리':
            # 데이터 전처리
            print(f"{worksheet_name} 워크시트 전처리 완료")
        elif worksheet_name == '03_캠페인관리':
            # 데이터 전처리
            print(f"{worksheet_name} 워크시트 전처리 완료")
        
        # parquet 파일로 저장
        df.to_parquet(save_parquet_path, index=False)
        print(f"파일 덮어씌우기: {en_worksheet_name}.parquet")
    
    def read_and_xcom_push(self, worksheet_name, task_instance=None):
        en_worksheet_name = self.convert_filename(worksheet_name)
        save_parquet_path = os.path.join(self.airflow_file_path, en_worksheet_name, f"{en_worksheet_name}.parquet")

        if task_instance:
            schema = generate_hive_schema_from_parquet(save_parquet_path)
            task_instance.xcom_push(key=f"{en_worksheet_name}", value=schema)
            print(f"{en_worksheet_name}.parquet => 스키마 생성, xcom_push")

    
    @staticmethod
    def convert_filename(kor_name):
        english_name = kor_name.split('.')[0]
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
        

        
