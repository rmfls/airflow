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
        "HP": 'hp',
        "5월프로모션": "may_promotion",
        "비고": "remarks",
        "계약번호": "contract_number",
        "f/u": "f_u"
    }

    COLUMN_NAME_MAPPING_02 = {
        "계약번호": "contract_number",
        "계약종류": "contract_type",
        "계약일": "contract_date",
        "대상분류": "target_category",
        "계약대상": "contract_target",
        "정산기간(일)": "settlement_period_days",
        "메모": "memo",
        "관련자료": "related_data",
        "완료 및 보관 여부 (Y/N)": "completion_and_storage_status"
    }  

    COLUMN_NAME_MAPPING_03 = {
        "계약번호": "contract_number", # int
        "캠페인번호": "campaign_number",
        "캠페인명": "campaign_name",
        "광고주": "advertiser",
        "대행사": "agency",
        "렙사": "representative",
        "담당자": "manager",
        "Placement": "placement",
        "집행금액": "execution_amount", # 결측치 처리(0), int
        "수수료율": "commission_rate",
        "수익": "profit", # 결측치 처리(0), int
        "요청일(수주일)": "request_date",
        "게재시작일": "publish_start_date",
        "게재종료일": "publish_end_date",
        "계산서발행일 ": "invoice_date",
        "정산일(1차)": "first_settlement_date",
        "업종대분류": "major_business_category",
        "업종중분류": "sub_business_category",
        "정산기간(일)": "settlement_period_days",
        "캠페인 완료": "campaign_completion",
        "세금계산서 발행": "tax_invoice_issued",
        "정산 완료": "settlement_completed",
        "관련 자료": "related_documents",
        "비고": "remarks",
        "Targeting": "targeting"
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
                task_instance.xcom_push(key=f"{english_name}", value=schema)
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
        if name == '01_ContactList':
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
        
        elif name == '02_계약관리':
            columns = worksheet.get('B4:O4')[0]
            values = worksheet.get('B5:O')[0:]
            df = pd.DataFrame(values, columns=columns)

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_02[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_02}
            df.rename(columns=to_rename, inplace=True)

            # contract_number 컬럼을 int로 변환
            for col in ['contract_number']:
                if col in df.columns:
                    df[col] = df[col].astype(int)

        elif name == '03_캠페인관리':
            columns = worksheet.get('A1:AB')[0]
            values = worksheet.get('A2:AB')[0:]
            df = pd.DataFrame(values, columns=columns)

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_03[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_03}
            df.rename(columns=to_rename, inplace=True)

            # contract_number 컬럼을 int로 변환
            for col in ['contract_number']:
                if col in df.columns:
                    df[col] = df[col].astype(int)

            # execution_amount, profit 컬럼 결측치 처리, int로 변환
            for col in ['execution_amount', 'profit']:
                if col in df.columns:
                    df[col].fillna('₩0', inplace=True)
                    # 통화 기호와 쉼표 제거 후 int로 변환
                    df[col] = pd.to_numeric(df[col].str.replace('[₩,]', '', regex=True), errors='coerce')

        else:
            data = worksheet.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])

        return df