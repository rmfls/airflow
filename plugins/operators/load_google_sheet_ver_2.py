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
        "Targeting": "targeting",
        "총일수": "duration_date",
        "일자별금액": "amount_per_date",
        "1월": "1_month"
    }  

    COLUMN_NAME_MAPPING_04 = {
        "날짜": "push_date",
        "Log 등록 여부": "Log_Registration_Status",
        "Target url": "target_url",
        "#push": "push_price"
    }

    COLUMN_NAME_MAPPING_05 = {
        "캠페인명": "campaign_name",
        "광고주명": "advertiser_name",
        "업종": "industry",
        "대행사명": "agency_name",
        "렙사명": "rep_company_name",
        "담당자": "manager",
        "tel": "tel",
        "HP": "mobile",
        "email": "email",
        "현재 상태": "current_status",
        "f/u": "follow_up",
        "상품명": "product_name",
        "총 집행 금액": "total_execution_amount",
        "예상 일정": "expected_schedule",
        "예상 Target": "expected_target"
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
            df = df.iloc[:, 1:]
            df.columns = df.iloc[2]
            df = df.iloc[3:].reset_index(drop=True)
            df.columns.name = None

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_02[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_02}
            df.rename(columns=to_rename, inplace=True)

            # contract_number 컬럼을 int로 변환
            for col in ['contract_number']:
                if col in df.columns:
                    df[col] = df[col].astype(int)

            print(f"{worksheet_name} 워크시트 전처리 완료")
        elif worksheet_name == '03_캠페인관리':
            # 데이터 전처리
            df = df.iloc[1:].reset_index(drop=True)

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
    
            print(f"{worksheet_name} 워크시트 전처리 완료")
        elif worksheet_name == 'Push 관리':
            # 데이터 전처리
            df = df.iloc[1:].reset_index(drop=True)

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_04[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_04}
            df.rename(columns=to_rename, inplace=True)

            # push 컬럼 결측치 처리, int로 변환
            for col in ['#push']:
                if col in df.columns:
                    df[col].fillna('0', inplace=True)
                    df[col] = pd.to_numeric(df[col].str.replace('[,]', '', regex=True), errors='coerce')

            print(f"{worksheet_name} 워크시트 전처리 완료")
        elif worksheet_name == '제안 관리':
            # 데이터 전처리
            df = df.iloc[:, 2:]  # 첫, 두 번째 열 제거
            df.columns = df.iloc[2]  # 4번 행을 열 이름으로 사용
            df = df.iloc[3:].reset_index(drop=True)  # 5번 행부터 시작하도록 설정, 인덱스 초기화
            df.columns.name = None  # 컬럼 name 제거

            # 이름 매핑 적용
            to_rename = {col: self.COLUMN_NAME_MAPPING_05[col] for col in df.columns if col in self.COLUMN_NAME_MAPPING_05}
            df.rename(columns=to_rename, inplace=True)

            # total_execution_amount 컬럼 결측치 처리, int로 변환
            for col in ['total_execution_amount']:
                if col in df.columns:
                    df[col].fillna('0', inplace=True)
                    df[col] = pd.to_numeric(df[col].str.replace('[,]', '', regex=True), errors='coerce')

            # 중복컬럼 이름 제거
            self.rename_duplicated_columns(df)

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
        

        
