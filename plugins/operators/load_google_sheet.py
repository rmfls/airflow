from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread

class GoogleSheetsHook(GoogleBaseHook):
    def __init__(self, gcp_conn_id='google_sheet_conn_id', *args, **kwargs):
        super().__init__(gcp_conn_id=gcp_conn_id, *args, **kwargs)
        # self.gcp_conn_id = gcp_conn_id

    def get_service(self):
        """Google Sheets API 서비스 객체를 반환합니다."""
        credentials = self.get_credentials()
        gc = gspread.authorize(credentials)
        return gc
    

