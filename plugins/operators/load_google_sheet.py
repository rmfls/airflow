from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import gspread

class GoogleSheetsHook(GoogleBaseHook):
    def __init__(self, gcp_conn_id='google_cloud_default', *args, **kwargs):
        super().__init__(gcp_conn_id=gcp_conn_id, *args, **kwargs)
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
    

