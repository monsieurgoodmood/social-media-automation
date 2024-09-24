import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_google_sheets_client():
    """Authentification à Google Sheets."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('path_to_creds.json', scope)
    return gspread.authorize(creds)

def upload_to_google_sheets(csv_file, sheet_name):
    """Charge les données du CSV dans Google Sheets."""
    client = get_google_sheets_client()
    sheet = client.open(sheet_name).sheet1
    
    with open(csv_file, 'r') as file:
        content = file.readlines()
        for i, row in enumerate(content):
            sheet.insert_row(row.strip().split(','), i + 1)
