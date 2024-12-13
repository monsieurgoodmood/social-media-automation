import gspread
from google.oauth2.service_account import Credentials

from config import SERVICE_ACCOUNT_FILE, SPREADSHEET_ID

def get_google_sheet(sheet_name):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        sheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="20")
    return sheet

def write_page_metrics(sheet, metrics, start_date, end_date):
    sheet.append_row(["Date"] + list(metrics.keys()))
    for date, data in metrics["timeSeries"].items():
        row = [date] + [data.get(key, 0) for key in metrics.keys()]
        sheet.append_row(row)

def write_post_metrics(sheet, post_metrics):
    for post_id, metrics in post_metrics.items():
        worksheet = get_google_sheet(post_id)
        worksheet.append_row(["Date"] + list(metrics.keys()))
        for date, data in metrics["timeSeries"].items():
            row = [date] + [data.get(key, 0) for key in metrics.keys()]
            worksheet.append_row(row)
