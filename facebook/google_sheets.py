# google_sheets.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
from config import GOOGLE_CREDENTIALS_JSON, GOOGLE_SHEET_NAME_POSTS, GOOGLE_SHEET_NAME_PAGES


def get_google_sheets_client():
    """Initialise et retourne un client Google Sheets autorisé."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_JSON, scope)
    client = gspread.authorize(creds)
    return client

def upload_to_google_sheets(data, sheet_name):
    """Upload les données dans la feuille Google Sheets spécifiée."""
    client = get_google_sheets_client()

    try:
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        print(f"Erreur: La feuille '{sheet_name}' n'a pas été trouvée.")
        return

    # Supprime toutes les données avant l'upload
    sheet.clear()

    # Convertir les données en format de liste pour Google Sheets
    if isinstance(data, pd.DataFrame):
        data = [data.columns.values.tolist()] + data.values.tolist()

    for i, row in enumerate(data):
        sheet.insert_row(row, i + 1)

    print(f"Upload terminé dans la feuille '{sheet_name}'.")