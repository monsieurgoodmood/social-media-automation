import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import csv

def get_google_sheets_client():
    """Initialise et retourne un client Google Sheets autorisé."""
    # Scopes nécessaires pour accéder à Google Sheets et Drive
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Chemin vers le fichier JSON des credentials
    creds_path = 'credentials/service_account_credentials.json'  # Assurez-vous que ce chemin est correct

    # Charger les credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)

    # Autoriser l'application avec les credentials
    client = gspread.authorize(creds)
    return client

def upload_to_google_sheets(csv_file, sheet_name):
    """Upload les données CSV dans une feuille Google Sheets."""
    client = get_google_sheets_client()
    
    try:
        # Ouvrir le Google Sheet par son nom
        sheet = client.open(sheet_name).sheet1
    except gspread.SpreadsheetNotFound:
        print(f"Erreur: La feuille '{sheet_name}' n'a pas été trouvée.")
        return

    # Vérifier si le fichier CSV existe avant l'upload
    if not os.path.exists(csv_file):
        print(f"Erreur: Le fichier {csv_file} n'existe pas.")
        return

    # Lire le fichier CSV et uploader les données dans Google Sheets
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        rows = list(reader)  # Charger toutes les lignes

    # Insérer les lignes dans Google Sheets
    for i, row in enumerate(rows):
        if row:  # Éviter les lignes vides
            sheet.insert_row(row, i + 1)

    print("Upload terminé.")
