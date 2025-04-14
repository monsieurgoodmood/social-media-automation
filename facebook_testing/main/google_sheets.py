# google_sheets.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
from .config import GOOGLE_CREDENTIALS_JSON
from utils import convert_to_json_compatible
import numpy as np

logger = logging.getLogger(__name__)

def get_google_sheets_client():
    """Initialise et retourne un client Google Sheets."""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDENTIALS_JSON, scope)
        client = gspread.authorize(creds)
        logger.info("Client Google Sheets initialisé avec succès.")
        return client
    except Exception as e:
        logger.error(f"Erreur lors de l'initialisation du client Google Sheets: {e}")
        raise e

def get_shareable_link(spreadsheet, tab_name):
    """Génère un lien partageable pour un onglet spécifique dans un document Google Sheets."""
    worksheet = spreadsheet.worksheet(tab_name)
    link = f"https://docs.google.com/spreadsheets/d/{spreadsheet.id}/edit#gid={worksheet.id}"
    return link

def upload_to_google_sheets(data, sheet_name, tab_name):
    """
    Upload les données vers Google Sheets et retourne le lien partageable.
    """
    try:
        if isinstance(data, pd.DataFrame):
            # Convertir DataFrame en format compatible avec JSON
            data = data.replace([np.inf, -np.inf], None).fillna(0).astype(str)

        # Initialisation du client Google Sheets
        client = get_google_sheets_client()
        spreadsheet = client.open(sheet_name)

        # Vérifier ou créer l'onglet
        try:
            sheet = spreadsheet.worksheet(tab_name)
        except gspread.WorksheetNotFound:
            sheet = spreadsheet.add_worksheet(title=tab_name, rows="1000", cols="20")

        # Nettoyer et insérer les données
        sheet.clear()
        sheet.append_rows([data.columns.tolist()] + data.values.tolist(), table_range="A1")

        # Générer le lien partageable
        link = get_shareable_link(spreadsheet, tab_name)
        print(f"✅ Données uploadées avec succès. Lien : {link}")
        return link
    except Exception as e:
        print(f"❌ Erreur lors de l'upload : {e}")
        return None
