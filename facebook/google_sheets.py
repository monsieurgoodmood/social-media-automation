# google_sheets.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import logging
import time
from datetime import datetime
from config import GOOGLE_CREDENTIALS_JSON
from utils import convert_to_json_compatible
import json
from gspread.utils import rowcol_to_a1
from time import sleep
import random

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

def upload_to_google_sheets(data, sheet_name, tab_name, retries=3, delay=5):
    """Upload des données vers Google Sheets en ajoutant un mécanisme de réessai."""
    attempt = 0
    while attempt < retries:
        try:
            client = get_google_sheets_client()
            spreadsheet = client.open(sheet_name)
            
            # Reste du code d’upload...
            # Si tout est réussi, on sort de la boucle
            logger.info(f"Données ajoutées ou mises à jour dans l'onglet '{tab_name}'.")
            break
        
        except Exception as e:
            attempt += 1
            if attempt < retries:
                logger.warning(f"Échec d'upload, nouvelle tentative {attempt}/{retries} dans {delay} secondes.")
                sleep(delay + random.uniform(0, 3))  # Ajouter un léger aléatoire pour éviter les conflits
            else:
                logger.error(f"Échec définitif de l'upload vers Google Sheets : {e}")
                raise

    # Appliquer map à chaque colonne et convertir explicitement en types Python natifs
    for col in data.columns:
        data[col] = data[col].map(convert_to_json_compatible).astype(object)

    # Supprimer la colonne 'Date' en fin de DataFrame s'il existe une redondance
    if data.columns[-1] == 'Date' and data.columns[0] == 'Date':
        data = data.iloc[:, :-1]

    data_records = data.to_dict(orient="records")

    # Vérification de sérialisation JSON
    try:
        json.dumps(data_records)
    except TypeError as e:
        logger.error(f"Erreur de sérialisation JSON : {e}")
        return

    # Obtenir ou créer l'onglet
    try:
        sheet = spreadsheet.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        # Crée l'onglet si non existant et le place en premier
        sheet = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="20")
        sheet.insert_row(["Date"] + list(data.columns), index=1)
        time.sleep(1)
        # Réordonne l'onglet pour qu'il soit le premier
        spreadsheet.batch_update({
            "requests": [
                {"updateSheetProperties": {
                    "properties": {"sheetId": sheet.id, "index": 0},
                    "fields": "index"
                }}
            ]
        })
    
    # Procéder à l'upload
    today_str = datetime.now().strftime('%Y-%m-%d')
    dates = sheet.col_values(1)
    time.sleep(1)
    row_index = dates.index(today_str) + 1 if today_str in dates else None

    if row_index:
        logger.info(f"La date du jour existe déjà à la ligne {row_index}. Mise à jour des données.")
        updated_row = [today_str] + list(data.iloc[0].values)

        # Utilisation de gspread.utils.rowcol_to_a1 pour créer la plage
        start_cell = rowcol_to_a1(row_index, 1)
        end_cell = rowcol_to_a1(row_index, len(updated_row))
        cell_range = f"{start_cell}:{end_cell}"
        cell_list = sheet.range(cell_range)

        for i, cell in enumerate(cell_list):
            cell.value = updated_row[i]
        sheet.update_cells(cell_list)
    else:
        new_row = [today_str] + list(data.iloc[0].values)
        sheet.append_row(new_row)
    logger.info(f"Données ajoutées ou mises à jour dans l'onglet '{tab_name}'.")
