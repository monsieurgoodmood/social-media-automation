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
    """Upload des données vers Google Sheets en vérifiant et remplaçant la première ligne si nécessaire pour correspondre aux en-têtes."""
    attempt = 0
    while attempt < retries:
        try:
            client = get_google_sheets_client()
            spreadsheet = client.open(sheet_name)
            
            # Obtenir ou créer l'onglet
            try:
                sheet = spreadsheet.worksheet(tab_name)
            except gspread.WorksheetNotFound:
                # Créer l'onglet s'il n'existe pas et insérer les en-têtes de colonnes
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

            # Vérifier et définir les colonnes de Google Sheets
            existing_columns = sheet.row_values(1)
            required_columns = ["Date"] + list(data.columns)
            
            # Si les colonnes sont incorrectes, remplacez la première ligne
            if existing_columns != required_columns:
                logger.info(f"Réinitialisation des colonnes pour correspondre aux en-têtes requis.")
                sheet.delete_rows(1)  # Utiliser delete_rows(1) au lieu de delete_row(1)
                sheet.insert_row(required_columns, index=1)  # Insérer les en-têtes corrects

            # Préparer les données pour l'upload
            for col in data.columns:
                data[col] = data[col].map(convert_to_json_compatible).astype(object)

            # Supprimer la colonne 'Date' en doublon si elle est présente à la fin du DataFrame
            if data.columns[-1] == 'Date' and data.columns[0] == 'Date':
                data = data.iloc[:, :-1]

            # Conversion des données pour compatibilité JSON
            data_records = data.to_dict(orient="records")
            try:
                json.dumps(data_records)
            except TypeError as e:
                logger.error(f"Erreur de sérialisation JSON : {e}")
                return

            # Vérifier la présence de la date d'aujourd'hui et déterminer l'index de mise à jour ou d'ajout
            today_str = datetime.now().strftime('%Y-%m-%d')
            dates = sheet.col_values(1)[1:]  # Exclure l'en-tête lors de la vérification des dates
            time.sleep(1)
            row_index = dates.index(today_str) + 2 if today_str in dates else None  # +2 pour exclure l'en-tête

            # Mise à jour ou ajout des données
            if row_index:
                logger.info(f"La date du jour existe déjà à la ligne {row_index}. Mise à jour des données.")
                updated_row = [today_str] + list(data.iloc[0].values)

                # Utilisation de gspread.utils.rowcol_to_a1 pour définir la plage de cellules
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
            break  # Sortie de la boucle en cas de succès

        except Exception as e:
            attempt += 1
            if attempt < retries:
                logger.warning(f"Échec d'upload, nouvelle tentative {attempt}/{retries} dans {delay} secondes.")
                sleep(delay + random.uniform(0, 3))  # Attente avec un léger décalage aléatoire pour éviter les conflits
            else:
                logger.error(f"Échec définitif de l'upload vers Google Sheets : {e}")
                raise
