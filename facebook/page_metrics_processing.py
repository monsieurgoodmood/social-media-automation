# page_metrics_processing.py

import logging
from config import GOOGLE_SHEET_NAME_PAGES
from facebook_api import get_page_insights
from utils import save_and_upload
from datetime import datetime
import pandas as pd
import re

logger = logging.getLogger(__name__)

def process_page_metrics():
    """Traite les métriques de la page Facebook en supprimant les doublons de colonne Date."""
    logger.info("Début du traitement des métriques de la page.")
    try:
        page_metrics = get_page_insights()
        if page_metrics:
            # Ajouter la date uniquement dans la première colonne "Date"
            page_metrics['Date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Créer le DataFrame
            df = pd.DataFrame([page_metrics])
            
            # Vérifier si la dernière colonne contient une date au format YYYY-MM-DD
            last_column_value = df.iloc[0, -1]  # Récupérer la dernière valeur de la première ligne
            if isinstance(last_column_value, str) and re.match(r"\d{4}-\d{2}-\d{2}", last_column_value):
                df = df.iloc[:, :-1]  # Supprimer la dernière colonne si elle contient une date

            # Utiliser save_and_upload pour sauvegarder et uploader sans la colonne dupliquée
            save_and_upload(df.to_dict(orient="records"), 'facebook_page_metrics.csv', GOOGLE_SHEET_NAME_PAGES, "Agence RSP")
            logger.info("Les métriques de la page ont été traitées et enregistrées sans doublons.")
    except Exception as e:
        logger.error(f"Erreur lors du traitement des métriques de la page : {e}")