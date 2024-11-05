# page_metrics_processing.py

import os  # Ajoutez ceci au début
import logging
from config import GOOGLE_SHEET_NAME_PAGES
from facebook_api import get_page_insights
from utils import save_and_upload
from datetime import datetime
import pandas as pd
import re


logger = logging.getLogger(__name__)

def process_page_metrics():
    """Traite les métriques de la page Facebook en ajoutant la colonne Date et en supprimant les doublons."""
    logger.info("Début du traitement des métriques de la page.")
    try:
        page_metrics = get_page_insights()
        if page_metrics:
            # Ajouter la date pour les calculs
            page_metrics['Date'] = datetime.now().strftime('%Y-%m-%d')
            
            # Créer le DataFrame avec la colonne Date
            df = pd.DataFrame([page_metrics])
            
            # Charger l'historique existant ou créer un DataFrame vide
            insights_file_path = '/home/arthur/code/social-media-automation/facebook/data/facebook_page_insights.csv'
            historical_df = pd.read_csv(insights_file_path) if os.path.exists(insights_file_path) else pd.DataFrame(columns=df.columns)
            
            # Concaténer les nouvelles données avec l'historique
            updated_df = pd.concat([historical_df, df], ignore_index=True)
            
            # Sauvegarder en CSV
            updated_df.to_csv(insights_file_path, index=False)
            logger.info("Données sauvegardées dans le CSV local avec la colonne Date.")

            # Exclure la colonne Date pour l'upload
            save_and_upload(df.drop(columns=['Date']).to_dict(orient="records"), "facebook_page_insights.csv", GOOGLE_SHEET_NAME_PAGES, "Agence RSP", exclude_date_in_sheet=True)
            logger.info("Les métriques de la page ont été traitées et enregistrées sans doublons.")
    except Exception as e:
        logger.error(f"Erreur lors du traitement des métriques de la page : {e}")
