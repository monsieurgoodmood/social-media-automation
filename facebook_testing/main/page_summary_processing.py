# page_summary_processing.py

import logging
import pandas as pd
import os
from google_sheets import upload_to_google_sheets

logger = logging.getLogger(__name__)

def generate_page_summary():
    logger.info("Début de l'agrégation des données pour le résumé de la page.")
    try:
        insights_file_path = '/home/arthur/code/social-media-automation/facebook/data/facebook_page_insights.csv'

        if os.path.exists(insights_file_path):
            historical_df = pd.read_csv(insights_file_path)

            # Vérifier et ajouter des colonnes manquantes
            required_columns = [
                'fan_count', 'page_total_actions_day', 'page_impressions_viral_day',
                'page_impressions_day', 'page_fan_adds_day', 'page_fan_removes_day',
                'page_video_views_day', 'page_impressions_paid_day', 'page_impressions_unique_day'
            ]
            for col in required_columns:
                if col not in historical_df.columns:
                    historical_df[col] = 0

            # Calculer les métriques de résumé
            first_fan_count = historical_df['fan_count'].iloc[0]
            today_fan_count = historical_df['fan_count'].iloc[-1]
            growth_rate = ((today_fan_count - first_fan_count) / first_fan_count * 100) if first_fan_count else 0

            # Autres calculs...
            # (comme défini dans le fichier existant)

            # Sauvegarder dans Google Sheets
            link = upload_to_google_sheets(summary_df, "facebook_page_insights", "Résumé Consolidé")
            if link:
                logger.info(f"Lien pour le résumé de la page : {link}")

        else:
            logger.warning("Fichier insights non trouvé pour le résumé.")
    except Exception as e:
        logger.error(f"Erreur lors de la génération du résumé des métriques de la page : {e}")
