# page_metrics_processing.py

from metrics.api_utils import fetch_insights_daily
from config import FACEBOOK_PAGE_ID
from metrics import PAGE_METRICS
import pandas as pd
import os
import logging
from datetime import datetime, timedelta
from google_sheets import clean_and_upload_to_google_sheets, upload_to_google_sheets

logger = logging.getLogger(__name__)

def process_page_metrics():
    logger.info("Début du traitement des métriques de la page Facebook.")
    try:
        start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
        page_insights = fetch_insights_daily(FACEBOOK_PAGE_ID, PAGE_METRICS, start_date=start_date)

        if page_insights:
            df = pd.DataFrame(page_insights)
            df = df.pivot_table(index='end_time', columns='name', values='value', aggfunc='first').reset_index()
            df.rename(columns={'end_time': 'Date'}, inplace=True)

            # Uploader dans Google Sheets
            link = upload_to_google_sheets(df, "facebook_page_insights", "Données Brutes")
            if link:
                logger.info(f"Lien pour les métriques de la page : {link}")

    except Exception as e:
        logger.error(f"Erreur lors du traitement des métriques de la page : {e}")

