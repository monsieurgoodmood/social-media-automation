# page_summary_processing.py

import logging
from datetime import datetime
import pandas as pd
from config import GOOGLE_SHEET_NAME_PAGES_RESUME
from facebook_api import get_page_insights
from utils import save_and_upload, clean_data_for_json
import os

logger = logging.getLogger(__name__)

def process_page_summary():
    logger.info("Début du traitement du résumé des métriques de la page.")
    try:
        page_metrics = get_page_insights()
        today_fan_count = page_metrics.get('fan_count', 0)
        today_followers_count = page_metrics.get('followers_count', 0)

        # Chargement de l'historique des données
        insights_file_path = '/home/arthur/code/social-media-automation/facebook/data/facebook_page_insights.csv'
        
        try:
            historical_df = pd.read_csv(insights_file_path)
            
            # Vérification et ajout de la colonne 'Date' si elle est absente
            if 'Date' not in historical_df.columns:
                historical_df['Date'] = pd.NaT  # Ajoute des valeurs NaT pour les dates manquantes
            historical_df['Date'] = pd.to_datetime(historical_df['Date'], errors='coerce')  # Convertir en datetime

        except FileNotFoundError:
            # Création d'un DataFrame vide avec les colonnes attendues si le fichier n'existe pas
            historical_df = pd.DataFrame(columns=['Date', 'fan_count', 'followers_count', 'page_total_actions_day', 
                                                  'page_impressions_viral_day', 'page_impressions_day', 
                                                  'page_fan_adds_day', 'page_fan_removes_day', 
                                                  'page_video_views_day', 'page_impressions_paid_day', 
                                                  'page_impressions_unique_day'])

        # Calcul des nouvelles métriques
        first_fan_count = historical_df['fan_count'].iloc[0] if len(historical_df) > 0 else today_fan_count
        growth_rate = ((today_fan_count - first_fan_count) / first_fan_count * 100) if first_fan_count else 0

        engagement_total = historical_df['page_total_actions_day'].sum() if 'page_total_actions_day' in historical_df.columns else 0
        average_fan_count = historical_df['fan_count'].mean() if 'fan_count' in historical_df.columns else today_fan_count
        engagement_rate_per_follower = (engagement_total / average_fan_count * 100) if average_fan_count else 0

        viral_impressions_avg = historical_df['page_impressions_viral_day'].mean() if 'page_impressions_viral_day' in historical_df.columns else 0
        total_impressions = historical_df['page_impressions_day'].sum() if 'page_impressions_day' in historical_df.columns else 0
        viral_impressions_ratio = (historical_df['page_impressions_viral_day'].sum() / total_impressions * 100) if total_impressions else 0

        fan_adds = historical_df['page_fan_adds_day'].sum() if 'page_fan_adds_day' in historical_df.columns else 0
        fan_removes = historical_df['page_fan_removes_day'].sum() if 'page_fan_removes_day' in historical_df.columns else 0
        retention_rate = ((fan_adds - fan_removes) / average_fan_count * 100) if average_fan_count else 0

        video_views = historical_df['page_video_views_day'].sum() if 'page_video_views_day' in historical_df.columns else 0
        video_view_rate = (video_views / average_fan_count * 100) if average_fan_count else 0

        paid_impressions = historical_df['page_impressions_paid_day'].sum() if 'page_impressions_paid_day' in historical_df.columns else 0
        paid_vs_organic_ratio = (paid_impressions / total_impressions * 100) if total_impressions else 0

        unique_impressions = historical_df['page_impressions_unique_day'].sum() if 'page_impressions_unique_day' in historical_df.columns else 0
        unique_impressions_rate = (unique_impressions / total_impressions * 100) if total_impressions else 0

        # Préparer les données de résumé
        summary_data = {
            'Dernier fan_count': today_fan_count,
            'Dernier followers_count': today_followers_count,
            'Taux de Croissance des Abonnés (%)': growth_rate,
            'Engagement Total': engagement_total,
            'Taux d’Engagement par Abonné (%)': engagement_rate_per_follower,
            'Impressions Virales Moyennes par Jour': viral_impressions_avg,
            'Ratio d’Impressions Virales (%)': viral_impressions_ratio,
            'Taux de Conversion des Abonnés (%)': retention_rate,
            'Taux de Visionnage de Vidéo (%)': video_view_rate,
            'Ratio Impressions Payantes vs Organiques (%)': paid_vs_organic_ratio,
            'Taux d’Impressions Uniques (%)': unique_impressions_rate
        }

        # Créer un DataFrame pour les données de résumé sans la colonne Date
        summary_df = pd.DataFrame([summary_data])

        # Sauvegarde et upload
        save_and_upload(summary_df.to_dict(orient="records"), 'facebook_page_resume.csv', GOOGLE_SHEET_NAME_PAGES_RESUME, "Résumé", exclude_date_in_sheet=True)
        logger.info("Résumé des métriques de la page traité et sauvegardé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du traitement du résumé des métriques de la page : {e}")
