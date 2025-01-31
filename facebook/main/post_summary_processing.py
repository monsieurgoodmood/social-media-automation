# post_summary_processing.py


import pandas as pd
import logging
import os
from google_sheets import upload_to_google_sheets

logger = logging.getLogger(__name__)

def generate_post_summary():
    logger.info("Début de l'agrégation des données pour le résumé des posts.")
    try:
        data_dir = '/path/to/data/directory'
        all_data = []

        for file_name in os.listdir(data_dir):
            if file_name.startswith("facebook_post_insights"):
                file_path = os.path.join(data_dir, file_name)
                post_df = pd.read_csv(file_path)
                all_data.append(post_df)

        if all_data:
            consolidated_df = pd.concat(all_data, ignore_index=True)

            # Calculer le résumé
            summary = {
                "Total des Impressions des Publications": consolidated_df['post_impressions'].sum(),
                "Impressions Organiques": consolidated_df['post_impressions_organic'].sum(),
                "Impressions Payantes": consolidated_df['post_impressions_paid'].sum(),
                "Impressions Virales": consolidated_df['post_impressions_viral'].sum(),
                "Clics Totaux": consolidated_df['post_clicks'].sum(),
                "Total des Réactions (J’aime)": consolidated_df['post_reactions_like_total'].sum(),
                "Total des Réactions (J’adore)": consolidated_df['post_reactions_love_total'].sum(),
                "Total des Réactions (Wouah)": consolidated_df['post_reactions_wow_total'].sum(),
                "Total des Réactions (Haha)": consolidated_df['post_reactions_haha_total'].sum(),
                "Temps Moyen de Visionnage de Vidéo": consolidated_df['post_video_avg_time_watched'].mean(),
                "Total des Vues de Vidéos": consolidated_df['post_video_views'].sum(),
                "Nombre de Vues Complètes (30 secondes)": consolidated_df['post_video_complete_views_30s'].sum(),
                "Vues Organiques": consolidated_df['post_video_views_organic'].sum(),
                "Vues Payantes": consolidated_df['post_video_views_paid'].sum(),
                "Vues avec Son Activé": consolidated_df['post_video_views_sound_on'].sum(),
            }
            summary_df = pd.DataFrame([summary])

            link = upload_to_google_sheets(summary_df, "facebook_posts_summary", "Résumé Consolidé")
            if link:
                logger.info(f"Lien pour le résumé des posts : {link}")
        else:
            logger.warning("Aucune donnée trouvée pour les posts.")
    except Exception as e:
        logger.error(f"Erreur lors de la génération du résumé des posts : {e}")