# post_metrics_processing.py

import logging
import pandas as pd
from config import GOOGLE_SHEET_NAME_POSTS
from facebook_api import get_facebook_posts, get_post_insights
from metrics import POST_METRICS
from datetime import datetime
from utils import generate_title, save_and_upload, calculate_metrics

logger = logging.getLogger(__name__)

def process_post_data():
    """Traite les posts Facebook et stocke les données."""
    logger.info("Début du traitement des posts Facebook.")
    try:
        posts = get_facebook_posts()
        if not posts:
            logger.warning("Aucun post récupéré depuis l'API Facebook.")
            return

        for post in posts:
            post_id = post.get('id', None)
            created_time = post.get('created_time', '')

            if not post_id:
                logger.warning("Post sans ID détecté, passage au suivant.")
                continue

            # Format d'onglet unique pour chaque post (ID ou date)
            try:
                post_datetime = pd.to_datetime(created_time)
                tab_name = post_datetime.strftime('%Y-%m-%d %H-%M')
            except Exception as e:
                logger.error(f"Erreur de format de date pour le post ID: {post_id} - {e}")
                tab_name = f"Post_{post_id}"

            insights = get_post_insights(post_id)
            if not isinstance(insights, dict):
                logger.warning(f"Aucun insight récupéré pour le post ID: {post_id}")
                continue

            post_data = {
                'Post ID': post_id,
                'Created Time': created_time,
                'Title': generate_title(post)
            }
            for metric in POST_METRICS:
                metric_value = insights.get(metric, None)
                post_data[metric] = metric_value

            # Calculer les métriques personnalisées et uploader chaque post individuellement
            post_data = calculate_metrics(post_data)
            save_and_upload([post_data], 'facebook_post_insights.csv', GOOGLE_SHEET_NAME_POSTS, tab_name)

    except Exception as e:
        logger.error(f"Erreur lors du traitement des posts Facebook : {e}")