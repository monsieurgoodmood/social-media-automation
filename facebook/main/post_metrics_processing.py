from facebook_api import get_facebook_posts
from metrics.api_utils import fetch_insights_daily, test_metric
from google_sheets import upload_to_google_sheets
import pandas as pd
import logging
from metrics import POST_METRICS
from utils import normalize_metrics

logger = logging.getLogger(__name__)

def validate_metrics(post_id, all_metrics):
    """
    Valide les métriques pour un post donné en testant leur validité.
    """
    valid_metrics = []
    for metric in all_metrics:
        logger.info(f"Validation de la métrique : {metric}")
        if test_metric(post_id, metric):
            valid_metrics.append(metric)
            logger.info(f"Métrique valide : {metric}")
        else:
            logger.warning(f"Métrique non valide : {metric}")
    return valid_metrics

def process_post_data(limit=2):
    """
    Traite les données des posts Facebook pour les 2 plus récents posts.
    """
    logger.info("Début du traitement des posts Facebook.")
    try:
        posts = get_facebook_posts()
        logger.info(f"Nombre total de posts récupérés : {len(posts)}")

        # Trier les posts par date de création et limiter au nombre requis
        posts_sorted = sorted(posts, key=lambda x: x.get('created_time', ''), reverse=True)
        recent_posts = posts_sorted[:limit]
        links_generated = []

        for post in recent_posts:
            post_id = post.get('id')
            created_time = post.get('created_time')

            if not post_id or not created_time:
                logger.warning("Post sans ID ou Created Time.")
                continue

            logger.info(f"Traitement du post {post_id} créé le {created_time}.")

            # Validation des métriques pour le post
            valid_metrics = validate_metrics(post_id, POST_METRICS)
            if not valid_metrics:
                logger.warning(f"Aucune métrique valide pour le post {post_id}.")
                continue

            # Collecte des données journalières
            post_insights = fetch_insights_daily(post_id, valid_metrics, start_date=created_time[:10])
            if post_insights:
                # Transformation des données en DataFrame
                df = pd.DataFrame(post_insights)
                if df.empty:
                    logger.warning(f"Aucune donnée pour le post {post_id}.")
                    continue

                # Pivot et normalisation
                df = df.pivot_table(index='Date', columns='name', values='value', aggfunc='first').reset_index()
                df = normalize_metrics(df, valid_metrics)

                # Générer un nom d'onglet sûr basé sur la date de création
                tab_name = created_time[:10].replace("-", "_")

                # Upload vers Google Sheets
                sheet_name = "Facebook Post Insights"
                tab_name = f"Post_{POST_ID}_Daily"
                try:
                    link = upload_to_google_sheet(daily_deltas, sheet_name, tab_name)
                    if link:
                        print(f"✅ Données uploadées avec succès. Lien : {link}")
                    else:
                        print("❌ Erreur : Le lien de Google Sheets est vide malgré un upload réussi.")
                except Exception as e:
                    print(f"❌ Erreur lors de l'upload vers Google Sheets : {e}")


        if links_generated:
            logger.info("Liens générés pour les posts :")
            for link in links_generated:
                logger.info(link)
    except Exception as e:
        logger.error(f"Erreur lors du traitement des posts Facebook : {e}")
