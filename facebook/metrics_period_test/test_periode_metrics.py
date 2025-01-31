import requests
from datetime import datetime
import pandas as pd
import sys
import os

# Ajouter le dossier racine au chemin Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main.facebook_api import get_access_token

# Configuration
POST_ID = "814743708622327_941948824609475"
START_DATE = "2024-10-30"
END_DATE = datetime.now().strftime('%Y-%m-%d')
API_VERSION = "v21.0"

# Liste complète des 231 métriques
METRICS = [
    # Impressions et portées
    'post_impressions', 'post_impressions_organic', 'post_impressions_paid',
    'post_impressions_viral', 'post_impressions_unique', 'post_impressions_fan',
    'post_impressions_non_fan', 'post_reach', 'post_reach_organic', 'post_reach_paid',
    'post_fan_reach', 'post_reach_non_fan', 'viral_impressions', 'non_viral_impressions',
    'organic_impressions_of_posts', 'non_viral_reach_of_posts', 'total_impressions_of_posts',
    'total_reach_of_posts', 'page_posts_impressions', 'page_posts_organic_impressions',
    'page_posts_paid_impressions', 'page_posts_viral_impressions',

    # Engagement (Réactions, clics, partages)
    'post_engaged_users', 'post_engagement_rate', 'post_engagement_rate_impressions',
    'post_engagement_rate_reach', 'post_reactions_total', 'post_reactions_like_total',
    'post_reactions_love_total', 'post_reactions_wow_total', 'post_reactions_haha_total',
    'post_reactions_sad_total', 'post_reactions_angry_total', 'post_actions_like',
    'post_actions_love', 'post_actions_wow', 'post_actions_haha', 'post_actions_sad',
    'post_actions_angry', 'post_clicks', 'post_clicks_unique', 'post_link_clicks',
    'post_photo_views', 'post_other_clicks', 'post_clicks_by_type', 'post_link_clicks_unique',
    'post_story_adds', 'post_shares', 'post_comments', 'post_comments_unique',
    'page_engagement_rate', 'page_engagement_rate_impressions', 'page_engagement_rate_reach',
    'page_engagements',

    # Vidéos et interactions associées
    'post_video_avg_time_watched', 'post_video_complete_views_30s', 'post_video_retention_10s',
    'post_video_retention_25', 'post_video_retention_50', 'post_video_retention_75',
    'post_video_retention_95', 'post_video_retention_100', 'post_video_total_time_watched',
    'post_video_views_organic', 'post_video_views_paid', 'post_video_views_sound_on',
    'post_video_views_unique', 'post_video_views_time', 'post_video_paid_views_to_95',
    'post_video_organic_views_to_95', 'post_video_length', 'page_video_views',
    'page_video_watch_30s_rate', 'page_video_30_sec_views', 'page_video_autoplayed_30_sec_views',
    'page_video_paid_30_sec_views', 'page_video_organic_30_sec_views', 'page_video_clicked_to_play_30_sec_views',

    # Pages (Abonnés, likes, messages, interactions)
    'page_likes', 'page_new_likes', 'page_new_unlikes', 'page_new_followers',
    'page_total_followers', 'page_likes_growth_rate', 'page_likes_churn_rate',
    'net_likes_growth', 'page_total_impressions', 'page_new_unique_likes',
    'page_new_unique_unlikes', 'page_messages_total_messages_sent',
    'page_messages_new_conversations', 'page_messages_reported_conversations',
    'page_messages_blocked_conversations',

    # Stories, reels et posts associés
    'reel_post_video_avg_time_watched', 'reel_post_video_view_time', 'reel_post_video_social_actions',
    'reel_post_impressions_unique', 'reel_post_reactions_total', 'reel_post_reactions_like',
    'reel_post_reactions_love', 'reel_post_reactions_wow', 'reel_post_reactions_haha',
    'reel_post_reactions_angry', 'post_total_comments_count', 'reel_views',
    'reel_reactions_total',

    # Métadonnées et contenus associés
    'post_id', 'post_type', 'post_message', 'post_description', 'post_name', 'post_picture',
    'post_full_picture_url', 'post_status_type', 'post_privacy', 'page_name',
    'page_profile_picture_url', 'shared_post_link',

    # Dérivés et analyses avancées
    'likes_growth_rate', 'likes_churn_rate', 'organic_reach_perc', 'paid_reach_perc',
    'viral_reach_perc'
]

# Périodes à tester
PERIODS = ['day', 'week', 'month', 'lifetime']


def fetch_metric_with_period(api_version, post_id, metric, period, start_date=None, end_date=None):
    """Récupère les données pour une métrique avec une période spécifiée."""
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {
        'metric': metric,
        'access_token': token,
        'period': period,
    }

    if start_date and end_date and period in ['day', 'week', 'month']:
        params['since'] = start_date
        params['until'] = end_date

    response = requests.get(insights_url, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if not data:
            return "valid_but_no_data", None
        else:
            return "valid_with_data", data
    elif response.status_code == 400 and "must be a valid insights metric" in response.text:
        return "invalid", None
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
        return "error", None


def test_metrics(api_version, post_id, metrics, periods, start_date, end_date):
    """Teste toutes les métriques sur toutes les périodes et collecte les résultats."""
    results = []
    for metric in metrics:
        print(f"\n=== Test de la métrique '{metric}' ===")
        for period in periods:
            print(f"--- Période : {period} ---")
            status, data = fetch_metric_with_period(api_version, post_id, metric, period, start_date, end_date)

            if status == "valid_with_data":
                print(f"✅ La métrique '{metric}' est valide avec des données pour la période '{period}'.")
                results.append({"metric": metric, "period": period, "status": "valid_with_data", "data": data})
            elif status == "valid_but_no_data":
                print(f"⚠️ La métrique '{metric}' est valide, mais aucune donnée n'est disponible pour la période '{period}'.")
                results.append({"metric": metric, "period": period, "status": "valid_but_no_data", "data": None})
            elif status == "invalid":
                print(f"❌ La métrique '{metric}' n'est pas valide pour la période '{period}'.")
                results.append({"metric": metric, "period": period, "status": "invalid", "data": None})
            else:
                print(f"⚠️ Une erreur est survenue pour la métrique '{metric}' et la période '{period}'.")
                results.append({"metric": metric, "period": period, "status": "error", "data": None})

    return results


# Exécution pour toutes les métriques et périodes
results = test_metrics(API_VERSION, POST_ID, METRICS, PERIODS, START_DATE, END_DATE)

# Résumé des résultats
summary = pd.DataFrame(results)

# Sous-résumés des métriques valides
valid_metrics = summary[summary['status'].str.contains('valid')]
valid_with_data = valid_metrics[valid_metrics['status'] == 'valid_with_data']
valid_no_data = valid_metrics[valid_metrics['status'] == 'valid_but_no_data']
invalid_metrics = summary[summary['status'] == 'invalid']

# Affichage des résumés
print("\n=== Résumé structuré par période ===")
for period in PERIODS:
    print(f"\n--- Période : {period} ---")
    print("Métriques valides avec des données :", valid_with_data[valid_with_data['period'] == period]['metric'].tolist())
    print("Métriques valides sans données :", valid_no_data[valid_no_data['period'] == period]['metric'].tolist())
    print("Métriques invalides :", invalid_metrics[invalid_metrics['period'] == period]['metric'].tolist())