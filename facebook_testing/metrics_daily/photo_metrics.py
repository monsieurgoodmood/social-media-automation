import requests
import sys
import os

# Ajouter le dossier racine au chemin Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main.facebook_api import get_access_token

# Configuration
POST_ID = "814743708622327_941948824609475"
API_VERSION = "v21.0"

# Liste des métriques maximales possibles pour un post de type photo
PHOTO_METRICS = [
    # Impressions (visualisations du post)
    'post_impressions', 
    'post_impressions_organic', 
    'post_impressions_paid',
    'post_impressions_viral', 
    'post_impressions_unique', 
    'post_impressions_fan',

    # Reach (portée des fans)
    'post_fan_reach',

    # Reactions (réactions au post)
    'post_reactions_like_total', 
    'post_reactions_love_total', 
    'post_reactions_wow_total', 
    'post_reactions_haha_total',

    # Clics (interactions spécifiques)
    'post_clicks', 
    'post_clicks_by_type',

    # Vidéo (interactions vidéo pour les posts multimédias ou vidéos intégrées)
    'post_video_avg_time_watched', 
    'post_video_complete_views_30s',
    'post_video_views_organic', 
    'post_video_views_paid',
    'post_video_views_sound_on', 
    'post_video_views_unique',

    # Story additions (ajouts aux stories à partir du post)
    'post_story_adds'
]


def test_metric(api_version, post_id, metric):
    """Teste si une métrique est valide et vérifie la disponibilité de données."""
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {
        'metric': metric,
        'access_token': token,
        'period': 'day'
    }

    response = requests.get(insights_url, params=params)
    if response.status_code == 200:
        # Vérifie si des données sont retournées
        data = response.json().get('data', [])
        if data:
            return {"metric": metric, "valid": True, "data": True}
        else:
            return {"metric": metric, "valid": True, "data": False}
    else:
        return {"metric": metric, "valid": False, "data": False}

def get_valid_metrics(api_version, post_id, metrics):
    """Teste chaque métrique pour un type de post donné et retourne les résultats."""
    results = []
    for metric in metrics:
        print(f"Test de la métrique : {metric}")
        result = test_metric(api_version, post_id, metric)
        if result["valid"]:
            if result["data"]:
                print(f"✅ Métrique valide avec données : {metric}")
            else:
                print(f"⚠️ Métrique valide mais sans données : {metric}")
        else:
            print(f"❌ Métrique non valide : {metric}")
        results.append(result)
    return results

if __name__ == "__main__":
    print(f"=== Détection des métriques disponibles pour un post de type photo ===")
    metric_results = get_valid_metrics(API_VERSION, POST_ID, PHOTO_METRICS)

    # Filtrer les métriques valides
    valid_metrics = [res["metric"] for res in metric_results if res["valid"]]
    valid_with_data = [res["metric"] for res in metric_results if res["valid"] and res["data"]]
    valid_without_data = [res["metric"] for res in metric_results if res["valid"] and not res["data"]]

    print("\n✅ Métriques valides pour le post (type: photo) :")
    for metric in valid_metrics:
        print(f"- {metric}")

    print("\n✅ Métriques valides avec données :")
    for metric in valid_with_data:
        print(f"- {metric}")

    print("\n⚠️ Métriques valides mais sans données :")
    for metric in valid_without_data:
        print(f"- {metric}")