import requests
from datetime import datetime
from facebook_api import get_access_token
import pandas as pd
import numpy as np

# Configuration
POST_ID = "814743708622327_1008475567956800"
START_DATE = "2024-10-30"
END_DATE = datetime.now().strftime('%Y-%m-%d')
API_VERSION = "v21.0"

# Liste des métriques possibles
ALL_METRICS = [
    'post_impressions', 'post_impressions_organic', 'post_impressions_paid',
    'post_impressions_viral', 'post_reactions_like_total', 'post_reactions_love_total',
    'post_reactions_wow_total', 'post_reactions_haha_total', 'post_reactions_sad_total',
    'post_reactions_angry_total', 'post_video_views', 'post_video_views_organic',
    'post_video_views_paid', 'post_video_complete_views_30s', 'post_reach',
    'post_shares', 'post_clicks'
]

# Tester une métrique
def test_metric(api_version, post_id, metric, start_date, end_date):
    """Teste si une métrique est valide pour le post."""
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {
        'metric': metric,
        'access_token': token,
        'period': 'day',
        'since': start_date,
        'until': end_date
    }

    response = requests.get(insights_url, params=params)
    return response.status_code == 200

# Récupérer les métriques valides
def get_valid_metrics(api_version, post_id, metrics, start_date, end_date):
    """Teste chaque métrique et retourne celles qui sont valides."""
    valid_metrics = []
    for metric in metrics:
        print(f"Test de la métrique : {metric}")
        if test_metric(api_version, post_id, metric, start_date, end_date):
            valid_metrics.append(metric)
            print(f"-> Métrique valide : {metric}")
        else:
            print(f"-> Métrique non valide : {metric}")
    return valid_metrics

# Récupérer les métriques journalières
def fetch_daily_metrics(api_version, post_id, metrics, start_date, end_date):
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {
        'metric': ','.join(metrics),
        'access_token': token,
        'period': 'lifetime',  # Agrégats sur toute la durée de vie
        'since': start_date,
        'until': end_date
    }

    response = requests.get(insights_url, params=params)
    if response.status_code == 200:
        data = response.json().get('data', [])
        if not data:
            print("⚠️ Aucune donnée reçue, vérifiez les paramètres ou la portée de l'API.")
        else:
            print(f"✅ Données reçues : {data}")
        return data
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
        return None

def aggregate_daily_data(data):
    df = pd.DataFrame(data)
    # Pivot des données pour organiser par métrique
    df_pivot = df.pivot(index="Date", columns="name", values="value")
    df_pivot["Total_Views"] = df_pivot.get("post_video_views", 0).sum()
    df_pivot["Total_Reactions"] = (
        df_pivot.get("post_reactions_like_total", 0) +
        df_pivot.get("post_reactions_love_total", 0) +
        df_pivot.get("post_reactions_wow_total", 0) +
        df_pivot.get("post_reactions_haha_total", 0)
    )
    # Ajouter une colonne d'interactions totales
    df_pivot["Total_Interactions"] = (
        df_pivot["Total_Reactions"] +
        df_pivot.get("post_shares", 0) +
        df_pivot.get("post_clicks", 0)
    )
    return df_pivot.reset_index()

def calculate_deltas(data):
    """
    Calcule les différences journalières pour les métriques cumulatives.
    """
    df = pd.DataFrame(data)
    for col in df.columns[1:]:  # Ignorer la colonne 'Date'
        if df[col].dtype in [np.int64, np.float64]:
            df[col] = df[col].diff().fillna(df[col])  # Calculer la différence journalière
            df[col] = df[col].clip(lower=0)  # Éviter les valeurs négatives
    return df

def validate_and_calculate(data, expected_metrics):
    """
    Valide les données récupérées et calcule les métriques manquantes estimées.
    """
    retrieved_metrics = [metric['name'] for metric in data]
    missing_metrics = [metric for metric in expected_metrics if metric not in retrieved_metrics]

    # Log des métriques manquantes
    if missing_metrics:
        print(f"Métriques manquantes : {missing_metrics}")
    else:
        print("Toutes les métriques attendues ont été récupérées.")

    # Calcul des métriques estimées
    impressions_organic = next((item['values'][0]['value'] for item in data if item['name'] == 'post_impressions_organic'), 0)
    shares = next((item['values'][0]['value'] for item in data if item['name'] == 'post_shares'), 0)
    clicks = next((item['values'][0]['value'] for item in data if item['name'] == 'post_clicks'), 0)

    total_interactions = (
        next((item['values'][0]['value'] for item in data if item['name'] == 'post_reactions_like_total'), 0) +
        next((item['values'][0]['value'] for item in data if item['name'] == 'post_reactions_love_total'), 0) +
        clicks + shares
    )

    return {
        'impressions_organic': impressions_organic,
        'shares': shares,
        'clicks': clicks,
        'total_interactions': total_interactions
    }

# Script principal
if __name__ == "__main__":
    print(f"=== Test des métriques avec API Version {API_VERSION} ===")

    # Tester toutes les métriques et récupérer celles qui sont valides
    valid_metrics = get_valid_metrics(API_VERSION, POST_ID, ALL_METRICS, START_DATE, END_DATE)

    if not valid_metrics:
        print("Aucune métrique valide trouvée pour ce post.")
    else:
        print(f"Métriques valides : {valid_metrics}")

        # Récupérer les données pour les métriques valides
        print(f"\nRécupération des métriques journalières pour le post {POST_ID}...")
        data = fetch_daily_metrics(API_VERSION, POST_ID, valid_metrics, START_DATE, END_DATE)

        if data:
            print(f"Métriques récupérées avec succès pour {POST_ID}:")
            for metric in data:
                print(f"- Metric: {metric['name']} ({metric.get('title', 'No title')})")
                for value in metric.get('values', []):
                    print(f"  Date: {value.get('end_time')}, Value: {value.get('value')}")
            calculated_metrics = validate_and_calculate(data, ALL_METRICS)  # Validation et calcul
            print(f"Métriques calculées : {calculated_metrics}")
        else:
            print(f"Aucune donnée récupérée pour le post {POST_ID} avec la version {API_VERSION}.")
