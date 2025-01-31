# sheet_python_test.py

import requests
from datetime import datetime
import pandas as pd
from pandas import date_range
from facebook_api import get_access_token
from google_sheets import upload_to_google_sheets as upload_to_gsheet

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
    """
    Récupère les métriques journalières d'un post entre start_date et end_date.
    """
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {
        'metric': ','.join(metrics),
        'access_token': token,
        'period': 'day',
        'since': start_date,
        'until': end_date
    }
    response = requests.get(insights_url, params=params)

    if response.status_code == 200:
        data = response.json().get('data', [])
        if not data:
            print("⚠️ Aucune donnée reçue, vérifiez les paramètres ou la portée de l'API.")
        else:
            print(f"✅ Données journalières reçues : {data}")
        return data
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
        return None

# Agréger les données journalières
def aggregate_daily_data(data):
    """
    Transforme les données API en DataFrame avec une colonne par métrique, calculée par jour.
    """
    rows = []
    for metric in data:
        metric_name = metric['name']
        for value in metric['values']:
            rows.append({
                'Date': value.get('end_time')[:10],
                'name': metric_name,
                'value': value.get('value', 0)
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print("⚠️ Aucune donnée valide après transformation.")
        return pd.DataFrame(columns=["Date"] + ALL_METRICS)

    df_pivot = df.pivot(index="Date", columns="name", values="value").fillna(0)

    df_pivot["Total_Views"] = df_pivot.get("post_video_views", 0)
    df_pivot["Total_Reactions"] = (
        df_pivot.get("post_reactions_like_total", 0) +
        df_pivot.get("post_reactions_love_total", 0) +
        df_pivot.get("post_reactions_wow_total", 0) +
        df_pivot.get("post_reactions_haha_total", 0)
    )
    df_pivot["Total_Interactions"] = (
        df_pivot["Total_Reactions"] +
        df_pivot.get("post_shares", 0) +
        df_pivot.get("post_clicks", 0)
    )

    return df_pivot.reset_index()

# Normaliser les données
def normalize_metrics(df, expected_metrics):
    existing_columns = df.columns.tolist()
    for metric in expected_metrics:
        if metric not in existing_columns:
            df[metric] = 0
    ordered_columns = ["Date"] + expected_metrics + ["Total_Views", "Total_Reactions", "Total_Interactions"]
    return df.reindex(columns=ordered_columns, fill_value=0)

# Préparer les données finales
def prepare_full_dataset(data, start_date, end_date, metrics):
    all_dates = pd.DataFrame({'Date': date_range(start_date, end_date).strftime('%Y-%m-%d')})
    df = aggregate_daily_data(data)
    df_full = pd.merge(all_dates, df, on="Date", how="left")
    df_full = normalize_metrics(df_full, metrics)
    return df_full.fillna(0)

# Script principal
if __name__ == "__main__":
    print(f"=== Test des métriques avec API Version {API_VERSION} ===")

    valid_metrics = get_valid_metrics(API_VERSION, POST_ID, ALL_METRICS, START_DATE, END_DATE)

    if not valid_metrics:
        print("❌ Aucune métrique valide trouvée.")
    else:
        print(f"Métriques valides : {valid_metrics}")
        data = fetch_daily_metrics(API_VERSION, POST_ID, valid_metrics, START_DATE, END_DATE)

        if data:
            daily_data = prepare_full_dataset(data, START_DATE, END_DATE, valid_metrics)
            print("✅ Données journalières prêtes :")
            print(daily_data)

            # Upload vers Google Sheets
            try:
                sheet_name = "Facebook Post Insights"
                tab_name = f"Post_{POST_ID}_Daily"
                link = upload_to_gsheet(daily_data, sheet_name, tab_name)
                print(f"✅ Données accessibles ici : {link}")
            except Exception as e:
                print(f"❌ Erreur lors de l'upload Google Sheets : {e}")
        else:
            print("❌ Aucune donnée journalière récupérée.")
