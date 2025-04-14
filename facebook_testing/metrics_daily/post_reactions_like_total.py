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
TEST_METRIC = "post_reactions_like_total"  # La métrique à tester

def test_metric_validity(api_version, post_id, metric):
    """Teste si une métrique est valide pour le post."""
    token = get_access_token()
    insights_url = f"https://graph.facebook.com/{api_version}/{post_id}/insights"
    params = {'metric': metric, 'access_token': token, 'period': 'day'}

    response = requests.get(insights_url, params=params)
    if response.status_code == 200:
        print(f"✅ La métrique '{metric}' est valide pour ce post.")
        return True
    else:
        print(f"❌ La métrique '{metric}' n'est pas valide ou une erreur est survenue : {response.status_code}.")
        return False

def fetch_single_metric(api_version, post_id, metric, start_date, end_date):
    """Récupère les données journalières pour une seule métrique."""
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
    if response.status_code == 200:
        data = response.json().get('data', [])
        if not data:
            print(f"⚠️ Aucun relevé quotidien pour la métrique {metric}.")
        else:
            print(f"✅ Données reçues pour la métrique {metric}: {data}")
        return data
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
        return None

def process_single_metric(data, metric_name):
    """Transforme les données d'une métrique en DataFrame."""
    if not data:
        return pd.DataFrame(columns=['Date', metric_name, f"{metric_name}_daily"])

    rows = []
    for entry in data:
        for value in entry.get('values', []):
            date = value.get('end_time')[:10]  # Extraire la date
            metric_value = value.get('value', 0)
            rows.append({'Date': date, metric_name: metric_value})
    
    df = pd.DataFrame(rows)
    
    # Calculer les valeurs journalières (différentielles)
    df[f"{metric_name}_daily"] = df[metric_name].diff().fillna(df[metric_name])  # La première valeur reste la même

    return df

# Test de la métrique unique
data = fetch_single_metric(API_VERSION, POST_ID, TEST_METRIC, START_DATE, END_DATE)

# Traitement des données si elles existent
if data:
    df_metric = process_single_metric(data, TEST_METRIC)
    print("\n=== Relevé quotidien avec les valeurs différentielles ===")
    print(df_metric.to_string(index=False))
else:
    print("Aucune donnée à afficher pour la métrique.")