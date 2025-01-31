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
TEST_METRIC = "post_impressions"  # La métrique à tester
PERIODS = ['day', 'week', 'month', 'lifetime']  # Périodes disponibles, de la plus détaillée à la plus globale

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
            print(f"⚠️ La métrique '{metric}' est valide, mais aucune donnée n'est disponible pour la période '{period}'.")
            return None
        else:
            print(f"✅ Données reçues pour la métrique '{metric}' avec la période '{period}': {data}")
            return data
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
    return None


def process_metric_data(data, metric_name, period, start_date=None, end_date=None):
    """Transforme les données récupérées en DataFrame."""
    if period == "lifetime":
        # Si lifetime, créer une ligne unique
        if data:
            value = data[0]['values'][0]['value']
            return pd.DataFrame([{metric_name: value, 'period': 'lifetime'}])
        else:
            return pd.DataFrame(columns=[metric_name, 'period'])
    else:
        # Si données temporelles
        date_range = pd.date_range(start=start_date, end=end_date)
        df = pd.DataFrame({'Date': date_range.strftime('%Y-%m-%d')})

        if data:
            rows = []
            for entry in data:
                for value in entry.get('values', []):
                    date = value.get('end_time')[:10]
                    metric_value = value.get('value', 0)
                    rows.append({'Date': date, metric_name: metric_value})

            df_data = pd.DataFrame(rows)
            df = df.merge(df_data, on='Date', how='left').fillna(0)
        else:
            df[metric_name] = 0

        # Calcul différentiel (daily values)
        df[f"{metric_name}_daily"] = df[metric_name].diff().fillna(df[metric_name])
        return df


def fetch_best_available_metric(api_version, post_id, metric, start_date, end_date):
    """Teste les périodes de la plus détaillée à la plus globale pour trouver les données disponibles."""
    for period in PERIODS:
        print(f"\n=== Test de la métrique '{metric}' avec la période '{period}' ===")
        data = fetch_metric_with_period(api_version, post_id, metric, period, start_date, end_date)
        if data:
            df = process_metric_data(data, metric, period, start_date, end_date)
            print(f"\n=== Données disponibles pour la période '{period}' ===")
            print(df.to_string(index=False))
            return df

    print(f"⚠️ Aucune donnée disponible pour la métrique '{metric}' sur toutes les périodes.")
    return None


# Exécution
df_results = fetch_best_available_metric(API_VERSION, POST_ID, TEST_METRIC, START_DATE, END_DATE)

if df_results is not None:
    print("\n=== Résultats finaux ===")
    print(df_results.to_string(index=False))
else:
    print("Aucune donnée à afficher.")