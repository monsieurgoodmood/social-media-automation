# api_utils.py

import requests
from main.config import ACCESS_TOKEN, API_VERSION
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def fetch_insights(entity_id, metrics_list):
    """Effectue une requête pour récupérer les insights Facebook."""
    insights_url = f"https://graph.facebook.com/{API_VERSION}/{entity_id}/insights"
    params = {
        'metric': ','.join(metrics_list),
        'access_token': ACCESS_TOKEN,
    }
    response = requests.get(insights_url, params=params)
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching insights: {response.status_code} - {response.text}")


def fetch_insights_daily(entity_id, metrics_list, start_date=None):
    """Récupère les insights journaliers avec gestion de la limite de 90 jours."""
    if not start_date:
        raise ValueError("start_date est requis pour déterminer la période.")

    end_date = datetime.now()
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    insights_data = []

    while start_date <= end_date:
        day_str = start_date.strftime('%Y-%m-%d')
        period_end_date = start_date + timedelta(days=90)
        if period_end_date > end_date:
            period_end_date = end_date
        period_end_str = period_end_date.strftime('%Y-%m-%d')

        logger.info(f"Récupération des données pour la période de {day_str} à {period_end_str}.")
        params = {
            'metric': ','.join(metrics_list),
            'access_token': ACCESS_TOKEN,
            'since': day_str,
            'until': period_end_str,
            'period': 'day',
        }
        response = requests.get(f"https://graph.facebook.com/{API_VERSION}/{entity_id}/insights", params=params)

        if response.status_code == 200:
            daily_data = response.json().get('data', [])
            if daily_data:
                for entry in daily_data:
                    for value in entry.get("values", []):
                        insights_data.append({
                            "Date": value.get("end_time", day_str)[:10],
                            "name": entry.get("name"),
                            "value": value.get("value", 0)
                        })
        else:
            logger.warning(f"Erreur API pour la période {day_str} à {period_end_str}: {response.status_code} - {response.text}")

        start_date = period_end_date + timedelta(days=1)

    return insights_data


def test_metric(post_id, metric):
    """
    Vérifie si une métrique est valide pour un post spécifique.
    """
    insights_url = f"https://graph.facebook.com/{API_VERSION}/{post_id}/insights"
    params = {
        'metric': metric,
        'access_token': ACCESS_TOKEN,
        'period': 'day'
    }
    response = requests.get(insights_url, params=params)
    return response.status_code == 200
