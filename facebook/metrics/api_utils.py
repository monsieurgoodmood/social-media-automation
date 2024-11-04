# api_utils.py

import requests
from config import ACCESS_TOKEN, API_VERSION

def fetch_insights(entity_id, metrics_list):
    """Effectue une requête pour récupérer les insights Facebook en fonction des métriques spécifiées."""
    insights_url = f"https://graph.facebook.com/{API_VERSION}/{entity_id}/insights"
    
    params = {
        'metric': ','.join(metrics_list),
        'access_token': ACCESS_TOKEN
    }
    
    response = requests.get(insights_url, params=params)
    
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching insights: {response.status_code} - {response.text}")
