# facebook_api.py

import requests
from config import FACEBOOK_PAGE_ID, ACCESS_TOKEN, API_VERSION

BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

def get_facebook_posts():
    """Récupère les posts Facebook récents."""
    posts_url = f"{BASE_URL}/{FACEBOOK_PAGE_ID}/posts"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'id,created_time,message,attachments'
    }
    response = requests.get(posts_url, params=params)
    
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching posts: {response.status_code} - {response.text}")

def get_post_insights(post_id):
    """Récupère les insights pour un post spécifique."""
    insights_url = f"https://graph.facebook.com/v20.0/{post_id}/insights"
    params = {
        'metric': 'post_impressions_unique,post_impressions_organic,post_clicks,post_reactions_by_type_total',
        'access_token': ACCESS_TOKEN
    }
    response = requests.get(insights_url, params=params)
    
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching insights: {response.status_code} - {response.text}")

def get_page_metrics():
    """Récupère les métriques globales de la page pour différentes périodes."""
    base_url = f"https://graph.facebook.com/{FACEBOOK_PAGE_ID}/insights"
    params = {'access_token': ACCESS_TOKEN}
    
    metrics_list = ['page_views_total', 'page_views_logged_in_unique', 'page_impressions']
    periods = ['day', 'week', 'days_28']
    
    metrics = {}
    
    for metric in metrics_list:
        for period in periods:
            response = requests.get(base_url, params={**params, 'metric': metric, 'period': period})
            if response.status_code == 200:
                data = response.json().get('data', [])
                if data:
                    metrics[f'{metric}_{period}'] = data[0]['values'][0]['value']
            else:
                metrics[f'{metric}_{period}'] = 0
    
    return metrics
