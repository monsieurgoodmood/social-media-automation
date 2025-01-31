# facebook_api.py

import requests
from .config import ACCESS_TOKEN, API_VERSION, FACEBOOK_PAGE_ID, CLIENT_ID, CLIENT_SECRET
from metrics import PAGE_METRICS, POST_METRICS
import logging

logger = logging.getLogger(__name__)

BASE_URL = f"https://graph.facebook.com/{API_VERSION}"

def refresh_access_token():
    """Rafraîchit le token d'accès pour obtenir un nouveau token longue durée et le sauvegarde."""
    refresh_url = f"https://graph.facebook.com/{API_VERSION}/oauth/access_token"
    params = {
        'grant_type': 'fb_exchange_token',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'fb_exchange_token': ACCESS_TOKEN
    }
    
    response = requests.get(refresh_url, params=params)
    if response.status_code == 200:
        data = response.json()
        new_access_token = data.get('access_token', None)
        if new_access_token:
            print("Nouveau token obtenu: ", new_access_token)
            with open('access_token.txt', 'w') as token_file:
                token_file.write(new_access_token)
            return new_access_token
    else:
        raise Exception(f"Erreur lors du rafraîchissement du token: {response.status_code} - {response.text}")

def get_access_token():
    """Récupère le token d'accès, en le rafraîchissant si nécessaire."""
    try:
        with open('access_token.txt', 'r') as token_file:
            token = token_file.read().strip()
    except FileNotFoundError:
        token = ACCESS_TOKEN
        token = refresh_access_token()
    return token

def get_facebook_posts():
    """Récupère les posts Facebook récents."""
    token = get_access_token()
    posts_url = f"{BASE_URL}/{FACEBOOK_PAGE_ID}/posts"
    params = {'access_token': token, 'fields': 'id,created_time,message'}
    response = requests.get(posts_url, params=params)

    if response.status_code == 200:
        return response.json().get('data', [])
    elif response.status_code == 401:
        token = refresh_access_token()
        return get_facebook_posts()
    else:
        raise Exception(f"Error fetching posts: {response.status_code} - {response.text}")
    
def get_page_likes():
    """Vérifie si la page a plus de 100 likes."""
    token = get_access_token()
    url = f"https://graph.facebook.com/{FACEBOOK_PAGE_ID}?fields=fan_count&access_token={token}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        likes_count = data.get('fan_count', 0)
        if likes_count >= 100:
            logger.info(f"Page a {likes_count} likes.")
            return True
        else:
            logger.warning(f"La page a seulement {likes_count} likes, les insights ne seront pas récupérés.")
            return False
    else:
        logger.error(f"Erreur lors de la récupération des likes de la page : {response.status_code}")
        return False
    

def get_post_insights(post_id):
    """Récupère les insights pour un post spécifique."""
    token = get_access_token()
    insights_url = f"{BASE_URL}/{post_id}/insights"
    params = {'metric': ','.join(POST_METRICS), 'access_token': token}
    response = requests.get(insights_url, params=params)
    
    if response.status_code == 200:
        insights_data = response.json().get('data', [])
        metrics = {}
        
        for metric in insights_data:
            metric_name = metric['name']
            metric_value = metric['values'][0]['value']
            metrics[metric_name] = metric_value
            logger.info(f"Métrique récupérée pour le post {post_id}: {metric_name} = {metric_value}")
        
        # Vérification des métriques manquantes
        for metric in POST_METRICS:
            if metric not in metrics:
                logger.warning(f"Métrique {metric} manquante pour le post {post_id}.")
        
        return metrics
    elif response.status_code == 401:
        token = refresh_access_token()
        return get_post_insights(post_id)
    else:
        raise Exception(f"Error fetching insights: {response.status_code} - {response.text}")
    
def get_page_insights():
    """Récupère les métriques globales de la page."""
    token = get_access_token()
    base_url = f"{BASE_URL}/{FACEBOOK_PAGE_ID}/insights"
    params = {'access_token': token}
    metrics = {}

    fan_follower_url = f"{BASE_URL}/{FACEBOOK_PAGE_ID}"
    fan_follower_params = {'access_token': token, 'fields': 'fan_count,followers_count'}
    try:
        fan_follower_response = requests.get(fan_follower_url, params=fan_follower_params)
        fan_follower_response.raise_for_status()
        fan_follower_data = fan_follower_response.json()
        metrics['fan_count'] = fan_follower_data.get('fan_count', 'N/A')
        metrics['followers_count'] = fan_follower_data.get('followers_count', 'N/A')
    except requests.exceptions.RequestException as e:
        metrics['fan_count'] = 'N/A'
        metrics['followers_count'] = 'N/A'

    for metric in PAGE_METRICS:
        for period in ['day', 'week', 'days_28']:
            try:
                response = requests.get(base_url, params={**params, 'metric': metric, 'period': period})
                response.raise_for_status()
                data = response.json().get('data', [])
                if data:
                    metrics[f'{metric}_{period}'] = data[0]['values'][0]['value']
            except requests.exceptions.RequestException as e:
                metrics[f'{metric}_{period}'] = 'N/A'

    return metrics