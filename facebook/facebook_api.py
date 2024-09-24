# facebook_api.py

import requests
from config import FACEBOOK_PAGE_ID, ACCESS_TOKEN

BASE_URL = "https://graph.facebook.com/v20.0"

def get_facebook_posts():
    """Fetch the latest posts from the Facebook page."""
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
    """Fetch insights for a specific post."""
    insights_url = f"{BASE_URL}/{post_id}/insights"
    params = {
        'metric': 'post_impressions,post_engaged_users,post_reactions_by_type_total',
        'access_token': ACCESS_TOKEN
    }
    
    response = requests.get(insights_url, params=params)
    
    if response.status_code == 200:
        return response.json().get('data', [])
    else:
        raise Exception(f"Error fetching insights: {response.status_code} - {response.text}")

def get_page_metrics():
    """Fetch page-level metrics like followers and fan count."""
    page_url = f"{BASE_URL}/{FACEBOOK_PAGE_ID}/"
    params = {
        'access_token': ACCESS_TOKEN,
        'fields': 'fan_count,followers_count'  # Get both fan_count and followers_count
    }
    
    response = requests.get(page_url, params=params)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Error fetching page metrics: {response.status_code} - {response.text}")

