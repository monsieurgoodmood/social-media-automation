import requests
from config import dma_client_id, dma_client_secret, redirect_uri

def get_access_token(auth_code):
    url = "https://www.linkedin.com/oauth/v2/accessToken"
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "client_id": dma_client_id,
        "client_secret": dma_client_secret,
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]

def refresh_access_token(refresh_token):
    url = "https://www.linkedin.com/oauth/v2/accessToken"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": dma_client_id,
        "client_secret": dma_client_secret,
    }
    response = requests.post(url, data=data)
    response.raise_for_status()
    return response.json()
