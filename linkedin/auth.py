# auth.py

import os
import json
import requests
import time
from urllib.parse import urlencode
from config import dma_client_id, dma_client_secret, redirect_uri

TOKEN_FILE = "data/tokens.json"

def save_token(token_data):
    os.makedirs(os.path.dirname(TOKEN_FILE), exist_ok=True)
    with open(TOKEN_FILE, "w") as file:
        json.dump(token_data, file)

def load_token():
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as file:
            return json.load(file)
    return None

def is_token_valid(token_data):
    """
    Check if the token is still valid based on its expiration time.
    """
    return "expires_at" in token_data and token_data["expires_at"] > time.time()

def get_new_access_token():
    """
    Obtain a new access token by prompting the user for an authorization code.
    """
    auth_code = get_authorization_code()
    return fetch_access_token(auth_code)

def get_authorization_code():
    """
    Generate an authorization URL and prompt the user to authorize the application.
    """
    params = {
        "response_type": "code",
        "client_id": dma_client_id,
        "redirect_uri": redirect_uri,
        "scope": "r_dma_admin_pages_content",
    }
    auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(params)}"
    print(f"Opening browser for authorization: {auth_url}")
    import webbrowser
    webbrowser.open(auth_url)
    return input("Paste the authorization code from the browser here: ")

def fetch_access_token(auth_code):
    """
    Exchange the authorization code for an access token.
    """
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
    token_data = response.json()
    token_data["expires_at"] = time.time() + token_data["expires_in"]
    save_token(token_data)
    return token_data

def get_valid_access_token():
    """
    Load a valid access token, or fetch a new one if the current token is invalid or expired.
    """
    token_data = load_token()
    if token_data and is_token_valid(token_data):
        return token_data["access_token"]

    print("Access token is invalid or expired. Generating a new one...")
    token_data = get_new_access_token()
    return token_data["access_token"]


