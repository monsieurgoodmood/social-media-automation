import requests
import sys
import os

# Ajouter le dossier racine au chemin Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main.facebook_api import get_access_token

# Configuration
POST_ID = "814743708622327_941948824609475"
API_VERSION = "v21.0"

def fetch_post_type(api_version, post_id):
    """Récupère et retourne le type de post à partir des attachements."""
    token = get_access_token()
    post_url = f"https://graph.facebook.com/{api_version}/{post_id}"
    params = {
        'fields': 'attachments',
        'access_token': token
    }

    response = requests.get(post_url, params=params)
    if response.status_code == 200:
        data = response.json()
        attachments = data.get('attachments', {}).get('data', [])
        if attachments:
            post_type = attachments[0].get('type', 'unknown')
            return post_type
        else:
            return "unknown"  # Aucun attachement détecté
    else:
        print(f"❌ Erreur API {response.status_code}: {response.text}")
        return None

if __name__ == "__main__":
    print(f"=== Détection du type de post (ID: {POST_ID}) ===")
    post_type = fetch_post_type(API_VERSION, POST_ID)

    if post_type:
        print(f"✅ Type de post détecté : {post_type}")
    else:
        print("❌ Impossible de détecter le type de post.")
