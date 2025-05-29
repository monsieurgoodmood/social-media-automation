# main.py - Point d'entrée pour Cloud Functions

import os
import json
import logging
import subprocess
import sys
from datetime import datetime
from google.cloud import secretmanager
from google.cloud import storage
import functions_framework
import tempfile
import shutil

# Configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'authentic-ether-457013-t5')
CONFIG_BUCKET = f"{PROJECT_ID}-config"

# Importer le wrapper
sys.path.insert(0, '/workspace')
from cloud_wrapper import execute_script

def setup_environment():
    """Configure l'environnement avec les secrets"""
    print("Configuration de l'environnement...")
    
    secrets_client = secretmanager.SecretManagerServiceClient()
    
    # Mapping des secrets
    secrets_mapping = {
        'LINKEDIN_CLIENT_ID': 'community-client-id',
        'LINKEDIN_CLIENT_SECRET': 'community-client-secret',
        'LINKEDIN_ACCESS_TOKEN': 'community-access-token',
        'LINKEDIN_REFRESH_TOKEN': 'community-refresh-token',
        'COMMUNITY_LINKEDIN_TOKEN': 'community-access-token',
        'PORTABILITY_LINKEDIN_TOKEN': 'portability-access-token',
        'PORTABILITY_CLIENT_ID': 'portability-client-id',
        'PORTABILITY_CLIENT_SECRET': 'portability-client-secret',
        'PORTABILITY_ACCESS_TOKEN': 'portability-access-token',
        'GOOGLE_ADMIN_EMAIL': 'google-admin-email',
        'GOOGLE_SERVICE_ACCOUNT_JSON': 'google-service-account-json'
    }
    
    # Charger tous les secrets
    for env_var, secret_name in secrets_mapping.items():
        try:
            secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
            response = secrets_client.access_secret_version(name=secret_path)
            value = response.payload.data.decode('UTF-8')
            os.environ[env_var] = value
            print(f"✓ Secret {secret_name} chargé")
        except Exception as e:
            print(f"⚠ Impossible de charger {secret_name}: {e}")
    
    # Créer le fichier credentials
    if 'GOOGLE_SERVICE_ACCOUNT_JSON' in os.environ:
        # Créer le dossier credentials dans le workspace
        creds_dir = '/workspace/credentials'
        os.makedirs(creds_dir, exist_ok=True)
        
        creds_path = f'{creds_dir}/service_account_credentials.json'
        with open(creds_path, 'w') as f:
            f.write(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
        print(f"✓ Fichier credentials créé dans {creds_path}")

def download_config_files():
    """Télécharge les fichiers de configuration depuis Cloud Storage"""
    print("Téléchargement des fichiers de configuration...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(CONFIG_BUCKET)
    
    # Liste des fichiers à télécharger
    files = [
        'organizations_config.json',
        'follower_stats_mapping.json',
        'page_stats_mapping.json', 
        'post_metrics_mapping.json',
        'share_stats_mapping.json',
        'sheet_mapping.json'
    ]
    
    # Télécharger dans le workspace
    workspace_dir = '/workspace'
    for filename in files:
        try:
            blob = bucket.blob(filename)
            if blob.exists():
                filepath = f'{workspace_dir}/{filename}'
                blob.download_to_filename(filepath)
                print(f"✓ {filename} téléchargé")
        except Exception as e:
            print(f"⚠ Erreur téléchargement {filename}: {e}")

def upload_mapping_files():
    """Upload les fichiers de mapping mis à jour vers Cloud Storage"""
    print("Upload des fichiers de mapping...")
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(CONFIG_BUCKET)
    
    # Liste des fichiers de mapping
    mapping_files = [
        'follower_stats_mapping.json',
        'page_stats_mapping.json',
        'post_metrics_mapping.json',
        'share_stats_mapping.json',
        'sheet_mapping.json',
        'organizations_config.json'  # Ajouter aussi organizations_config au cas où il a été mis à jour
    ]
    
    workspace_dir = '/workspace'
    for filename in mapping_files:
        filepath = f'{workspace_dir}/{filename}'
        if os.path.exists(filepath):
            try:
                blob = bucket.blob(filename)
                blob.upload_from_filename(filepath)
                print(f"✓ {filename} uploadé")
            except Exception as e:
                print(f"⚠ Erreur upload {filename}: {e}")

@functions_framework.http
def run_linkedin_analytics(request):
    """Point d'entrée principal pour Cloud Functions"""
    
    # Récupérer le script à exécuter
    request_json = request.get_json(silent=True)
    script_name = 'discover_organizations'  # Par défaut
    
    if request_json and 'script' in request_json:
        script_name = request_json['script']
    
    print(f"Demande d'exécution du script: {script_name}")
    
    # Configuration de l'environnement
    setup_environment()
    
    # Télécharger les fichiers de configuration
    download_config_files()
    
    # Définir le mode automatique
    os.environ['AUTOMATED_MODE'] = 'true'
    
    # Changer le répertoire de travail vers workspace
    os.chdir('/workspace')
    
    # Scripts disponibles
    available_scripts = [
        'discover_organizations',
        'follower_statistics',
        'share_statistics',
        'page_statistics',
        'post_metrics',
        'daily_statistics'
    ]
    
    # Exécuter le script demandé
    if script_name in available_scripts:
        try:
            success = execute_script(script_name)
            
            # Uploader les fichiers de mapping mis à jour
            if success:
                upload_mapping_files()
            
            return {
                'status': 'success' if success else 'error',
                'script': script_name,
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            print(f"Erreur lors de l'exécution: {str(e)}")
            return {
                'status': 'error',
                'script': script_name,
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            }
    else:
        return {
            'status': 'error',
            'message': f'Script inconnu: {script_name}',
            'available_scripts': available_scripts,
            'timestamp': datetime.now().isoformat()
        }

# Pour les tests locaux
if __name__ == "__main__":
    # Simuler une requête
    class MockRequest:
        def get_json(self, silent=False):
            return {'script': 'discover_organizations'}
    
    result = run_linkedin_analytics(MockRequest())
    print(json.dumps(result, indent=2))
