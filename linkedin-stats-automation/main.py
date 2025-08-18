import os
import json
import logging
import subprocess
import sys
import traceback
import importlib.util
from datetime import datetime
from pathlib import Path
import functions_framework
from google.cloud import secretmanager, storage

# Configuration
PROJECT_ID = os.environ.get('GCP_PROJECT', 'authentic-ether-457013-t5')
CONFIG_BUCKET = f"{PROJECT_ID}-config"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def ensure_percentage_as_decimal(value):
    """Convertit une valeur en décimal pour Google Sheets PERCENT"""
    if value is None:
        return 0.0
    
    if isinstance(value, str):
        value = value.replace('%', '').strip()
        try:
            value = float(value)
        except:
            return 0.0
    
    if isinstance(value, (int, float)):
        if value > 1:
            return float(value / 100)
        else:
            return float(value)
    
    return 0.0

def setup_environment():
    """Configure l'environnement avec les secrets"""
    logger.info("Configuration de l'environnement...")
    
    # Créer les répertoires nécessaires
    os.makedirs('/tmp/credentials', exist_ok=True)
    
    try:
        # Importer les secrets
        secrets_client = secretmanager.SecretManagerServiceClient()
        
        secrets_mapping = {
            'LINKEDIN_CLIENT_ID': 'community-client-id',
            'LINKEDIN_CLIENT_SECRET': 'community-client-secret',
            'LINKEDIN_ACCESS_TOKEN': 'community-access-token',
            'LINKEDIN_REFRESH_TOKEN': 'community-refresh-token',
            'COMMUNITY_LINKEDIN_TOKEN': 'community-access-token',
            'PORTABILITY_LINKEDIN_TOKEN': 'portability-access-token',
            'GOOGLE_ADMIN_EMAIL': 'google-admin-email',
            'GOOGLE_SERVICE_ACCOUNT_JSON': 'google-service-account-json'
        }
        
        for env_var, secret_name in secrets_mapping.items():
            if env_var not in os.environ:
                try:
                    secret_path = f"projects/{PROJECT_ID}/secrets/{secret_name}/versions/latest"
                    response = secrets_client.access_secret_version(request={"name": secret_path})
                    value = response.payload.data.decode('UTF-8')
                    os.environ[env_var] = value
                    logger.info(f"✓ Secret {secret_name} chargé")
                except Exception as e:
                    logger.warning(f"⚠ Impossible de charger {secret_name}: {str(e)[:100]}")
        
        # Créer le fichier de credentials dans /tmp ET dans le répertoire de travail
        if 'GOOGLE_SERVICE_ACCOUNT_JSON' in os.environ:
            # Dans /tmp pour les scripts patchés
            creds_path_tmp = '/tmp/credentials/service_account_credentials.json'
            with open(creds_path_tmp, 'w') as f:
                f.write(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
            
            # Dans le répertoire de travail pour les scripts originaux
            os.makedirs('/workspace/credentials', exist_ok=True)
            creds_path_workspace = '/workspace/credentials/service_account_credentials.json'
            with open(creds_path_workspace, 'w') as f:
                f.write(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
                
            logger.info(f"✓ Fichiers credentials créés")
            
    except Exception as e:
        logger.error(f"Erreur configuration environnement: {e}")
        return False
    
    # Variables d'environnement importantes
    os.environ['AUTOMATED_MODE'] = 'true'
    os.environ['LINKEDIN_SORT_SHEET_DATA'] = 'False'
    os.environ['PYTHONPATH'] = '/workspace:/tmp'
    
    logger.info("DEBUG: download_config_files appelé")
    return True

def download_config_files():
    """Télécharge les fichiers de configuration depuis Cloud Storage"""
    logger.info("Téléchargement des fichiers de configuration...")
    
    try:
        logger.info(f"DEBUG: Tentative accès bucket {CONFIG_BUCKET}")
        storage_client = storage.Client()
        bucket = storage_client.bucket(CONFIG_BUCKET)
        
        files = [
            'organizations_config.json',
            'follower_stats_mapping.json',
            'page_stats_mapping.json',
            'post_metrics_mapping.json',
            'share_stats_mapping.json',
            'sheet_mapping.json',
            'batch_state.json'
        ]
        
        for filename in files:
            try:
                blob = bucket.blob(filename)
                logger.info(f"DEBUG: Blob {filename} existe: {blob.exists()}")
                
                # Télécharger dans les deux emplacements
                for directory in ['/tmp', '/workspace']:
                    filepath = os.path.join(directory, filename)
                    
                    if blob.exists():
                        blob.download_to_filename(filepath)
                        logger.info(f"✓ {filename} téléchargé dans {directory}")
                    else:
                        # Créer un fichier par défaut
                        if filename == 'organizations_config.json':
                            with open(filepath, 'w') as f:
                                json.dump([], f)
                        elif filename != 'batch_state.json':
                            with open(filepath, 'w') as f:
                                json.dump({}, f)
                        logger.info(f"✓ {filename} créé par défaut dans {directory}")
                    
            except Exception as e:
                logger.warning(f"⚠ Erreur téléchargement {filename}: {e}")
                
    except Exception as e:
        logger.error(f"Erreur accès Storage: {e}")

def upload_config_files():
    """Upload les fichiers de configuration vers Cloud Storage"""
    logger.info("Upload des fichiers de configuration...")
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(CONFIG_BUCKET)
        
        files = [
            'organizations_config.json',
            'follower_stats_mapping.json',
            'page_stats_mapping.json',
            'post_metrics_mapping.json',
            'share_stats_mapping.json',
            'sheet_mapping.json',
            'batch_state.json'
        ]
        
        for filename in files:
            # Essayer depuis /workspace d'abord, puis /tmp
            uploaded = False
            for directory in ['/workspace', '/tmp']:
                filepath = os.path.join(directory, filename)
                if os.path.exists(filepath):
                    blob = bucket.blob(filename)
                    blob.upload_from_filename(filepath)
                    logger.info(f"✓ {filename} uploadé depuis {directory}")
                    uploaded = True
                    break
            
            if not uploaded:
                logger.warning(f"⚠ {filename} non trouvé pour upload")
                
    except Exception as e:
        logger.error(f"Erreur upload: {e}")

def execute_batch_script(script_name, force_run=True):
    """Exécute un script en mode batch avec force_run=True par défaut"""
    logger.info(f"Exécution en mode BATCH pour {script_name} (force_run={force_run})")
    
    # Ajouter les deux chemins au PYTHONPATH
    sys.path.insert(0, '/workspace')
    sys.path.insert(0, '/tmp')
    
    try:
        # Utiliser directement la fonction execute_batch_script du batch_processor
        # qui gère déjà toute la logique de batch et les configurations
        from batch_processor import execute_batch_script as run_batch
        
        # Exécuter le script avec force_run passé en paramètre
        success = run_batch(script_name, force_run=force_run)
        
        # Upload l'état après chaque batch
        upload_config_files()
        
        # Retourner le résultat au format attendu
        return {
            'status': 'success' if success else 'error',
            'script': script_name,
            'batch_executed': True,
            'force_run': force_run
        }
        
    except Exception as e:
        logger.error(f"Erreur batch processing: {e}")
        traceback.print_exc()
        return {'status': 'error', 'error': str(e)}

def execute_direct_script(script_name):
    """Exécute un script directement"""
    script_mapping = {
        'discover_organizations': 'discover_organizations_auto.py',
        'diagnostic': 'diagnostic.py'
    }
    
    script_file = script_mapping.get(script_name)
    if not script_file:
        raise ValueError(f"Script inconnu: {script_name}")
    
    # Essayer depuis /workspace d'abord
    script_path = f'/workspace/{script_file}'
    if not os.path.exists(script_path):
        script_path = f'/tmp/{script_file}'
    
    logger.info(f"Exécution de {script_file} depuis {script_path}...")
    
    env = os.environ.copy()
    env['PYTHONPATH'] = '/workspace:/tmp'
    env['AUTOMATED_MODE'] = 'true'
    
    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.path.dirname(script_path),
            timeout=300
        )
        
        if result.stdout:
            logger.info(f"STDOUT: {result.stdout[-2000:]}")  # Derniers 2000 caractères
            
        if result.stderr:
            logger.error(f"STDERR: {result.stderr[-2000:]}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("Timeout dépassé")
        return False
    except Exception as e:
        logger.error(f"Erreur exécution: {e}")
        traceback.print_exc()
        return False

@functions_framework.http
def run_linkedin_analytics(request):
    """Point d'entrée principal pour Cloud Functions"""
    
    start_time = datetime.now()
    logger.info("=== DÉBUT DE L'EXÉCUTION ===")
    
    try:
        # Récupérer le script à exécuter
        request_json = request.get_json(silent=True)
        script_name = request_json.get('script', 'diagnostic') if request_json else 'diagnostic'
        force_run = request_json.get('force_run', True) if request_json else True  # TRUE par défaut
            
        logger.info(f"Script demandé: {script_name}")
        logger.info(f"Force run: {force_run}")
        
        # Configuration de l'environnement
        if not setup_environment():
            return {'status': 'error', 'error': 'Configuration échouée'}

        # Fix pour les erreurs de partage
        os.environ['GOOGLE_ADMIN_EMAIL'] = 'byteberry.analytics@gmail.com'
        logger.info("✅ Email admin corrigé")
        
        # Télécharger les fichiers de configuration
        download_config_files()
        
        # Debug: Afficher le contenu des mappings
        try:
            with open("/tmp/follower_stats_mapping.json", "r") as f:
                mapping = json.load(f)
            logger.info(f"DEBUG: Mapping follower_stats - {len(mapping)} organisations")
            for org_id, data in mapping.items():
                logger.info(f"  - Org {org_id}: {data.get('org_name', 'N/A')} -> {data.get('sheet_id', 'NO_ID')}")
        except Exception as e:
            logger.info(f"DEBUG: Erreur lecture mapping: {e}")
        
        # Scripts disponibles
        batch_scripts = ['page_statistics', 'follower_statistics', 'share_statistics', 
                        'post_metrics', 'daily_statistics']
        direct_scripts = ['discover_organizations', 'diagnostic']
        
        # Exécuter le script approprié
        if script_name in batch_scripts:
            # Traitement unifié pour tous les scripts batch avec force_run
            result = execute_batch_script(script_name, force_run=force_run)
            success = result.get('status') == 'success'
        elif script_name in direct_scripts:
            success = execute_direct_script(script_name)
            result = {'status': 'success' if success else 'error'}
        else:
            return {
                'status': 'error',
                'error': f'Script inconnu: {script_name}',
                'available_scripts': batch_scripts + direct_scripts
            }
        
        # Upload des fichiers modifiés
        if script_name != 'diagnostic':
            upload_config_files()
        
        # Calculer la durée
        duration = (datetime.now() - start_time).total_seconds()
        
        response = {
            'status': 'success' if success else 'error',
            'script': script_name,
            'duration_seconds': round(duration, 2),
            'timestamp': datetime.now().isoformat()
        }
        
        if isinstance(result, dict):
            response.update(result)
        
        return response
        
    except Exception as e:
        logger.error(f"ERREUR GLOBALE: {str(e)}")
        traceback.print_exc()
        
        return {
            'status': 'error',
            'script': script_name if 'script_name' in locals() else 'unknown',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

# Pour tests locaux
if __name__ == "__main__":
    import sys
    script = sys.argv[1] if len(sys.argv) > 1 else 'diagnostic'
    
    class MockRequest:
        def get_json(self, silent=False):
            return {'script': script}
    
    result = run_linkedin_analytics(MockRequest())
    print(json.dumps(result, indent=2))