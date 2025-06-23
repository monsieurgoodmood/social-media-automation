#!/usr/bin/env python3
"""
Script pour mettre à jour fb_base_collector.py et config_manager.py
pour utiliser les Application Default Credentials
"""

import os
import re

def fix_fb_base_collector():
    """Corrige fb_base_collector.py pour utiliser ADC"""
    file_path = "scripts/fb_base_collector.py"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Remplacer la logique de credentials
    old_pattern = r"""# Initialiser les services Google
        credentials_path = os.getenv\("GOOGLE_APPLICATION_CREDENTIALS", "/workspace/credentials/service_account_credentials.json"\)
        
        if os.path.exists\(credentials_path\):
            credentials = service_account.Credentials.from_service_account_file\(
                credentials_path,
                scopes=\['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'\]
            \)
        else:
            credentials = None"""
    
    new_code = """# Initialiser les services Google
        try:
            # Essayer d'abord les Application Default Credentials
            from google.auth import default
            credentials, project = default(
                scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
            )
            logger.info("Utilisation des Application Default Credentials")
        except:
            # Fallback vers le fichier si disponible
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/workspace/credentials/service_account_credentials.json")
            if os.path.exists(credentials_path):
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info(f"Utilisation des credentials depuis {credentials_path}")
            else:
                # Dernier recours - essayer sans credentials explicites
                credentials = None
                logger.warning("Aucune credential trouvée, utilisation des credentials implicites")"""
    
    # Remplacer dans le contenu
    content = re.sub(re.escape(old_pattern.strip()), new_code.strip(), content, flags=re.DOTALL)
    
    # Si le remplacement direct ne marche pas, chercher une version simplifiée
    if "Application Default Credentials" not in content:
        # Chercher juste la partie credentials
        start_marker = "# Initialiser les services Google"
        end_marker = "self.drive_service = build"
        
        start_idx = content.find(start_marker)
        end_idx = content.find(end_marker)
        
        if start_idx != -1 and end_idx != -1:
            before = content[:start_idx]
            after = content[end_idx:]
            content = before + new_code + "\n        \n        " + after
    
    # Ajouter l'import si nécessaire
    if "from google.auth import default" not in content:
        # Ajouter après les autres imports google
        content = content.replace(
            "from google.oauth2 import service_account",
            "from google.oauth2 import service_account\nfrom google.auth import default"
        )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✓ Mis à jour: {file_path}")

def fix_config_manager():
    """Corrige config_manager.py pour utiliser ADC"""
    file_path = "utils/config_manager.py"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Ajouter l'import pour le logging
    if "import logging" not in content:
        content = content.replace(
            "from datetime import datetime",
            "from datetime import datetime\nimport logging"
        )
    
    # Modifier le constructeur pour utiliser ADC
    old_init = """def __init__(self):
        self.bucket_name = f"{os.getenv('GCP_PROJECT_ID', 'linkedin-analytics-auto')}-fb-configs"
        self.storage_client = storage.Client()
        self._ensure_bucket_exists()"""
    
    new_init = """def __init__(self):
        self.bucket_name = f"{os.getenv('GCP_PROJECT_ID', 'authentic-ether-457013-t5')}-facebook-configs"
        try:
            # Utiliser les Application Default Credentials
            self.storage_client = storage.Client()
        except Exception as e:
            logging.warning(f"Impossible d'initialiser le client Storage: {e}")
            # Essayer avec le projet explicite
            project_id = os.getenv('GCP_PROJECT_ID', 'authentic-ether-457013-t5')
            self.storage_client = storage.Client(project=project_id)
        self._ensure_bucket_exists()"""
    
    content = content.replace(old_init.strip(), new_init.strip())
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✓ Mis à jour: {file_path}")

def fix_token_manager():
    """Corrige token_manager.py pour utiliser ADC"""
    file_path = "utils/token_manager.py"
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Modifier l'initialisation du client Secret Manager
    old_pattern = "self.client = secretmanager.SecretManagerServiceClient()"
    new_pattern = """try:
            self.client = secretmanager.SecretManagerServiceClient()
        except Exception as e:
            logger.warning(f"Erreur init Secret Manager avec ADC: {e}")
            # Essayer avec les credentials par défaut
            from google.auth import default
            credentials, project = default()
            self.client = secretmanager.SecretManagerServiceClient(credentials=credentials)"""
    
    content = content.replace(old_pattern, new_pattern)
    
    # Corriger aussi le project_id par défaut
    content = content.replace(
        'os.getenv("GCP_PROJECT_ID", "linkedin-analytics-auto")',
        'os.getenv("GCP_PROJECT_ID", "authentic-ether-457013-t5")'
    )
    
    with open(file_path, 'w') as f:
        f.write(content)
    
    print(f"✓ Mis à jour: {file_path}")

if __name__ == "__main__":
    print("🔧 Correction des fichiers pour utiliser Application Default Credentials...")
    
    try:
        fix_fb_base_collector()
        fix_config_manager()
        fix_token_manager()
        print("\n✅ Tous les fichiers ont été mis à jour !")
        print("\nProchaine étape : Redéployer la fonction avec les fichiers corrigés")
    except Exception as e:
        print(f"\n❌ Erreur : {e}")
        import traceback
        traceback.print_exc()