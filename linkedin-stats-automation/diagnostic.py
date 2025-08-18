#!/usr/bin/env python3
"""diagnostic.py - Script de diagnostic amélioré pour Cloud Functions"""

import os
import sys
import json
import traceback
import subprocess
from pathlib import Path

def run_diagnostic():
    """Exécute un diagnostic complet de l'environnement"""
    
    print("=== DIAGNOSTIC CLOUD FUNCTIONS V2 ===")
    print(f"Date: {os.popen('date').read().strip()}")
    
    # 1. Environnement de base
    print("\n1. ENVIRONNEMENT DE BASE")
    print(f"Version Python: {sys.version}")
    print(f"Répertoire de travail: {os.getcwd()}")
    print(f"Utilisateur: {os.environ.get('USER', 'inconnu')}")
    print(f"Service Cloud Run: {os.environ.get('K_SERVICE', 'non défini')}")
    
    # 2. Variables d'environnement critiques
    print("\n2. VARIABLES D'ENVIRONNEMENT CRITIQUES")
    critical_vars = [
        'AUTOMATED_MODE',
        'GCP_PROJECT',
        'LINKEDIN_ACCESS_TOKEN',
        'COMMUNITY_LINKEDIN_TOKEN',
        'PORTABILITY_LINKEDIN_TOKEN',
        'GOOGLE_ADMIN_EMAIL',
        'GOOGLE_SERVICE_ACCOUNT_JSON'
    ]
    
    for var in critical_vars:
        value = os.environ.get(var, 'NON DÉFINI')
        if 'TOKEN' in var and value != 'NON DÉFINI':
            # Masquer les tokens
            value = value[:10] + '...' + value[-10:] if len(value) > 20 else 'MASQUÉ'
        elif var == 'GOOGLE_SERVICE_ACCOUNT_JSON' and value != 'NON DÉFINI':
            # Vérifier si c'est du JSON valide
            try:
                json.loads(value)
                value = 'JSON VALIDE (masqué)'
            except:
                value = 'JSON INVALIDE'
        print(f"{var}: {value}")
    
    # 3. Structure des fichiers
    print("\n3. STRUCTURE DES FICHIERS")
    
    # Lister les répertoires importants
    directories = [
        os.getcwd(),
        '/workspace',
        '/tmp',
        os.path.join(os.getcwd(), 'credentials'),
        '/tmp/credentials'
    ]
    
    for directory in directories:
        print(f"\n{directory}:")
        if os.path.exists(directory):
            try:
                files = os.listdir(directory)
                print(f"  Nombre de fichiers: {len(files)}")
                # Afficher les fichiers Python et JSON
                for f in sorted(files)[:20]:
                    filepath = os.path.join(directory, f)
                    if os.path.isfile(filepath):
                        size = os.path.getsize(filepath)
                        if f.endswith('.py') or f.endswith('.json'):
                            print(f"  - {f} ({size} bytes)")
                    else:
                        print(f"  - {f}/ (répertoire)")
            except Exception as e:
                print(f"  Erreur: {e}")
        else:
            print("  N'EXISTE PAS")
    
    # 4. Scripts LinkedIn disponibles
    print("\n4. SCRIPTS LINKEDIN DISPONIBLES")
    scripts = [
        'discover_organizations.py',
        'linkedin_multi_follower_stats.py',
        'linkedin_multi_org_share_tracker.py',
        'linkedin_multi_page_stats.py',
        'linkedin_multi_post_metrics.py',
        'linkedin_multi_org_tracker.py'
    ]
    
    for script in scripts:
        script_path = os.path.join(os.getcwd(), script)
        if os.path.exists(script_path):
            size = os.path.getsize(script_path)
            print(f"✓ {script} ({size} bytes)")
            # Vérifier si importable
            try:
                module_name = script.replace('.py', '')
                spec = __import__(module_name)
                if hasattr(spec, 'main'):
                    print(f"  → Fonction main() trouvée")
                else:
                    print(f"  → Pas de fonction main()")
            except Exception as e:
                print(f"  → Erreur d'import: {str(e)[:50]}")
        else:
            print(f"✗ {script} - MANQUANT")
    
    # 5. Fichiers de configuration
    print("\n5. FICHIERS DE CONFIGURATION")
    config_files = [
        'organizations_config.json',
        'follower_stats_mapping.json',
        'page_stats_mapping.json',
        'post_metrics_mapping.json',
        'share_stats_mapping.json',
        'sheet_mapping.json'
    ]
    
    for filename in config_files:
        filepath = os.path.join(os.getcwd(), filename)
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                if isinstance(data, list):
                    print(f"✓ {filename} - {len(data)} entrées")
                elif isinstance(data, dict):
                    print(f"✓ {filename} - {len(data)} clés")
            except Exception as e:
                print(f"✗ {filename} - Erreur: {e}")
        else:
            print(f"✗ {filename} - N'EXISTE PAS")
    
    # 6. Modules Python
    print("\n6. MODULES PYTHON CRITIQUES")
    modules_to_check = [
        'gspread',
        'google.cloud.secretmanager',
        'google.cloud.storage',
        'requests',
        'oauth2client',
        'functions_framework'
    ]
    
    for module in modules_to_check:
        try:
            __import__(module.split('.')[0])
            print(f"✓ {module}")
        except ImportError as e:
            print(f"✗ {module} - {e}")
    
    # 7. Test d'accès aux secrets
    print("\n7. TEST D'ACCÈS AUX SECRETS")
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.environ.get('GCP_PROJECT', 'authentic-ether-457013-t5')
        
        # Tester l'accès à un secret
        secret_name = f"projects/{project_id}/secrets/google-admin-email/versions/latest"
        try:
            response = client.access_secret_version(request={"name": secret_name})
            print("✓ Accès aux secrets fonctionnel")
        except Exception as e:
            print(f"✗ Erreur d'accès aux secrets: {str(e)[:100]}")
    except Exception as e:
        print(f"✗ Module secretmanager non disponible: {e}")
    
    # 8. Test d'accès au storage
    print("\n8. TEST D'ACCÈS AU STORAGE")
    try:
        from google.cloud import storage
        storage_client = storage.Client()
        bucket_name = f"{project_id}-config"
        
        try:
            bucket = storage_client.bucket(bucket_name)
            if bucket.exists():
                print(f"✓ Bucket {bucket_name} accessible")
            else:
                print(f"✗ Bucket {bucket_name} n'existe pas")
        except Exception as e:
            print(f"✗ Erreur d'accès au bucket: {str(e)[:100]}")
    except Exception as e:
        print(f"✗ Module storage non disponible: {e}")
    
    print("\n=== FIN DU DIAGNOSTIC ===")

if __name__ == "__main__":
    run_diagnostic()