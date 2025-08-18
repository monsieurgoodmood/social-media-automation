#!/usr/bin/env python3
"""
Diagnostic amélioré pour Cloud Functions avec capture complète des erreurs
"""
import os
import sys
import json
import traceback
import subprocess
from pathlib import Path

def run_diagnostic():
    """Exécute un diagnostic complet avec capture des erreurs"""
    
    print("=== DIAGNOSTIC CLOUD FUNCTIONS AMÉLIORÉ ===")
    print(f"Python: {sys.version}")
    print(f"Répertoire: {os.getcwd()}")
    print(f"K_SERVICE: {os.environ.get('K_SERVICE', 'NON DÉFINI')}")
    
    # 1. Lister TOUS les fichiers Python
    print("\n1. FICHIERS PYTHON DISPONIBLES:")
    for directory in ["/workspace", os.getcwd(), "/tmp"]:
        if os.path.exists(directory):
            print(f"\n{directory}:")
            try:
                for root, dirs, files in os.walk(directory):
                    py_files = [f for f in files if f.endswith('.py')]
                    if py_files and not root.startswith('/tmp/pip'):
                        print(f"  {root}:")
                        for f in py_files[:10]:
                            print(f"    - {f}")
            except:
                print(f"  Erreur de lecture")
    
    # 2. Vérifier spécifiquement linkedin_multi_follower_stats.py
    print("\n2. RECHERCHE DE linkedin_multi_follower_stats.py:")
    script_found = False
    for directory in ["/workspace", os.getcwd()]:
        path = os.path.join(directory, "linkedin_multi_follower_stats.py")
        if os.path.exists(path):
            script_found = True
            print(f"✅ TROUVÉ: {path}")
            print(f"   Taille: {os.path.getsize(path)} bytes")
            
            # Vérifier le contenu
            try:
                with open(path, 'r') as f:
                    lines = f.readlines()
                print(f"   Lignes: {len(lines)}")
                
                # Chercher des éléments clés
                has_ensure = any('def ensure_percentage_as_decimal' in line for line in lines)
                has_main = any('def main(' in line for line in lines)
                has_class = any('class MultiOrganizationFollowerStatsTracker' in line for line in lines)
                
                print(f"   - ensure_percentage_as_decimal: {'✅' if has_ensure else '❌'}")
                print(f"   - main(): {'✅' if has_main else '❌'}")
                print(f"   - MultiOrganizationFollowerStatsTracker: {'✅' if has_class else '❌'}")
                
            except Exception as e:
                print(f"   Erreur lecture: {e}")
    
    if not script_found:
        print("❌ Script NON TROUVÉ!")
    
    # 3. Vérifier les secrets et variables d'environnement
    print("\n3. SECRETS ET VARIABLES D'ENVIRONNEMENT:")
    
    # Tester l'accès aux secrets
    try:
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.environ.get('GCP_PROJECT', 'authentic-ether-457013-t5')
        
        print(f"Project ID: {project_id}")
        
        # Tester un secret
        try:
            secret_path = f"projects/{project_id}/secrets/community-access-token/versions/latest"
            response = client.access_secret_version(request={"name": secret_path})
            token_value = response.payload.data.decode('UTF-8')
            print(f"✅ Accès aux secrets OK (token: {len(token_value)} caractères)")
            
            # Définir la variable d'environnement si elle n'existe pas
            if 'LINKEDIN_ACCESS_TOKEN' not in os.environ:
                os.environ['LINKEDIN_ACCESS_TOKEN'] = token_value
                print("   → LINKEDIN_ACCESS_TOKEN défini depuis le secret")
                
        except Exception as e:
            print(f"❌ Erreur accès secret: {str(e)[:100]}")
            
    except ImportError:
        print("⚠️  Module secretmanager non disponible")
    
    # Variables critiques
    critical_vars = [
        'LINKEDIN_ACCESS_TOKEN',
        'GOOGLE_ADMIN_EMAIL', 
        'GOOGLE_SERVICE_ACCOUNT_JSON',
        'AUTOMATED_MODE',
        'PYTHONPATH'
    ]
    
    for var in critical_vars:
        value = os.environ.get(var)
        if value:
            if 'TOKEN' in var or 'JSON' in var:
                print(f"✅ {var}: [{len(value)} caractères]")
            else:
                print(f"✅ {var}: {value[:50]}...")
        else:
            print(f"❌ {var}: NON DÉFINI")
    
    # 4. Test d'exécution directe
    print("\n4. TEST D'EXÉCUTION PYTHON DIRECTE:")
    
    # Essayer d'importer le module
    try:
        sys.path.insert(0, '/workspace')
        sys.path.insert(0, os.getcwd())
        
        print("Import du module...")
        import linkedin_multi_follower_stats
        print("✅ Import réussi")
        
        # Essayer de créer une instance
        print("Création du tracker...")
        tracker = linkedin_multi_follower_stats.MultiOrganizationFollowerStatsTracker()
        print(f"✅ Tracker créé ({len(tracker.organizations)} orgs)")
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        traceback.print_exc()
    
    # 5. Test subprocess avec capture complète
    print("\n5. TEST SUBPROCESS:")
    
    script_path = None
    for directory in ["/workspace", os.getcwd()]:
        test_path = os.path.join(directory, "linkedin_multi_follower_stats.py")
        if os.path.exists(test_path):
            script_path = test_path
            break
    
    if script_path:
        env = os.environ.copy()
        env['AUTOMATED_MODE'] = 'true'
        
        print(f"Exécution: {sys.executable} {script_path}")
        print("Environment PYTHONPATH:", env.get('PYTHONPATH', 'Non défini'))
        
        try:
            # Test rapide avec timeout court
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                env=env,
                cwd=os.path.dirname(script_path),
                timeout=10
            )
            
            print(f"\nCode retour: {result.returncode}")
            
            if result.stdout:
                print("\nSTDOUT (premières lignes):")
                for line in result.stdout.split('\n')[:20]:
                    if line.strip():
                        print(f"  {line}")
            
            if result.stderr:
                print("\nSTDERR:")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        print(f"  {line}")
                        
        except subprocess.TimeoutExpired:
            print("⏱️  Timeout (normal si le script démarre)")
        except Exception as e:
            print(f"❌ Erreur subprocess: {e}")
            traceback.print_exc()
    
    # 6. Fichiers de configuration
    print("\n6. FICHIERS DE CONFIGURATION:")
    config_files = ['organizations_config.json', 'follower_stats_mapping.json']
    
    for filename in config_files:
        found = False
        for directory in ["/workspace", os.getcwd()]:
            path = os.path.join(directory, filename)
            if os.path.exists(path):
                try:
                    with open(path, 'r') as f:
                        data = json.load(f)
                    if isinstance(data, list):
                        print(f"✅ {filename}: {len(data)} entrées")
                    else:
                        print(f"✅ {filename}: {len(data)} clés")
                    found = True
                    break
                except:
                    print(f"⚠️  {filename}: erreur de lecture")
        
        if not found:
            print(f"❌ {filename}: NON TROUVÉ")
    
    print("\n=== FIN DU DIAGNOSTIC ===")

if __name__ == "__main__":
    run_diagnostic()