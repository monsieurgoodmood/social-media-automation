#!/usr/bin/env python3
"""
Script de test local complet pour tous les scripts LinkedIn
Exécute le workflow complet : découverte des organisations + collecte de toutes les métriques
"""

import os
import sys
import json
import time
import subprocess
import traceback
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

# Configuration
SCRIPTS_DIR = Path(__file__).parent  # Répertoire où sont vos scripts
AUTOMATED_MODE = True  # Mode automatique pour éviter les confirmations

class LinkedInTestRunner:
    """Orchestrateur pour tester tous les scripts LinkedIn"""
    
    def __init__(self):
        self.scripts_dir = SCRIPTS_DIR
        self.results = {}
        self.start_time = datetime.now()
        
    def setup_environment(self):
        """Configure l'environnement pour les tests"""
        print("🔧 Configuration de l'environnement...")
        
        # Définir le mode automatique
        os.environ['AUTOMATED_MODE'] = 'true'
        
        # Vérifier les variables essentielles
        required_vars = [
            'LINKEDIN_ACCESS_TOKEN',
            'GOOGLE_ADMIN_EMAIL'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print("❌ Variables d'environnement manquantes:")
            for var in missing_vars:
                print(f"   - {var}")
            print("\nCréez un fichier .env avec ces variables ou exportez-les")
            return False
        
        # Créer le répertoire credentials si nécessaire
        creds_dir = self.scripts_dir / 'credentials'
        creds_dir.mkdir(exist_ok=True)
        
        # Vérifier le fichier de credentials Google
        creds_file = creds_dir / 'service_account_credentials.json'
        if not creds_file.exists():
            # Essayer de créer depuis la variable d'environnement
            creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            if creds_json:
                with open(creds_file, 'w') as f:
                    f.write(creds_json)
                print("✅ Fichier credentials créé depuis GOOGLE_SERVICE_ACCOUNT_JSON")
            else:
                print("⚠️  Fichier credentials manquant - Les exports Google Sheets échoueront")
        
        print("✅ Environnement configuré")
        return True
    
    def run_script(self, script_name, description):
        """Exécute un script Python et capture le résultat"""
        print(f"\n{'='*60}")
        print(f"🚀 {description}")
        print(f"Script: {script_name}")
        print(f"{'='*60}")
        
        script_path = self.scripts_dir / script_name
        
        if not script_path.exists():
            print(f"❌ Script non trouvé: {script_path}")
            self.results[script_name] = {
                'status': 'error',
                'error': 'Script non trouvé',
                'duration': 0
            }
            return False
        
        start_time = time.time()
        
        try:
            # Préparer l'environnement
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.scripts_dir)
            
            # Exécuter le script
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                env=env,
                cwd=str(self.scripts_dir)
            )
            
            duration = time.time() - start_time
            
            # Afficher la sortie
            if result.stdout:
                print("\n--- SORTIE ---")
                print(result.stdout)
            
            if result.stderr:
                print("\n--- ERREURS ---")
                print(result.stderr)
            
            # Enregistrer le résultat
            self.results[script_name] = {
                'status': 'success' if result.returncode == 0 else 'error',
                'returncode': result.returncode,
                'duration': round(duration, 2)
            }
            
            if result.returncode == 0:
                print(f"\n✅ {description} - Succès ({duration:.2f}s)")
                return True
            else:
                print(f"\n❌ {description} - Échec (code: {result.returncode})")
                return False
                
        except Exception as e:
            duration = time.time() - start_time
            print(f"\n❌ Erreur lors de l'exécution: {e}")
            traceback.print_exc()
            
            self.results[script_name] = {
                'status': 'error',
                'error': str(e),
                'duration': round(duration, 2)
            }
            return False
    
    def wait_between_scripts(self, seconds=5):
        """Pause entre les scripts pour éviter les limites d'API"""
        print(f"\n⏱️  Pause de {seconds} secondes...")
        time.sleep(seconds)
    
    def check_organizations_config(self):
        """Vérifie le fichier organizations_config.json"""
        config_file = self.scripts_dir / 'organizations_config.json'
        
        if not config_file.exists():
            print("❌ Fichier organizations_config.json non trouvé")
            return []
        
        try:
            with open(config_file, 'r') as f:
                orgs = json.load(f)
            
            print(f"\n📋 Organisations configurées: {len(orgs)}")
            for org in orgs:
                print(f"   - {org['name']} (ID: {org['id']})")
            
            return orgs
        except Exception as e:
            print(f"❌ Erreur lecture organizations_config.json: {e}")
            return []
    
    def run_complete_workflow(self):
        """Exécute le workflow complet"""
        print("\n🎯 DÉMARRAGE DU TEST COMPLET")
        print(f"Heure de début: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 1. Configuration de l'environnement
        if not self.setup_environment():
            return False
        
        # 2. Découverte des organisations
        success = self.run_script(
            'discover_organizations_auto.py',
            'Étape 1/6: Découverte des organisations'
        )
        
        if not success:
            print("\n❌ Échec de la découverte des organisations - Arrêt du test")
            return False
        
        self.wait_between_scripts(3)
        
        # 3. Vérifier les organisations découvertes
        orgs = self.check_organizations_config()
        if not orgs:
            print("\n❌ Aucune organisation trouvée - Arrêt du test")
            return False
        
        # 4. Exécuter les scripts de collecte de métriques
        scripts_to_run = [
            ('linkedin_multi_follower_stats.py', 'Étape 2/6: Statistiques des followers'),
            ('linkedin_multi_org_share_tracker.py', 'Étape 3/6: Statistiques de partage'),
            ('linkedin_multi_page_stats.py', 'Étape 4/6: Statistiques des pages'),
            ('linkedin_multi_post_metrics.py', 'Étape 5/6: Métriques des posts'),
            ('linkedin_multi_org_tracker.py', 'Étape 6/6: Statistiques quotidiennes')
        ]
        
        for script_name, description in scripts_to_run:
            self.wait_between_scripts(10)  # Pause plus longue entre les scripts
            self.run_script(script_name, description)
    
    def generate_report(self):
        """Génère un rapport des résultats"""
        print("\n" + "="*60)
        print("📊 RAPPORT DES TESTS")
        print("="*60)
        
        total_duration = (datetime.now() - self.start_time).total_seconds()
        
        # Résumé
        success_count = sum(1 for r in self.results.values() if r['status'] == 'success')
        error_count = len(self.results) - success_count
        
        print(f"\n✅ Scripts réussis: {success_count}/{len(self.results)}")
        print(f"❌ Scripts en échec: {error_count}/{len(self.results)}")
        print(f"⏱️  Durée totale: {total_duration:.2f} secondes")
        
        # Détail par script
        print("\n📋 Détail par script:")
        for script_name, result in self.results.items():
            status_icon = "✅" if result['status'] == 'success' else "❌"
            print(f"\n{status_icon} {script_name}")
            print(f"   - Status: {result['status']}")
            print(f"   - Durée: {result['duration']}s")
            if 'error' in result:
                print(f"   - Erreur: {result['error']}")
        
        # Fichiers créés
        print("\n📁 Fichiers de mapping créés:")
        mapping_files = [
            'organizations_config.json',
            'follower_stats_mapping.json',
            'share_stats_mapping.json',
            'page_stats_mapping.json',
            'post_metrics_mapping.json',
            'sheet_mapping.json'
        ]
        
        for filename in mapping_files:
            filepath = self.scripts_dir / filename
            if filepath.exists():
                size = filepath.stat().st_size
                print(f"   ✅ {filename} ({size} bytes)")
            else:
                print(f"   ❌ {filename} (non créé)")
        
        # Google Sheets créés
        print("\n📊 Google Sheets créés:")
        sheet_mapping_file = self.scripts_dir / 'sheet_mapping.json'
        if sheet_mapping_file.exists():
            try:
                with open(sheet_mapping_file, 'r') as f:
                    mapping = json.load(f)
                
                for org_id, info in mapping.items():
                    if 'sheet_url' in info:
                        print(f"   - {info['org_name']}: {info['sheet_url']}")
            except Exception as e:
                print(f"   ❌ Erreur lecture sheet_mapping.json: {e}")

def main():
    """Fonction principale"""
    print("🧪 TEST LOCAL COMPLET DES SCRIPTS LINKEDIN")
    print("==========================================")
    
    # Vérifier qu'on est dans le bon répertoire
    required_scripts = [
        'discover_organizations_auto.py',
        'linkedin_multi_follower_stats.py',
        'linkedin_multi_org_share_tracker.py',
        'linkedin_multi_page_stats.py',
        'linkedin_multi_post_metrics.py',
        'linkedin_multi_org_tracker.py'
    ]
    
    missing_scripts = []
    for script in required_scripts:
        if not (SCRIPTS_DIR / script).exists():
            missing_scripts.append(script)
    
    if missing_scripts:
        print("\n❌ Scripts manquants dans le répertoire courant:")
        for script in missing_scripts:
            print(f"   - {script}")
        print("\nAssurez-vous d'être dans le bon répertoire")
        sys.exit(1)
    
    # Créer et exécuter le test runner
    runner = LinkedInTestRunner()
    
    try:
        # Exécuter le workflow complet
        runner.run_complete_workflow()
        
        # Générer le rapport
        runner.generate_report()
        
        # Afficher les prochaines étapes
        print("\n📝 PROCHAINES ÉTAPES:")
        print("1. Vérifiez les Google Sheets créés")
        print("2. Configurez Looker Studio avec ces sheets")
        print("3. Planifiez l'exécution automatique dans Cloud Functions")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrompu par l'utilisateur")
        runner.generate_report()
    except Exception as e:
        print(f"\n\n❌ Erreur fatale: {e}")
        traceback.print_exc()
        runner.generate_report()

if __name__ == "__main__":
    main()