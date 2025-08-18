#!/usr/bin/env python3
"""
Script pour vérifier que tous les fichiers nécessaires sont présents avant le déploiement
"""

import os
import sys
from pathlib import Path

def check_deployment_files():
    """Vérifie la présence de tous les fichiers nécessaires"""
    
    print("🔍 VÉRIFICATION DES FICHIERS POUR LE DÉPLOIEMENT")
    print("=" * 60)
    
    # Fichiers requis
    required_files = {
        'Scripts principaux': [
            'discover_organizations_auto.py',
            'linkedin_multi_follower_stats.py',
            'linkedin_multi_org_share_tracker.py',
            'linkedin_multi_page_stats.py',
            'linkedin_multi_post_metrics.py',
            'linkedin_multi_org_tracker.py',
            'batch_processor.py'
        ],
        'Configuration': [
            'main.py',
            'requirements.txt'
        ],
        'Optionnel': [
            '.gcloudignore',
            'diagnostic.py'
        ]
    }
    
    # Fichiers à exclure
    excluded_files = [
        'follower_statistics.py',  # Brouillon
        'env_vars.yaml',          # Variables dans Secret Manager
        '.env',                   # Variables locales
        'test_local.py',          # Tests locaux
        'automate_linkedin.py'    # Script wrapper local
    ]
    
    all_good = True
    missing_required = []
    
    # Vérifier les fichiers requis
    for category, files in required_files.items():
        print(f"\n📂 {category}:")
        
        for file in files:
            if os.path.exists(file):
                size = os.path.getsize(file)
                print(f"   ✅ {file} ({size:,} bytes)")
            else:
                if category != 'Optionnel':
                    print(f"   ❌ {file} - MANQUANT!")
                    missing_required.append(file)
                    all_good = False
                else:
                    print(f"   ⚠️  {file} - Optionnel, non présent")
    
    # Vérifier les fichiers à exclure
    print("\n🚫 Fichiers à exclure:")
    excluded_present = []
    
    for file in excluded_files:
        if os.path.exists(file):
            print(f"   ⚠️  {file} - Présent (sera ignoré)")
            excluded_present.append(file)
        else:
            print(f"   ✅ {file} - Non présent")
    
    # Vérifier le contenu de requirements.txt
    print("\n📋 Contenu de requirements.txt:")
    if os.path.exists('requirements.txt'):
        with open('requirements.txt', 'r') as f:
            requirements = f.read().strip().split('\n')
        
        required_packages = [
            'functions-framework',
            'google-cloud-secret-manager',
            'google-cloud-storage',
            'gspread',
            'oauth2client',
            'requests',
            'python-dotenv',
            'pytz',
            'python-dateutil'
        ]
        
        for package in required_packages:
            found = any(package in req for req in requirements)
            if found:
                print(f"   ✅ {package}")
            else:
                print(f"   ❌ {package} - MANQUANT!")
                all_good = False
    
    # Créer .gcloudignore si nécessaire
    if not os.path.exists('.gcloudignore'):
        print("\n📝 Création de .gcloudignore...")
        
        gcloudignore_content = """# Fichiers à ignorer lors du déploiement
.env
*.pyc
__pycache__/
.git/
.gitignore
*.md
test_*.py
automate_linkedin.py
follower_statistics.py
env_vars.yaml
credentials/
*.log
*.bak
.DS_Store
"""
        
        with open('.gcloudignore', 'w') as f:
            f.write(gcloudignore_content)
        
        print("   ✅ .gcloudignore créé")
    
    # Résumé
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ:")
    
    if missing_required:
        print(f"❌ Fichiers manquants: {len(missing_required)}")
        for file in missing_required:
            print(f"   - {file}")
        print("\n⚠️  Ces fichiers doivent être présents avant le déploiement!")
    
    if excluded_present:
        print(f"\n⚠️  Fichiers à exclure présents: {len(excluded_present)}")
        print("   Ces fichiers seront ignorés grâce à .gcloudignore")
    
    if all_good:
        print("\n✅ Tous les fichiers requis sont présents!")
        print("   Vous pouvez lancer le déploiement avec:")
        print("   gcloud functions deploy linkedin-analytics ...")
        return True
    else:
        print("\n❌ Des fichiers requis sont manquants!")
        print("   Corrigez les problèmes avant de déployer.")
        return False

def create_minimal_requirements():
    """Crée un requirements.txt minimal si nécessaire"""
    requirements_content = """functions-framework==3.*
google-cloud-secret-manager==2.16.*
google-cloud-storage==2.10.*
gspread==5.12.*
oauth2client==4.1.*
requests==2.31.*
python-dotenv==1.0.*
pytz==2023.3
python-dateutil==2.8.*
"""
    
    if not os.path.exists('requirements.txt'):
        with open('requirements.txt', 'w') as f:
            f.write(requirements_content)
        print("✅ requirements.txt créé")

if __name__ == "__main__":
    # Vérifier les fichiers
    ready = check_deployment_files()
    
    # Si requirements.txt manque, proposer de le créer
    if not os.path.exists('requirements.txt'):
        response = input("\nCréer requirements.txt avec les dépendances minimales? (o/n): ")
        if response.lower() == 'o':
            create_minimal_requirements()
            ready = check_deployment_files()
    
    sys.exit(0 if ready else 1)