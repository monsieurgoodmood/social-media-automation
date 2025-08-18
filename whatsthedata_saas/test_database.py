#!/usr/bin/env python3
# test_database.py
# ================
# 🧪 PRIORITÉ 3 - Script de test pour valider votre base et API
# Exécutez ceci AVANT de créer le connecteur Looker Studio

import os
import sys
from dotenv import load_dotenv
import requests
import json
from datetime import datetime, timedelta

# Charger les variables d'environnement
load_dotenv()

# Ajouter le dossier app au path pour les imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

def test_database_connection():
    """Test 1: Connexion à la base de données"""
    print("🧪 Test 1: Connexion à la base de données")
    print("-" * 50)
    
    try:
        from app.database.connection import init_database, check_user_data
        
        # Initialiser la base
        init_database()
        
        # Tester avec l'utilisateur ID 1
        user_data = check_user_data(1)
        
        if "error" in user_data:
            print(f"❌ Erreur utilisateur: {user_data['error']}")
            return False
        
        print(f"✅ Utilisateur trouvé: {user_data['user']['email']}")
        print(f"📊 Plan: {user_data['user']['plan_type']}")
        print(f"🔗 Comptes LinkedIn: {len(user_data['connected_accounts']['linkedin'])}")
        print(f"🔗 Comptes Facebook: {len(user_data['connected_accounts']['facebook'])}")
        print(f"📈 Posts LinkedIn: {user_data['data_counts']['linkedin_posts']}")
        print(f"📈 Posts Facebook: {user_data['data_counts']['facebook_posts']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur de connexion: {e}")
        return False

def test_api_endpoints():
    """Test 2: API endpoints pour Looker Studio"""
    print("\n🧪 Test 2: API Endpoints")
    print("-" * 50)
    
    try:
        from app.api.looker_endpoints import get_user_accessible_platforms, get_linkedin_page_metrics, get_facebook_page_metrics
        from datetime import date
        
        # Test permissions utilisateur
        platforms = get_user_accessible_platforms(1)
        print(f"✅ Plateformes accessibles: {platforms}")
        
        # Test données LinkedIn (7 derniers jours)
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        
        if "linkedin" in platforms:
            linkedin_data = get_linkedin_page_metrics(1, start_date, end_date)
            print(f"✅ Données LinkedIn récupérées: {len(linkedin_data)} enregistrements")
            
            if linkedin_data:
                sample = linkedin_data[0]
                print(f"   📊 Échantillon: {sample['page_name']} - {sample['followers_total']} followers")
        
        if "facebook" in platforms:
            facebook_data = get_facebook_page_metrics(1, start_date, end_date)
            print(f"✅ Données Facebook récupérées: {len(facebook_data)} enregistrements")
            
            if facebook_data:
                sample = facebook_data[0]
                print(f"   📊 Échantillon: {sample['page_name']} - {sample['followers_total']} fans")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur API: {e}")
        return False

def test_fastapi_server():
    """Test 3: Serveur FastAPI (si en cours d'exécution)"""
    print("\n🧪 Test 3: Serveur FastAPI")
    print("-" * 50)
    
    base_url = "http://localhost:8000"  # Adaptez selon votre config
    
    try:
        # Test de base
        response = requests.get(f"{base_url}/api/v1/test-connection", timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Serveur FastAPI accessible")
            print(f"📊 Tables en base: {len(data.get('table_counts', {}))}")
            
            # Test échantillon de données
            sample_response = requests.get(f"{base_url}/api/v1/sample-data", timeout=5)
            if sample_response.status_code == 200:
                sample_data = sample_response.json()
                total_records = sample_data.get('total_records', 0)
                print(f"✅ Données échantillon: {total_records} enregistrements")
            
            return True
        else:
            print(f"❌ Serveur répond avec erreur: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("⚠️ Serveur FastAPI non accessible (normal si pas encore lancé)")
        print("👉 Pour le lancer: uvicorn app.main:app --reload")
        return False
    except Exception as e:
        print(f"❌ Erreur serveur: {e}")
        return False

def test_looker_api_simulation():
    """Test 4: Simulation d'appel Looker Studio"""
    print("\n🧪 Test 4: Simulation appel Looker Studio")
    print("-" * 50)
    
    try:
        from app.api.looker_endpoints import validate_api_token, get_linkedin_page_metrics, get_facebook_page_metrics
        from datetime import date
        
        # Simulation token API
        fake_token = "test_token_1234567890_looker_studio"
        
        try:
            user_info = validate_api_token(fake_token)
            print(f"✅ Token validé: user_id={user_info['user_id']}")
        except Exception as e:
            print(f"❌ Validation token échouée: {e}")
            return False
        
        # Simulation requête Looker
        looker_request = {
            "platforms": ["linkedin", "facebook"],
            "dateRange": "7",
            "metricsType": "overview",
            "startDate": (date.today() - timedelta(days=7)).isoformat(),
            "endDate": date.today().isoformat(),
            "fields": ["platform", "date", "page_name", "followers_total", "impressions", "engagement_rate"]
        }
        
        print("✅ Requête Looker simulée:")
        print(f"   🗓️ Période: {looker_request['startDate']} à {looker_request['endDate']}")
        print(f"   📊 Plateformes: {looker_request['platforms']}")
        print(f"   📈 Type: {looker_request['metricsType']}")
        
        # Test avec données réelles
        end_date = date.today()
        start_date = end_date - timedelta(days=7)
        
        linkedin_data = get_linkedin_page_metrics(1, start_date, end_date)
        facebook_data = get_facebook_page_metrics(1, start_date, end_date)
        
        total_records = len(linkedin_data) + len(facebook_data)
        print(f"✅ Données pour Looker: {total_records} enregistrements")
        
        # Simulation réponse API
        api_response = {
            "success": True,
            "data": linkedin_data + facebook_data,
            "total_records": total_records,
            "platforms_returned": ["linkedin", "facebook"]
        }
        
        print(f"✅ Réponse API prête pour Looker Studio")
        print(f"   📊 Enregistrements: {api_response['total_records']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur simulation Looker: {e}")
        return False

def generate_looker_config():
    """Test 5: Génère la configuration pour Looker Studio"""
    print("\n🧪 Test 5: Configuration Looker Studio")
    print("-" * 50)
    
    try:
        # URL de votre API (à adapter)
        api_base_url = os.getenv('API_BASE_URL', 'https://votre-api.herokuapp.com')
        
        looker_config = {
            "name": "WhatsTheData - Social Media Analytics",
            "description": "Connectez vos pages LinkedIn et Facebook pour visualiser toutes vos métriques",
            "api_endpoints": {
                "validate_token": f"{api_base_url}/api/v1/validate-token",
                "get_data": f"{api_base_url}/api/v1/looker-data"
            },
            "authentication": "Bearer Token (API Key)",
            "supported_platforms": ["linkedin", "facebook"],
            "data_types": ["page_metrics", "post_metrics", "overview"],
            "fields_available": [
                "platform", "date", "page_id", "page_name", "content_type",
                "followers_total", "followers_gained", "followers_lost",
                "impressions", "unique_impressions", "page_views",
                "total_engagement", "likes", "comments", "shares", "clicks",
                "engagement_rate", "click_through_rate",
                "reactions_positive", "reactions_negative",
                "video_views", "video_complete_views"
            ]
        }
        
        print("✅ Configuration Looker Studio générée:")
        print(f"   🌐 API Base URL: {looker_config['api_endpoints']['get_data']}")
        print(f"   🔐 Auth: {looker_config['authentication']}")
        print(f"   📊 Plateformes: {looker_config['supported_platforms']}")
        print(f"   📈 Champs: {len(looker_config['fields_available'])} disponibles")
        
        # Sauvegarder la config
        with open('looker_studio_config.json', 'w') as f:
            json.dump(looker_config, f, indent=2)
        
        print("✅ Configuration sauvée dans: looker_studio_config.json")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur configuration: {e}")
        return False

def main():
    """Exécute tous les tests dans l'ordre"""
    print("🚀 Tests de validation WhatsTheData")
    print("=" * 60)
    
    tests = [
        ("Base de données", test_database_connection),
        ("API Endpoints", test_api_endpoints),
        ("Serveur FastAPI", test_fastapi_server),
        ("Simulation Looker", test_looker_api_simulation),
        ("Config Looker", generate_looker_config)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ Erreur dans {test_name}: {e}")
            results.append((test_name, False))
    
    # Résumé final
    print("\n" + "=" * 60)
    print("📊 RÉSUMÉ DES TESTS")
    print("=" * 60)
    
    passed = 0
    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")
        if success:
            passed += 1
    
    print(f"\n🎯 Résultat: {passed}/{len(tests)} tests réussis")
    
    if passed >= 4:  # Au moins 4 tests sur 5
        print("\n🎉 PRÊT POUR LOOKER STUDIO!")
        print("👉 Prochaines étapes:")
        print("   1. Lancer votre serveur: uvicorn app.main:app --reload")
        print("   2. Déployer sur Heroku/Railway")
        print("   3. Créer le connecteur Google Apps Script")
        print("   4. Tester dans Looker Studio")
    else:
        print("\n⚠️ CORRECTIONS NÉCESSAIRES")
        print("👉 Corrigez les erreurs avant de continuer")
    
    return passed >= 4

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)