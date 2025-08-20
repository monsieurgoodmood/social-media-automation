#!/usr/bin/env python3
"""
Test simple des collecteurs sans OAuth
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Charger l'environnement
load_dotenv()

def test_config_only():
    """Tester juste la configuration sans OAuth"""
    print("🧪 Test configuration des collecteurs")
    print("-" * 40)
    
    try:
        from app.utils.config import get_env_var
        print("✅ Configuration utils OK")
        
        # Tester les variables essentielles
        linkedin_token = get_env_var('LINKEDIN_ACCESS_TOKEN')
        facebook_id = get_env_var('FB_CLIENT_ID')
        stripe_key = get_env_var('STRIPE_SECRET_KEY')
        
        print(f"✅ LinkedIn token: {'✓' if linkedin_token else '✗'}")
        print(f"✅ Facebook ID: {'✓' if facebook_id else '✗'}")
        print(f"✅ Stripe key: {'✓' if stripe_key else '✗'}")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur config: {e}")
        return False

def test_database_models():
    """Tester les modèles de base de données"""
    print("\n🧪 Test modèles base de données")
    print("-" * 40)
    
    try:
        from app.database.models import User, LinkedinAccount, FacebookAccount
        print("✅ Modèles de base OK")
        
        from app.database.connection import db_manager
        print("✅ Database manager OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur modèles DB: {e}")
        return False

def test_apis_directly():
    """Tester les APIs LinkedIn et Facebook directement"""
    print("\n🧪 Test APIs directement")
    print("-" * 40)
    
    import requests
    
    # Test LinkedIn API
    linkedin_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
    if linkedin_token:
        try:
            headers = {
                'Authorization': f'Bearer {linkedin_token}',
                'LinkedIn-Version': '202408'
            }
            response = requests.get('https://api.linkedin.com/rest/me', headers=headers, timeout=10)
            print(f"✅ LinkedIn API: {response.status_code}")
        except Exception as e:
            print(f"❌ LinkedIn API: {e}")
    else:
        print("⚠️  Pas de token LinkedIn")
    
    # Test Facebook API  
    facebook_id = os.getenv('FB_CLIENT_ID')
    if facebook_id:
        try:
            response = requests.get('https://graph.facebook.com/v21.0/me', 
                                  params={'access_token': 'test'}, timeout=5)
            print(f"✅ Facebook API: {response.status_code} (normal si 400)")
        except Exception as e:
            print(f"❌ Facebook API: {e}")
    else:
        print("⚠️  Pas d'ID Facebook")

def test_simple_fastapi():
    """Créer une API FastAPI simple pour tester"""
    print("\n🧪 Test FastAPI simple")
    print("-" * 40)
    
    try:
        from fastapi import FastAPI
        app = FastAPI(title="WhatsTheData Test")
        
        @app.get("/")
        def root():
            return {"status": "OK", "message": "WhatsTheData API Test"}
        
        @app.get("/test-config")
        def test_config():
            return {
                "linkedin_configured": bool(os.getenv('LINKEDIN_ACCESS_TOKEN')),
                "facebook_configured": bool(os.getenv('FB_CLIENT_ID')),
                "stripe_configured": bool(os.getenv('STRIPE_SECRET_KEY')),
                "database_url": bool(os.getenv('DATABASE_URL'))
            }
        
        print("✅ FastAPI app créée")
        print("🚀 Pour la lancer: uvicorn test_collectors_simple:app --reload")
        
        return app
        
    except Exception as e:
        print(f"❌ Erreur FastAPI: {e}")
        return None

def main():
    """Test principal"""
    print("🚀 WHATSTHEDATA - TEST COLLECTEURS SIMPLE")
    print("=" * 50)
    
    success = True
    success &= test_config_only()
    success &= test_database_models()
    
    test_apis_directly()
    app = test_simple_fastapi()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 TESTS DE BASE RÉUSSIS !")
        print("\n📋 Prochaines étapes :")
        print("1. Lancer l'API test : uvicorn test_collectors_simple:app --reload")
        print("2. Aller sur http://localhost:8000/test-config")
        print("3. Si OK → corriger le bug OAuth dans linkedin_oauth.py")
    else:
        print("❌ CERTAINS TESTS ONT ÉCHOUÉ")
        print("Corrigez les erreurs avant de continuer")

# Export pour uvicorn
def create_app():
    """Créer l'app FastAPI"""
    from fastapi import FastAPI
    
    app = FastAPI(title="WhatsTheData Test")
    
    @app.get("/")
    def root():
        return {"status": "OK", "message": "WhatsTheData API Test"}
    
    @app.get("/test-config")
    def test_config():
        return {
            "linkedin_configured": bool(os.getenv('LINKEDIN_ACCESS_TOKEN')),
            "facebook_configured": bool(os.getenv('FB_CLIENT_ID')),
            "stripe_configured": bool(os.getenv('STRIPE_SECRET_KEY')),
            "database_url": bool(os.getenv('DATABASE_URL'))
        }
    
    return app

app = create_app()

if __name__ == "__main__":
    main()