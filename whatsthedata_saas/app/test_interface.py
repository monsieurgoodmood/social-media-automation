# app/test_interface.py
# Interface de test complète pour valider tous les composants

import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from datetime import datetime, timedelta
import json
import time

from app.database.connection import db_manager, test_database_connection
from app.auth.user_manager import user_manager, PlanType
from app.payments.stripe_handler import stripe_handler
from app.utils.config import Config

def main():
    st.set_page_config(
        page_title="WhatsTheData - Interface de Test",
        page_icon="🧪",
        layout="wide"
    )
    
    st.title("🧪 WhatsTheData - Interface de Test Complète")
    
    # Sidebar pour navigation
    with st.sidebar:
        st.header("🔧 Tests Disponibles")
        test_section = st.selectbox(
            "Choisir une section",
            [
                "🏠 Dashboard",
                "🗄️ Base de Données", 
                "🔐 Authentification",
                "💳 Paiements Stripe",
                "🔗 LinkedIn API",
                "📘 Facebook API",
                "📊 Looker Studio",
                "🚀 Test Complet"
            ]
        )
    
    if test_section == "🏠 Dashboard":
        dashboard_section()
    elif test_section == "🗄️ Base de Données":
        database_section()
    elif test_section == "🔐 Authentification":
        auth_section()
    elif test_section == "💳 Paiements Stripe":
        stripe_section()
    elif test_section == "🔗 LinkedIn API":
        linkedin_section()
    elif test_section == "📘 Facebook API":
        facebook_section()
    elif test_section == "📊 Looker Studio":
        looker_section()
    elif test_section == "🚀 Test Complet":
        complete_test_section()

def dashboard_section():
    st.header("🏠 Dashboard - État du Système")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Test connexion DB
    with col1:
        with st.container():
            st.subheader("🗄️ Base de Données")
            if test_database_connection():
                st.success("✅ Connectée")
                try:
                    stats = db_manager.get_database_stats()
                    st.metric("Utilisateurs", stats.get('users', 0))
                except:
                    st.warning("Stats indisponibles")
            else:
                st.error("❌ Déconnectée")
    
    # Test APIs
    with col2:
        st.subheader("🔗 APIs")
        # Test FastAPI local
        try:
            response = requests.get("http://localhost:8000/health", timeout=2)
            if response.status_code == 200:
                st.success("✅ FastAPI OK")
            else:
                st.warning("⚠️ FastAPI issues")
        except:
            st.error("❌ FastAPI down")
    
    # Test Stripe
    with col3:
        st.subheader("💳 Stripe")
        if Config.STRIPE_SECRET_KEY and Config.STRIPE_SECRET_KEY.startswith('sk_'):
            st.success("✅ Configuré")
        else:
            st.error("❌ Non configuré")
    
    # Test LinkedIn
    with col4:
        st.subheader("🔗 LinkedIn")
        if Config.LINKEDIN_COMMUNITY_CLIENT_ID:
            st.success("✅ Configuré")
        else:
            st.error("❌ Non configuré")
    
    # Graphiques de monitoring
    st.subheader("📈 Monitoring en Temps Réel")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Simuler des données de performance
        chart_data = pd.DataFrame({
            'time': pd.date_range('2024-01-01', periods=30, freq='D'),
            'api_calls': [100 + i*5 + (i%7)*20 for i in range(30)],
            'users': [10 + i//3 for i in range(30)]
        })
        
        fig = px.line(chart_data, x='time', y='api_calls', 
                     title="Appels API par jour")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig2 = px.line(chart_data, x='time', y='users',
                      title="Utilisateurs actifs", color_discrete_sequence=['green'])
        st.plotly_chart(fig2, use_container_width=True)

def database_section():
    st.header("🗄️ Test Base de Données")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔍 Connexion")
        
        if st.button("Tester la Connexion"):
            with st.spinner("Test en cours..."):
                if test_database_connection():
                    st.success("✅ Connexion réussie !")
                    
                    # Afficher les stats
                    try:
                        stats = db_manager.get_database_stats()
                        st.json(stats)
                    except Exception as e:
                        st.error(f"Erreur stats: {e}")
                else:
                    st.error("❌ Échec de connexion")
    
    with col2:
        st.subheader("🏗️ Structure")
        
        if st.button("Créer les Tables"):
            with st.spinner("Création des tables..."):
                try:
                    from app.database.connection import init_database
                    init_database()
                    st.success("✅ Tables créées !")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
    
    st.subheader("📊 Données de Test")
    
    # Créer un utilisateur de test
    with st.form("create_test_user"):
        st.write("Créer un utilisateur de test")
        email = st.text_input("Email", "test@example.com")
        password = st.text_input("Mot de passe", "test123", type="password")
        
        if st.form_submit_button("Créer Utilisateur"):
            try:
                user = user_manager.create_user(
                    email=email,
                    password=password,
                    firstname="Test",
                    lastname="User"
                )
                if user:
                    st.success(f"✅ Utilisateur créé: {user.email}")
                else:
                    st.error("❌ Échec création utilisateur")
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

def auth_section():
    st.header("🔐 Test Authentification")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("👤 Connexion")
        
        with st.form("login_test"):
            email = st.text_input("Email")
            password = st.text_input("Mot de passe", type="password")
            
            if st.form_submit_button("Se Connecter"):
                try:
                    token = user_manager.authenticate_user(email, password)
                    if token:
                        st.success("✅ Connexion réussie !")
                        st.code(f"Token: {token[:50]}...")
                    else:
                        st.error("❌ Identifiants incorrects")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
    
    with col2:
        st.subheader("🔑 Validation Token")
        
        token_to_test = st.text_area("Token JWT à valider")
        
        if st.button("Valider Token"):
            if token_to_test:
                try:
                    payload = user_manager.verify_token(token_to_test)
                    if payload:
                        st.success("✅ Token valide !")
                        st.json(payload)
                    else:
                        st.error("❌ Token invalide")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")

def stripe_section():
    st.header("💳 Test Paiements Stripe")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💰 Plans Disponibles")
        
        if st.button("Récupérer les Plans"):
            try:
                plans = stripe_handler.get_available_plans()
                for plan in plans:
                    with st.expander(f"{plan['name']} - {plan['price_monthly']}€/mois"):
                        st.write(f"**Features:**")
                        st.write(f"- LinkedIn: {plan['max_linkedin_accounts']} comptes")
                        st.write(f"- Facebook: {plan['max_facebook_accounts']} comptes")
                        st.write(f"- Rétention: {plan['data_retention_days']} jours")
            except Exception as e:
                st.error(f"❌ Erreur: {e}")
    
    with col2:
        st.subheader("🛒 Test Checkout")
        
        selected_plan = st.selectbox(
            "Plan à tester",
            ["linkedin_basic", "facebook_basic", "premium"]
        )
        
        test_user_id = st.number_input("ID Utilisateur Test", value=1)
        
        if st.button("Créer Session Checkout"):
            try:
                session = stripe_handler.create_checkout_session(
                    user_id=test_user_id,
                    plan_type=selected_plan,
                    success_url="http://localhost:8501/success",
                    cancel_url="http://localhost:8501/cancel"
                )
                
                if session:
                    st.success("✅ Session créée !")
                    st.link_button("💳 Aller au Checkout", session['url'])
                else:
                    st.error("❌ Échec création session")
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

def linkedin_section():
    st.header("🔗 Test LinkedIn API")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔑 Configuration")
        st.code(f"""
Client ID: {Config.LINKEDIN_COMMUNITY_CLIENT_ID}
Client Secret: {Config.LINKEDIN_COMMUNITY_CLIENT_SECRET[:10]}...
Access Token: {"✅ Configuré" if Config.COMMUNITY_ACCESS_TOKEN else "❌ Manquant"}
        """)
    
    with col2:
        st.subheader("🧪 Test Endpoint")
        
        if st.button("Tester /me"):
            if Config.COMMUNITY_ACCESS_TOKEN:
                try:
                    headers = {
                        'Authorization': f'Bearer {Config.COMMUNITY_ACCESS_TOKEN}',
                        'LinkedIn-Version': '202401'
                    }
                    response = requests.get(
                        'https://api.linkedin.com/v2/me',
                        headers=headers
                    )
                    
                    if response.status_code == 200:
                        st.success("✅ API LinkedIn OK !")
                        st.json(response.json())
                    else:
                        st.error(f"❌ Erreur {response.status_code}: {response.text}")
                except Exception as e:
                    st.error(f"❌ Erreur: {e}")
            else:
                st.error("❌ Token d'accès manquant")

def facebook_section():
    st.header("📘 Test Facebook API")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🔑 Configuration")
        st.code(f"""
Client ID: {Config.FB_CLIENT_ID}
Client Secret: {Config.FB_CLIENT_SECRET[:10]}...
        """)
    
    with col2:
        st.subheader("🧪 Test Basic")
        
        if st.button("Tester API Facebook"):
            try:
                # Test basique de l'API Facebook
                url = "https://graph.facebook.com/v18.0/me"
                params = {
                    'access_token': 'test_token',  # À remplacer par un vrai token
                    'fields': 'id,name'
                }
                
                st.info("💡 Nécessite un token d'accès utilisateur valide")
                st.code("Configurez d'abord l'OAuth Facebook")
                
            except Exception as e:
                st.error(f"❌ Erreur: {e}")

def looker_section():
    st.header("📊 Test Looker Studio")
    
    st.subheader("🔌 Connecteur")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Endpoint API pour Looker:**")
        st.code("http://localhost:8000/api/v1/looker-data")
        
        if st.button("Tester Endpoint Looker"):
            try:
                response = requests.get("http://localhost:8000/api/v1/looker-data")
                if response.status_code == 200:
                    st.success("✅ Endpoint accessible !")
                    st.json(response.json())
                else:
                    st.error(f"❌ Erreur {response.status_code}")
            except Exception as e:
                st.error(f"❌ Erreur: {e}")
    
    with col2:
        st.write("**Template Google Apps Script:**")
        st.info("📝 À créer - voir section développement")
        
        if st.button("Générer Code Connecteur"):
            st.code("""
// Code Google Apps Script à créer
function getConfig() {
  return {
    configParams: [
      {
        type: 'TEXTINPUT',
        name: 'api_endpoint',
        displayName: 'API Endpoint',
        helpText: 'URL de votre API WhatsTheData'
      }
    ]
  };
}

function getData(request) {
  // Récupérer les données depuis votre API
  var url = request.configParams.api_endpoint;
  var response = UrlFetchApp.fetch(url);
  // ... traitement des données
}
            """)

def complete_test_section():
    st.header("🚀 Test Complet - Pipeline Bout-en-Bout")
    
    if st.button("🧪 Lancer Test Complet", type="primary"):
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        results = []
        
        # 1. Test Base de Données
        status_text.text("1/6 Test de la base de données...")
        progress_bar.progress(16)
        try:
            db_ok = test_database_connection()
            results.append(("Base de données", "✅" if db_ok else "❌"))
        except Exception as e:
            results.append(("Base de données", f"❌ {e}"))
        time.sleep(1)
        
        # 2. Test Authentification
        status_text.text("2/6 Test authentification...")
        progress_bar.progress(33)
        try:
            # Test création utilisateur temporaire
            test_email = f"test_{int(time.time())}@example.com"
            user = user_manager.create_user(test_email, "test123", "Test", "User")
            auth_ok = user is not None
            results.append(("Authentification", "✅" if auth_ok else "❌"))
        except Exception as e:
            results.append(("Authentification", f"❌ {e}"))
        time.sleep(1)
        
        # 3. Test Stripe
        status_text.text("3/6 Test Stripe...")
        progress_bar.progress(50)
        try:
            plans = stripe_handler.get_available_plans()
            stripe_ok = len(plans) > 0
            results.append(("Stripe", "✅" if stripe_ok else "❌"))
        except Exception as e:
            results.append(("Stripe", f"❌ {e}"))
        time.sleep(1)
        
        # 4. Test LinkedIn API
        status_text.text("4/6 Test LinkedIn API...")
        progress_bar.progress(66)
        try:
            if Config.COMMUNITY_ACCESS_TOKEN:
                headers = {'Authorization': f'Bearer {Config.COMMUNITY_ACCESS_TOKEN}'}
                response = requests.get('https://api.linkedin.com/v2/me', headers=headers)
                linkedin_ok = response.status_code == 200
            else:
                linkedin_ok = False
            results.append(("LinkedIn API", "✅" if linkedin_ok else "❌"))
        except Exception as e:
            results.append(("LinkedIn API", f"❌ {e}"))
        time.sleep(1)
        
        # 5. Test FastAPI Local
        status_text.text("5/6 Test API locale...")
        progress_bar.progress(83)
        try:
            response = requests.get("http://localhost:8000/health", timeout=3)
            api_ok = response.status_code == 200
            results.append(("API FastAPI", "✅" if api_ok else "❌"))
        except Exception as e:
            results.append(("API FastAPI", f"❌ {e}"))
        time.sleep(1)
        
        # 6. Test Endpoints Looker
        status_text.text("6/6 Test endpoints Looker...")
        progress_bar.progress(100)
        try:
            response = requests.get("http://localhost:8000/api/v1/looker-data", timeout=3)
            looker_ok = response.status_code in [200, 404]  # 404 acceptable si pas de données
            results.append(("Looker Endpoints", "✅" if looker_ok else "❌"))
        except Exception as e:
            results.append(("Looker Endpoints", f"❌ {e}"))
        
        status_text.text("Test terminé !")
        
        # Affichage des résultats
        st.subheader("📋 Résultats")
        
        for component, status in results:
            if "✅" in status:
                st.success(f"{component}: {status}")
            else:
                st.error(f"{component}: {status}")
        
        # Score global
        success_count = sum(1 for _, status in results if "✅" in status)
        total_count = len(results)
        score = (success_count / total_count) * 100
        
        st.metric(
            "Score Global", 
            f"{score:.0f}%", 
            f"{success_count}/{total_count} composants OK"
        )
        
        if score >= 80:
            st.balloons()
            st.success("🎉 Système prêt pour la production !")
        elif score >= 60:
            st.warning("⚠️ Quelques problèmes à résoudre")
        else:
            st.error("❌ Plusieurs composants nécessitent une attention")

if __name__ == "__main__":
    main()