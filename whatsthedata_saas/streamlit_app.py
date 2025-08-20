#!/usr/bin/env python3
"""
WhatsTheData - Interface Streamlit simple
"""

import streamlit as st
import os
from dotenv import load_dotenv
import requests

# Charger l'environnement
load_dotenv()

# Configuration de la page
st.set_page_config(
    page_title="WhatsTheData - Social Media Analytics",
    page_icon="📊",
    layout="wide"
)

# CSS simple
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #0077B5 0%, #1877F2 100%);
        color: white;
        padding: 2rem;
        border-radius: 10px;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

def main():
    """Interface principale"""
    
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>📊 WhatsTheData</h1>
        <p>Analytics automatisés LinkedIn & Facebook pour Looker Studio</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Status de l'API
    st.header("🔧 Status du système")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Configuration")
        
        # Vérifier les variables d'environnement
        linkedin_ok = bool(os.getenv('LINKEDIN_ACCESS_TOKEN'))
        facebook_ok = bool(os.getenv('FB_CLIENT_ID'))
        stripe_ok = bool(os.getenv('STRIPE_SECRET_KEY'))
        
        st.write("✅ LinkedIn:" if linkedin_ok else "❌ LinkedIn:", "Configuré" if linkedin_ok else "Non configuré")
        st.write("✅ Facebook:" if facebook_ok else "❌ Facebook:", "Configuré" if facebook_ok else "Non configuré")
        st.write("✅ Stripe:" if stripe_ok else "❌ Stripe:", "Configuré" if stripe_ok else "Non configuré")
    
    with col2:
        st.subheader("API Backend")
        
        # Tester l'API FastAPI
        try:
            response = requests.get("http://localhost:8000/", timeout=3)
            if response.status_code == 200:
                st.write("✅ API FastAPI: En ligne")
                st.write(f"📡 Port: 8000")
            else:
                st.write(f"⚠️ API FastAPI: Status {response.status_code}")
        except:
            st.write("❌ API FastAPI: Hors ligne")
            st.write("💡 Lancez: `uvicorn app.main:app --reload`")
    
    # Section démo
    st.header("🚀 Fonctionnalités")
    
    tab1, tab2, tab3 = st.tabs(["LinkedIn", "Facebook", "Looker Studio"])
    
    with tab1:
        st.subheader("📈 Analytics LinkedIn")
        st.write("• Métriques de pages et posts")
        st.write("• Segmentation des followers")
        st.write("• Analyse des réactions")
        st.write("• Export vers Looker Studio")
        
        if st.button("Tester API LinkedIn"):
            linkedin_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
            if linkedin_token:
                try:
                    headers = {'Authorization': f'Bearer {linkedin_token}'}
                    response = requests.get('https://api.linkedin.com/rest/me', headers=headers, timeout=10)
                    st.success(f"✅ LinkedIn API: Status {response.status_code}")
                except Exception as e:
                    st.error(f"❌ Erreur LinkedIn: {e}")
            else:
                st.error("❌ Token LinkedIn manquant")
    
    with tab2:
        st.subheader("📘 Analytics Facebook")
        st.write("• Métriques de pages Facebook")
        st.write("• Insights détaillés des posts")
        st.write("• Analyse des réactions")
        st.write("• Export vers Looker Studio")
        
        if st.button("Tester API Facebook"):
            fb_id = os.getenv('FB_CLIENT_ID')
            if fb_id:
                try:
                    response = requests.get('https://graph.facebook.com/v21.0/me', 
                                          params={'access_token': 'test'}, timeout=5)
                    st.success(f"✅ Facebook API accessible (Status: {response.status_code})")
                except Exception as e:
                    st.error(f"❌ Erreur Facebook: {e}")
            else:
                st.error("❌ ID Facebook manquant")
    
    with tab3:
        st.subheader("📊 Connecteur Looker Studio")
        st.write("• Connecteur Google Apps Script")
        st.write("• Templates pré-configurés")
        st.write("• Dashboards automatiques")
        st.write("• Métriques en temps réel")
        
        st.code("""
        // Endpoint du connecteur
        https://script.google.com/macros/d/YOUR_SCRIPT_ID/exec
        
        // API de données
        http://localhost:8000/api/v1/looker-data
        """)
    
    # Section abonnements
    st.header("💳 Plans & Tarifs")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 🆓 Gratuit
        - 1 page LinkedIn ou Facebook
        - Métriques de base
        - 30 jours d'historique
        
        **0€/mois**
        """)
    
    with col2:
        st.markdown("""
        ### 📈 Pro
        - LinkedIn + Facebook
        - Toutes les métriques
        - 12 mois d'historique
        - Support prioritaire
        
        **49€/mois**
        """)
    
    with col3:
        st.markdown("""
        ### 🏢 Enterprise
        - Multi-comptes
        - API access
        - White-label
        - Support dédié
        
        **Sur mesure**
        """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666;">
        <p>WhatsTheData - Analytics automatisés pour Looker Studio</p>
        <p>🔗 <a href="http://localhost:8000/docs">Documentation API</a> | 
           📊 <a href="https://lookerstudio.google.com">Looker Studio</a></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()