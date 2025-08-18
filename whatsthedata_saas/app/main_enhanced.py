# app/main_enhanced.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import threading
import subprocess
import sys
import time

# NOUVEAU  
from app.utils.config import Config
from app.database.models import Base, User, Subscription
from app.auth.user_manager import UserManager
from app.payments.stripe_handler import StripeHandler
from collectors.linkedin_collector import LinkedInCollector
from collectors.facebook_collector import FacebookCollector
from collectors.scheduler import metrics_scheduler

# Configuration de la page
st.set_page_config(
    page_title="WhatTheData - Social Media Analytics SaaS",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé amélioré
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #0077B5 0%, #1877F2 50%, #E4405F 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        border-left: 5px solid #0077B5;
        margin-bottom: 1rem;
        transition: transform 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }
    
    .platform-card {
        border: 2px solid #e1e5e9;
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
        transition: all 0.3s ease;
    }
    
    .platform-card:hover {
        border-color: #0077B5;
        box-shadow: 0 4px 15px rgba(0,119,181,0.1);
    }
    
    .connected {
        border-color: #28a745;
        background: linear-gradient(135deg, #d4edda 0%, #f8fff9 100%);
    }
    
    .subscription-card {
        border: 3px solid #007bff;
        border-radius: 20px;
        padding: 2rem;
        margin: 1rem 0;
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        position: relative;
        overflow: hidden;
    }
    
    .subscription-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 4px;
        background: linear-gradient(90deg, #0077B5, #1877F2, #E4405F);
    }
    
    .price-tag {
        font-size: 2.5rem;
        font-weight: bold;
        color: #007bff;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.1);
    }
    
    .success-alert {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        border-left: 5px solid #28a745;
        color: #155724;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .error-alert {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        border-left: 5px solid #dc3545;
        color: #721c24;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .info-alert {
        background: linear-gradient(135deg, #d1ecf1 0%, #bee5eb 100%);
        border-left: 5px solid #17a2b8;
        color: #0c5460;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin: 2rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialisation de la base de données
@st.cache_resource
def init_database():
    engine = create_engine(Config.DATABASE_URL)
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal

# Initialisation des services
@st.cache_resource
def init_services():
    return {
        'user_manager': UserManager(),
        'stripe_handler': StripeHandler(),
        'linkedin_collector': LinkedInCollector(),
        'facebook_collector': FacebookCollector()
    }

# Démarrer le serveur OAuth en arrière-plan
@st.cache_resource
def start_oauth_server():
    """Démarre le serveur OAuth FastAPI en arrière-plan"""
    def run_server():
        try:
            subprocess.run([
                sys.executable, "-c",
                "from oauth_routes import start_oauth_server; start_oauth_server()"
            ], check=True)
        except Exception as e:
            print(f"Erreur démarrage serveur OAuth: {e}")
    
    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    time.sleep(2)  # Attendre que le serveur démarre
    return True

def handle_oauth_responses():
    """Gère les réponses OAuth depuis les paramètres d'URL"""
    query_params = st.experimental_get_query_params()
    
    if 'success' in query_params:
        success_type = query_params['success'][0]
        if success_type == 'linkedin_connected':
            st.markdown("""
            <div class="success-alert">
                ✅ <strong>LinkedIn connecté avec succès!</strong><br>
                Vos métriques LinkedIn sont en cours de collecte en arrière-plan.
            </div>
            """, unsafe_allow_html=True)
        elif success_type == 'facebook_connected':
            st.markdown("""
            <div class="success-alert">
                ✅ <strong>Facebook connecté avec succès!</strong><br>
                Vos métriques Facebook sont en cours de collecte en arrière-plan.
            </div>
            """, unsafe_allow_html=True)
        
        # Nettoyer les paramètres après affichage
        st.experimental_set_query_params()
    
    elif 'error' in query_params:
        error_type = query_params['error'][0]
        error_messages = {
            'linkedin_auth_failed': 'Échec de l\'authentification LinkedIn',
            'facebook_auth_failed': 'Échec de l\'authentification Facebook',
            'missing_parameters': 'Paramètres manquants dans la réponse OAuth',
            'invalid_user': 'Utilisateur invalide ou non trouvé',
            'linkedin_connection_failed': 'Échec de la connexion LinkedIn',
            'facebook_connection_failed': 'Échec de la connexion Facebook',
            'linkedin_internal_error': 'Erreur interne lors de la connexion LinkedIn',
            'facebook_internal_error': 'Erreur interne lors de la connexion Facebook'
        }
        
        error_msg = error_messages.get(error_type, 'Erreur inconnue')
        st.markdown(f"""
        <div class="error-alert">
            ❌ <strong>Erreur:</strong> {error_msg}<br>
            Veuillez réessayer ou contactez le support si le problème persiste.
        </div>
        """, unsafe_allow_html=True)
        
        # Nettoyer les paramètres après affichage
        st.experimental_set_query_params()

def show_enhanced_accounts_page(session_local, services):
    """Page de gestion des comptes avec interface améliorée"""
    st.markdown("## 🔗 Connectez vos réseaux sociaux")
    
    # Vérifier l'abonnement de l'utilisateur
    session = session_local()
    try:
        subscription = session.query(Subscription).filter_by(
            user_id=st.session_state.user.id,
            status='active'
        ).first()
        
        if not subscription:
            st.markdown("""
            <div class="info-alert">
                ℹ️ <strong>Abonnement requis</strong><br>
                Vous devez avoir un abonnement actif pour connecter vos comptes sociaux.
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("📋 Voir les plans d'abonnement", type="primary"):
                st.session_state.page = 'subscription'
                st.rerun()
            return
        
        # Récupérer les plateformes autorisées
        plan = Config.SUBSCRIPTION_PLANS.get(subscription.plan_type, {})
        allowed_platforms = plan.get('platforms', [])
        
        # Interface LinkedIn
        if 'linkedin' in allowed_platforms:
            with st.container():
                st.markdown("""
                <div class="platform-card">
                    <h3>📘 LinkedIn Business</h3>
                    <p>Connectez votre page LinkedIn pour analyser vos performances professionnelles</p>
                </div>
                """, unsafe_allow_html=True)
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("""
                    **Métriques disponibles:**
                    - 👥 Nombre de followers et évolution
                    - 👀 Vues de page et statistiques d'audience
                    - 💬 Engagement des posts et réactions
                    - 📊 Statistiques démographiques détaillées
                    - 🎯 Analyse des impressions et clics
                    """)
                
                with col2:
                    # Vérifier si LinkedIn est déjà connecté
                    linkedin_account = session.query(SocialAccount).filter_by(
                        user_id=st.session_state.user.id,
                        platform='linkedin',
                        is_active=True
                    ).first()
                    
                    if linkedin_account:
                        st.success("✅ Connecté")
                        st.caption(f"Page: {linkedin_account.page_name}")
                        if linkedin_account.last_sync:
                            st.caption(f"Dernière sync: {linkedin_account.last_sync.strftime('%d/%m %H:%M')}")
                        
                        if st.button("🔄 Reconnecter", key="reconnect_linkedin"):
                            linkedin_account.is_active = False
                            session.commit()
                            st.rerun()
                    else:
                        if st.button("🔗 Connecter LinkedIn", type="primary", key="connect_linkedin"):
                            linkedin_auth_url = services['linkedin_collector'].get_auth_url(st.session_state.user.id)
                            st.markdown(f"[🚀 Cliquez ici pour vous connecter à LinkedIn]({linkedin_auth_url})")
        
        # Interface Facebook
        if 'facebook' in allowed_platforms:
            with st.container():
                st.markdown("""
                <div class="platform-card">
                    <h3>📘 Facebook Pages</h3>
                    <p>Connectez votre page Facebook pour analyser vos performances sociales</p>
                </div>
                """, unsafe_allow_html=True)
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown("""
                    **Métriques disponibles:**
                    - 👥 Nombre de fans et croissance
                    - 📈 Portée des publications et impressions
                    - 💬 Engagement et interactions
                    - 👁️ Vues de page et clics
                    - 🎥 Statistiques vidéo
                    """)
                
                with col2:
                    # Vérifier si Facebook est déjà connecté
                    facebook_account = session.query(SocialAccount).filter_by(
                        user_id=st.session_state.user.id,
                        platform='facebook',
                        is_active=True
                    ).first()
                    
                    if facebook_account:
                        st.success("✅ Connecté")
                        st.caption(f"Page: {facebook_account.page_name}")
                        if facebook_account.last_sync:
                            st.caption(f"Dernière sync: {facebook_account.last_sync.strftime('%d/%m %H:%M')}")
                        
                        if st.button("🔄 Reconnecter", key="reconnect_facebook"):
                            facebook_account.is_active = False
                            session.commit()
                            st.rerun()
                    else:
                        if st.button("🔗 Connecter Facebook", type="primary", key="connect_facebook"):
                            facebook_auth_url = services['facebook_collector'].get_auth_url(st.session_state.user.id)
                            st.markdown(f"[🚀 Cliquez ici pour vous connecter à Facebook]({facebook_auth_url})")
        
        # Afficher les comptes connectés
        st.divider()
        st.markdown("### 📱 Comptes connectés")
        
        accounts = session.query(SocialAccount).filter_by(
            user_id=st.session_state.user.id,
            is_active=True
        ).all()
        
        if accounts:
            for account in accounts:
                platform_icon = "📘" if account.platform == "linkedin" else "📘"
                platform_color = "#0077B5" if account.platform == "linkedin" else "#1877F2"
                
                col1, col2, col3, col4 = st.columns([1, 3, 2, 1])
                
                with col1:
                    st.markdown(f"<h2 style='color: {platform_color}; margin: 0;'>{platform_icon}</h2>", unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"**{account.page_name}**")
                    st.caption(f"{account.platform.title()} • ID: {account.page_id}")
                
                with col3:
                    if account.last_sync:
                        time_diff = datetime.utcnow() - account.last_sync
                        if time_diff.total_seconds() < 3600:  # Moins d'1h
                            st.success("🟢 Synchronisé récemment")
                        elif time_diff.total_seconds() < 86400:  # Moins d'1j
                            st.warning("🟡 Synchronisé aujourd'hui")
                        else:
                            st.error("🔴 Synchronisation ancienne")
                        st.caption(f"Dernière sync: {account.last_sync.strftime('%d/%m/%Y %H:%M')}")
                    else:
                        st.info("⚪ Jamais synchronisé")
                
                with col4:
                    if st.button("🗑️", key=f"delete_{account.id}", help="Supprimer ce compte"):
                        account.is_active = False
                        session.commit()
                        st.rerun()
        else:
            st.info("Aucun compte connecté pour le moment.")
    
    finally:
        session.close()

def show_enhanced_dashboard(session_local):
    """Dashboard amélioré avec métriques temps réel"""
    if not st.session_state.user:
        st.error("Vous devez être connecté pour accéder au dashboard")
        return
    
    st.markdown("## 📊 Dashboard Analytics")
    
    session = session_local()
    try:
        # Récupérer les comptes connectés
        accounts = session.query(SocialAccount).filter_by(
            user_id=st.session_state.user.id,
            is_active=True
        ).all()
        
        if not accounts:
            st.markdown("""
            <div class="info-alert">
                ℹ️ <strong>Aucun compte connecté</strong><br>
                Connectez vos comptes LinkedIn et Facebook pour voir vos analytics en temps réel.
            </div>
            """, unsafe_allow_html=True)
            
            if st.button("🔗 Connecter des comptes", type="primary"):
                st.session_state.page = 'accounts'
                st.rerun()
            return
        
        # Période d'analyse
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("### 📈 Métriques globales")
        with col2:
            period = st.selectbox("Période", ["7 jours", "30 jours", "90 jours"], index=1)
            days = {"7 jours": 7, "30 jours": 30, "90 jours": 90}[period]
        
        # Calculer les métriques globales
        start_date = datetime.utcnow() - timedelta(days=days)
        
        metrics_summary = {}
        for account in accounts:
            recent_metrics = session.query(SocialMetric).filter(
                SocialMetric.account_id == account.id,
                SocialMetric.date >= start_date
            ).all()
            
            platform_metrics = metrics_summary.setdefault(account.platform, {
                'followers': 0,
                'engagement': 0,
                'reach': 0,
                'account_name': account.page_name
            })
            
            for metric in recent_metrics:
                if 'followers' in metric.metric_name or 'fans' in metric.metric_name:
                    platform_metrics['followers'] = max(platform_metrics['followers'], metric.metric_value or 0)
                elif 'engagement' in metric.metric_name or 'engaged' in metric.metric_name:
                    platform_metrics['engagement'] += metric.metric_value or 0
                elif 'reach' in metric.metric_name or 'impression' in metric.metric_name:
                    platform_metrics['reach'] += metric.metric_value or 0
        
        # Affichage des métriques par plateforme
        if metrics_summary:
            for platform, metrics in metrics_summary.items():
                platform_icon = "📘" if platform == "linkedin" else "📘"
                platform_name = "LinkedIn" if platform == "linkedin" else "Facebook"
                platform_color = "#0077B5" if platform == "linkedin" else "#1877F2"
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, {platform_color}20 0%, {platform_color}10 100%); 
                            border-left: 5px solid {platform_color}; 
                            padding: 1rem; border-radius: 10px; margin: 1rem 0;">
                    <h4 style="color: {platform_color}; margin: 0;">{platform_icon} {platform_name} - {metrics['account_name']}</h4>
                </div>
                """, unsafe_allow_html=True)
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric(
                        "👥 Followers/Fans", 
                        f"{int(metrics['followers']):,}",
                        help=f"Nombre total de followers sur {platform_name}"
                    )
                
                with col2:
                    st.metric(
                        "💬 Engagement", 
                        f"{int(metrics['engagement']):,}",
                        help=f"Total des interactions sur {platform_name}"
                    )
                
                with col3:
                    st.metric(
                        "👁️ Portée/Impressions", 
                        f"{int(metrics['reach']):,}",
                        help=f"Portée totale sur {platform_name}"
                    )
        
        # Graphiques d'évolution
        st.markdown("### 📈 Évolution des métriques")
        
        # Préparer les données pour les graphiques
        chart_data = []
        for account in accounts:
            account_metrics = session.query(SocialMetric).filter(
                SocialMetric.account_id == account.id,
                SocialMetric.date >= start_date,
                SocialMetric.metric_name.in_(['followers_count', 'fans_count'])
            ).order_by(SocialMetric.date).all()
            
            for metric in account_metrics:
                chart_data.append({
                    'date': metric.date.date(),
                    'followers': metric.metric_value,
                    'platform': account.platform.title(),
                    'account': account.page_name
                })
        
        if chart_data:
            df = pd.DataFrame(chart_data)
            df = df.groupby(['date', 'platform']).agg({'followers': 'max'}).reset_index()
            
            fig = px.line(
                df, x='date', y='followers', color='platform',
                title=f"Évolution des followers - {period}",
                labels={'followers': 'Nombre de followers', 'date': 'Date'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Bouton de synchronisation manuelle
        st.divider()
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("### 🔄 Synchronisation des données")
            st.caption("Les données sont automatiquement synchronisées toutes les 4 heures.")
        
        with col2:
            if st.button("🔄 Synchroniser maintenant", type="secondary"):
                with st.spinner("Synchronisation en cours..."):
                    success_count = 0
                    for account in accounts:
                        try:
                            if account.platform == 'linkedin':
                                success = services['linkedin_collector'].collect_metrics(session, account)
                            elif account.platform == 'facebook':
                                success = services['facebook_collector'].collect_metrics(session, account)
                            else:
                                continue
                            
                            if success:
                                success_count += 1
                        except Exception as e:
                            st.error(f"Erreur sync {account.page_name}: {str(e)}")
                    
                    if success_count > 0:
                        st.success(f"✅ {success_count} compte(s) synchronisé(s)")
                        st.rerun()
                    else:
                        st.error("❌ Échec de la synchronisation")
    
    finally:
        session.close()

def main():
    # Démarrer le serveur OAuth
    start_oauth_server()
    
    SessionLocal = init_database()
    services = init_services()
    
    # Démarrer le planificateur de métriques
    if not hasattr(st.session_state, 'scheduler_started'):
        metrics_scheduler.start_scheduler()
        st.session_state.scheduler_started = True
    
    # Header principal
    st.markdown("""
    <div class="main-header">
        <h1>📊 WhatTheData</h1>
        <p>Plateforme SaaS d'analyse automatisée des réseaux sociaux professionnels</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Gérer les réponses OAuth
    handle_oauth_responses()
    
    # Initialisation des variables de session
    if 'user' not in st.session_state:
        st.session_state.user = None
    if 'page' not in st.session_state:
        st.session_state.page = 'home'
    
    # Sidebar - Navigation
    with st.sidebar:
        st.markdown("### 🧭 Navigation")
        
        if st.session_state.user:
            st.success(f"👋 {st.session_state.user.firstname}")
            
            pages = {
                "🏠 Dashboard": "dashboard",
                "🔗 Comptes connectés": "accounts",
                "💳 Abonnement": "subscription", 
                "📊 Analytics avancées": "analytics",
                "⚙️ Paramètres": "settings"
            }
            
            for label, page in pages.items():
                if st.button(label, key=f"nav_{page}", use_container_width=True):
                    st.session_state.page = page
                    st.rerun()
            
            st.divider()
            
            # Statistiques rapides
            session = SessionLocal()
            try:
                accounts_count = session.query(SocialAccount).filter_by(
                    user_id=st.session_state.user.id,
                    is_active=True
                ).count()
                
                subscription = session.query(Subscription).filter_by(
                    user_id=st.session_state.user.id,
                    status='active'
                ).first()
                
                st.markdown("### 📊 Résumé")
                st.metric("Comptes connectés", accounts_count)
                
                if subscription:
                    plan = Config.SUBSCRIPTION_PLANS.get(subscription.plan_type, {})
                    st.caption(f"Plan: {plan.get('name', 'Inconnu')}")
                else:
                    st.caption("Aucun abonnement actif")
            
            finally:
                session.close()
            
            st.divider()
            if st.button("🚪 Déconnexion", use_container_width=True):
                st.session_state.user = None
                st.session_state.page = 'home'
                st.rerun()
        else:
            st.info("Connectez-vous pour accéder à vos analytics")
            if st.button("🔑 Se connecter", use_container_width=True):
                st.session_state.page = 'login'
                st.rerun()
            if st.button("📝 S'inscrire", use_container_width=True):
                st.session_state.page = 'register'
                st.rerun()
    
    # Contenu principal selon la page
    if st.session_state.page == 'home':
        show_home_page()
    elif st.session_state.page == 'login':
        show_login_page(services['user_manager'])
    elif st.session_state.page == 'register':
        show_register_page(services['user_manager'])
    elif st.session_state.page == 'dashboard':
        show_enhanced_dashboard(SessionLocal)
    elif st.session_state.page == 'accounts':
        show_enhanced_accounts_page(SessionLocal, services)
    elif st.session_state.page == 'subscription':
        show_subscription_page(SessionLocal, services['stripe_handler'])
    elif st.session_state.page == 'analytics':
        show_analytics_page(SessionLocal)
    elif st.session_state.page == 'settings':
        show_settings_page(SessionLocal)

def show_home_page():
    """Page d'accueil avec présentation du service"""
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("## 🚀 Transformez vos données sociales en croissance")
        
        st.markdown("""
        **WhatTheData** est la première plateforme SaaS française dédiée à l'analyse 
        automatisée des performances LinkedIn et Facebook pour les entreprises.
        
        ### ✨ Pourquoi choisir WhatTheData?
        
        🎯 **Analyse intelligente** - Algorithmes propriétaires pour identifier les contenus performants  
        📊 **Dashboards Looker Studio** - Visualisations professionnelles prêtes à l'emploi  
        🔄 **Synchronisation temps réel** - Données actualisées automatiquement  
        💡 **Insights actionables** - Recommandations personnalisées basées sur vos données  
        🔒 **Sécurité enterprise** - Authentification OAuth sécurisée, conformité RGPD  
        🎨 **Templates personnalisables** - Adaptez les rapports à votre charte graphique  
        
        ### 🎯 Parfait pour:
        - **Agences marketing** - Gérez plusieurs clients facilement
        - **Entreprises B2B** - Optimisez votre présence LinkedIn
        - **Community managers** - Analysez l'engagement en détail
        - **Dirigeants** - Dashboards executives clairs et précis
        """)
        
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("🚀 Essai gratuit 14 jours", type="primary", use_container_width=True):
                st.session_state.page = 'register'
                st.rerun()
        with col_b:
            if st.button("🔑 Se connecter", use_container_width=True):
                st.session_state.page = 'login'
                st.rerun()
    
    with col2:
        st.markdown("### 💰 Plans tarifaires")
        
        # Plan LinkedIn
        st.markdown("""
        <div class="subscription-card" style="border-color: #0077B5;">
            <h4>📘 LinkedIn Pro</h4>
            <div class="price-tag" style="color: #0077B5;">29€<span style="font-size: 1rem;">/mois</span></div>
            <ul style="margin: 1rem 0; padding-left: 1.5rem;">
                <li>Analytics LinkedIn complets</li>
                <li>Dashboard Looker Studio</li>
                <li>Synchronisation quotidienne</li>
                <li>Support email</li>
                <li>Export des données</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Plan Facebook
        st.markdown("""
        <div class="subscription-card" style="border-color: #1877F2;">
            <h4>📘 Facebook Pro</h4>
            <div class="price-tag" style="color: #1877F2;">29€<span style="font-size: 1rem;">/mois</span></div>
            <ul style="margin: 1rem 0; padding-left: 1.5rem;">
                <li>Analytics Facebook complets</li>
                <li>Dashboard Looker Studio</li>
                <li>Synchronisation quotidienne</li>
                <li>Support email</li>
                <li>Export des données</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Plan Premium
        st.markdown("""
        <div class="subscription-card" style="border-color: #28a745; background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);">
            <h4>🌟 Premium Multi-Platform</h4>
            <div class="price-tag" style="color: #28a745;">49€<span style="font-size: 1rem;">/mois</span></div>
            <ul style="margin: 1rem 0; padding-left: 1.5rem;">
                <li><strong>LinkedIn + Facebook</strong></li>
                <li>Dashboards avancés</li>
                <li>Synchronisation temps réel</li>
                <li>Support prioritaire</li>
                <li>API access</li>
                <li>Rapports personnalisés</li>
            </ul>
            <div style="background: #28a745; color: white; padding: 0.5rem; border-radius: 5px; text-align: center; margin-top: 1rem;">
                <strong>🎉 POPULAIRE</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Section témoignages / cas d'usage
    st.divider()
    
    st.markdown("### 🎯 Cas d'usage concrets")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **📈 Agence Marketing**
        
        *"Nous gérons 15 comptes clients avec WhatTheData. 
        Les dashboards automatiques nous font gagner 10h/semaine 
        et nos clients adorent les rapports Looker Studio."*
        
        - Marie L., Directrice Agence
        """)
    
    with col2:
        st.markdown("""
        **🏢 Startup B2B**
        
        *"Grâce aux insights LinkedIn, nous avons identifié 
        les contenus qui génèrent le plus de leads qualifiés. 
        Notre ROI social a augmenté de 300%."*
        
        - Thomas K., CMO
        """)
    
    with col3:
        st.markdown("""
        **🎨 Community Manager**
        
        *"L'analyse automatique des performances m'aide 
        à optimiser mes contenus en temps réel. 
        Interface intuitive et données précises."*
        
        - Sarah M., CM
        """)

def show_login_page(user_manager):
    """Page de connexion améliorée"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 2rem;">
            <h2>🔑 Connexion à votre compte</h2>
            <p>Accédez à vos analytics et dashboards personnalisés</p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            email = st.text_input("📧 Email", placeholder="votre@email.com")
            password = st.text_input("🔒 Mot de passe", type="password", placeholder="••••••••")
            
            remember_me = st.checkbox("Se souvenir de moi")
            
            if st.form_submit_button("🚀 Se connecter", type="primary", use_container_width=True):
                if not email or not password:
                    st.error("Veuillez remplir tous les champs")
                else:
                    with st.spinner("Connexion en cours..."):
                        user = user_manager.authenticate(email, password)
                        if user:
                            st.session_state.user = user
                            st.session_state.page = 'dashboard'
                            st.success("✅ Connexion réussie!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Email ou mot de passe incorrect")
        
        st.divider()
        
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button("📝 Créer un compte", use_container_width=True):
                st.session_state.page = 'register'
                st.rerun()
        
        with col_b:
            if st.button("🔒 Mot de passe oublié?", use_container_width=True):
                st.info("Fonctionnalité bientôt disponible")

def show_register_page(user_manager):
    """Page d'inscription améliorée"""
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 2rem;">
            <h2>📝 Créez votre compte</h2>
            <p>Commencez votre essai gratuit de 14 jours</p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("register_form"):
            col_a, col_b = st.columns(2)
            with col_a:
                firstname = st.text_input("👤 Prénom", placeholder="Jean")
                email = st.text_input("📧 Email", placeholder="jean@entreprise.com")
            with col_b:
                lastname = st.text_input("👤 Nom", placeholder="Dupont")
                company = st.text_input("🏢 Entreprise", placeholder="Mon Entreprise SAS")
            
            password = st.text_input("🔒 Mot de passe", type="password", 
                                   placeholder="••••••••", 
                                   help="Minimum 6 caractères")
            password_confirm = st.text_input("🔒 Confirmer le mot de passe", 
                                           type="password", placeholder="••••••••")
            
            st.divider()
            
            terms = st.checkbox("✅ J'accepte les conditions d'utilisation et la politique de confidentialité")
            newsletter = st.checkbox("📬 Je souhaite recevoir les actualités produit (optionnel)")
            
            if st.form_submit_button("🚀 Créer mon compte gratuit", type="primary", use_container_width=True):
                # Validation
                errors = []
                if not all([firstname, lastname, email, password, password_confirm]):
                    errors.append("Tous les champs obligatoires doivent être remplis")
                if password != password_confirm:
                    errors.append("Les mots de passe ne correspondent pas")
                if len(password) < 6:
                    errors.append("Le mot de passe doit contenir au moins 6 caractères")
                if not terms:
                    errors.append("Vous devez accepter les conditions d'utilisation")
                if "@" not in email or "." not in email:
                    errors.append("Email invalide")
                
                if errors:
                    for error in errors:
                        st.error(f"❌ {error}")
                else:
                    with st.spinner("Création du compte..."):
                        user = user_manager.create_user(email, password, firstname, lastname, company)
                        if user:
                            st.session_state.user = user
                            st.session_state.page = 'subscription'
                            st.success("✅ Compte créé avec succès! Choisissez votre plan.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Erreur lors de la création. Cet email existe peut-être déjà.")
        
        st.divider()
        
        st.markdown("""
        <div style="text-align: center;">
            <p>Déjà un compte?</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔑 Se connecter", use_container_width=True):
            st.session_state.page = 'login'
            st.rerun()

def show_subscription_page(session_local, stripe_handler):
    """Page d'abonnement améliorée"""
    st.markdown("## 💳 Gestion de l'abonnement")
    
    session = session_local()
    try:
        # Vérifier l'abonnement actuel
        current_subscription = session.query(Subscription).filter_by(
            user_id=st.session_state.user.id,
            status='active'
        ).first()
        
        if current_subscription:
            plan = Config.SUBSCRIPTION_PLANS.get(current_subscription.plan_type, {})
            
            # Affichage de l'abonnement actuel
            st.markdown(f"""
            <div class="success-alert">
                <h3>🎉 Abonnement actif: {plan.get('name', 'Plan inconnu')}</h3>
                <p>Vous profitez actuellement de toutes les fonctionnalités de votre plan.</p>
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("💰 Plan actuel", plan.get('name', 'Inconnu'))
            with col2:
                st.metric("💵 Prix mensuel", f"{plan.get('price', 0)}€")
            with col3:
                if current_subscription.current_period_end:
                    st.metric("📅 Renouvellement", 
                            current_subscription.current_period_end.strftime('%d/%m/%Y'))
                else:
                    st.metric("📊 Statut", current_subscription.status.title())
            
            # Fonctionnalités du plan actuel
            st.markdown("### ✨ Fonctionnalités incluses")
            features = plan.get('features', [])
            cols = st.columns(2)
            for i, feature in enumerate(features):
                with cols[i % 2]:
                    st.markdown(f"✅ {feature}")
            
            st.divider()
            
            # Actions sur l'abonnement
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("📱 Gérer mon abonnement sur Stripe", type="primary"):
                    if current_subscription.stripe_customer_id:
                        portal_url = stripe_handler.create_billing_portal_session(
                            current_subscription.stripe_customer_id,
                            return_url="http://localhost:8501"
                        )
                        if portal_url:
                            st.markdown(f"[🔗 Accéder au portail de facturation]({portal_url})")
                        else:
                            st.error("Impossible d'accéder au portail de facturation")
                    else:
                        st.error("Aucun customer ID Stripe trouvé")
            
            with col2:
                if st.button("🔄 Changer de plan"):
                    st.info("Contactez le support pour changer de plan: support@whatsthedata.fr")
        
        else:
            # Aucun abonnement actuel - Afficher les plans
            st.markdown("""
            <div class="info-alert">
                <h3>🎯 Choisissez votre plan</h3>
                <p>Sélectionnez le plan qui correspond à vos besoins pour commencer votre analyse.</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Afficher les plans en grand format
            plans = Config.SUBSCRIPTION_PLANS
            
            for plan_id, plan in plans.items():
                # Couleur selon le plan
                if plan_id == 'linkedin_basic':
                    color = "#0077B5"
                    icon = "📘"
                elif plan_id == 'facebook_basic':
                    color = "#1877F2" 
                    icon = "📘"
                else:
                    color = "#28a745"
                    icon = "🌟"
                
                # Container du plan
                is_premium = plan_id == 'premium'
                border_style = "3px solid #28a745" if is_premium else f"2px solid {color}"
                
                st.markdown(f"""
                <div style="border: {border_style}; border-radius: 15px; padding: 2rem; margin: 1.5rem 0; 
                           background: linear-gradient(135deg, {color}10 0%, {color}05 100%);">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1rem;">
                        <h3 style="color: {color}; margin: 0;">{icon} {plan['name']}</h3>
                        {'<span style="background: #28a745; color: white; padding: 0.3rem 0.8rem; border-radius: 20px; font-size: 0.8rem; font-weight: bold;">POPULAIRE</span>' if is_premium else ''}
                    </div>
                    <div style="font-size: 2.5rem; font-weight: bold; color: {color}; margin: 1rem 0;">
                        {plan['price']}€<span style="font-size: 1rem; font-weight: normal;">/mois</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Fonctionnalités en colonnes
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.markdown("**Fonctionnalités incluses:**")
                    for feature in plan['features']:
                        st.markdown(f"✅ {feature}")
                    
                    platforms_text = " + ".join([p.title() for p in plan['platforms']])
                    st.markdown(f"🔗 **Plateformes:** {platforms_text}")
                
                with col2:
                    if st.button(f"Choisir {plan['name']}", 
                               key=f"select_{plan_id}", 
                               type="primary" if is_premium else "secondary",
                               use_container_width=True):
                        
                        with st.spinner("Redirection vers le paiement..."):
                            # Créer une session de checkout Stripe
                            checkout_url = stripe_handler.create_checkout_session(
                                price_id=plan['stripe_price_id'],
                                customer_email=st.session_state.user.email,
                                success_url="http://localhost:8501/?payment=success",
                                cancel_url="http://localhost:8501/?payment=canceled",
                                metadata={
                                    'user_id': str(st.session_state.user.id), 
                                    'plan_type': plan_id
                                }
                            )
                            
                            if checkout_url:
                                st.markdown(f"[💳 Procéder au paiement sécurisé]({checkout_url})")
                                st.info("Vous allez être redirigé vers Stripe pour finaliser votre paiement.")
                            else:
                                st.error("Erreur lors de la création de la session de paiement")
                
                st.divider()
    
    finally:
        session.close()

def show_analytics_page(session_local):
    """Page d'analytics avancées"""
    st.markdown("## 📊 Analytics avancées")
    
    st.markdown("""
    <div class="info-alert">
        <h3>🚧 Fonctionnalité en développement</h3>
        <p>Les analytics avancées avec IA et recommandations personnalisées arriveront bientôt!</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Aperçu des fonctionnalités à venir
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🎯 Prochainement disponible:
        
        🤖 **Analyse IA des contenus**  
        - Identification automatique des posts performants  
        - Recommandations de timing optimal  
        - Analyse de sentiment des commentaires  
        
        📈 **Prédictions de performance**  
        - Prévisions d'engagement  
        - Optimisation automatique des hashtags  
        - Score de qualité du contenu  
        """)
    
    with col2:
        st.markdown("""
        ### 🎨 Templates Looker Studio:
        
        📊 **Dashboard Executive**  
        - KPIs consolidés multi-plateformes  
        - Rapports mensuels automatiques  
        
        🎯 **Analyse d'audience**  
        - Démographie détaillée  
        - Centres d'intérêt et comportements  
        
        💡 **Recommandations actionables**  
        - Suggestions de contenu  
        - Optimisation des horaires de publication  
        """)

def show_settings_page(session_local):
    """Page de paramètres utilisateur"""
    st.markdown("## ⚙️ Paramètres du compte")
    
    session = session_local()
    try:
        user = session.query(User).filter_by(id=st.session_state.user.id).first()
        
        if not user:
            st.error("Utilisateur non trouvé")
            return
        
        # Informations personnelles
        st.markdown("### 👤 Informations personnelles")
        
        with st.form("user_settings"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_firstname = st.text_input("Prénom", value=user.firstname or "")
                new_email = st.text_input("Email", value=user.email or "")
            
            with col2:
                new_lastname = st.text_input("Nom", value=user.lastname or "")
                new_company = st.text_input("Entreprise", value=user.company or "")
            
            if st.form_submit_button("💾 Sauvegarder", type="primary"):
                user.firstname = new_firstname
                user.lastname = new_lastname
                user.email = new_email
                user.company = new_company
                user.updated_at = datetime.utcnow()
                
                session.commit()
                st.success("✅ Informations mises à jour")
                st.rerun()
        
        st.divider()
        
        # Sécurité
        st.markdown("### 🔒 Sécurité")
        
        with st.expander("Changer le mot de passe"):
            with st.form("change_password"):
                current_password = st.text_input("Mot de passe actuel", type="password")
                new_password = st.text_input("Nouveau mot de passe", type="password")
                confirm_password = st.text_input("Confirmer le nouveau mot de passe", type="password")
                
                if st.form_submit_button("🔄 Changer le mot de passe"):
                    if new_password != confirm_password:
                        st.error("Les nouveaux mots de passe ne correspondent pas")
                    elif len(new_password) < 6:
                        st.error("Le mot de passe doit contenir au moins 6 caractères")
                    else:
                        # Vérifier le mot de passe actuel
                        user_manager = UserManager()
                        if user_manager.authenticate(user.email, current_password):
                            # Mettre à jour le mot de passe
                            user.password_hash = user_manager._hash_password(new_password)
                            session.commit()
                            st.success("✅ Mot de passe mis à jour")
                        else:
                            st.error("❌ Mot de passe actuel incorrect")
        
        st.divider()
        
        # Préférences
        st.markdown("### 🎛️ Préférences")
        
        # Notifications (placeholder)
        st.checkbox("📧 Recevoir les rapports hebdomadaires par email", value=True)
        st.checkbox("🔔 Notifications de synchronisation des données", value=True)
        st.checkbox("📰 Newsletter et actualités produit", value=False)
        
        st.divider()
        
        # Zone dangereuse
        st.markdown("### ⚠️ Zone dangereuse")
        
        with st.expander("🗑️ Supprimer mon compte", expanded=False):
            st.warning("""
            **Attention:** Cette action est irréversible. 
            Toutes vos données, comptes connectés et abonnements seront supprimés définitivement.
            """)
            
            confirm_deletion = st.text_input(
                "Tapez 'SUPPRIMER' pour confirmer", 
                placeholder="SUPPRIMER"
            )
            
            if st.button("🗑️ Supprimer définitivement mon compte", type="primary"):
                if confirm_deletion == "SUPPRIMER":
                    # Ici, on implémenterait la suppression complète
                    st.error("Fonctionnalité de suppression non implémentée (sécurité)")
                else:
                    st.error("Veuillez taper 'SUPPRIMER' pour confirmer")
    
    finally:
        session.close()

if __name__ == "__main__":
    main()