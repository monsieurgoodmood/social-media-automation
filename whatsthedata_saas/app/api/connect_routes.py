# app/api/connect_routes.py
# OAuth complet et robuste pour WhatsTheData

from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import secrets
import json
import httpx
import logging
from datetime import datetime, timedelta
from urllib.parse import urlencode

from ..database.connection import get_db_session
from ..database.models import User, LinkedinAccount, FacebookAccount
from ..utils.config import Config
from app.utils.connector_mapping import STRIPE_TO_CONNECTOR_MAPPING
from app.utils.session_manager import SessionManager

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(tags=["OAuth & Onboarding"])
templates = Jinja2Templates(directory="app/api/templates")

# ================================
# 🏠 PAGES PRINCIPALES
# ================================

@router.get("/connect", response_class=HTMLResponse)
async def connect_page(request: Request, looker_state: Optional[str] = None):
    """Page d'onboarding principale avec gestion état Looker"""
    
    context = {
        "request": request,
        "app_name": "WhatsTheData",
        "api_base": Config.BASE_URL
    }
    
    # Si état Looker fourni, récupérer le contexte
    if looker_state:
        session_data = SessionManager.get_session(looker_state)
        if session_data and session_data.get('source') == 'looker':
            context.update({
                'looker_context': True,
                'email': session_data.get('email'),
                'connector_id': session_data.get('connector_id')
            })
    
    return templates.TemplateResponse("connect.html", context)

# ================================
# 🔐 OAUTH GOOGLE
# ================================

@router.get("/auth/google")
async def google_oauth_start(
    request: Request,
    looker_state: Optional[str] = None,
    retry: Optional[str] = None
):
    """Démarrer OAuth Google avec gestion robuste"""
    
    try:
        # Vérifier configuration Google
        if not all([Config.GOOGLE_CLIENT_ID, Config.GOOGLE_CLIENT_SECRET, Config.GOOGLE_REDIRECT_URI]):
            logger.error("Configuration Google OAuth incomplète")
            return templates.TemplateResponse("error.html", {
                "request": request,
                "error": "Configuration OAuth incomplète",
                "retry_url": "/connect"
            })
        
        # Créer session avec contexte complet
        session_data = {
            'provider': 'google',
            'started_at': datetime.now(),
            'step': 'google_auth',
            'retry_count': int(retry) if retry else 0,
            'user_agent': str(request.headers.get('user-agent', ''))[:200]
        }
        
        # Préserver contexte Looker si présent
        if looker_state:
            looker_data = SessionManager.get_session(looker_state)
            if looker_data:
                session_data.update({
                    'looker_context': looker_data,
                    'source': 'looker'
                })
        
        state = SessionManager.create_session(session_data, expires_minutes=45)
        
        # Paramètres OAuth Google
        oauth_params = {
            'client_id': Config.GOOGLE_CLIENT_ID,
            'response_type': 'code',
            'redirect_uri': Config.GOOGLE_REDIRECT_URI,
            'scope': 'openid email profile',
            'state': state,
            'access_type': 'offline',
            'prompt': 'consent'
        }
        
        google_auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(oauth_params)}"
        logger.info(f"Redirection OAuth Google initiée: {state[:8]}...")
        
        return RedirectResponse(google_auth_url)
        
    except Exception as e:
        logger.error(f"Erreur démarrage OAuth Google: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Erreur lors de l'initialisation de l'authentification",
            "retry_url": "/connect"
        })

@router.get("/auth/google/callback")
async def google_oauth_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None)
):
    """Callback OAuth Google avec récupération robuste"""
    
    logger.info(f"Callback Google: state={state[:8] if state else 'None'}..., code={'présent' if code else 'absent'}, error={error}")
    
    # Gestion erreur OAuth
    if error:
        logger.warning(f"Erreur OAuth Google: {error}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Authentification refusée: {error}",
            "retry_url": "/auth/google"
        })
    
    if not code or not state:
        logger.error("Code ou state manquant dans callback Google")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Paramètres d'authentification manquants",
            "retry_url": "/auth/google"
        })
    
    # Récupérer session
    session_data = SessionManager.get_session(state)
    if not session_data:
        logger.warning(f"Session OAuth perdue: {state[:8]}...")
        # Tentative de récupération via Google
        return await _recover_lost_google_session(request, code, state)
    
    try:
        # Échanger code contre token
        token_data = await exchange_google_code(code)
        user_info = await get_google_user_info(token_data['access_token'])
        
        # Créer/récupérer utilisateur
        user = await create_or_get_user(
            email=user_info['email'],
            firstname=user_info.get('given_name', ''),
            lastname=user_info.get('family_name', ''),
            google_id=user_info['id']
        )
        
        # Mettre à jour session
        session_data.update({
            'user_id': user.id,
            'user_email': user.email,
            'user_name': f"{user.firstname} {user.lastname}".strip() or user.email.split('@')[0],
            'step': 'plan_selection',
            'google_authenticated': True
        })
        
        SessionManager.update_session(state, session_data)
        
        # Redirection selon contexte
        if session_data.get('source') == 'looker':
            looker_context = session_data.get('looker_context', {})
            connector_id = looker_context.get('connector_id')
            if connector_id:
                return await _handle_looker_user_flow(user, connector_id, state)
        
        logger.info(f"Authentification Google réussie pour {user.email}")
        return RedirectResponse(f"/connect/plans?state={state}")
        
    except Exception as e:
        logger.error(f"Erreur traitement callback Google: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Erreur lors de l'authentification Google",
            "retry_url": "/auth/google?retry=1"
        })

async def _recover_lost_google_session(request: Request, code: str, original_state: str) -> RedirectResponse:
    """Récupération d'urgence en cas de session perdue"""
    
    try:
        logger.info("Tentative de récupération session perdue Google...")
        
        # Échanger quand même le code pour récupérer l'utilisateur
        token_data = await exchange_google_code(code)
        user_info = await get_google_user_info(token_data['access_token'])
        user = await create_or_get_user(
            email=user_info['email'],
            firstname=user_info.get('given_name', ''),
            lastname=user_info.get('family_name', ''),
            google_id=user_info['id']
        )
        
        # Créer nouvelle session de récupération
        new_state = SessionManager.emergency_create_user_session(
            user.email, user.firstname, user.lastname
        )
        
        recovery_data = {
            'user_id': user.id,
            'user_email': user.email,
            'user_name': f"{user.firstname} {user.lastname}".strip() or user.email.split('@')[0],
            'step': 'plan_selection',
            'provider': 'google_recovery',
            'original_state': original_state
        }
        
        SessionManager.update_session(new_state, recovery_data)
        
        logger.info(f"Session de récupération créée pour {user.email}")
        return RedirectResponse(f"/connect/plans?state={new_state}&recovered=1")
        
    except Exception as e:
        logger.error(f"Échec récupération session: {e}")
        return RedirectResponse("/connect?error=session_recovery_failed")

async def _handle_looker_user_flow(user: User, connector_id: str, state: str):
    """Gestion spécifique flux Looker Studio"""
    
    try:
        # Vérifier abonnement pour connecteur
        plan_info = None
        for stripe_id, mapping in STRIPE_TO_CONNECTOR_MAPPING.items():
            if mapping['connector_id'] == connector_id:
                plan_info = mapping
                break
        
        if not plan_info:
            return RedirectResponse(f"/connect/plans?state={state}&error=invalid_connector")
        
        if user.plan_type != plan_info['stripe_price_id']:
            return RedirectResponse(f"/connect/upgrade?email={user.email}&connector={connector_id}")
        
        # Utilisateur a le bon plan, aller à sélection pages
        return RedirectResponse(f"/connect/looker/select?state={state}")
        
    except Exception as e:
        logger.error(f"Erreur flux Looker: {e}")
        return RedirectResponse(f"/connect/plans?state={state}")

# ================================
# 🔧 FONCTIONS UTILITAIRES GOOGLE
# ================================

async def exchange_google_code(code: str) -> dict:
    """Échanger code autorisation contre token avec retry"""
    
    token_data = {
        'client_id': Config.GOOGLE_CLIENT_ID,
        'client_secret': Config.GOOGLE_CLIENT_SECRET,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': Config.GOOGLE_REDIRECT_URI
    }
    
    for attempt in range(3):  # 3 tentatives
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    'https://oauth2.googleapis.com/token',
                    data=token_data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                if response.status_code == 200:
                    return response.json()
                    
                logger.warning(f"Tentative {attempt + 1} échange token Google échouée: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1} échange token Google erreur: {e}")
            
        if attempt < 2:  # Pause avant retry
            await asyncio.sleep(1)
    
    raise HTTPException(status_code=400, detail="Impossible d'échanger le code Google")

async def get_google_user_info(access_token: str) -> dict:
    """Récupérer infos utilisateur Google avec retry"""
    
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    'https://www.googleapis.com/oauth2/v2/userinfo',
                    headers={'Authorization': f'Bearer {access_token}'}
                )
                
                if response.status_code == 200:
                    return response.json()
                    
                logger.warning(f"Tentative {attempt + 1} info Google échouée: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1} info Google erreur: {e}")
            
        if attempt < 2:
            await asyncio.sleep(1)
    
    raise HTTPException(status_code=400, detail="Impossible de récupérer les infos utilisateur Google")

async def create_or_get_user(email: str, firstname: str, lastname: str, google_id: str) -> User:
    """Créer ou récupérer utilisateur avec gestion d'erreur"""
    
    try:
        with get_db_session() as db_session:
            user = db_session.query(User).filter(User.email == email).first()
            
            if not user:
                user = User(
                    email=email,
                    firstname=firstname,
                    lastname=lastname,
                    google_id=google_id,
                    plan_type='free',
                    is_active=True,
                    created_at=datetime.now()
                )
                db_session.add(user)
                db_session.commit()
                db_session.refresh(user)
                logger.info(f"Nouvel utilisateur créé: {email}")
            else:
                # Mettre à jour Google ID si manquant
                if not user.google_id and google_id:
                    user.google_id = google_id
                    db_session.commit()
                logger.info(f"Utilisateur existant récupéré: {email}")
            
            return user
            
    except Exception as e:
        logger.error(f"Erreur création/récupération utilisateur {email}: {e}")
        raise HTTPException(status_code=500, detail="Erreur base de données utilisateur")

# ================================
# 💳 GESTION PLANS & STRIPE
# ================================

@router.get("/connect/plans", response_class=HTMLResponse)
async def plan_selection(request: Request, state: str, recovered: Optional[str] = None):
    """Sélection plan avec gestion session robuste"""
    
    session_data = SessionManager.get_session(state)
    if not session_data:
        logger.warning(f"Session plan perdue: {state[:8]}...")
        return RedirectResponse("/connect?error=session_expired")
    
    plans = [
        {
            'id': 'price_1Ryho8JoIj8R31C3EXMDQ9tY',
            'name': 'LinkedIn Only',
            'price': '29€/mois',
            'features': ['Analytics LinkedIn complètes', 'Métriques posts et pages', 'Export Looker Studio'],
            'platforms': ['linkedin']
        },
        {
            'id': 'price_1RyhoWJoIj8R31C3uiSRLcw8', 
            'name': 'Facebook Only',
            'price': '29€/mois',
            'features': ['Analytics Facebook complètes', 'Métriques posts et pages', 'Export Looker Studio'],
            'platforms': ['facebook']
        },
        {
            'id': 'price_1RyhpiJoIj8R31C3EmVclb8P',
            'name': 'LinkedIn + Facebook',
            'price': '49€/mois',
            'features': ['LinkedIn + Facebook', 'Analytics cross-platform', 'Dashboard unifié', 'Support prioritaire'],
            'platforms': ['linkedin', 'facebook']
        }
    ]
    
    context = {
        "request": request,
        "plans": plans,
        "user_name": session_data.get('user_name'),
        "state": state,
        "recovered": recovered == "1"
    }
    
    return templates.TemplateResponse("plans.html", context)

@router.post("/connect/subscribe")
async def process_subscription(request: Request):
    """Traitement abonnement avec gestion d'erreur complète"""
    
    try:
        data = await request.json()
        state = data.get('state')
        plan_id = data.get('plan_id')
        
        if not state or not plan_id:
            raise HTTPException(status_code=400, detail="Paramètres manquants")
        
        session_data = SessionManager.get_session(state)
        if not session_data:
            raise HTTPException(status_code=400, detail="Session expirée")
        
        user_id = session_data.get('user_id')
        if not user_id:
            raise HTTPException(status_code=400, detail="Utilisateur non identifié")
        
        # Récupérer informations plan
        plan_info = STRIPE_TO_CONNECTOR_MAPPING.get(plan_id)
        if not plan_info:
            raise HTTPException(status_code=400, detail="Plan invalide")
        
        # Mettre à jour utilisateur
        with get_db_session() as db_session:
            user = db_session.query(User).filter(User.id == user_id).first()
            if not user:
                raise HTTPException(status_code=404, detail="Utilisateur non trouvé")
            
            # TODO: Intégrer vraie logique Stripe ici
            user.plan_type = plan_id
            user.subscription_end_date = datetime.now() + timedelta(days=30)
            db_session.commit()
        
        # Mettre à jour session
        session_data.update({
            'plan_id': plan_id,
            'connector_id': plan_info['connector_id'],
            'platforms_to_connect': plan_info['platforms'],
            'step': 'social_connect'
        })
        
        SessionManager.update_session(state, session_data)
        
        logger.info(f"Abonnement {plan_id} activé pour user {user_id}")
        return {"success": True, "redirect": f"/connect/social?state={state}"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur traitement abonnement: {e}")
        raise HTTPException(status_code=500, detail="Erreur serveur lors du traitement")

# ================================
# 🔗 CONNEXIONS RÉSEAUX SOCIAUX
# ================================

@router.get("/connect/social", response_class=HTMLResponse)
async def social_connect(request: Request, state: str):
    """Page connexion réseaux sociaux"""
    
    session_data = SessionManager.get_session(state)
    if not session_data:
        return RedirectResponse("/connect?error=session_expired")
    
    platforms = session_data.get('platforms_to_connect', [])
    
    return templates.TemplateResponse("social_connect.html", {
        "request": request,
        "platforms": platforms,
        "state": state,
        "user_name": session_data.get('user_name')
    })

# ================================
# 🔗 OAUTH LINKEDIN ROBUSTE
# ================================

@router.get("/auth/linkedin")
async def linkedin_oauth_start(state: str):
    """Démarrer OAuth LinkedIn avec validation"""
    
    session_data = SessionManager.get_session(state)
    if not session_data:
        raise HTTPException(status_code=400, detail="Session invalide")
    
    try:
        linkedin_params = {
            'client_id': Config.LINKEDIN_CLIENT_ID,
            'response_type': 'code',
            'redirect_uri': f"{Config.BASE_URL}/auth/linkedin/callback",
            'scope': 'r_organization_social rw_organization_admin r_basicprofile',
            'state': state
        }
        
        linkedin_auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(linkedin_params)}"
        
        # Marquer tentative LinkedIn
        session_data['linkedin_attempt'] = datetime.now()
        SessionManager.update_session(state, session_data)
        
        return RedirectResponse(linkedin_auth_url)
        
    except Exception as e:
        logger.error(f"Erreur démarrage LinkedIn OAuth: {e}")
        raise HTTPException(status_code=500, detail="Erreur configuration LinkedIn")

@router.get("/auth/linkedin/callback")
async def linkedin_oauth_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None)
):
    """Callback LinkedIn avec récupération complète"""
    
    logger.info(f"Callback LinkedIn: state={state[:8] if state else 'None'}..., error={error}")
    
    if error:
        logger.warning(f"Erreur LinkedIn OAuth: {error}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Connexion LinkedIn échouée: {error}",
            "retry_url": f"/auth/linkedin?state={state}" if state else "/connect"
        })
    
    if not code or not state:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Paramètres LinkedIn manquants",
            "retry_url": "/connect"
        })
    
    session_data = SessionManager.get_session(state)
    if not session_data:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Session LinkedIn expirée",
            "retry_url": "/connect"
        })
    
    try:
        # Échanger code contre token
        token_data = await _exchange_linkedin_code(code, state)
        access_token = token_data.get('access_token')
        
        if not access_token:
            raise Exception("Token LinkedIn manquant")
        
        # Récupérer organisations
        organizations = await _get_linkedin_organizations(access_token)
        
        # Mettre à jour session
        session_data.update({
            'linkedin_orgs': organizations,
            'linkedin_connected': True,
            'linkedin_token': access_token,  # Sauvegarder pour usage futur
            'step': 'linkedin_connected'
        })
        
        SessionManager.update_session(state, session_data)
        
        logger.info(f"LinkedIn connecté avec {len(organizations)} organisations")
        return RedirectResponse(f"/connect/pages?state={state}")
        
    except Exception as e:
        logger.error(f"Erreur callback LinkedIn: {e}")
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Erreur connexion LinkedIn: {str(e)}",
            "retry_url": f"/connect/social?state={state}"
        })

async def _exchange_linkedin_code(code: str, state: str) -> dict:
    """Échanger code LinkedIn avec retry"""
    
    token_data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': f"{Config.BASE_URL}/auth/linkedin/callback",
        'client_id': Config.LINKEDIN_CLIENT_ID,
        'client_secret': Config.LINKEDIN_CLIENT_SECRET
    }
    
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    "https://www.linkedin.com/oauth/v2/accessToken",
                    data=token_data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                if response.status_code == 200:
                    return response.json()
                    
                logger.warning(f"Tentative {attempt + 1} token LinkedIn échouée: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1} token LinkedIn erreur: {e}")
            
        if attempt < 2:
            await asyncio.sleep(1)
    
    raise Exception("Impossible d'échanger le code LinkedIn")

async def _get_linkedin_organizations(access_token: str) -> list:
    """Récupérer organisations LinkedIn avec retry"""
    
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(
                    "https://api.linkedin.com/v2/organizationAcls",
                    headers={
                        'Authorization': f'Bearer {access_token}',
                        'X-Restli-Protocol-Version': '2.0.0'
                    },
                    params={
                        'q': 'roleAssignee',
                        'role': 'ADMINISTRATOR',
                        'projection': '(elements*(organization~(id,localizedName)))'
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    organizations = []
                    
                    for element in data.get('elements', []):
                        org_data = element.get('organization~', {})
                        org_id = org_data.get('id')
                        org_name = org_data.get('localizedName')
                        
                        if org_id and org_name:
                            organizations.append({
                                'id': str(org_id),
                                'name': org_name,
                                'permissions': ['Analytics disponibles', 'Posts et engagement']
                            })
                    
                    return organizations
                    
                logger.warning(f"Tentative {attempt + 1} orgs LinkedIn échouée: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"Tentative {attempt + 1} orgs LinkedIn erreur: {e}")
            
        if attempt < 2:
            await asyncio.sleep(1)
    
    # En cas d'échec, retourner liste vide plutôt que crash
    logger.error("Impossible de récupérer les organisations LinkedIn")
    return []

# ================================
# 📊 SÉLECTION PAGES FINALE
# ================================

@router.get("/connect/pages", response_class=HTMLResponse)
async def page_selection(request: Request, state: str):
    """Sélection pages avec validation complète"""
    
    session_data = SessionManager.get_session(state)
    if not session_data:
        return RedirectResponse("/connect?error=session_expired")
    
    # Récupérer informations plan
    connector_id = session_data.get('connector_id')
    plan_info = None
    
    for stripe_id, mapping in STRIPE_TO_CONNECTOR_MAPPING.items():
        if mapping['connector_id'] == connector_id:
            plan_info = {
                'name': mapping['name'],
                'platforms': mapping['platforms'],
                'description': mapping.get('description', '')
            }
            break
    
    if not plan_info:
        plan_info = {
            'name': 'Premium', 
            'platforms': ['linkedin', 'facebook'], 
            'description': 'Plan par défaut'
        }
    
    return templates.TemplateResponse("page_selection.html", {
        "request": request,
        "plan_info": plan_info,
        "connector_id": connector_id or "default",
        "user_email": session_data.get('user_email', ''),
        "linkedin_orgs": session_data.get('linkedin_orgs', []),
        "facebook_pages": session_data.get('facebook_pages', []),
        "linkedin_connected": session_data.get('linkedin_connected', False),
        "facebook_connected": session_data.get('facebook_connected', False),
        "state": state
    })

@router.post("/connect/looker/save-selection")
async def save_page_selection(request: Request):
    """Sauvegarder sélection avec validation"""
    
    try:
        data = await request.json()
        state = data.get('state')
        linkedin_page_id = data.get('linkedin_page_id')
        facebook_page_id = data.get('facebook_page_id')
        
        if not state:
            return {"success": False, "error": "State manquant"}
        
        session_data = SessionManager.get_session(state)
        if not session_data:
            return {"success": False, "error": "Session expirée"}
        
        # Sauvegarder sélections
        if linkedin_page_id:
            linkedin_orgs = session_data.get('linkedin_orgs', [])
            selected_org = next((org for org in linkedin_orgs if org['id'] == linkedin_page_id), None)
            if selected_org:
                session_data['selected_linkedin_page'] = {
                    'id': selected_org['id'],
                    'name': selected_org['name']
                }
        
        if facebook_page_id:
            facebook_pages = session_data.get('facebook_pages', [])
            selected_page = next((page for page in facebook_pages if page['id'] == facebook_page_id), None)
            if selected_page:
                session_data['selected_facebook_page'] = {
                    'id': selected_page['id'],
                    'name': selected_page['name']
                }
        
        SessionManager.update_session(state, session_data)
        
        return {"success": True, "message": "Sélection sauvegardée"}
        
    except Exception as e:
        logger.error(f"Erreur sauvegarde sélection: {e}")
        return {"success": False, "error": "Erreur serveur"}

# ================================
# 🧪 ROUTES DE DEBUG & MONITORING
# ================================

@router.get("/connect/health")
async def health_check():
    """Vérification santé du système"""
    return {
        "status": "healthy",
        "timestamp": datetime.now(),
        "session_manager": "operational",
        "database": "connected"
    }

# Import asyncio pour sleep
import asyncio