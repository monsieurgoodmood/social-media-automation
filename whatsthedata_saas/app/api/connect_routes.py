# app/api/connect_routes.py
# OAuth Google réel pour WhatsTheData

from fastapi import APIRouter, Request, HTTPException, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from typing import Optional
import secrets
import json
import httpx
from datetime import datetime, timedelta
from urllib.parse import urlencode

from ..database.connection import get_db_session
from ..database.models import User, LinkedinAccount, FacebookAccount
from ..utils.config import Config
from app.utils.connector_mapping import STRIPE_TO_CONNECTOR_MAPPING

router = APIRouter(tags=["OAuth & Onboarding"])
templates = Jinja2Templates(directory="app/api/templates")

# Store temporaire pour les sessions OAuth (en production utiliser Redis)
oauth_sessions = {}

@router.get("/connect", response_class=HTMLResponse)
async def connect_page(request: Request):
    """Page d'onboarding principale"""
    
    return templates.TemplateResponse("connect.html", {
        "request": request,
        "app_name": "WhatsTheData",
        "api_base": Config.BASE_URL
    })

@router.get("/auth/google")
async def google_oauth_start(request: Request):
    """Démarrer l'OAuth Google - IMPLÉMENTATION RÉELLE"""
    
    # Générer un state unique pour sécuriser OAuth
    state = secrets.token_urlsafe(32)
    
    # Stocker la session
    oauth_sessions[state] = {
        'provider': 'google',
        'started_at': datetime.now(),
        'step': 'google_auth'
    }
    
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
    
    # URL OAuth Google officielle
    google_auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(oauth_params)}"
    
    return RedirectResponse(google_auth_url)

@router.get("/auth/google/callback")
async def google_oauth_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    error: Optional[str] = Query(None)
):
    """Callback OAuth Google - IMPLÉMENTATION RÉELLE"""
    
    if error:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Connexion Google annulée: {error}",
            "retry_url": "/connect"
        })
    
    if not state or state not in oauth_sessions:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Session invalide ou expirée",
            "retry_url": "/connect"
        })
    
    if not code:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Code d'autorisation manquant",
            "retry_url": "/connect"
        })
    
    session = oauth_sessions[state]
    
    try:
        # Échanger le code contre un token d'accès
        token_data = await exchange_google_code(code)
        
        # Récupérer les informations utilisateur
        user_info = await get_google_user_info(token_data['access_token'])
        
        # Créer ou récupérer l'utilisateur en base
        user = await create_or_get_user(
            email=user_info['email'],
            firstname=user_info.get('given_name', ''),
            lastname=user_info.get('family_name', ''),
            google_id=user_info['id']
        )
        
        # Mettre à jour la session
        session.update({
            'user_id': user.id,
            'user_email': user.email,
            'user_name': f"{user.firstname} {user.lastname}",
            'step': 'plan_selection'
        })
        
        return RedirectResponse(f"/connect/plans?state={state}")
        
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Erreur lors de l'authentification Google: {str(e)}",
            "retry_url": "/connect"
        })

async def exchange_google_code(code: str) -> dict:
    """Échanger le code d'autorisation contre un token d'accès"""
    
    token_data = {
        'client_id': Config.GOOGLE_CLIENT_ID,
        'client_secret': Config.GOOGLE_CLIENT_SECRET,
        'code': code,
        'grant_type': 'authorization_code',
        'redirect_uri': Config.GOOGLE_REDIRECT_URI
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            'https://oauth2.googleapis.com/token',
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=400, 
                detail=f"Erreur échange token Google: {response.text}"
            )
        
        return response.json()

async def get_google_user_info(access_token: str) -> dict:
    """Récupérer les informations utilisateur depuis Google"""
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Erreur récupération info Google: {response.text}"
            )
        
        return response.json()

async def create_or_get_user(email: str, firstname: str, lastname: str, google_id: str) -> User:
    """Créer ou récupérer un utilisateur"""
    
    with get_db_session() as db_session:
        # Chercher utilisateur existant
        user = db_session.query(User).filter(User.email == email).first()
        
        if not user:
            # Créer nouvel utilisateur
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
        
        return user

@router.get("/connect/plans", response_class=HTMLResponse)
async def plan_selection(request: Request, state: str):
    """Sélection du plan d'abonnement"""
    
    if state not in oauth_sessions:
        return RedirectResponse("/connect")
    
    session = oauth_sessions[state]
    
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
    
    return templates.TemplateResponse("plans.html", {
        "request": request,
        "plans": plans,
        "user_name": session.get('user_name'),
        "state": state
    })

@router.post("/connect/subscribe")
async def process_subscription(request: Request):
    """Traiter l'abonnement Stripe - À IMPLÉMENTER"""
    
    data = await request.json()
    state = data.get('state')
    plan_id = data.get('plan_id')
    
    if state not in oauth_sessions:
        raise HTTPException(status_code=400, detail="Session invalide")
    
    session = oauth_sessions[state]
    
    # TODO: Intégrer Stripe pour le paiement réel
    # Pour le moment, simulation d'un paiement réussi
    
    # Mettre à jour l'utilisateur avec le plan
    with get_db_session() as db_session:
        user = db_session.query(User).filter(User.id == session['user_id']).first()
        if user:
            user.plan_type = plan_id
            user.subscription_end_date = datetime.now() + timedelta(days=30)
            db_session.commit()
    
    # Récupérer les informations du plan depuis le mapping
    plan_info = STRIPE_TO_CONNECTOR_MAPPING.get(plan_id)
    if not plan_info:
        raise HTTPException(status_code=400, detail="Plan invalide")

    session.update({
        'plan_id': plan_id,
        'connector_id': plan_info['connector_id'],
        'platforms_to_connect': plan_info['platforms'],
        'step': 'social_connect'
    })
    
    return {"success": True, "redirect": f"/connect/social?state={state}"}

@router.get("/connect/social", response_class=HTMLResponse)
async def social_connect(request: Request, state: str):
    """Page de connexion aux réseaux sociaux"""
    
    if state not in oauth_sessions:
        return RedirectResponse("/connect")
    
    session = oauth_sessions[state]
    platforms = session.get('platforms_to_connect', [])
    
    return templates.TemplateResponse("social_connect.html", {
        "request": request,
        "platforms": platforms,
        "state": state,
        "user_name": session.get('user_name')
    })

@router.get("/auth/linkedin")
async def linkedin_oauth_start(state: str):
    """Démarrer l'OAuth LinkedIn"""
    
    if state not in oauth_sessions:
        raise HTTPException(status_code=400, detail="Session invalide")
    
    # OAuth LinkedIn (Community Management)
    # OAuth LinkedIn (Community Management)
    # OAuth LinkedIn (Community Management) - SCOPES CORRECTS
    # OAuth LinkedIn (Community Management) - SCOPES CORRECTS
    linkedin_params = {
        'client_id': Config.LINKEDIN_CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': f"{Config.BASE_URL}/auth/linkedin/callback",
        'scope': 'r_organization_social rw_organization_admin r_basicprofile',  # ✅ SCOPES VALIDES
        'state': state
    }
        
    linkedin_auth_url = f"https://www.linkedin.com/oauth/v2/authorization?{urlencode(linkedin_params)}"
    
    return RedirectResponse(linkedin_auth_url)

@router.get("/auth/linkedin/callback")
async def linkedin_oauth_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None)
):
    """Callback OAuth LinkedIn - IMPLEMENTATION RÉELLE"""
    
    if state not in oauth_sessions:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Session invalide",
            "retry_url": "/connect"
        })
    
    if not code:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Code d'autorisation LinkedIn manquant",
            "retry_url": "/connect"
        })
    
    session = oauth_sessions[state]
    
    try:
        import httpx
        
        # Échanger le code contre un token
        token_data = {
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': f"{Config.BASE_URL}/auth/linkedin/callback",
            'client_id': Config.LINKEDIN_CLIENT_ID,
            'client_secret': Config.LINKEDIN_CLIENT_SECRET
        }
        
        async with httpx.AsyncClient() as client:
            # Échanger le code contre un token
            token_response = await client.post(
                "https://www.linkedin.com/oauth/v2/accessToken",
                data=token_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=30
            )
            
            if token_response.status_code != 200:
                raise Exception(f"Erreur échange token: {token_response.text}")
            
            token_result = token_response.json()
            access_token = token_result.get('access_token')
            
            if not access_token:
                raise Exception("Token d'accès LinkedIn manquant")
            
            # Récupérer les vraies organisations LinkedIn
            orgs_response = await client.get(
                "https://api.linkedin.com/v2/organizationAcls",
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'X-Restli-Protocol-Version': '2.0.0'
                },
                params={
                    'q': 'roleAssignee',
                    'role': 'ADMINISTRATOR',  # Ajouter ce paramètre
                    'projection': '(elements*(organization~(id,localizedName)))'
                },
                timeout=30
            )
            
            if orgs_response.status_code != 200:
                raise Exception(f"Erreur récupération organisations: {orgs_response.text}")
            
            orgs_data = orgs_response.json()
            linkedin_organizations = []
            
            for element in orgs_data.get('elements', []):
                org_data = element.get('organization~', {})
                org_id = org_data.get('id')
                org_name = org_data.get('localizedName')
                
                if org_id and org_name:
                    linkedin_organizations.append({
                        'id': str(org_id),
                        'name': org_name,
                        'permissions': ['Analytics disponibles', 'Posts et engagement']
                    })
        
        session['linkedin_orgs'] = linkedin_organizations
        session['linkedin_connected'] = True
        
        return RedirectResponse(f"/connect/pages?state={state}")
        
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Erreur connexion LinkedIn: {str(e)}",
            "retry_url": f"/connect/social?state={state}"
        })
        
        
@router.get("/auth/facebook")
async def facebook_oauth_start(state: str):
    """Démarrer l'OAuth Facebook"""
    
    if state not in oauth_sessions:
        raise HTTPException(status_code=400, detail="Session invalide")
    
    # OAuth Facebook
    facebook_params = {
        'client_id': Config.FB_CLIENT_ID,
        'redirect_uri': f"{Config.BASE_URL}/auth/facebook/callback",
        'scope': 'pages_read_engagement,pages_show_list,read_insights',
        'state': state
    }
    
    facebook_auth_url = f"https://www.facebook.com/v21.0/dialog/oauth?{urlencode(facebook_params)}"
    
    return RedirectResponse(facebook_auth_url)

@router.get("/auth/facebook/callback")
async def facebook_oauth_callback(
    request: Request,
    code: Optional[str] = Query(None),
    state: Optional[str] = Query(None)
):
    """Callback OAuth Facebook - IMPLEMENTATION DIRECTE"""
    
    if state not in oauth_sessions:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Session invalide",
            "retry_url": "/connect"
        })
    
    if not code:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Code d'autorisation Facebook manquant",
            "retry_url": "/connect"
        })
    
    session = oauth_sessions[state]
    
    try:
        # Import direct des modules nécessaires
        import httpx
        from app.utils.config import Config
        
        # Échanger le code contre un token directement avec l'API Facebook
        token_data = {
            'client_id': Config.FB_CLIENT_ID,
            'client_secret': Config.FB_CLIENT_SECRET,
            'redirect_uri': f"{Config.BASE_URL}/auth/facebook/callback",
            'code': code
        }
        
        async with httpx.AsyncClient() as client:
            # Échanger le code contre un token
            token_response = await client.get(
                "https://graph.facebook.com/v21.0/oauth/access_token",
                params=token_data,
                timeout=30
            )
            
            if token_response.status_code != 200:
                raise Exception(f"Erreur échange token: {token_response.text}")
            
            token_result = token_response.json()
            access_token = token_result.get('access_token')
            
            if not access_token:
                raise Exception("Token d'accès manquant")
            
            # Récupérer les pages de l'utilisateur
            pages_response = await client.get(
                "https://graph.facebook.com/v21.0/me/accounts",
                params={
                    'access_token': access_token,
                    'fields': 'id,name,category,access_token,tasks,about,picture.type(large)',
                    'limit': 100
                },
                timeout=30
            )
            
            if pages_response.status_code != 200:
                raise Exception(f"Erreur récupération pages: {pages_response.text}")
            
            pages_data = pages_response.json()
            facebook_pages = []
            
            for page_data in pages_data.get('data', []):
                # Filtrer les pages avec permissions appropriées
                tasks = page_data.get('tasks', [])
                if any(task in ['MANAGE', 'CREATE_CONTENT', 'MODERATE', 'ADVERTISE', 'ANALYZE'] for task in tasks):
                    facebook_pages.append({
                        'id': page_data.get('id'),
                        'name': page_data.get('name'),
                        'category': page_data.get('category'),
                        'access_token': page_data.get('access_token'),
                        'tasks': tasks,
                        'about': page_data.get('about', ''),
                        'picture_url': page_data.get('picture', {}).get('data', {}).get('url'),
                        'can_analyze': 'ANALYZE' in tasks,
                        'can_manage': 'MANAGE' in tasks
                    })
        
        # Stocker les pages dans la session
        session['facebook_pages'] = facebook_pages
        session['facebook_connected'] = True
        
        return RedirectResponse(f"/connect/pages?state={state}")
            
    except Exception as e:
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": f"Erreur connexion Facebook: {str(e)}",
            "retry_url": f"/connect/social?state={state}"
        })
        
        
@router.get("/connect/pages", response_class=HTMLResponse)
async def page_selection(request: Request, state: str):
    """Sélection des pages à analyser"""
    
    if state not in oauth_sessions:
        return RedirectResponse("/connect")
    
    session = oauth_sessions[state]
    
    return templates.TemplateResponse("page_selection.html", {
        "request": request,
        "linkedin_orgs": session.get('linkedin_orgs', []),
        "facebook_pages": session.get('facebook_pages', []),
        "linkedin_connected": session.get('linkedin_connected', False),
        "facebook_connected": session.get('facebook_connected', False),
        "state": state
    })


# Endpoints spécifiques pour Looker Studio
@router.get("/connect/oauth/start")
async def looker_oauth_start(
    source: str = Query("looker"),
    email: str = Query(...),
    connector: str = Query(...)
):
    """Point d'entrée OAuth depuis Looker Studio"""
    
    # Vérifier l'abonnement pour ce connecteur
    with get_db_session() as db_session:
        user = db_session.query(User).filter(User.email == email).first()
        
        if not user:
            # Rediriger vers inscription avec préservation du contexte
            state = secrets.token_urlsafe(32)
            oauth_sessions[state] = {
                'source': 'looker',
                'email': email,
                'connector_id': connector,
                'step': 'registration_needed'
            }
            return RedirectResponse(f"/connect?looker_state={state}")
        
        # Vérifier si l'utilisateur a l'abonnement requis pour ce connecteur
        plan_info = None
        for stripe_id, mapping in STRIPE_TO_CONNECTOR_MAPPING.items():
            if mapping['connector_id'] == connector:
                plan_info = mapping
                break
        
        if not plan_info or user.plan_type != plan_info['stripe_price_id']:
            # Rediriger vers upgrade
            return RedirectResponse(f"/connect/upgrade?email={email}&connector={connector}")
    
    # Créer session OAuth pour sélection de pages
    state = secrets.token_urlsafe(32)
    oauth_sessions[state] = {
        'source': 'looker',
        'email': email,
        'user_id': user.id,
        'connector_id': connector,
        'platforms': plan_info['platforms'],
        'step': 'page_selection'
    }
    
    # Rediriger vers sélection des plateformes
    return RedirectResponse(f"/connect/looker/select?state={state}")

@router.get("/connect/looker/select", response_class=HTMLResponse)
async def looker_page_selection(request: Request, state: str):
    """Interface de sélection des pages pour Looker Studio avec info du plan"""
    
    if state not in oauth_sessions:
        return RedirectResponse("/connect")
    
    session = oauth_sessions[state]
    connector_id = session.get('connector_id')
    
    # Récupérer les informations du plan depuis le mapping
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
        return templates.TemplateResponse("error.html", {
            "request": request,
            "error": "Configuration du connecteur introuvable"
        })
    
    return templates.TemplateResponse("looker_page_selection.html", {
        "request": request,
        "plan_info": plan_info,
        "platforms": plan_info['platforms'],
        "state": state,
        "connector_id": connector_id,
        "user_email": session.get('email'),
        "linkedin_orgs": session.get('linkedin_orgs', []),
        "facebook_pages": session.get('facebook_pages', []),
        "linkedin_connected": session.get('linkedin_connected', False),
        "facebook_connected": session.get('facebook_connected', False)
    })
    
    
@router.post("/connect/oauth/callback")
async def looker_oauth_callback(request: Request):
    """Callback pour Looker Studio après sélection des pages"""
    
    data = await request.json()
    code = data.get('code')  # Code simulé après sélection
    state = data.get('state')
    email = data.get('email')
    connector_id = data.get('connector_id')
    
    if state not in oauth_sessions:
        return {"success": False, "error": "Session invalide"}
    
    session = oauth_sessions[state]
    
    # Récupérer les pages sélectionnées depuis la session
    linkedin_page = session.get('selected_linkedin_page')
    facebook_page = session.get('selected_facebook_page')
    
    if not linkedin_page and not facebook_page:
        return {"success": False, "error": "Aucune page sélectionnée"}
    
    # Nettoyer la session
    del oauth_sessions[state]
    
    return {
        "success": True,
        "linkedin_page": linkedin_page,
        "facebook_page": facebook_page,
        "email": email,
        "connector_id": connector_id
    }

@router.post("/connect/looker/save-selection")
async def save_page_selection(request: Request):
    """Sauvegarder la sélection de pages pour Looker"""
    
    data = await request.json()
    state = data.get('state')
    linkedin_page_id = data.get('linkedin_page_id')
    facebook_page_id = data.get('facebook_page_id')
    
    if state not in oauth_sessions:
        return {"success": False, "error": "Session invalide"}
    
    session = oauth_sessions[state]
    
    # Sauvegarder les sélections dans la session
    if linkedin_page_id:
        # Trouver les détails de la page LinkedIn
        linkedin_orgs = session.get('linkedin_orgs', [])
        selected_org = next((org for org in linkedin_orgs if org['id'] == linkedin_page_id), None)
        if selected_org:
            session['selected_linkedin_page'] = {
                'id': selected_org['id'],
                'name': selected_org['name']
            }
    
    if facebook_page_id:
        # Trouver les détails de la page Facebook
        facebook_pages = session.get('facebook_pages', [])
        selected_page = next((page for page in facebook_pages if page['id'] == facebook_page_id), None)
        if selected_page:
            session['selected_facebook_page'] = {
                'id': selected_page['id'],
                'name': selected_page['name']
            }
    
    return {"success": True, "message": "Sélection sauvegardée"}


@router.post("/connect/complete")
async def complete_onboarding(request: Request):
    """Finaliser l'onboarding et générer le token API"""
    
    data = await request.json()
    state = data.get('state')
    selected_linkedin = data.get('linkedin_orgs', [])
    selected_facebook = data.get('facebook_pages', [])
    
    if state not in oauth_sessions:
        raise HTTPException(status_code=400, detail="Session invalide")
    
    session = oauth_sessions[state]
    user_id = session['user_id']
    
    # Sauvegarder les comptes sélectionnés en base
    with get_db_session() as db_session:
        # Ajouter les comptes LinkedIn
        for org in selected_linkedin:
            linkedin_account = LinkedinAccount(
                user_id=user_id,
                organization_id=org['id'],
                organization_name=org['name'],
                is_active=True
            )
            db_session.add(linkedin_account)
        
        # Ajouter les comptes Facebook
        for page in selected_facebook:
            facebook_account = FacebookAccount(
                user_id=user_id,
                page_id=page['id'],
                page_name=page['name'],
                is_active=True
            )
            db_session.add(facebook_account)
        
        db_session.commit()
    
    # Générer un token API unique pour l'utilisateur
    api_token = f"wtd_{user_id}_{secrets.token_urlsafe(32)}"
    
    # TODO: Sauvegarder le token en base de données
    
    # Nettoyer la session
    user_email = session['user_email']
    del oauth_sessions[state]
    
    # RETOURNER JSON AU LIEU DE TEMPLATE
    return {
        "success": True,
        "api_token": api_token,
        "user_email": user_email,
        "connected_platforms": {
            "linkedin": len(selected_linkedin),
            "facebook": len(selected_facebook)
        }
    }
    
@router.get("/connect/success", response_class=HTMLResponse)
async def success_page(
    request: Request,
    token: str = Query(...),
    email: str = Query(...),
    linkedin: int = Query(0),
    facebook: int = Query(0)
):
    """Page de succès finale"""
    return templates.TemplateResponse("success.html", {
        "request": request,
        "api_token": token,
        "user_email": email,
        "connected_platforms": {
            "linkedin": linkedin,
            "facebook": facebook
        }
    })