# app/api/looker_endpoints.py
# Endpoints complets et robustes pour le connecteur Looker Studio
# Gère tous les scénarios : authentification, rate limits, erreurs API, tokens expirés

from fastapi import APIRouter, Depends, HTTPException, Query, Header, Request
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any, Union
from datetime import datetime, timedelta, date
from pydantic import BaseModel, Field, validator
import logging
import json
import httpx
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
import time
import hashlib
from functools import wraps

# Imports locaux
from ..auth.user_manager import user_manager
from ..database.connection import db_manager
from ..database.models import User, FacebookAccount, LinkedinAccount, SocialAccessToken
from ..utils.config import Config
from ..utils.metrics import MetricsManager


# Ajouter en haut du fichier
from functools import lru_cache
import hashlib

# Cache simple pour les métriques (24h)
@lru_cache(maxsize=1000)
def cache_key(account_id: str, date_str: str, metrics_type: str) -> str:
    return hashlib.md5(f"{account_id}_{date_str}_{metrics_type}".encode()).hexdigest()

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Looker Studio"])

# ========================================
# ENUMS ET CONSTANTES
# ========================================

class PlatformType(str, Enum):
    LINKEDIN = "linkedin"
    FACEBOOK = "facebook"
    BOTH = "both"

class MetricsType(str, Enum):
    OVERVIEW = "overview"
    PAGES = "pages"
    POSTS = "posts"
    FOLLOWERS_BREAKDOWN = "followers_breakdown"
    VIDEO_DETAILED = "video_detailed"

class APIStatus(str, Enum):
    SUCCESS = "success"
    ERROR = "error"
    RATE_LIMITED = "rate_limited"
    TOKEN_EXPIRED = "token_expired"
    PERMISSION_DENIED = "permission_denied"
    NOT_FOUND = "not_found"

# ========================================
# MODELS PYDANTIC ROBUSTES
# ========================================

class LookerAuthRequest(BaseModel):
    email: str = Field(..., description="Email de l'utilisateur")
    connector_id: str = Field(..., description="ID du connecteur Looker")
    platforms: List[str] = Field(default=["linkedin", "facebook"], description="Plateformes demandées")
    source: str = Field(default="looker_studio", description="Source de la requête")

class LookerDataRequest(BaseModel):
    platforms: List[PlatformType] = Field(default=[PlatformType.LINKEDIN, PlatformType.FACEBOOK])
    date_range: int = Field(default=30, ge=1, le=365, description="Nombre de jours (1-365)")
    start_date: Optional[str] = Field(None, description="Date début YYYY-MM-DD")
    end_date: Optional[str] = Field(None, description="Date fin YYYY-MM-DD")
    metrics_type: MetricsType = Field(default=MetricsType.OVERVIEW)
    include_linkedin_reactions: bool = Field(default=False)
    include_facebook_reactions: bool = Field(default=False)
    include_video_metrics: bool = Field(default=False)
    include_breakdown: bool = Field(default=False)
    
    @validator('start_date', 'end_date')
    def validate_dates(cls, v):
        if v:
            try:
                datetime.strptime(v, '%Y-%m-%d')
                return v
            except ValueError:
                raise ValueError('Format de date invalide, utilisez YYYY-MM-DD')
        return v

class LookerDataResponse(BaseModel):
    success: bool
    data: Dict[str, Any]
    total_records: int
    generated_at: str
    user_email: str
    date_range: Dict[str, Any]
    request_params: Dict[str, Any]
    platform_summaries: Optional[Dict[str, Any]] = None
    errors: List[str] = []
    warnings: List[str] = []

class APIError(BaseModel):
    error_code: str
    message: str
    details: Optional[Dict[str, Any]] = None
    retry_after: Optional[int] = None

@dataclass
class TokenInfo:
    access_token: str
    expires_at: Optional[datetime] = None
    refresh_token: Optional[str] = None
    platform: str = ""
    account_id: str = ""
    
    @property
    def is_expired(self) -> bool:
        if self.expires_at:
            return datetime.now() >= self.expires_at
        return False

# ========================================
# DÉCORATEURS ET UTILITAIRES
# ========================================

def rate_limit(calls_per_minute: int = 60):
    """Décorateur pour gérer le rate limiting"""
    
    def decorator(func):
        last_calls = {}
        
        @wraps(func)
        async def wrapper(*args, **kwargs):
            now = time.time()
            minute_ago = now - 60
            
            # Nettoyer les anciens appels
            key = f"{func.__name__}"
            if key not in last_calls:
                last_calls[key] = []
            
            last_calls[key] = [call_time for call_time in last_calls[key] if call_time > minute_ago]
            
            # Vérifier la limite
            if len(last_calls[key]) >= calls_per_minute:
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit dépassé: {calls_per_minute} appels/minute maximum"
                )
            
            # Enregistrer l'appel
            last_calls[key].append(now)
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator

async def handle_api_errors(func, *args, **kwargs):
    """Gestionnaire d'erreurs API centralisé"""
    
    try:
        return await func(*args, **kwargs)
    except httpx.TimeoutException:
        logger.error(f"Timeout API pour {func.__name__}")
        raise HTTPException(status_code=504, detail="Timeout de l'API externe")
    except httpx.HTTPStatusError as e:
        logger.error(f"Erreur HTTP {e.response.status_code} pour {func.__name__}: {e.response.text}")
        
        if e.response.status_code == 429:
            raise HTTPException(status_code=429, detail="Rate limit API atteint")
        elif e.response.status_code == 401:
            raise HTTPException(status_code=401, detail="Token d'accès invalide ou expiré")
        elif e.response.status_code == 403:
            raise HTTPException(status_code=403, detail="Permissions insuffisantes")
        else:
            raise HTTPException(status_code=502, detail="Erreur de l'API externe")
    except Exception as e:
        logger.error(f"Erreur inattendue dans {func.__name__}: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne du serveur")

# ========================================
# GESTION DES TOKENS
# ========================================

class TokenManager:
    """Gestionnaire centralisé des tokens d'accès"""
    
    @staticmethod
    async def get_linkedin_token(account_id: str) -> Optional[TokenInfo]:
        """Récupérer le token LinkedIn pour un compte"""
        
        try:
            with db_manager.get_session() as session:
                # Chercher dans SocialAccessToken
                token_record = session.query(SocialAccessToken).filter(
                    SocialAccessToken.platform == "linkedin",
                    SocialAccessToken.account_id == account_id,
                    SocialAccessToken.is_active == True
                ).first()
                
                if token_record:
                    token_info = TokenInfo(
                        access_token=token_record.access_token,
                        expires_at=token_record.expires_at,
                        refresh_token=token_record.refresh_token,
                        platform="linkedin",
                        account_id=account_id
                    )
                    
                    # Vérifier si expiré et rafraîchir si possible
                    if token_info.is_expired and token_info.refresh_token:
                        return await TokenManager.refresh_linkedin_token(token_info)
                    
                    return token_info
                
                # Fallback sur token global
                return TokenInfo(
                    access_token=Config.COMMUNITY_ACCESS_TOKEN,
                    platform="linkedin",
                    account_id=account_id
                )
                
        except Exception as e:
            logger.error(f"Erreur récupération token LinkedIn: {e}")
            return None
    
    @staticmethod
    async def get_facebook_token(page_id: str) -> Optional[TokenInfo]:
        """Récupérer le token Facebook pour une page"""
        
        try:
            with db_manager.get_session() as session:
                token_record = session.query(SocialAccessToken).filter(
                    SocialAccessToken.platform == "facebook",
                    SocialAccessToken.account_id == page_id,
                    SocialAccessToken.is_active == True
                ).first()
                
                if token_record:
                    return TokenInfo(
                        access_token=token_record.access_token,
                        expires_at=token_record.expires_at,
                        platform="facebook",
                        account_id=page_id
                    )
                    
        except Exception as e:
            logger.error(f"Erreur récupération token Facebook: {e}")
            
        return None
    
    @staticmethod
    async def refresh_linkedin_token(token_info: TokenInfo) -> Optional[TokenInfo]:
        """Rafraîchir un token LinkedIn expiré"""
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                refresh_data = {
                    'grant_type': 'refresh_token',
                    'refresh_token': token_info.refresh_token,
                    'client_id': Config.LINKEDIN_CLIENT_ID,
                    'client_secret': Config.LINKEDIN_CLIENT_SECRET
                }
                
                response = await client.post(
                    'https://www.linkedin.com/oauth/v2/accessToken',
                    data=refresh_data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Mettre à jour en base
                    with db_manager.get_session() as session:
                        token_record = session.query(SocialAccessToken).filter(
                            SocialAccessToken.platform == "linkedin",
                            SocialAccessToken.account_id == token_info.account_id
                        ).first()
                        
                        if token_record:
                            token_record.access_token = data['access_token']
                            token_record.expires_at = datetime.now() + timedelta(seconds=data.get('expires_in', 3600))
                            session.commit()
                    
                    return TokenInfo(
                        access_token=data['access_token'],
                        expires_at=datetime.now() + timedelta(seconds=data.get('expires_in', 3600)),
                        refresh_token=token_info.refresh_token,
                        platform="linkedin",
                        account_id=token_info.account_id
                    )
                    
        except Exception as e:
            logger.error(f"Erreur rafraîchissement token LinkedIn: {e}")
            
        return None

# ========================================
# IMPLÉMENTATIONS API RÉELLES
# ========================================

class LinkedInAPIClient:
    """Client API LinkedIn robuste"""
    
    def __init__(self):
        self.base_url = "https://api.linkedin.com/rest"
        self.timeout = 30
        
    async def get_page_metrics(self, account_id: str, date_obj: date, metrics: List[str]) -> Dict[str, int]:
        """Récupérer les métriques de page LinkedIn"""
        
        token_info = await TokenManager.get_linkedin_token(account_id)
        if not token_info:
            raise ValueError(f"Token LinkedIn manquant pour {account_id}")
        
        result = {}
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            
            # Page Statistics
            if any(m in metrics for m in ['total_page_views', 'unique_page_views', 'desktop_page_views', 'mobile_page_views']):
                try:
                    response = await client.get(
                        f"{self.base_url}/organizationPageStatistics",
                        headers={
                            'Authorization': f'Bearer {token_info.access_token}',
                            'X-Restli-Protocol-Version': '2.0.0',
                            'LinkedIn-Version': '202305'
                        },
                        params={
                            'q': 'organization',
                            'organization': f'urn:li:organization:{account_id}',
                            'timeIntervals.timeGranularityType': 'DAY',
                            'timeIntervals.timeRange.start': int(date_obj.timestamp() * 1000),
                            'timeIntervals.timeRange.end': int((date_obj + timedelta(days=1)).timestamp() * 1000)
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        elements = data.get('elements', [])
                        if elements:
                            stats = elements[0].get('totalPageStatistics', {})
                            
                            # Mapper les métriques
                            if 'total_page_views' in metrics:
                                result['total_page_views'] = self._get_nested_value(stats, 'views.allPageViews.pageViews', 0)
                            if 'unique_page_views' in metrics:
                                result['unique_page_views'] = self._get_nested_value(stats, 'views.allPageViews.uniquePageViews', 0)
                            if 'desktop_page_views' in metrics:
                                result['desktop_page_views'] = self._get_nested_value(stats, 'views.desktopPageViews.pageViews', 0)
                            if 'mobile_page_views' in metrics:
                                result['mobile_page_views'] = self._get_nested_value(stats, 'views.mobilePageViews.pageViews', 0)
                    
                except Exception as e:
                    logger.error(f"Erreur page statistics LinkedIn: {e}")
            
            # Follower Statistics
            if any(m in metrics for m in ['total_followers', 'followers_by_country', 'followers_by_industry']):
                try:
                    response = await client.get(
                        f"{self.base_url}/networkSizes",
                        headers={
                            'Authorization': f'Bearer {token_info.access_token}',
                            'X-Restli-Protocol-Version': '2.0.0'
                        },
                        params={
                            'q': 'viewerConnection',
                            'edgeType': 'COMPANY_FOLLOWED_BY_MEMBER'
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if 'total_followers' in metrics:
                            result['total_followers'] = data.get('firstDegreeSize', 0)
                    
                except Exception as e:
                    logger.error(f"Erreur follower statistics LinkedIn: {e}")
        
        # Remplir les métriques manquantes avec 0
        for metric in metrics:
            if metric not in result:
                result[metric] = 0
                
        return result
    
    async def get_posts(self, account_id: str, start_date: date, end_date: date) -> List[Dict]:
        """Récupérer les posts LinkedIn d'une période"""
        
        token_info = await TokenManager.get_linkedin_token(account_id)
        if not token_info:
            return []
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/posts",
                    headers={
                        'Authorization': f'Bearer {token_info.access_token}',
                        'X-Restli-Protocol-Version': '2.0.0',
                        'LinkedIn-Version': '202305'
                    },
                    params={
                        'q': 'author',
                        'author': f'urn:li:organization:{account_id}',
                        'count': 50,
                        'sortBy': 'LAST_MODIFIED'
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    posts = []
                    
                    for element in data.get('elements', []):
                        created_time = element.get('createdAt', 0)
                        if created_time:
                            post_date = datetime.fromtimestamp(created_time / 1000).date()
                            if start_date <= post_date <= end_date:
                                posts.append({
                                    'id': element.get('id'),
                                    'type': element.get('contentType', 'UNKNOWN'),
                                    'date': post_date,
                                    'created_at': created_time,
                                    'text': self._extract_post_text(element),
                                    'author': element.get('author')
                                })
                    
                    return posts
                    
        except Exception as e:
            logger.error(f"Erreur récupération posts LinkedIn: {e}")
            
        return []
    
    async def get_post_metrics(self, post_id: str, metrics: List[str]) -> Dict[str, int]:
        """Récupérer les métriques d'un post LinkedIn"""
        
        # Extraire l'account_id du post_id si possible
        account_id = post_id.split(':')[-1] if ':' in post_id else ""
        token_info = await TokenManager.get_linkedin_token(account_id)
        
        if not token_info:
            return {metric: 0 for metric in metrics}
        
        result = {}
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/socialActions/{post_id}",
                    headers={
                        'Authorization': f'Bearer {token_info.access_token}',
                        'X-Restli-Protocol-Version': '2.0.0'
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Mapper les métriques disponibles
                    metric_mapping = {
                        'post_impressions': 'impressionCount',
                        'post_clicks': 'clickCount',
                        'post_shares': 'shareCount',
                        'post_comments': 'commentCount',
                        'reactions_like': 'likeCount',
                        'total_reactions': 'reactionCount'
                    }
                    
                    for metric in metrics:
                        api_field = metric_mapping.get(metric)
                        if api_field:
                            result[metric] = data.get(api_field, 0)
                        else:
                            result[metric] = 0
                            
        except Exception as e:
            logger.error(f"Erreur métriques post LinkedIn {post_id}: {e}")
            result = {metric: 0 for metric in metrics}
        
        return result
    
    def _get_nested_value(self, data: dict, path: str, default=0):
        """Récupérer une valeur dans un dictionnaire imbriqué"""
        try:
            current = data
            for key in path.split('.'):
                current = current[key]
            return current
        except (KeyError, TypeError):
            return default
    
    def _extract_post_text(self, post_element: dict) -> str:
        """Extraire le texte d'un post LinkedIn"""
        try:
            commentary = post_element.get('commentary')
            if commentary:
                return commentary.get('text', '')
        except:
            pass
        return ''

class FacebookAPIClient:
    """Client API Facebook robuste"""
    
    def __init__(self):
        self.base_url = "https://graph.facebook.com/v21.0"
        self.timeout = 30
        
    async def get_page_metrics(self, page_id: str, date_obj: date, metrics: List[str]) -> Dict[str, int]:
        """Récupérer les métriques de page Facebook"""
        
        token_info = await TokenManager.get_facebook_token(page_id)
        if not token_info:
            raise ValueError(f"Token Facebook manquant pour {page_id}")
        
        result = {}
        
        # Mapper les métriques aux noms Facebook
        facebook_metrics = {
            'page_impressions': 'page_impressions',
            'page_impressions_unique': 'page_impressions_unique',
            'page_impressions_viral': 'page_impressions_viral',
            'page_fans': 'page_fans',
            'page_fan_adds': 'page_fan_adds',
            'page_fan_removes': 'page_fan_removes',
            'page_views_total': 'page_views_total',
            'page_post_engagements': 'page_post_engagements'
        }
        
        # Grouper les métriques pour minimiser les appels API
        fb_metrics_to_fetch = []
        for metric in metrics:
            if metric in facebook_metrics:
                fb_metrics_to_fetch.append(facebook_metrics[metric])
        
        if not fb_metrics_to_fetch:
            return {metric: 0 for metric in metrics}
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/{page_id}/insights",
                    params={
                        'metric': ','.join(fb_metrics_to_fetch),
                        'period': 'day',
                        'since': date_obj.strftime('%Y-%m-%d'),
                        'until': (date_obj + timedelta(days=1)).strftime('%Y-%m-%d'),
                        'access_token': token_info.access_token
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    insights_data = data.get('data', [])
                    
                    # Parser les résultats
                    for insight in insights_data:
                        metric_name = insight.get('name')
                        values = insight.get('values', [])
                        
                        if values and metric_name:
                            # Trouver la métrique correspondante
                            for original_metric, fb_metric in facebook_metrics.items():
                                if fb_metric == metric_name and original_metric in metrics:
                                    result[original_metric] = values[0].get('value', 0)
                                    
        except Exception as e:
            logger.error(f"Erreur métriques page Facebook: {e}")
        
        # Remplir les métriques manquantes
        for metric in metrics:
            if metric not in result:
                result[metric] = 0
                
        return result
    
    async def get_posts(self, page_id: str, start_date: date, end_date: date) -> List[Dict]:
        """Récupérer les posts Facebook d'une période"""
        
        token_info = await TokenManager.get_facebook_token(page_id)
        if not token_info:
            return []
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/{page_id}/posts",
                    params={
                        'fields': 'id,message,created_time,type,story,permalink_url,attachments',
                        'since': start_date.strftime('%Y-%m-%d'),
                        'until': end_date.strftime('%Y-%m-%d'),
                        'limit': 100,
                        'access_token': token_info.access_token
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    posts = []
                    
                    for post in data.get('data', []):
                        created_time = post.get('created_time')
                        if created_time:
                            post_date = datetime.fromisoformat(created_time.replace('Z', '+00:00')).date()
                            
                            posts.append({
                                'id': post.get('id'),
                                'type': post.get('type', 'status'),
                                'date': post_date,
                                'message': post.get('message', ''),
                                'story': post.get('story', ''),
                                'permalink_url': post.get('permalink_url')
                            })
                    
                    return posts
                    
        except Exception as e:
            logger.error(f"Erreur récupération posts Facebook: {e}")
            
        return []
    
    async def get_post_metrics(self, post_id: str, metrics: List[str]) -> Dict[str, int]:
        """Récupérer les métriques d'un post Facebook"""
        
        page_id = post_id.split('_')[0]
        token_info = await TokenManager.get_facebook_token(page_id)
        
        if not token_info:
            return {metric: 0 for metric in metrics}
        
        facebook_post_metrics = {
            'post_impressions': 'post_impressions',
            'post_impressions_unique': 'post_impressions_unique',
            'post_clicks': 'post_clicks',
            'post_reactions_like_total': 'post_reactions_like_total',
            'post_reactions_love_total': 'post_reactions_love_total',
            'post_reactions_wow_total': 'post_reactions_wow_total'
        }
        
        result = {}
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Récupérer les métriques disponibles
                fb_metrics = [facebook_post_metrics[m] for m in metrics if m in facebook_post_metrics]
                
                if fb_metrics:
                    response = await client.get(
                        f"{self.base_url}/{post_id}/insights",
                        params={
                            'metric': ','.join(fb_metrics),
                            'access_token': token_info.access_token
                        }
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        insights_data = data.get('data', [])
                        
                        for insight in insights_data:
                            metric_name = insight.get('name')
                            values = insight.get('values', [])
                            
                            if values and metric_name:
                                for original_metric, fb_metric in facebook_post_metrics.items():
                                    if fb_metric == metric_name and original_metric in metrics:
                                        result[original_metric] = values[0].get('value', 0)
                        
        except Exception as e:
            logger.error(f"Erreur métriques post Facebook {post_id}: {e}")
        
        # Remplir les métriques manquantes
        for metric in metrics:
            if metric not in result:
                result[metric] = 0
                
        return result

# ========================================
# ENDPOINTS PRINCIPAUX
# ========================================

@router.post("/check-user-looker")
@rate_limit(calls_per_minute=100)
async def check_user_looker(request: Request):
    """Vérification robuste de l'accès utilisateur au connecteur Looker"""
    
    try:
        data = await request.json()
        looker_request = LookerAuthRequest(**data)
        
        # Import du mapping avec gestion d'erreur
        try:
            from app.utils.connector_mapping import STRIPE_TO_CONNECTOR_MAPPING
        except ImportError:
            logger.error("Mapping connecteur non trouvé")
            return JSONResponse(
                status_code=500,
                content={"valid": False, "error": "Configuration connecteur manquante"}
            )
        
        # Validation du connecteur
        user_plan_info = None
        for price_id, plan_info in STRIPE_TO_CONNECTOR_MAPPING.items():
            if plan_info['connector_id'] == looker_request.connector_id:
                user_plan_info = plan_info
                break
        
        if not user_plan_info:
            logger.warning(f"Connecteur inconnu: {looker_request.connector_id}")
            return JSONResponse(
                status_code=400,
                content={
                    "valid": False,
                    "error": "INVALID_CONNECTOR",
                    "message": "Ce connecteur n'est pas reconnu",
                    "connector_id": looker_request.connector_id
                }
            )
        
        # Récupération utilisateur avec vérifications complètes
        with db_manager.get_session() as session:
            user = session.query(User).filter(User.email == looker_request.email).first()
            
            if not user:
                redirect_url = f"{Config.BASE_URL}/connect?source=looker&email={looker_request.email}&connector={looker_request.connector_id}"
                return JSONResponse(
                    status_code=404,
                    content={
                        "valid": False,
                        "error": "USER_NOT_FOUND",
                        "message": "Utilisateur non trouvé",
                        "action": "signup",
                        "redirect_url": redirect_url
                    }
                )
            
            if not user.is_active:
                return JSONResponse(
                    status_code=403,
                    content={
                        "valid": False,
                        "error": "ACCOUNT_DISABLED",
                        "message": "Compte désactivé",
                        "action": "contact_support"
                    }
                )
            
            # Vérification du plan
            if user.plan_type != user_plan_info['price_id']:
                redirect_url = f"{Config.BASE_URL}/connect/plans?source=looker&email={looker_request.email}&connector={looker_request.connector_id}"
                return JSONResponse(
                    status_code=403,
                    content={
                        "valid": False,
                        "error": "PLAN_INCOMPATIBLE",
                        "message": f"Plan incompatible. Requis: {user_plan_info['name']}",
                        "current_plan": user.plan_type,
                        "required_plan": user_plan_info['name'],
                        "action": "upgrade",
                        "redirect_url": redirect_url
                    }
                )
            
            # Vérification expiration abonnement
            if user.subscription_end_date and user.subscription_end_date < datetime.now().date():
                redirect_url = f"{Config.BASE_URL}/connect/plans?source=looker&email={looker_request.email}&action=renew"
                return JSONResponse(
                    status_code=403,
                    content={
                        "valid": False,
                        "error": "SUBSCRIPTION_EXPIRED",
                        "message": "Abonnement expiré",
                        "expired_date": user.subscription_end_date.isoformat(),
                        "action": "renew",
                        "redirect_url": redirect_url
                    }
                )
            
            # Vérification des comptes connectés
            platforms_missing = []
            for platform in user_plan_info['platforms']:
                if platform == 'linkedin':
                    linkedin_count = session.query(LinkedinAccount).filter(
                        LinkedinAccount.user_id == user.id,
                        LinkedinAccount.is_active == True
                    ).count()
                    if linkedin_count == 0:
                        platforms_missing.append('linkedin')
                        
                elif platform == 'facebook':
                    facebook_count = session.query(FacebookAccount).filter(
                        FacebookAccount.user_id == user.id,
                        FacebookAccount.is_active == True
                    ).count()
                    if facebook_count == 0:
                        platforms_missing.append('facebook')
            
            warnings = []
            if platforms_missing:
                warnings.append(f"Plateformes non connectées: {', '.join(platforms_missing)}")
            
            # Succès
            return JSONResponse(
                status_code=200,
                content={
                    "valid": True,
                    "user": {
                        "id": user.id,
                        "email": user.email,
                        "plan_type": user.plan_type,
                        "plan_name": user_plan_info['name'],
                        "platforms_accessible": user_plan_info['platforms'],
                        "subscription_end": user.subscription_end_date.isoformat() if user.subscription_end_date else None
                    },
                    "warnings": warnings
                }
            )
            
    except Exception as e:
        logger.error(f"Erreur check_user_looker: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "valid": False,
                "error": "INTERNAL_ERROR",
                "message": "Erreur interne du serveur"
            }
        )

@router.post("/combined/metrics")
@rate_limit(calls_per_minute=30)
async def get_combined_metrics(request: Request):
    """Endpoint principal pour récupérer les métriques combinées avec gestion complète des erreurs"""
    
    start_time = time.time()
    
    try:
        # Récupération et validation de l'email
        auth_header = request.headers.get('authorization', '')
        if auth_header.startswith('Bearer '):
            user_email = auth_header.replace('Bearer ', '')
        else:
            return JSONResponse(
                status_code=401,
                content={"success": False, "error": "TOKEN_MISSING", "message": "Token d'autorisation manquant"}
            )
        
        # Validation des données d'entrée
        try:
            data = await request.json()
            looker_request = LookerDataRequest(**data)
        except Exception as e:
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "INVALID_REQUEST", "message": f"Données de requête invalides: {str(e)}"}
            )
        
        logger.info(f"Requête combined/metrics pour {user_email}: platforms={looker_request.platforms}, range={looker_request.date_range}j")
        
        # Vérification utilisateur
        with db_manager.get_session() as session:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "USER_NOT_FOUND", "message": "Utilisateur non trouvé"}
                )
            
            if not user.is_active:
                return JSONResponse(
                    status_code=403,
                    content={"success": False, "error": "ACCOUNT_DISABLED", "message": "Compte désactivé"}
                )
        
        # Calcul des dates
        if looker_request.start_date and looker_request.end_date:
            start_date = datetime.strptime(looker_request.start_date, '%Y-%m-%d').date()
            end_date = datetime.strptime(looker_request.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=looker_request.date_range)
        
        # Validation de la plage de dates
        if start_date > end_date:
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "INVALID_DATE_RANGE", "message": "La date de début doit être antérieure à la date de fin"}
            )
        
        if (end_date - start_date).days > 365:
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "DATE_RANGE_TOO_LARGE", "message": "La plage de dates ne peut pas dépasser 365 jours"}
            )
        
        # Initialisation des clients API
        linkedin_client = LinkedInAPIClient()
        facebook_client = FacebookAPIClient()
        
        # Préparer la réponse
        result_data = {
            "success": True,
            "data": {},
            "generated_at": datetime.now().isoformat(),
            "user_email": user_email,
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "days": (end_date - start_date).days + 1
            },
            "request_params": {
                "platforms": [p.value for p in looker_request.platforms],
                "metrics_type": looker_request.metrics_type.value,
                "include_reactions": looker_request.include_linkedin_reactions or looker_request.include_facebook_reactions,
                "include_video": looker_request.include_video_metrics,
                "include_breakdown": looker_request.include_breakdown
            },
            "errors": [],
            "warnings": []
        }
        
        total_records = 0
        
        # Récupération données LinkedIn
        if PlatformType.LINKEDIN in looker_request.platforms or PlatformType.BOTH in looker_request.platforms:
            logger.info(f"Récupération données LinkedIn pour {user_email}")
            
            try:
                linkedin_data = await get_real_linkedin_data(
                    user, linkedin_client, start_date, end_date, 
                    looker_request.metrics_type.value, looker_request.include_linkedin_reactions
                )
                result_data["data"]["linkedin_data"] = linkedin_data
                
                # Compter les enregistrements
                for category in linkedin_data.values():
                    if isinstance(category, list):
                        total_records += len(category)
                        
            except Exception as e:
                error_msg = f"Erreur récupération LinkedIn: {str(e)}"
                logger.error(error_msg)
                result_data["errors"].append(error_msg)
                result_data["data"]["linkedin_data"] = {"error": error_msg}
        
        # Récupération données Facebook
        if PlatformType.FACEBOOK in looker_request.platforms or PlatformType.BOTH in looker_request.platforms:
            logger.info(f"Récupération données Facebook pour {user_email}")
            
            try:
                facebook_data = await get_real_facebook_data(
                    user, facebook_client, start_date, end_date, 
                    looker_request.metrics_type.value, looker_request.include_facebook_reactions
                )
                result_data["data"]["facebook_data"] = facebook_data
                
                # Compter les enregistrements
                for category in facebook_data.values():
                    if isinstance(category, list):
                        total_records += len(category)
                        
            except Exception as e:
                error_msg = f"Erreur récupération Facebook: {str(e)}"
                logger.error(error_msg)
                result_data["errors"].append(error_msg)
                result_data["data"]["facebook_data"] = {"error": error_msg}
        
        # Métadonnées de performance
        execution_time = time.time() - start_time
        result_data["total_records"] = total_records
        result_data["execution_time"] = round(execution_time, 3)
        
        # Générer un résumé des plateformes
        platform_summaries = {}
        for platform in ["linkedin", "facebook"]:
            if f"{platform}_data" in result_data["data"] and "error" not in result_data["data"][f"{platform}_data"]:
                platform_data = result_data["data"][f"{platform}_data"]
                platform_summaries[platform] = {
                    "available": True,
                    "page_metrics_count": len(platform_data.get("page_metrics", [])),
                    "post_metrics_count": len(platform_data.get("post_metrics", [])),
                    "total_metrics": sum(len(v) for v in platform_data.values() if isinstance(v, list))
                }
            else:
                platform_summaries[platform] = {"available": False}
        
        result_data["platform_summaries"] = platform_summaries
        
        # Déterminer le statut de réponse
        if result_data["errors"]:
            if total_records == 0:
                status_code = 503  # Service indisponible
            else:
                status_code = 207  # Multi-status (succès partiel)
        else:
            status_code = 200
        
        logger.info(f"Données générées pour {user_email}: {total_records} enregistrements en {execution_time:.3f}s")
        
        return JSONResponse(status_code=status_code, content=result_data)
        
    except Exception as e:
        logger.error(f"Erreur inattendue combined_metrics: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "UNEXPECTED_ERROR",
                "message": "Erreur interne du serveur",
                "execution_time": round(time.time() - start_time, 3)
            }
        )

# ========================================
# FONCTIONS DE RÉCUPÉRATION DE DONNÉES
# ========================================

async def get_real_linkedin_data(user: User, linkedin_client: LinkedInAPIClient, 
                                 start_date: date, end_date: date, 
                                 metrics_type: str, include_reactions: bool) -> Dict:
    """Récupération complète des données LinkedIn avec gestion d'erreurs"""
    
    data = {
        "page_metrics": [],
        "post_metrics": [],
        "follower_metrics": [],
        "breakdown_data": []
    }
    
    try:
        # Récupérer les comptes LinkedIn
        with db_manager.get_session() as session:
            linkedin_accounts = session.query(LinkedinAccount).filter(
                LinkedinAccount.user_id == user.id,
                LinkedinAccount.is_active == True
            ).all()
        
        if not linkedin_accounts:
            logger.warning(f"Aucun compte LinkedIn trouvé pour {user.email}")
            return data
        
        # Initialiser les métriques selon le type demandé
        metrics_manager = MetricsManager()
        linkedin_metrics_obj = metrics_manager.get_platform_metrics('linkedin')
        
        if not linkedin_metrics_obj:
            raise Exception("Module LinkedIn Metrics non disponible")
        
        # Définir les métriques à récupérer
        page_metrics_list = linkedin_metrics_obj.get_page_metrics() if metrics_type in ['overview', 'pages'] else []
        post_metrics_list = linkedin_metrics_obj.get_post_metrics() if metrics_type in ['overview', 'posts'] else []
        
        # Récupération des métriques de pages
        if page_metrics_list:
            for account in linkedin_accounts:
                logger.info(f"Récupération métriques page LinkedIn {account.organization_id}")
                
                # Pour chaque jour de la période
                current_date = start_date
                while current_date <= end_date:
                    try:
                        page_metrics = await linkedin_client.get_page_metrics(
                            account.organization_id, current_date, page_metrics_list
                        )
                        
                        page_data = {
                            "date": current_date.strftime("%Y-%m-%d"),
                            "account_name": account.organization_name or f"LinkedIn {account.organization_id}",
                            "account_id": account.organization_id,
                            "platform": "linkedin"
                        }
                        
                        # Ajouter les métriques avec préfixe linkedin_
                        for metric, value in page_metrics.items():
                            page_data[f"linkedin_{metric}"] = value
                        
                        data["page_metrics"].append(page_data)
                        
                    except Exception as e:
                        logger.error(f"Erreur métriques page LinkedIn {account.organization_id} pour {current_date}: {e}")
                    
                    current_date += timedelta(days=1)
        
        # Récupération des métriques de posts
        if post_metrics_list and include_reactions:
            for account in linkedin_accounts:
                logger.info(f"Récupération posts LinkedIn {account.organization_id}")
                
                try:
                    posts = await linkedin_client.get_posts(account.organization_id, start_date, end_date)
                    
                    for post in posts:
                        try:
                            post_metrics = await linkedin_client.get_post_metrics(post['id'], post_metrics_list)
                            
                            post_data = {
                                "post_id": post['id'],
                                "post_type": post['type'],
                                "post_creation_date": post.get('created_at', ''),
                                "post_text": post.get('text', ''),
                                "account_name": account.organization_name,
                                "account_id": account.organization_id,
                                "platform": "linkedin",
                                "date": post['date'].strftime("%Y-%m-%d")
                            }
                            
                            # Ajouter les métriques avec préfixe
                            for metric, value in post_metrics.items():
                                post_data[f"linkedin_{metric}"] = value
                            
                            data["post_metrics"].append(post_data)
                            
                        except Exception as e:
                            logger.error(f"Erreur métriques post LinkedIn {post['id']}: {e}")
                
                except Exception as e:
                    logger.error(f"Erreur récupération posts LinkedIn {account.organization_id}: {e}")
        
        logger.info(f"LinkedIn data récupérée: {len(data['page_metrics'])} pages, {len(data['post_metrics'])} posts")
        
    except Exception as e:
        logger.error(f"Erreur globale récupération LinkedIn: {e}")
        raise
    
    return data

async def get_real_facebook_data(user: User, facebook_client: FacebookAPIClient,
                                start_date: date, end_date: date,
                                metrics_type: str, include_reactions: bool) -> Dict:
    """Récupération complète des données Facebook avec gestion d'erreurs"""
    
    data = {
        "page_metrics": [],
        "post_metrics": [],
        "fan_metrics": [],
        "video_metrics": []
    }
    
    try:
        # Récupérer les comptes Facebook
        with db_manager.get_session() as session:
            facebook_accounts = session.query(FacebookAccount).filter(
                FacebookAccount.user_id == user.id,
                FacebookAccount.is_active == True
            ).all()
        
        if not facebook_accounts:
            logger.warning(f"Aucun compte Facebook trouvé pour {user.email}")
            return data
        
        # Initialiser les métriques
        metrics_manager = MetricsManager()
        facebook_metrics_obj = metrics_manager.get_platform_metrics('facebook')
        
        if not facebook_metrics_obj:
            raise Exception("Module Facebook Metrics non disponible")
        
        # Définir les métriques à récupérer
        page_metrics_list = facebook_metrics_obj.get_page_metrics() if metrics_type in ['overview', 'pages'] else []
        post_metrics_list = facebook_metrics_obj.get_post_metrics() if metrics_type in ['overview', 'posts'] else []
        
        # Récupération des métriques de pages
        if page_metrics_list:
            for account in facebook_accounts:
                logger.info(f"Récupération métriques page Facebook {account.page_id}")
                
                # Pour chaque jour de la période
                current_date = start_date
                while current_date <= end_date:
                    try:
                        page_metrics = await facebook_client.get_page_metrics(
                            account.page_id, current_date, page_metrics_list
                        )
                        
                        page_data = {
                            "date": current_date.strftime("%Y-%m-%d"),
                            "account_name": account.page_name or f"Facebook {account.page_id}",
                            "account_id": account.page_id,
                            "platform": "facebook"
                        }
                        
                        # Ajouter les métriques avec préfixe facebook_
                        for metric, value in page_metrics.items():
                            page_data[f"facebook_{metric}"] = value
                        
                        data["page_metrics"].append(page_data)
                        
                    except Exception as e:
                        logger.error(f"Erreur métriques page Facebook {account.page_id} pour {current_date}: {e}")
                    
                    current_date += timedelta(days=1)
        
        # Récupération des métriques de posts
        if post_metrics_list and include_reactions:
            for account in facebook_accounts:
                logger.info(f"Récupération posts Facebook {account.page_id}")
                
                try:
                    posts = await facebook_client.get_posts(account.page_id, start_date, end_date)
                    
                    for post in posts:
                        try:
                            post_metrics = await facebook_client.get_post_metrics(post['id'], post_metrics_list)
                            
                            post_data = {
                                "post_id": post['id'],
                                "post_type": post['type'],
                                "post_creation_date": post['date'].strftime("%Y-%m-%d"),
                                "post_text": post.get('message', ''),
                                "account_name": account.page_name,
                                "account_id": account.page_id,
                                "platform": "facebook",
                                "date": post['date'].strftime("%Y-%m-%d")
                            }
                            
                            # Ajouter les métriques avec préfixe
                            for metric, value in post_metrics.items():
                                post_data[f"facebook_{metric}"] = value
                            
                            data["post_metrics"].append(post_data)
                            
                        except Exception as e:
                            logger.error(f"Erreur métriques post Facebook {post['id']}: {e}")
                
                except Exception as e:
                    logger.error(f"Erreur récupération posts Facebook {account.page_id}: {e}")
        
        logger.info(f"Facebook data récupérée: {len(data['page_metrics'])} pages, {len(data['post_metrics'])} posts")
        
    except Exception as e:
        logger.error(f"Erreur globale récupération Facebook: {e}")
        raise
    
    return data

# ========================================
# ENDPOINTS UTILITAIRES
# ========================================

@router.get("/health")
async def health_check():
    """Health check complet du service"""
    
    start_time = time.time()
    
    try:
        # Test base de données
        db_status = "connected" if db_manager.test_connection() else "disconnected"
        
        # Test des APIs externes (rapide)
        api_status = {}
        
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                # Test LinkedIn (ping simple)
                linkedin_response = await client.get("https://api.linkedin.com", timeout=3)
                api_status["linkedin"] = "reachable" if linkedin_response.status_code < 500 else "unreachable"
        except:
            api_status["linkedin"] = "unreachable"
        
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                # Test Facebook (ping simple)
                facebook_response = await client.get("https://graph.facebook.com", timeout=3)
                api_status["facebook"] = "reachable" if facebook_response.status_code < 500 else "unreachable"
        except:
            api_status["facebook"] = "unreachable"
        
        execution_time = time.time() - start_time
        
        return {
            'status': 'healthy',
            'service': 'WhatsTheData Looker Connector',
            'timestamp': datetime.now().isoformat(),
            'database': db_status,
            'external_apis': api_status,
            'execution_time': round(execution_time, 3),
            'version': Config.APP_VERSION if hasattr(Config, 'APP_VERSION') else '1.0.0'
        }
        
    except Exception as e:
        logger.error(f"Erreur health check: {e}")
        return JSONResponse(
            status_code=503,
            content={
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
        )

@router.get("/test-data")
async def get_test_data():
    """Endpoint de test avec données réalistes"""
    
    return {
        "success": True,
        "data": [
            {
                "platform": "linkedin",
                "date": "2025-08-22",
                "account_name": "Test LinkedIn",
                "account_id": "12345",
                "linkedin_total_page_views": 1500,
                "linkedin_unique_page_views": 1200,
                "linkedin_total_followers": 2800,
                "linkedin_post_impressions": 3200,
                "linkedin_reactions_like": 45,
                "linkedin_reactions_celebrate": 12
            },
            {
                "platform": "facebook", 
                "date": "2025-08-22",
                "account_name": "Test Facebook",
                "account_id": "67890",
                "facebook_page_impressions": 4500,
                "facebook_page_fans": 5200,
                "facebook_post_impressions": 2800,
                "facebook_post_reactions_like_total": 85,
                "facebook_post_clicks": 120
            }
        ],
        "total_records": 2,
        "generated_at": datetime.now().isoformat(),
        "status": "test_data"
    }

@router.get("/validate-token-simple")
async def validate_token_simple():
    """Validation simplifiée pour tests"""
    
    return {
        'valid': True,
        'user_id': 1,
        'email': 'test@whatsthedata.com',
        'plan_type': 'premium',
        'subscription_active': True,
        'platforms_available': ['linkedin', 'facebook']
    }

# ========================================
# ENDPOINT DE DIAGNOSTIC
# ========================================

@router.get("/diagnostics/{user_email}")
async def get_user_diagnostics(user_email: str):
    """Diagnostic complet pour un utilisateur (debug)"""
    
    try:
        diagnostics = {
            "user_email": user_email,
            "timestamp": datetime.now().isoformat(),
            "user_exists": False,
            "linkedin_accounts": [],
            "facebook_accounts": [],
            "tokens": {},
            "api_connectivity": {}
        }
        
        # Vérifier utilisateur
        with db_manager.get_session() as session:
            user = session.query(User).filter(User.email == user_email).first()
            
            if user:
                diagnostics["user_exists"] = True
                diagnostics["user_id"] = user.id
                diagnostics["plan_type"] = user.plan_type
                diagnostics["is_active"] = user.is_active
                
                # Comptes LinkedIn
                linkedin_accounts = session.query(LinkedinAccount).filter(
                    LinkedinAccount.user_id == user.id
                ).all()
                
                for account in linkedin_accounts:
                    diagnostics["linkedin_accounts"].append({
                        "organization_id": account.organization_id,
                        "organization_name": account.organization_name,
                        "is_active": account.is_active
                    })
                
                # Comptes Facebook
                facebook_accounts = session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user.id
                ).all()
                
                for account in facebook_accounts:
                    diagnostics["facebook_accounts"].append({
                        "page_id": account.page_id,
                        "page_name": account.page_name,
                        "is_active": account.is_active
                    })
        
        # Test connectivité APIs
        if diagnostics["user_exists"]:
            try:
                api_test = await test_api_connections(user_email)
                diagnostics["api_connectivity"] = api_test
            except Exception as e:
                diagnostics["api_connectivity"]["error"] = str(e)
        
        return diagnostics
        
    except Exception as e:
        logger.error(f"Erreur diagnostics pour {user_email}: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "user_email": user_email}
        )

async def test_api_connections(user_email: str) -> Dict[str, Any]:
    """Test de connectivité des APIs pour un utilisateur"""
    
    results = {
        'linkedin': {'reachable': False, 'authenticated': False, 'error': None},
        'facebook': {'reachable': False, 'authenticated': False, 'error': None}
    }
    
    try:
        with db_manager.get_session() as session:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                return results
            
            # Test LinkedIn
            linkedin_account = session.query(LinkedinAccount).filter(
                LinkedinAccount.user_id == user.id,
                LinkedinAccount.is_active == True
            ).first()
            
            if linkedin_account:
                try:
                    linkedin_client = LinkedInAPIClient()
                    test_metrics = await linkedin_client.get_page_metrics(
                        linkedin_account.organization_id, 
                        date.today(), 
                        ['total_page_views']
                    )
                    results['linkedin']['reachable'] = True
                    results['linkedin']['authenticated'] = 'total_page_views' in test_metrics
                except Exception as e:
                    results['linkedin']['error'] = str(e)
            
            # Test Facebook
            facebook_account = session.query(FacebookAccount).filter(
                FacebookAccount.user_id == user.id,
                FacebookAccount.is_active == True
            ).first()
            
            if facebook_account:
                try:
                    facebook_client = FacebookAPIClient()
                    test_metrics = await facebook_client.get_page_metrics(
                        facebook_account.page_id,
                        date.today(),
                        ['page_impressions']
                    )
                    results['facebook']['reachable'] = True
                    results['facebook']['authenticated'] = 'page_impressions' in test_metrics
                except Exception as e:
                    results['facebook']['error'] = str(e)
                    
    except Exception as e:
        logger.error(f"Erreur test API connections: {e}")
    
    return results

@router.get("/test-data-extended")
async def get_test_data_extended():
    """Endpoint de test avec toutes les métriques LinkedIn + Facebook pour template Looker Studio"""
    
    from datetime import datetime, timedelta
    import random
    
    try:
        # Imports locaux dans la fonction pour éviter les conflits au niveau module
        from app.utils.metrics.linkedin_metrics import LinkedInMetrics
        from app.utils.metrics.facebook_metrics import FacebookMetrics
        
        linkedin_metrics = LinkedInMetrics()
        facebook_metrics = FacebookMetrics()
    except Exception as e:
        logger.error(f"Erreur import metrics: {e}")
        return {
            "success": False,
            "error": f"Import error: {str(e)}",
            "generated_at": datetime.now().isoformat()
        }
    
    # Générer 30 jours de données
    base_date = datetime.now().date()
    data = {
        "success": True,
        "data": {
            "linkedin_data": {
                "page_metrics": [],
                "post_metrics": []
            },
            "facebook_data": {
                "page_metrics": [],
                "post_metrics": []
            }
        },
        "generated_at": datetime.now().isoformat(),
        "user_email": "test@whatsthedata.com",
        "total_records": 0,
        "status": "test_data_extended"
    }
    
    # Données LinkedIn - Page Metrics (30 jours)
    linkedin_page_metrics_list = linkedin_metrics.get_page_metrics()
    for i in range(30):
        current_date = base_date - timedelta(days=i)
        page_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "platform": "linkedin",
            "account_name": "Test LinkedIn Company",
            "account_id": "12345678"
        }
        
        # Ajouter toutes les métriques LinkedIn page avec valeurs réalistes
        for metric in linkedin_page_metrics_list:
            if "percentage" in metric:
                page_data[f"linkedin_{metric}"] = round(random.uniform(10, 40), 2)
            elif "followers" in metric:
                page_data[f"linkedin_{metric}"] = random.randint(2000, 5000) + i * 10
            elif "views" in metric:
                page_data[f"linkedin_{metric}"] = random.randint(100, 2000)
            elif "clicks" in metric:
                page_data[f"linkedin_{metric}"] = random.randint(10, 200)
            else:
                page_data[f"linkedin_{metric}"] = random.randint(5, 500)
        
        data["data"]["linkedin_data"]["page_metrics"].append(page_data)
    
    # Données LinkedIn - Post Metrics (10 posts factices)
    linkedin_post_metrics_list = linkedin_metrics.get_post_metrics()
    for i in range(10):
        current_date = base_date - timedelta(days=i*3)
        post_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "platform": "linkedin",
            "account_name": "Test LinkedIn Company",
            "account_id": "12345678",
            "post_id": f"linkedin_post_{i}",
            "post_type": "ugcPost",
            "post_creation_date": current_date.strftime("%Y-%m-%d"),
            "post_text": f"Test LinkedIn post #{i+1}"
        }
        
        # Ajouter toutes les métriques LinkedIn post
        for metric in linkedin_post_metrics_list:
            if "percentage" in metric:
                post_data[f"linkedin_{metric}"] = round(random.uniform(5, 25), 2)
            elif "reactions" in metric:
                post_data[f"linkedin_{metric}"] = random.randint(1, 50)
            elif "impressions" in metric:
                post_data[f"linkedin_{metric}"] = random.randint(500, 5000)
            elif "clicks" in metric:
                post_data[f"linkedin_{metric}"] = random.randint(10, 100)
            else:
                post_data[f"linkedin_{metric}"] = random.randint(1, 200)
        
        data["data"]["linkedin_data"]["post_metrics"].append(post_data)
    
    # Données Facebook - Page Metrics (30 jours)
    facebook_page_metrics_list = facebook_metrics.get_page_metrics()
    for i in range(30):
        current_date = base_date - timedelta(days=i)
        page_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "platform": "facebook",
            "account_name": "Test Facebook Page",
            "account_id": "98765432"
        }
        
        # Ajouter toutes les métriques Facebook page
        for metric in facebook_page_metrics_list:
            if "fans" in metric:
                page_data[f"facebook_{metric}"] = random.randint(3000, 8000) + i * 15
            elif "impressions" in metric:
                page_data[f"facebook_{metric}"] = random.randint(1000, 10000)
            elif "views" in metric:
                page_data[f"facebook_{metric}"] = random.randint(200, 3000)
            elif "video" in metric:
                page_data[f"facebook_{metric}"] = random.randint(50, 1500)
            else:
                page_data[f"facebook_{metric}"] = random.randint(10, 800)
        
        data["data"]["facebook_data"]["page_metrics"].append(page_data)
    
    # Données Facebook - Post Metrics (10 posts factices)
    facebook_post_metrics_list = facebook_metrics.get_post_metrics()
    for i in range(10):
        current_date = base_date - timedelta(days=i*3)
        post_data = {
            "date": current_date.strftime("%Y-%m-%d"),
            "platform": "facebook",
            "account_name": "Test Facebook Page",
            "account_id": "98765432",
            "post_id": f"98765432_{i}",
            "post_type": "status",
            "post_creation_date": current_date.strftime("%Y-%m-%d"),
            "post_text": f"Test Facebook post #{i+1}"
        }
        
        # Ajouter toutes les métriques Facebook post
        for metric in facebook_post_metrics_list:
            if "impressions" in metric:
                post_data[f"facebook_{metric}"] = random.randint(800, 8000)
            elif "reactions" in metric:
                post_data[f"facebook_{metric}"] = random.randint(2, 80)
            elif "clicks" in metric:
                post_data[f"facebook_{metric}"] = random.randint(5, 150)
            elif "video" in metric:
                post_data[f"facebook_{metric}"] = random.randint(20, 2000)
            else:
                post_data[f"facebook_{metric}"] = random.randint(1, 300)
        
        data["data"]["facebook_data"]["post_metrics"].append(post_data)
    
    # Calculer total records
    total_records = (
        len(data["data"]["linkedin_data"]["page_metrics"]) +
        len(data["data"]["linkedin_data"]["post_metrics"]) +
        len(data["data"]["facebook_data"]["page_metrics"]) +
        len(data["data"]["facebook_data"]["post_metrics"])
    )
    data["total_records"] = total_records
    
    return data



@router.get("/debug-test")
def debug_test():
    return {"message": "Module loaded successfully"}