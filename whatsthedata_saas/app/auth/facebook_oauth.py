"""
Gestionnaire OAuth Facebook complet et robuste
Gère l'authentification Facebook avec Graph API v21.0
Avec gestion d'erreurs exhaustive, retry automatique, et validation complète
"""

import os
import json
import time
import logging
import secrets
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlencode, parse_qs, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .user_manager import user_manager, UserManager
from ..database.connection import db_manager
from ..database.models import User, SocialAccessToken, FacebookAccount
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

class FacebookScope(Enum):
    """Scopes Facebook disponibles pour les pages"""
    PAGES_READ_ENGAGEMENT = "pages_read_engagement"
    PAGES_SHOW_LIST = "pages_show_list"
    READ_INSIGHTS = "read_insights"
    PAGES_READ_USER_CONTENT = "pages_read_user_content"
    PAGES_MANAGE_POSTS = "pages_manage_posts"
    PAGES_MANAGE_ENGAGEMENT = "pages_manage_engagement"
    PAGES_MANAGE_METADATA = "pages_manage_metadata"
    PUBLIC_PROFILE = "public_profile"
    EMAIL = "email"

class FacebookTokenType(Enum):
    """Types de tokens Facebook"""
    USER_ACCESS_TOKEN = "user_access_token"
    PAGE_ACCESS_TOKEN = "page_access_token"
    LONG_LIVED_USER_TOKEN = "long_lived_user_token"
    LONG_LIVED_PAGE_TOKEN = "long_lived_page_token"

class FacebookApiVersion(Enum):
    """Versions de l'API Facebook Graph supportées"""
    V21_0 = "v21.0"
    V20_0 = "v20.0"
    V19_0 = "v19.0"

@dataclass
class FacebookAppConfig:
    """Configuration de l'application Facebook"""
    app_id: str
    app_secret: str
    redirect_uri: str
    scopes: List[str]
    api_version: str
    name: str
    description: str

class FacebookOAuthError(Exception):
    """Erreur OAuth Facebook personnalisée"""
    
    def __init__(self, message: str, error_code: str = None, error_subcode: str = None, 
                 error_type: str = None, fbtrace_id: str = None, http_status: int = None):
        super().__init__(message)
        self.error_code = error_code
        self.error_subcode = error_subcode
        self.error_type = error_type
        self.fbtrace_id = fbtrace_id
        self.http_status = http_status
        self.timestamp = datetime.utcnow()

class FacebookAPIError(Exception):
    """Erreur API Facebook Graph"""
    
    def __init__(self, message: str, status_code: int = None, response_data: dict = None,
                 error_code: int = None, error_subcode: int = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        self.error_code = error_code
        self.error_subcode = error_subcode
        self.timestamp = datetime.utcnow()

class FacebookTokenError(Exception):
    """Erreur de token Facebook"""
    pass

class FacebookPermissionError(Exception):
    """Erreur de permissions Facebook"""
    pass

class FacebookOAuthManager:
    """Gestionnaire OAuth Facebook avec Graph API"""
    
    def __init__(self):
        self.api_version = FacebookApiVersion.V21_0.value
        self.base_url = "https://www.facebook.com"
        self.graph_url = f"https://graph.facebook.com/{self.api_version}"
        self.session = self._create_session()
        
        # Configuration de l'application Facebook
        self.app_config = self._load_app_configuration()
        
        # URLs de redirection
        self.redirect_uri = get_env_var('FACEBOOK_REDIRECT_URI', 'http://localhost:8501/auth/facebook/callback')
        
        # Cache des états OAuth (en production, utiliser Redis)
        self._oauth_states = {}
        
        # Codes d'erreur Facebook spécifiques
        self.facebook_error_codes = {
            1: "API_UNKNOWN",
            2: "API_SERVICE", 
            4: "API_TOO_MANY_CALLS",
            10: "API_PERMISSION_DENIED",
            17: "API_USER_TOO_MANY_CALLS",
            100: "INVALID_PARAMETER",
            190: "ACCESS_TOKEN_ERROR",
            200: "PERMISSION_ERROR",
            230: "PASSWORD_CHANGED",
            459: "SESSION_EXPIRED",
            460: "PASSWORD_CHANGED",
            463: "EXPIRED_SESSION",
            464: "SESSION_INVALID",
            467: "INVALID_ACCESS_TOKEN"
        }
        
    def _create_session(self) -> requests.Session:
        """Créer une session HTTP avec retry automatique"""
        
        session = requests.Session()
        
        # Configuration du retry automatique pour Facebook
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            backoff_factor=2,  # Plus agressif pour Facebook
            raise_on_redirect=False,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers par défaut pour Facebook
        session.headers.update({
            'User-Agent': f'{get_env_var("APP_NAME", "WhatsTheData")}/{get_env_var("APP_VERSION", "1.0.0")} (+{get_env_var("BASE_URL", "http://localhost:8501")})',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        return session
    
    def _load_app_configuration(self) -> FacebookAppConfig:
        """Charger la configuration de l'application Facebook"""
        
        app_id = get_env_var('FB_CLIENT_ID')
        app_secret = get_env_var('FB_CLIENT_SECRET')
        
        if not app_id or not app_secret:
            raise ValueError("FB_CLIENT_ID et FB_CLIENT_SECRET sont requis")
        
        # Scopes par défaut depuis les variables d'environnement
        default_scopes = get_env_var('FB_PERMISSIONS', 'pages_read_engagement,pages_show_list,read_insights,pages_read_user_content').split(',')
        
        config = FacebookAppConfig(
            app_id=app_id,
            app_secret=app_secret,
            redirect_uri=get_env_var('FACEBOOK_REDIRECT_URI', 'http://localhost:8501/auth/facebook/callback'),
            scopes=[scope.strip() for scope in default_scopes],
            api_version=self.api_version,
            name="WhatsTheData Facebook Integration",
            description="Collecte et analyse des métriques Facebook Pages"
        )
        
        logger.info(f"✅ Application Facebook configurée: {app_id}")
        return config
    
    def generate_oauth_state(self, user_id: int = None) -> str:
        """Générer un état OAuth sécurisé pour Facebook"""
        
        state_data = {
            'timestamp': int(time.time()),
            'user_id': user_id,
            'platform': 'facebook',
            'random': secrets.token_urlsafe(16),
            'app_id': self.app_config.app_id
        }
        
        # Créer un hash de l'état
        state_json = json.dumps(state_data, sort_keys=True)
        state_hash = hashlib.sha256(state_json.encode()).hexdigest()[:16]
        
        # Stocker l'état avec TTL de 10 minutes
        self._oauth_states[state_hash] = {
            'data': state_data,
            'expires_at': time.time() + 600  # 10 minutes
        }
        
        # Nettoyer les états expirés
        self._cleanup_expired_states()
        
        logger.debug(f"🔑 État OAuth Facebook généré: {state_hash}")
        return state_hash
    
    def validate_oauth_state(self, state: str) -> Optional[Dict]:
        """Valider un état OAuth Facebook"""
        
        if not state or state not in self._oauth_states:
            logger.warning(f"⚠️  État OAuth Facebook invalide ou inexistant: {state}")
            return None
        
        state_info = self._oauth_states[state]
        
        # Vérifier l'expiration
        if time.time() > state_info['expires_at']:
            logger.warning(f"⚠️  État OAuth Facebook expiré: {state}")
            del self._oauth_states[state]
            return None
        
        # Supprimer l'état utilisé
        del self._oauth_states[state]
        
        logger.debug(f"✅ État OAuth Facebook validé: {state}")
        return state_info['data']
    
    def _cleanup_expired_states(self):
        """Nettoyer les états OAuth expirés"""
        
        current_time = time.time()
        expired_states = [
            state for state, info in self._oauth_states.items()
            if current_time > info['expires_at']
        ]
        
        for state in expired_states:
            del self._oauth_states[state]
        
        if expired_states:
            logger.debug(f"🧹 {len(expired_states)} états OAuth Facebook expirés nettoyés")
    
    def get_authorization_url(self, user_id: int = None, custom_scopes: List[str] = None,
                             custom_redirect_uri: str = None) -> Tuple[str, str]:
        """Générer l'URL d'autorisation Facebook"""
        
        try:
            # Générer l'état OAuth
            state = self.generate_oauth_state(user_id)
            
            # Scopes à utiliser
            scopes = custom_scopes or self.app_config.scopes
            redirect_uri = custom_redirect_uri or self.app_config.redirect_uri
            
            # Paramètres OAuth Facebook
            oauth_params = {
                'client_id': self.app_config.app_id,
                'redirect_uri': redirect_uri,
                'state': state,
                'scope': ','.join(scopes),
                'response_type': 'code',
                'auth_type': 'rerequest',  # Forcer la redemande de permissions
                'display': 'popup'  # Optimisé pour une popup
            }
            
            # URL d'autorisation Facebook
            auth_url = f"{self.base_url}/{self.api_version}/dialog/oauth?{urlencode(oauth_params)}"
            
            logger.info(f"✅ URL d'autorisation Facebook générée pour user_id={user_id}")
            return auth_url, state
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération de l'URL d'autorisation Facebook: {e}")
            raise FacebookOAuthError(
                f"Impossible de générer l'URL d'autorisation: {e}",
                error_code="AUTH_URL_GENERATION_FAILED"
            )
    
    def exchange_code_for_token(self, code: str, state: str, 
                               redirect_uri: str = None) -> Dict[str, Any]:
        """Échanger le code d'autorisation contre un token d'accès Facebook"""
        
        try:
            # Valider l'état OAuth
            state_data = self.validate_oauth_state(state)
            if not state_data:
                raise FacebookOAuthError(
                    "État OAuth invalide ou expiré",
                    error_code="INVALID_STATE"
                )
            
            # Paramètres pour l'échange de token
            token_params = {
                'client_id': self.app_config.app_id,
                'client_secret': self.app_config.app_secret,
                'redirect_uri': redirect_uri or self.app_config.redirect_uri,
                'code': code
            }
            
            logger.info("🔄 Échange de code pour token Facebook...")
            
            # Faire la requête d'échange
            response = self.session.get(
                f"{self.graph_url}/oauth/access_token",
                params=token_params,
                timeout=30
            )
            
            # Analyser la réponse
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise FacebookOAuthError(
                    f"Échec de l'échange de token: {error_data.get('error_description', error_data.get('message', 'Erreur inconnue'))}",
                    error_code=error_data.get('error', 'TOKEN_EXCHANGE_FAILED'),
                    error_subcode=error_data.get('error_subcode'),
                    error_type=error_data.get('type'),
                    fbtrace_id=error_data.get('fbtrace_id'),
                    http_status=response.status_code
                )
            
            token_data = response.json()
            
            # Valider les données du token
            if 'access_token' not in token_data:
                raise FacebookOAuthError(
                    "Réponse de token incomplète, access_token manquant",
                    error_code="INCOMPLETE_TOKEN_RESPONSE"
                )
            
            # Facebook peut retourner expires_in en tant que string
            expires_in = token_data.get('expires_in')
            if expires_in:
                try:
                    expires_in = int(expires_in)
                except (ValueError, TypeError):
                    logger.warning(f"⚠️  expires_in invalide: {expires_in}, utilisation de 60 jours par défaut")
                    expires_in = 60 * 24 * 60 * 60  # 60 jours par défaut
            else:
                expires_in = 60 * 24 * 60 * 60  # 60 jours par défaut
            
            # Calculer la date d'expiration
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            # Enrichir les données du token
            token_info = {
                'access_token': token_data['access_token'],
                'token_type': token_data.get('token_type', 'bearer'),
                'expires_in': expires_in,
                'expires_at': expires_at,
                'user_id': state_data.get('user_id'),
                'obtained_at': datetime.utcnow(),
                'token_category': FacebookTokenType.USER_ACCESS_TOKEN.value
            }
            
            logger.info(f"✅ Token Facebook obtenu avec succès, expire dans {expires_in}s")
            return token_info
            
        except FacebookOAuthError:
            raise
        except requests.RequestException as e:
            logger.error(f"❌ Erreur réseau lors de l'échange de token Facebook: {e}")
            raise FacebookOAuthError(
                f"Erreur réseau: {e}",
                error_code="NETWORK_ERROR"
            )
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de l'échange de token Facebook: {e}")
            raise FacebookOAuthError(
                f"Erreur inattendue: {e}",
                error_code="UNEXPECTED_ERROR"
            )
    
    def get_long_lived_token(self, short_lived_token: str) -> Dict[str, Any]:
        """Convertir un token de courte durée en token de longue durée"""
        
        try:
            params = {
                'grant_type': 'fb_exchange_token',
                'client_id': self.app_config.app_id,
                'client_secret': self.app_config.app_secret,
                'fb_exchange_token': short_lived_token
            }
            
            logger.info("🔄 Conversion en token Facebook longue durée...")
            
            response = self.session.get(
                f"{self.graph_url}/oauth/access_token",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise FacebookTokenError(
                    f"Échec de la conversion en token longue durée: {error_data.get('message', 'Erreur inconnue')}"
                )
            
            token_data = response.json()
            
            # Facebook renvoie parfois expires_in, parfois pas
            expires_in = token_data.get('expires_in', 60 * 24 * 60 * 60)  # 60 jours par défaut
            if isinstance(expires_in, str):
                expires_in = int(expires_in)
            
            expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
            
            long_lived_token_info = {
                'access_token': token_data['access_token'],
                'expires_in': expires_in,
                'expires_at': expires_at,
                'token_category': FacebookTokenType.LONG_LIVED_USER_TOKEN.value,
                'converted_at': datetime.utcnow()
            }
            
            logger.info(f"✅ Token Facebook longue durée obtenu, expire dans {expires_in}s")
            return long_lived_token_info
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion en token longue durée: {e}")
            raise
    
    def get_user_profile(self, access_token: str) -> Dict[str, Any]:
        """Récupérer le profil utilisateur Facebook"""
        
        try:
            params = {
                'access_token': access_token,
                'fields': 'id,name,email,first_name,last_name,picture.type(large)'
            }
            
            response = self.session.get(
                f"{self.graph_url}/me",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise FacebookAPIError(
                    f"Impossible de récupérer le profil: {error_data.get('message', 'Erreur inconnue')}",
                    status_code=response.status_code,
                    response_data=error_data,
                    error_code=error_data.get('code'),
                    error_subcode=error_data.get('error_subcode')
                )
            
            profile_data = response.json()
            
            # Normaliser les données du profil
            user_info = {
                'id': profile_data.get('id'),
                'name': profile_data.get('name'),
                'email': profile_data.get('email'),
                'first_name': profile_data.get('first_name'),
                'last_name': profile_data.get('last_name'),
                'picture_url': profile_data.get('picture', {}).get('data', {}).get('url')
            }
            
            logger.info(f"✅ Profil utilisateur Facebook récupéré: {user_info.get('id')}")
            return user_info
            
        except FacebookAPIError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du profil Facebook: {e}")
            raise FacebookAPIError(f"Erreur inattendue: {e}")
    
    def get_user_pages(self, access_token: str) -> List[Dict[str, Any]]:
        """Récupérer les pages administrées par l'utilisateur"""
        
        try:
            params = {
                'access_token': access_token,
                'fields': 'id,name,category,access_token,tasks,about,description,website,picture.type(large),cover,talking_about_count,username',
                'limit': 100
            }
            
            response = self.session.get(
                f"{self.graph_url}/me/accounts",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise FacebookAPIError(
                    f"Impossible de récupérer les pages: {error_data.get('message', 'Erreur inconnue')}",
                    status_code=response.status_code,
                    response_data=error_data,
                    error_code=error_data.get('code'),
                    error_subcode=error_data.get('error_subcode')
                )
            
            pages_data = response.json()
            pages = []
            
            for page_data in pages_data.get('data', []):
                # Vérifier les permissions
                tasks = page_data.get('tasks', [])
                
                # Filtrer uniquement les pages avec permissions d'administration/analyse
                if any(task in ['MANAGE', 'CREATE_CONTENT', 'MODERATE', 'ADVERTISE', 'ANALYZE'] for task in tasks):
                    page_info = {
                        'id': page_data.get('id'),
                        'name': page_data.get('name'),
                        'category': page_data.get('category'),
                        'access_token': page_data.get('access_token'),
                        'tasks': tasks,
                        'about': page_data.get('about'),
                        'description': page_data.get('description'),
                        'website': page_data.get('website'),
                        'username': page_data.get('username'),
                        'picture_url': page_data.get('picture', {}).get('data', {}).get('url'),
                        'cover_url': page_data.get('cover', {}).get('source'),
                        'talking_about_count': page_data.get('talking_about_count', 0),
                        'can_analyze': 'ANALYZE' in tasks,
                        'can_manage': 'MANAGE' in tasks
                    }
                    pages.append(page_info)
            
            logger.info(f"✅ {len(pages)} pages Facebook trouvées avec permissions appropriées")
            return pages
            
        except FacebookAPIError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des pages Facebook: {e}")
            raise FacebookAPIError(f"Erreur inattendue: {e}")
    
    def get_page_long_lived_token(self, page_access_token: str) -> Dict[str, Any]:
        """Obtenir un token de longue durée pour une page"""
        
        try:
            params = {
                'grant_type': 'fb_exchange_token',
                'client_id': self.app_config.app_id,
                'client_secret': self.app_config.app_secret,
                'fb_exchange_token': page_access_token
            }
            
            response = self.session.get(
                f"{self.graph_url}/oauth/access_token",
                params=params,
                timeout=30
            )
            
            if response.status_code != 200:
                # Pour les pages, le token peut déjà être de longue durée
                logger.info("ℹ️  Le token de page est peut-être déjà de longue durée")
                return {
                    'access_token': page_access_token,
                    'expires_in': None,  # Les tokens de page n'expirent généralement pas
                    'expires_at': None,
                    'token_category': FacebookTokenType.LONG_LIVED_PAGE_TOKEN.value,
                    'note': 'Token de page existant (potentiellement permanent)'
                }
            
            token_data = response.json()
            
            return {
                'access_token': token_data.get('access_token', page_access_token),
                'expires_in': token_data.get('expires_in'),
                'expires_at': datetime.utcnow() + timedelta(seconds=token_data['expires_in']) if token_data.get('expires_in') else None,
                'token_category': FacebookTokenType.LONG_LIVED_PAGE_TOKEN.value,
                'converted_at': datetime.utcnow()
            }
            
        except Exception as e:
            logger.warning(f"⚠️  Impossible de convertir le token de page, utilisation du token existant: {e}")
            return {
                'access_token': page_access_token,
                'expires_in': None,
                'expires_at': None,
                'token_category': FacebookTokenType.PAGE_ACCESS_TOKEN.value,
                'note': 'Token de page original'
            }
    
    def validate_token(self, access_token: str) -> Dict[str, Any]:
        """Valider un token d'accès Facebook et obtenir ses informations"""
        
        try:
            params = {
                'input_token': access_token,
                'access_token': f"{self.app_config.app_id}|{self.app_config.app_secret}"
            }
            
            response = self.session.get(
                f"{self.graph_url}/debug_token",
                params=params,
                timeout=15
            )
            
            if response.status_code != 200:
                logger.warning(f"⚠️  Impossible de valider le token (status: {response.status_code})")
                return {'is_valid': False, 'error': 'Validation failed'}
            
            token_info = response.json().get('data', {})
            
            is_valid = token_info.get('is_valid', False)
            expires_at = token_info.get('expires_at')
            
            validation_result = {
                'is_valid': is_valid,
                'app_id': token_info.get('app_id'),
                'user_id': token_info.get('user_id'),
                'expires_at': datetime.fromtimestamp(expires_at) if expires_at else None,
                'scopes': token_info.get('scopes', []),
                'token_type': token_info.get('type'),
                'issued_at': datetime.fromtimestamp(token_info['issued_at']) if token_info.get('issued_at') else None
            }
            
            if is_valid:
                logger.debug("✅ Token Facebook valide")
            else:
                error_info = token_info.get('error', {})
                validation_result['error'] = error_info.get('message', 'Token invalide')
                validation_result['error_code'] = error_info.get('code')
                logger.warning(f"⚠️  Token Facebook invalide: {validation_result['error']}")
            
            return validation_result
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la validation du token Facebook: {e}")
            return {'is_valid': False, 'error': str(e)}
    
    def get_user_permissions(self, access_token: str) -> List[str]:
        """Récupérer les permissions accordées par l'utilisateur"""
        
        try:
            params = {
                'access_token': access_token
            }
            
            response = self.session.get(
                f"{self.graph_url}/me/permissions",
                params=params,
                timeout=15
            )
            
            if response.status_code != 200:
                logger.warning(f"⚠️  Impossible de récupérer les permissions (status: {response.status_code})")
                return []
            
            permissions_data = response.json()
            granted_permissions = []
            
            for perm in permissions_data.get('data', []):
                if perm.get('status') == 'granted':
                    granted_permissions.append(perm.get('permission'))
            
            logger.info(f"✅ {len(granted_permissions)} permissions Facebook accordées")
            return granted_permissions
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des permissions: {e}")
            return []
    
    def revoke_token(self, access_token: str) -> bool:
        """Révoquer un token d'accès Facebook"""
        
        try:
            params = {
                'access_token': access_token
            }
            
            response = self.session.delete(
                f"{self.graph_url}/me/permissions",
                params=params,
                timeout=15
            )
            
            success = response.status_code == 200
            
            if success:
                logger.info("✅ Token Facebook révoqué avec succès")
            else:
                logger.warning(f"⚠️  Échec de la révocation du token (status: {response.status_code})")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la révocation du token Facebook: {e}")
            return False
    
    # ========================================
    # INTÉGRATION AVEC USER MANAGER
    # ========================================
    
    def store_user_tokens(self, user_id: int, user_token_info: Dict[str, Any], 
                         pages_info: List[Dict[str, Any]] = None) -> bool:
        """Stocker les tokens Facebook dans la base de données"""
        
        try:
            # Stocker le token utilisateur
            success = user_manager.store_social_token(
                user_id=user_id,
                platform="facebook",
                access_token=user_token_info['access_token'],
                expires_at=user_token_info.get('expires_at'),
                scope=','.join(self.app_config.scopes)
            )
            
            if not success:
                logger.error("❌ Échec du stockage du token utilisateur Facebook")
                return False
            
            # Stocker les tokens de page séparément si fournis
            if pages_info:
                for page_info in pages_info:
                    if page_info.get('access_token'):
                        user_manager.store_social_token(
                            user_id=user_id,
                            platform=f"facebook_page_{page_info['id']}",
                            access_token=page_info['access_token'],
                            scope=','.join(page_info.get('tasks', []))
                        )
            
            logger.info(f"✅ Tokens Facebook stockés pour user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du stockage des tokens Facebook: {e}")
            return False
    
    def get_user_token(self, user_id: int) -> Optional[SocialAccessToken]:
        """Récupérer le token Facebook actif d'un utilisateur"""
        
        return user_manager.get_social_token(user_id, "facebook")
    
    def connect_facebook_account(self, user_id: int, code: str, state: str,
                                redirect_uri: str = None) -> Dict[str, Any]:
        """Processus complet de connexion d'un compte Facebook"""
        
        try:
            logger.info(f"🔄 Connexion compte Facebook pour user {user_id}")
            
            # 1. Échanger le code contre un token
            user_token_info = self.exchange_code_for_token(code, state, redirect_uri)
            
            # 2. Convertir en token de longue durée
            try:
                long_lived_token_info = self.get_long_lived_token(user_token_info['access_token'])
                user_token_info.update(long_lived_token_info)
                logger.info("✅ Token Facebook converti en longue durée")
            except Exception as e:
                logger.warning(f"⚠️  Impossible de convertir en token longue durée: {e}")
            
            # 3. Récupérer le profil utilisateur
            try:
                profile = self.get_user_profile(user_token_info['access_token'])
            except Exception as e:
                logger.error(f"❌ Impossible de récupérer le profil: {e}")
                profile = {'id': 'unknown', 'name': 'Utilisateur Facebook'}
            
            # 4. Récupérer les pages administrées
            try:
                pages = self.get_user_pages(user_token_info['access_token'])
            except Exception as e:
                logger.error(f"❌ Impossible de récupérer les pages: {e}")
                pages = []
            
            # 5. Convertir les tokens de page en longue durée
            for page in pages:
                if page.get('access_token'):
                    try:
                        page_long_token = self.get_page_long_lived_token(page['access_token'])
                        page['access_token'] = page_long_token['access_token']
                        page['token_expires_at'] = page_long_token.get('expires_at')
                    except Exception as e:
                        logger.warning(f"⚠️  Impossible de convertir le token de la page {page['id']}: {e}")
            
            # 6. Vérifier les permissions
            try:
                permissions = self.get_user_permissions(user_token_info['access_token'])
                missing_permissions = [scope for scope in self.app_config.scopes if scope not in permissions]
                
                if missing_permissions:
                    logger.warning(f"⚠️  Permissions manquantes: {missing_permissions}")
            except Exception as e:
                logger.warning(f"⚠️  Impossible de vérifier les permissions: {e}")
                permissions = []
                missing_permissions = self.app_config.scopes
            
            # 7. Stocker les tokens
            if not self.store_user_tokens(user_id, user_token_info, pages):
                raise FacebookOAuthError("Impossible de stocker les tokens", error_code="TOKEN_STORAGE_FAILED")
            
            # 8. Ajouter les comptes de page
            added_accounts = []
            for page in pages:
                try:
                    account = user_manager.add_facebook_account(
                        user_id=user_id,
                        page_id=page['id'],
                        page_name=page['name']
                    )
                    if account:
                        added_accounts.append({
                            'id': page['id'],
                            'name': page['name'],
                            'category': page.get('category'),
                            'username': page.get('username'),
                            'picture_url': page.get('picture_url'),
                            'can_analyze': page.get('can_analyze', False),
                            'can_manage': page.get('can_manage', False),
                            'tasks': page.get('tasks', [])
                        })
                except Exception as e:
                    logger.error(f"❌ Erreur lors de l'ajout de la page {page['id']}: {e}")
            
            result = {
                'success': True,
                'profile': profile,
                'pages': added_accounts,
                'permissions': permissions,
                'missing_permissions': missing_permissions,
                'token_expires_at': user_token_info.get('expires_at').isoformat() if user_token_info.get('expires_at') else None,
                'message': f"Compte Facebook connecté avec succès ({len(added_accounts)} pages)",
                'warnings': []
            }
            
            # Ajouter des avertissements si nécessaire
            if missing_permissions:
                result['warnings'].append(f"Permissions manquantes: {', '.join(missing_permissions)}")
            
            if len(pages) > len(added_accounts):
                result['warnings'].append("Certaines pages n'ont pas pu être ajoutées (permissions insuffisantes)")
            
            logger.info(f"✅ Compte Facebook connecté: user {user_id}, {len(added_accounts)} pages")
            return result
            
        except FacebookOAuthError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la connexion du compte Facebook: {e}")
            raise FacebookOAuthError(
                f"Erreur lors de la connexion: {e}",
                error_code="CONNECTION_FAILED"
            )
    
    def disconnect_facebook_account(self, user_id: int, revoke_tokens: bool = True) -> bool:
        """Déconnecter un compte Facebook"""
        
        try:
            # Récupérer le token pour révocation si demandé
            if revoke_tokens:
                token = self.get_user_token(user_id)
                if token:
                    try:
                        self.revoke_token(token.access_token)
                    except Exception as e:
                        logger.warning(f"⚠️  Impossible de révoquer le token Facebook: {e}")
            
            # Révoquer les tokens stockés
            user_manager.revoke_social_token(user_id, "facebook")
            
            # Désactiver les comptes Facebook
            with db_manager.get_session() as session:
                session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user_id
                ).update({'is_active': False})
                
                session.commit()
            
            logger.info(f"✅ Compte Facebook déconnecté pour user {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la déconnexion Facebook: {e}")
            return False
    
    def get_user_facebook_status(self, user_id: int) -> Dict[str, Any]:
        """Récupérer le statut de la connexion Facebook d'un utilisateur"""
        
        try:
            status = {
                'user_id': user_id,
                'connected': False,
                'token_valid': False,
                'token_expires_at': None,
                'permissions': [],
                'pages': [],
                'last_check': datetime.utcnow().isoformat()
            }
            
            # Vérifier le token
            token = self.get_user_token(user_id)
            if token:
                status['connected'] = True
                status['token_expires_at'] = token.expires_at.isoformat() if token.expires_at else None
                
                # Valider le token
                try:
                    validation = self.validate_token(token.access_token)
                    status['token_valid'] = validation['is_valid']
                    
                    if validation['is_valid']:
                        # Récupérer les permissions
                        status['permissions'] = self.get_user_permissions(token.access_token)
                except Exception as e:
                    logger.warning(f"⚠️  Impossible de valider le token Facebook: {e}")
            
            # Récupérer les pages connectées
            with db_manager.get_session() as session:
                facebook_accounts = session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user_id,
                    FacebookAccount.is_active == True
                ).all()
                
                status['pages'] = [
                    {
                        'id': acc.page_id,
                        'name': acc.page_name,
                        'connected_at': acc.created_at.isoformat()
                    }
                    for acc in facebook_accounts
                ]
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du statut Facebook: {e}")
            return {'user_id': user_id, 'error': str(e)}
    
    # ========================================
    # MÉTHODES UTILITAIRES
    # ========================================
    
    def _parse_error_response(self, response: requests.Response) -> Dict[str, Any]:
        """Parser une réponse d'erreur Facebook"""
        
        try:
            error_data = response.json()
            
            # Facebook peut retourner l'erreur directement ou dans un champ 'error'
            if 'error' in error_data:
                error_info = error_data['error']
                return {
                    'error': error_info.get('type', 'unknown'),
                    'message': error_info.get('message', 'Erreur inconnue'),
                    'code': error_info.get('code'),
                    'error_subcode': error_info.get('error_subcode'),
                    'fbtrace_id': error_info.get('fbtrace_id'),
                    'error_user_title': error_info.get('error_user_title'),
                    'error_user_msg': error_info.get('error_user_msg')
                }
            else:
                return error_data
                
        except:
            return {
                'error': 'parse_error',
                'message': response.text or 'Réponse invalide',
                'status_code': response.status_code
            }
    
    def get_error_description(self, error_code: int) -> str:
        """Obtenir une description d'un code d'erreur Facebook"""
        
        return self.facebook_error_codes.get(error_code, f"Erreur Facebook inconnue ({error_code})")
    
    def get_app_config(self) -> FacebookAppConfig:
        """Récupérer la configuration de l'application"""
        return self.app_config
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du gestionnaire OAuth Facebook"""
        
        health = {
            'oauth_manager': 'ok',
            'app_configured': bool(self.app_config.app_id and self.app_config.app_secret),
            'api_version': self.api_version,
            'session_ready': self.session is not None,
            'app_details': {
                'app_id': self.app_config.app_id,
                'scopes_count': len(self.app_config.scopes),
                'scopes': self.app_config.scopes,
                'redirect_uri': self.app_config.redirect_uri
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Test basique de l'API Facebook
        try:
            response = self.session.get(f"{self.graph_url}/", timeout=10)
            health['api_reachable'] = response.status_code == 200
        except:
            health['api_reachable'] = False
        
        return health

# ========================================
# INSTANCE GLOBALE
# ========================================

facebook_oauth_manager = FacebookOAuthManager()

# ========================================
# FONCTIONS HELPER
# ========================================

def get_facebook_auth_url(user_id: int = None, custom_scopes: List[str] = None) -> Tuple[str, str]:
    """Fonction helper pour obtenir une URL d'authentification Facebook"""
    
    return facebook_oauth_manager.get_authorization_url(user_id, custom_scopes)

def handle_facebook_callback(code: str, state: str, user_id: int, 
                           redirect_uri: str = None) -> Dict[str, Any]:
    """Fonction helper pour gérer le callback OAuth Facebook"""
    
    return facebook_oauth_manager.connect_facebook_account(user_id, code, state, redirect_uri)

def get_user_facebook_info(user_id: int) -> Dict[str, Any]:
    """Fonction helper pour obtenir les infos Facebook d'un utilisateur"""
    return facebook_oauth_manager.get_user_facebook_status(user_id)

def disconnect_user_facebook(user_id: int, revoke_tokens: bool = True) -> bool:
    """Fonction helper pour déconnecter un utilisateur de Facebook"""
    return facebook_oauth_manager.disconnect_facebook_account(user_id, revoke_tokens)

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test FacebookOAuthManager...")
    
    try:
        # Test de configuration
        manager = FacebookOAuthManager()
        health = manager.health_check()
        print(f"Health check: {json.dumps(health, indent=2)}")
        
        # Test de génération d'URL
        auth_url, state = manager.get_authorization_url(user_id=1)
        print(f"URL d'autorisation: {auth_url}")
        print(f"État: {state}")
        
        # Test de validation d'état
        state_data = manager.validate_oauth_state(state)
        print(f"Données d'état: {state_data}")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")
        import traceback
        traceback.print_exc()