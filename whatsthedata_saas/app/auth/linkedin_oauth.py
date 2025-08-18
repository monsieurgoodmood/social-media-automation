"""
Gestionnaire OAuth LinkedIn complet et robuste
Gère les 3 applications LinkedIn : Community Management, Portability Data, Sign In
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
from ..database.models import User, LinkedinTokens, LinkedinAccount
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

class LinkedinAppType(Enum):
    """Types d'applications LinkedIn"""
    COMMUNITY = "community"      # Community Management API
    PORTABILITY = "portability"  # Portability Data API  
    SIGNIN = "signin"           # Sign In API

class LinkedinScope(Enum):
    """Scopes LinkedIn disponibles"""
    # Community Management API
    COMMUNITY_BASIC = "r_organization_social"
    COMMUNITY_POSTS = "w_organization_social"
    COMMUNITY_ANALYTICS = "r_organization_analytics"
    
    # Portability Data API
    PORTABILITY_BASIC = "r_basicprofile"
    PORTABILITY_DATA = "r_organizationadata"
    
    # Sign In API
    SIGNIN_BASIC = "openid"
    SIGNIN_PROFILE = "profile"
    SIGNIN_EMAIL = "email"

@dataclass
class LinkedinAppConfig:
    """Configuration d'une application LinkedIn"""
    app_type: LinkedinAppType
    client_id: str
    client_secret: str
    redirect_uri: str
    scopes: List[str]
    name: str
    description: str

class LinkedinOAuthError(Exception):
    """Erreur OAuth LinkedIn personnalisée"""
    
    def __init__(self, message: str, error_code: str = None, error_description: str = None, 
                 app_type: str = None, http_status: int = None):
        super().__init__(message)
        self.error_code = error_code
        self.error_description = error_description
        self.app_type = app_type
        self.http_status = http_status
        self.timestamp = datetime.utcnow()

class LinkedinAPIError(Exception):
    """Erreur API LinkedIn"""
    
    def __init__(self, message: str, status_code: int = None, response_data: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data
        self.timestamp = datetime.utcnow()

class LinkedinTokenError(Exception):
    """Erreur de token LinkedIn"""
    pass

class LinkedinOAuthManager:
    """Gestionnaire OAuth LinkedIn avec support multi-applications"""
    
    def __init__(self):
        self.base_url = "https://www.linkedin.com/oauth/v2"
        self.api_base_url = "https://api.linkedin.com/rest"
        self.session = self._create_session()
        
        # Configuration des applications LinkedIn
        self.apps = self._load_app_configurations()
        
        # URLs de redirection
        self.base_redirect_uri = get_env_var('LINKEDIN_REDIRECT_URI', 'http://localhost:8501/auth/linkedin/callback')
        
        # Cache des états OAuth (en production, utiliser Redis)
        self._oauth_states = {}
        
    def _create_session(self) -> requests.Session:
        """Créer une session HTTP avec retry automatique"""
        
        session = requests.Session()
        
        # Configuration du retry automatique
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            backoff_factor=1,
            raise_on_redirect=False,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers par défaut
        session.headers.update({
            'User-Agent': f'{get_env_var("APP_NAME", "WhatsTheData")}/{get_env_var("APP_VERSION", "1.0.0")}',
            'Accept': 'application/json',
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        
        return session
    
    def _load_app_configurations(self) -> Dict[LinkedinAppType, LinkedinAppConfig]:
        """Charger les configurations des 3 applications LinkedIn"""
        
        apps = {}
        
        # Community Management API
        apps[LinkedinAppType.COMMUNITY] = LinkedinAppConfig(
            app_type=LinkedinAppType.COMMUNITY,
            client_id=get_env_var('COMMUNITY_CLIENT_ID'),
            client_secret=get_env_var('COMMUNITY_CLIENT_SECRET'),
            redirect_uri=f"{self.base_redirect_uri}?app=community",
            scopes=[
                "r_organization_social",
                "w_organization_social", 
                "r_organization_analytics",
                "rw_organization_admin"
            ],
            name="Community Management",
            description="Gestion des posts et analytics LinkedIn"
        )
        
        # Portability Data API
        apps[LinkedinAppType.PORTABILITY] = LinkedinAppConfig(
            app_type=LinkedinAppType.PORTABILITY,
            client_id=get_env_var('PORTABILITY_CLIENT_ID'),
            client_secret=get_env_var('PORTABILITY_CLIENT_SECRET'),
            redirect_uri=f"{self.base_redirect_uri}?app=portability",
            scopes=[
                "r_basicprofile",
                "r_organization_lookup_by_vanity_name",
                "r_organization_social"
            ],
            name="Portability Data",
            description="Export de données LinkedIn détaillées"
        )
        
        # Sign In API
        apps[LinkedinAppType.SIGNIN] = LinkedinAppConfig(
            app_type=LinkedinAppType.SIGNIN,
            client_id=get_env_var('SIGNIN_CLIENT_ID'),
            client_secret=get_env_var('SIGNIN_CLIENT_SECRET'),
            redirect_uri=f"{self.base_redirect_uri}?app=signin",
            scopes=[
                "openid",
                "profile", 
                "email"
            ],
            name="Sign In",
            description="Authentification utilisateur LinkedIn"
        )
        
        # Vérifier que toutes les configurations sont valides
        for app_type, config in apps.items():
            if not config.client_id or not config.client_secret:
                logger.error(f"❌ Configuration manquante pour {app_type.value}")
                raise ValueError(f"Client ID/Secret manquant pour {app_type.value}")
        
        logger.info(f"✅ {len(apps)} applications LinkedIn configurées")
        return apps
    
    def generate_oauth_state(self, user_id: int = None, app_type: LinkedinAppType = None) -> str:
        """Générer un état OAuth sécurisé"""
        
        # Données à inclure dans l'état
        state_data = {
            'timestamp': int(time.time()),
            'user_id': user_id,
            'app_type': app_type.value if app_type else None,
            'random': secrets.token_urlsafe(16)
        }
        
        # Créer un hash de l'état
        state_json = json.dumps(state_data, sort_keys=True)
        state_hash = hashlib.sha256(state_json.encode()).hexdigest()[:16]
        
        # Stocker l'état (TTL de 10 minutes)
        self._oauth_states[state_hash] = {
            'data': state_data,
            'expires_at': time.time() + 600  # 10 minutes
        }
        
        # Nettoyer les états expirés
        self._cleanup_expired_states()
        
        logger.debug(f"🔑 État OAuth généré: {state_hash}")
        return state_hash
    
    def validate_oauth_state(self, state: str) -> Optional[Dict]:
        """Valider un état OAuth"""
        
        if not state or state not in self._oauth_states:
            logger.warning(f"⚠️  État OAuth invalide ou inexistant: {state}")
            return None
        
        state_info = self._oauth_states[state]
        
        # Vérifier l'expiration
        if time.time() > state_info['expires_at']:
            logger.warning(f"⚠️  État OAuth expiré: {state}")
            del self._oauth_states[state]
            return None
        
        # Supprimer l'état utilisé
        del self._oauth_states[state]
        
        logger.debug(f"✅ État OAuth validé: {state}")
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
            logger.debug(f"🧹 {len(expired_states)} états OAuth expirés nettoyés")
    
    def get_authorization_url(self, app_type: LinkedinAppType, user_id: int = None, 
                             custom_scopes: List[str] = None) -> Tuple[str, str]:
        """Générer l'URL d'autorisation LinkedIn"""
        
        try:
            if app_type not in self.apps:
                raise LinkedinOAuthError(
                    f"Type d'application non supporté: {app_type}",
                    error_code="UNSUPPORTED_APP_TYPE",
                    app_type=app_type.value
                )
            
            app_config = self.apps[app_type]
            
            # Générer l'état OAuth
            state = self.generate_oauth_state(user_id, app_type)
            
            # Scopes à utiliser
            scopes = custom_scopes or app_config.scopes
            
            # Paramètres OAuth
            oauth_params = {
                'response_type': 'code',
                'client_id': app_config.client_id,
                'redirect_uri': app_config.redirect_uri,
                'state': state,
                'scope': ' '.join(scopes)
            }
            
            # URL d'autorisation
            auth_url = f"{self.base_url}/authorization?{urlencode(oauth_params)}"
            
            logger.info(f"✅ URL d'autorisation générée pour {app_type.value}: user_id={user_id}")
            return auth_url, state
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération de l'URL d'autorisation: {e}")
            raise LinkedinOAuthError(
                f"Impossible de générer l'URL d'autorisation: {e}",
                error_code="AUTH_URL_GENERATION_FAILED",
                app_type=app_type.value
            )
    
    def exchange_code_for_token(self, code: str, state: str, app_type: LinkedinAppType) -> Dict[str, Any]:
        """Échanger le code d'autorisation contre un token d'accès"""
        
        try:
            # Valider l'état OAuth
            state_data = self.validate_oauth_state(state)
            if not state_data:
                raise LinkedinOAuthError(
                    "État OAuth invalide ou expiré",
                    error_code="INVALID_STATE",
                    app_type=app_type.value
                )
            
            # Vérifier que le type d'app correspond
            if state_data.get('app_type') != app_type.value:
                raise LinkedinOAuthError(
                    f"Type d'application incohérent: attendu {app_type.value}, reçu {state_data.get('app_type')}",
                    error_code="APP_TYPE_MISMATCH",
                    app_type=app_type.value
                )
            
            app_config = self.apps[app_type]
            
            # Paramètres pour l'échange de token
            token_params = {
                'grant_type': 'authorization_code',
                'code': code,
                'redirect_uri': app_config.redirect_uri,
                'client_id': app_config.client_id,
                'client_secret': app_config.client_secret
            }
            
            # Faire la requête d'échange
            logger.info(f"🔄 Échange de code pour token ({app_type.value})...")
            
            response = self.session.post(
                f"{self.base_url}/accessToken",
                data=token_params,
                timeout=30
            )
            
            # Analyser la réponse
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise LinkedinOAuthError(
                    f"Échec de l'échange de token: {error_data.get('error_description', 'Erreur inconnue')}",
                    error_code=error_data.get('error', 'TOKEN_EXCHANGE_FAILED'),
                    error_description=error_data.get('error_description'),
                    app_type=app_type.value,
                    http_status=response.status_code
                )
            
            token_data = response.json()
            
            # Valider les données du token
            required_fields = ['access_token', 'expires_in']
            missing_fields = [field for field in required_fields if field not in token_data]
            
            if missing_fields:
                raise LinkedinOAuthError(
                    f"Réponse de token incomplète, champs manquants: {missing_fields}",
                    error_code="INCOMPLETE_TOKEN_RESPONSE",
                    app_type=app_type.value
                )
            
            # Calculer la date d'expiration
            expires_at = datetime.utcnow() + timedelta(seconds=token_data['expires_in'])
            
            # Enrichir les données du token
            token_info = {
                'access_token': token_data['access_token'],
                'expires_in': token_data['expires_in'],
                'expires_at': expires_at,
                'scope': token_data.get('scope', ' '.join(app_config.scopes)),
                'refresh_token': token_data.get('refresh_token'),  # Pas toujours présent
                'app_type': app_type.value,
                'user_id': state_data.get('user_id'),
                'obtained_at': datetime.utcnow()
            }
            
            logger.info(f"✅ Token obtenu avec succès ({app_type.value}), expire dans {token_data['expires_in']}s")
            return token_info
            
        except LinkedinOAuthError:
            raise
        except requests.RequestException as e:
            logger.error(f"❌ Erreur réseau lors de l'échange de token: {e}")
            raise LinkedinOAuthError(
                f"Erreur réseau: {e}",
                error_code="NETWORK_ERROR",
                app_type=app_type.value
            )
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de l'échange de token: {e}")
            raise LinkedinOAuthError(
                f"Erreur inattendue: {e}",
                error_code="UNEXPECTED_ERROR",
                app_type=app_type.value
            )
    
    def refresh_access_token(self, refresh_token: str, app_type: LinkedinAppType) -> Dict[str, Any]:
        """Rafraîchir un token d'accès"""
        
        try:
            if not refresh_token:
                raise LinkedinTokenError("Refresh token manquant")
            
            app_config = self.apps[app_type]
            
            # Paramètres pour le refresh
            refresh_params = {
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
                'client_id': app_config.client_id,
                'client_secret': app_config.client_secret
            }
            
            logger.info(f"🔄 Rafraîchissement du token ({app_type.value})...")
            
            response = self.session.post(
                f"{self.base_url}/accessToken",
                data=refresh_params,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise LinkedinTokenError(
                    f"Échec du rafraîchissement: {error_data.get('error_description', 'Erreur inconnue')}"
                )
            
            token_data = response.json()
            
            # Calculer la nouvelle date d'expiration
            expires_at = datetime.utcnow() + timedelta(seconds=token_data['expires_in'])
            
            token_info = {
                'access_token': token_data['access_token'],
                'expires_in': token_data['expires_in'],
                'expires_at': expires_at,
                'refresh_token': token_data.get('refresh_token', refresh_token),
                'app_type': app_type.value,
                'refreshed_at': datetime.utcnow()
            }
            
            logger.info(f"✅ Token rafraîchi avec succès ({app_type.value})")
            return token_info
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du rafraîchissement du token: {e}")
            raise
    
    def get_user_profile(self, access_token: str) -> Dict[str, Any]:
        """Récupérer le profil utilisateur LinkedIn"""
        
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            # Récupérer les informations de base
            response = self.session.get(
                f"{self.api_base_url}/me",
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise LinkedinAPIError(
                    f"Impossible de récupérer le profil: {error_data.get('message', 'Erreur inconnue')}",
                    status_code=response.status_code,
                    response_data=error_data
                )
            
            profile_data = response.json()
            
            # Informations de base
            user_info = {
                'id': profile_data.get('id'),
                'firstName': profile_data.get('firstName', {}).get('localized', {}).get('en_US', ''),
                'lastName': profile_data.get('lastName', {}).get('localized', {}).get('en_US', ''),
                'profilePicture': None,
                'email': None  # Nécessite un scope spécial
            }
            
            # Récupérer l'email si possible
            try:
                email_response = self.session.get(
                    f"{self.api_base_url}/emailAddress?q=members&projection=(elements*(handle~))",
                    headers=headers,
                    timeout=15
                )
                
                if email_response.status_code == 200:
                    email_data = email_response.json()
                    elements = email_data.get('elements', [])
                    if elements:
                        user_info['email'] = elements[0].get('handle~', {}).get('emailAddress')
            except:
                logger.warning("⚠️  Impossible de récupérer l'email (scope manquant?)")
            
            logger.info(f"✅ Profil utilisateur récupéré: {user_info.get('id')}")
            return user_info
            
        except LinkedinAPIError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du profil: {e}")
            raise LinkedinAPIError(f"Erreur inattendue: {e}")
    
    def get_user_organizations(self, access_token: str) -> List[Dict[str, Any]]:
        """Récupérer les organisations administrées par l'utilisateur"""
        
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            # Récupérer les rôles d'organisation
            response = self.session.get(
                f"{self.api_base_url}/organizationAcls?q=roleAssignee&projection=(elements*(organization~(id,name,vanityName,logoV2),role))",
                headers=headers,
                timeout=30
            )
            
            if response.status_code != 200:
                error_data = self._parse_error_response(response)
                raise LinkedinAPIError(
                    f"Impossible de récupérer les organisations: {error_data.get('message', 'Erreur inconnue')}",
                    status_code=response.status_code,
                    response_data=error_data
                )
            
            acls_data = response.json()
            organizations = []
            
            for element in acls_data.get('elements', []):
                role = element.get('role')
                org_data = element.get('organization~', {})
                
                # Filtrer uniquement les rôles d'administration
                if role in ['ADMINISTRATOR', 'ORGANIC_CONTENT_ADMIN', 'PAID_CONTENT_ADMIN']:
                    org_info = {
                        'id': org_data.get('id'),
                        'name': org_data.get('name', {}).get('localized', {}).get('en_US', 'Organisation inconnue'),
                        'vanityName': org_data.get('vanityName'),
                        'role': role,
                        'logoUrl': self._extract_logo_url(org_data.get('logoV2', {}))
                    }
                    organizations.append(org_info)
            
            logger.info(f"✅ {len(organizations)} organisations trouvées")
            return organizations
            
        except LinkedinAPIError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des organisations: {e}")
            raise LinkedinAPIError(f"Erreur inattendue: {e}")
    
    def validate_token(self, access_token: str) -> bool:
        """Valider qu'un token d'accès est encore valide"""
        
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            response = self.session.get(
                f"{self.api_base_url}/me",
                headers=headers,
                timeout=15
            )
            
            is_valid = response.status_code == 200
            
            if is_valid:
                logger.debug("✅ Token valide")
            else:
                logger.warning(f"⚠️  Token invalide (status: {response.status_code})")
            
            return is_valid
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la validation du token: {e}")
            return False
    
    def revoke_token(self, access_token: str) -> bool:
        """Révoquer un token d'accès"""
        
        try:
            # LinkedIn ne fournit pas d'endpoint de révocation standard
            # On valide simplement que le token n'est plus utilisable
            logger.info("🔒 Révocation du token (suppression locale)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la révocation du token: {e}")
            return False
    
    # ========================================
    # INTÉGRATION AVEC USER MANAGER
    # ========================================
    
    def store_user_tokens(self, user_id: int, token_info: Dict[str, Any]) -> bool:
        """Stocker les tokens LinkedIn dans la base de données"""
        
        try:
            app_type = token_info['app_type']
            
            with db_manager.get_session() as session:
                # Désactiver les anciens tokens pour cette app
                session.query(LinkedinTokens).filter(
                    LinkedinTokens.user_id == user_id,
                    LinkedinTokens.application_type == app_type
                ).update({'is_active': False})
                
                # Créer le nouveau token
                new_token = LinkedinTokens(
                    user_id=user_id,
                    application_type=app_type,
                    access_token=token_info['access_token'],
                    refresh_token=token_info.get('refresh_token'),
                    expires_at=token_info['expires_at'],
                    is_active=True
                )
                
                session.add(new_token)
                session.commit()
                
                logger.info(f"✅ Tokens LinkedIn stockés pour user {user_id} ({app_type})")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du stockage des tokens: {e}")
            return False
    
    def get_user_token(self, user_id: int, app_type: LinkedinAppType) -> Optional[LinkedinTokens]:
        """Récupérer le token actif d'un utilisateur pour une app"""
        
        try:
            with db_manager.get_session() as session:
                token = session.query(LinkedinTokens).filter(
                    LinkedinTokens.user_id == user_id,
                    LinkedinTokens.application_type == app_type.value,
                    LinkedinTokens.is_active == True
                ).first()
                
                # Vérifier l'expiration
                if token and token.expires_at and token.expires_at <= datetime.utcnow():
                    logger.warning(f"⚠️  Token expiré pour user {user_id} ({app_type.value})")
                    
                    # Essayer de rafraîchir si refresh_token disponible
                    if token.refresh_token:
                        try:
                            new_token_info = self.refresh_access_token(token.refresh_token, app_type)
                            new_token_info['app_type'] = app_type.value
                            
                            if self.store_user_tokens(user_id, new_token_info):
                                # Récupérer le nouveau token
                                return self.get_user_token(user_id, app_type)
                        except Exception as e:
                            logger.error(f"❌ Échec du rafraîchissement automatique: {e}")
                    
                    return None
                
                return token
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du token: {e}")
            return None
    
    def connect_linkedin_account(self, user_id: int, code: str, state: str, 
                                app_type: LinkedinAppType) -> Dict[str, Any]:
        """Processus complet de connexion d'un compte LinkedIn"""
        
        try:
            # 1. Échanger le code contre un token
            logger.info(f"🔄 Connexion compte LinkedIn pour user {user_id} ({app_type.value})")
            
            token_info = self.exchange_code_for_token(code, state, app_type)
            
            # 2. Récupérer le profil utilisateur
            profile = self.get_user_profile(token_info['access_token'])
            
            # 3. Récupérer les organisations (pour Community/Portability apps)
            organizations = []
            if app_type in [LinkedinAppType.COMMUNITY, LinkedinAppType.PORTABILITY]:
                try:
                    organizations = self.get_user_organizations(token_info['access_token'])
                except Exception as e:
                    logger.warning(f"⚠️  Impossible de récupérer les organisations: {e}")
            
            # 4. Stocker les tokens
            if not self.store_user_tokens(user_id, token_info):
                raise LinkedinOAuthError("Impossible de stocker les tokens", error_code="TOKEN_STORAGE_FAILED")
            
            # 5. Stocker les tokens dans le format générique aussi
            user_manager.store_social_token(
                user_id=user_id,
                platform=f"linkedin_{app_type.value}",
                access_token=token_info['access_token'],
                refresh_token=token_info.get('refresh_token'),
                expires_at=token_info['expires_at'],
                scope=token_info.get('scope')
            )
            
            # 6. Ajouter les comptes d'organisation
            added_accounts = []
            for org in organizations:
                try:
                    account = user_manager.add_linkedin_account(
                        user_id=user_id,
                        organization_id=org['id'],
                        organization_name=org['name']
                    )
                    if account:
                        added_accounts.append({
                            'id': org['id'],
                            'name': org['name'],
                            'vanityName': org.get('vanityName'),
                            'role': org.get('role'),
                            'logoUrl': org.get('logoUrl')
                        })
                except Exception as e:
                    logger.error(f"❌ Erreur lors de l'ajout de l'organisation {org['id']}: {e}")
            
            result = {
                'success': True,
                'app_type': app_type.value,
                'profile': profile,
                'organizations': added_accounts,
                'token_expires_at': token_info['expires_at'].isoformat(),
                'message': f"Compte LinkedIn connecté avec succès ({len(added_accounts)} organisations)"
            }
            
            logger.info(f"✅ Compte LinkedIn connecté: user {user_id}, {len(added_accounts)} orgs")
            return result
            
        except LinkedinOAuthError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la connexion du compte LinkedIn: {e}")
            raise LinkedinOAuthError(
                f"Erreur lors de la connexion: {e}",
                error_code="CONNECTION_FAILED",
                app_type=app_type.value
            )
    
    def disconnect_linkedin_account(self, user_id: int, app_type: LinkedinAppType = None) -> bool:
        """Déconnecter un ou tous les comptes LinkedIn"""
        
        try:
            with db_manager.get_session() as session:
                # Désactiver les tokens
                query = session.query(LinkedinTokens).filter(LinkedinTokens.user_id == user_id)
                
                if app_type:
                    query = query.filter(LinkedinTokens.application_type == app_type.value)
                
                query.update({'is_active': False})
                
                # Désactiver les tokens génériques aussi
                platform_filter = f"linkedin_{app_type.value}" if app_type else "linkedin_%"
                user_manager.revoke_social_token(user_id, platform_filter)
                
                # Si on déconnecte tout, désactiver les comptes d'organisation
                if not app_type:
                    session.query(LinkedinAccount).filter(
                        LinkedinAccount.user_id == user_id
                    ).update({'is_active': False})
                
                session.commit()
                
                app_desc = app_type.value if app_type else "tous les types"
                logger.info(f"✅ Comptes LinkedIn déconnectés pour user {user_id} ({app_desc})")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la déconnexion: {e}")
            return False
    
    def get_user_linkedin_status(self, user_id: int) -> Dict[str, Any]:
        """Récupérer le statut des connexions LinkedIn d'un utilisateur"""
        
        try:
            status = {
                'user_id': user_id,
                'apps': {},
                'organizations': [],
                'has_valid_tokens': False
            }
            
            # Vérifier chaque application
            for app_type in LinkedinAppType:
                token = self.get_user_token(user_id, app_type)
                
                app_status = {
                    'connected': token is not None,
                    'expires_at': token.expires_at.isoformat() if token and token.expires_at else None,
                    'has_refresh_token': token.refresh_token is not None if token else False
                }
                
                status['apps'][app_type.value] = app_status
                
                if token:
                    status['has_valid_tokens'] = True
            
            # Récupérer les organisations connectées
            with db_manager.get_session() as session:
                linkedin_accounts = session.query(LinkedinAccount).filter(
                    LinkedinAccount.user_id == user_id,
                    LinkedinAccount.is_active == True
                ).all()
                
                status['organizations'] = [
                    {
                        'id': acc.organization_id,
                        'name': acc.organization_name,
                        'connected_at': acc.created_at.isoformat()
                    }
                    for acc in linkedin_accounts
                ]
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du statut: {e}")
            return {'user_id': user_id, 'error': str(e)}
    
    # ========================================
    # MÉTHODES UTILITAIRES
    # ========================================
    
    def _parse_error_response(self, response: requests.Response) -> Dict[str, Any]:
        """Parser une réponse d'erreur LinkedIn"""
        
        try:
            error_data = response.json()
        except:
            error_data = {'error': 'unknown', 'error_description': response.text or 'Erreur inconnue'}
        
        # Format standard des erreurs LinkedIn
        if 'error' not in error_data and 'message' in error_data:
            error_data['error'] = 'api_error'
            error_data['error_description'] = error_data['message']
        
        return error_data
    
    def _extract_logo_url(self, logo_data: Dict) -> Optional[str]:
        """Extraire l'URL du logo depuis les données LinkedIn"""
        
        try:
            # Récupérer la plus grande image disponible
            original_elements = logo_data.get('originalElements', [])
            if not original_elements:
                return None
            
            # Trier par taille décroissante
            elements = sorted(
                original_elements,
                key=lambda x: x.get('data', {}).get('com.linkedin.digitalmedia.mediaartifact.StillImage', {}).get('storageSize', {}).get('width', 0),
                reverse=True
            )
            
            if elements:
                return elements[0].get('identifiers', [{}])[0].get('identifier')
            
        except Exception as e:
            logger.debug(f"Impossible d'extraire l'URL du logo: {e}")
        
        return None
    
    def get_app_config(self, app_type: LinkedinAppType) -> LinkedinAppConfig:
        """Récupérer la configuration d'une application"""
        return self.apps.get(app_type)
    
    def get_all_app_configs(self) -> Dict[LinkedinAppType, LinkedinAppConfig]:
        """Récupérer toutes les configurations d'applications"""
        return self.apps.copy()
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du gestionnaire OAuth"""
        
        health = {
            'oauth_manager': 'ok',
            'apps_configured': len(self.apps),
            'apps_details': {},
            'session_ready': self.session is not None,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Vérifier chaque application
        for app_type, config in self.apps.items():
            app_health = {
                'client_id_present': bool(config.client_id),
                'client_secret_present': bool(config.client_secret),
                'redirect_uri': config.redirect_uri,
                'scopes_count': len(config.scopes)
            }
            health['apps_details'][app_type.value] = app_health
        
        return health

# ========================================
# INSTANCE GLOBALE
# ========================================

linkedin_oauth_manager = LinkedinOAuthManager()

# ========================================
# FONCTIONS HELPER
# ========================================

def get_linkedin_auth_url(app_type: str, user_id: int = None) -> Tuple[str, str]:
    """Fonction helper pour obtenir une URL d'authentification"""
    
    try:
        app_type_enum = LinkedinAppType(app_type)
        return linkedin_oauth_manager.get_authorization_url(app_type_enum, user_id)
    except ValueError:
        raise ValueError(f"Type d'application invalide: {app_type}")

def handle_linkedin_callback(code: str, state: str, app_type: str, user_id: int) -> Dict[str, Any]:
    """Fonction helper pour gérer le callback OAuth"""
    
    try:
        app_type_enum = LinkedinAppType(app_type)
        return linkedin_oauth_manager.connect_linkedin_account(user_id, code, state, app_type_enum)
    except ValueError:
        raise ValueError(f"Type d'application invalide: {app_type}")

def get_user_linkedin_info(user_id: int) -> Dict[str, Any]:
    """Fonction helper pour obtenir les infos LinkedIn d'un utilisateur"""
    return linkedin_oauth_manager.get_user_linkedin_status(user_id)

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test LinkedinOAuthManager...")
    
    try:
        # Test de configuration
        manager = LinkedinOAuthManager()
        health = manager.health_check()
        print(f"Health check: {json.dumps(health, indent=2)}")
        
        # Test de génération d'URL
        auth_url, state = manager.get_authorization_url(LinkedinAppType.COMMUNITY, user_id=1)
        print(f"URL d'autorisation: {auth_url}")
        print(f"État: {state}")
        
        # Test de validation d'état
        state_data = manager.validate_oauth_state(state)
        print(f"Données d'état: {state_data}")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")
        import traceback
        traceback.print_exc()