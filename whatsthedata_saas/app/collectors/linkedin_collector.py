"""
Collecteur LinkedIn complet et robuste
Récupère toutes les métriques LinkedIn via les 3 APIs (Community, Portability, Sign In)
Avec gestion d'erreurs exhaustive, retry automatique, et respect des quotas
"""

import os
import json
import time
import logging
import asyncio
import threading
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass, asdict
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..auth.linkedin_oauth import linkedin_oauth_manager, LinkedinAppType, LinkedinOAuthError, LinkedinAPIError
from ..auth.user_manager import user_manager
from ..database.connection import db_manager
from ..database.models import (
    User, LinkedinAccount, LinkedinTokens, LinkedinPagesMetadata,
    LinkedinPageDaily, LinkedinPageLifetime, LinkedinPostsMetadata, 
    LinkedinPostsDaily, LinkedinFollowerByCompanySize, LinkedinFollowerByFunction,
    LinkedinFollowerByIndustry, LinkedinFollowerBySeniority,
    LinkedinPageViewsByCountry, LinkedinPageViewsByIndustry, LinkedinPageViewsBySeniority
)
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

class CollectionStatus(Enum):
    """Statuts de collecte"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    QUOTA_EXCEEDED = "quota_exceeded"
    TOKEN_EXPIRED = "token_expired"
    PERMISSION_DENIED = "permission_denied"

class DataType(Enum):
    """Types de données collectées"""
    FOLLOWERS = "followers"
    FOLLOWERS_BREAKDOWN = "followers_breakdown"
    PAGE_METRICS = "page_metrics"
    PAGE_VIEWS = "page_views"
    POSTS = "posts"
    POSTS_METRICS = "posts_metrics"
    ORGANIZATION_INFO = "organization_info"

@dataclass
class CollectionResult:
    """Résultat d'une collecte"""
    status: CollectionStatus
    data_type: DataType
    organization_id: str
    records_collected: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    execution_time: float = 0.0
    api_calls_made: int = 0
    quota_remaining: Optional[int] = None
    next_collection_allowed: Optional[datetime] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []

@dataclass
class LinkedinQuota:
    """Informations sur les quotas LinkedIn"""
    app_type: LinkedinAppType
    daily_limit: int
    requests_made: int
    reset_time: datetime
    remaining: int
    
    @property
    def is_exhausted(self) -> bool:
        return self.remaining <= 0

class LinkedinCollectionError(Exception):
    """Erreur de collecte LinkedIn"""
    
    def __init__(self, message: str, error_code: str = None, 
                 organization_id: str = None, data_type: DataType = None):
        super().__init__(message)
        self.error_code = error_code
        self.organization_id = organization_id
        self.data_type = data_type
        self.timestamp = datetime.utcnow()

class LinkedinCollector:
    """Collecteur principal LinkedIn"""
    
    def __init__(self):
        self.oauth_manager = linkedin_oauth_manager
        self.session = self._create_session()
        
        # Configuration de collecte
        self.config = self._load_collection_config()
        
        # Cache des quotas par application
        self._quota_cache = {}
        
        # Verrous pour éviter les collectes concurrentes par organisation
        self._collection_locks = {}
        
        # Statistiques de session
        self.session_stats = {
            'collections_started': 0,
            'collections_completed': 0,
            'collections_failed': 0,
            'total_api_calls': 0,
            'total_records_collected': 0,
            'start_time': datetime.utcnow()
        }
        
        # Mapping des endpoints API LinkedIn
        self.api_endpoints = {
            # Community Management API
            'posts': '/rest/posts',
            'share_statistics': '/rest/organizationalEntityShareStatistics',
            'social_actions': '/rest/socialActions',
            'social_metadata': '/rest/socialMetadata',
            'follower_statistics': '/rest/organizationalEntityFollowerStatistics',
            'network_sizes': '/rest/networkSizes',
            
            # Portability Data API
            'dma_feed_contents': '/rest/dmaFeedContentsExternal',
            'dma_instant_reposts': '/rest/dmaInstantReposts',
            
            # Common endpoints
            'me': '/rest/me',
            'organization_acls': '/rest/organizationAcls'
        }
        
    def _create_session(self) -> requests.Session:
        """Créer une session HTTP optimisée pour LinkedIn"""
        
        session = requests.Session()
        
        # Configuration du retry spécifique à LinkedIn
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST"],
            backoff_factor=2,
            respect_retry_after_header=True,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers optimisés pour LinkedIn
        session.headers.update({
            'User-Agent': f'{get_env_var("APP_NAME", "WhatsTheData")}/{get_env_var("APP_VERSION", "1.0.0")}',
            'Accept': 'application/json',
            'LinkedIn-Version': '202408',
            'X-Restli-Protocol-Version': '2.0.0'
        })
        
        return session
    
    def _load_collection_config(self) -> Dict[str, Any]:
        """Charger la configuration de collecte"""
        
        config = {
            'collection_interval_hours': int(get_env_var('COLLECTION_INTERVAL_HOURS', '4')),
            'max_posts_per_collection': int(get_env_var('MAX_POSTS_PER_COLLECTION', '100')),
            'api_timeout_seconds': int(get_env_var('API_TIMEOUT_SECONDS', '30')),
            'api_retry_attempts': int(get_env_var('API_RETRY_ATTEMPTS', '3')),
            'parallel_requests': int(get_env_var('PARALLEL_REQUESTS', '3')),
            'skip_old_posts_days': int(get_env_var('SKIP_OLD_POSTS_DAYS', '90')),
            'max_posts_per_org': int(get_env_var('MAX_POSTS_PER_ORG', '1000')),
            'rate_limit_margin': int(get_env_var('RATE_LIMIT_MARGIN', '10')),  # % de marge sur les quotas
            'enable_concurrent_collection': get_env_var('ENABLE_CONCURRENT_COLLECTION', 'true').lower() == 'true',
            'debug_mode': get_env_var('DEBUG_MODE', 'false').lower() == 'true'
        }
        
        logger.info(f"✅ Configuration de collecte chargée: {config}")
        return config
    
    # ========================================
    # GESTION DES QUOTAS LINKEDIN
    # ========================================
    
    def _get_quota_info(self, app_type: LinkedinAppType, access_token: str) -> LinkedinQuota:
        """Récupérer les informations de quota pour une application"""
        
        try:
            # Vérifier le cache d'abord
            cache_key = f"{app_type.value}_{access_token[-10:]}"  # Utiliser les 10 derniers chars du token
            
            if cache_key in self._quota_cache:
                cached_quota = self._quota_cache[cache_key]
                # Si le cache n'est pas expiré (5 minutes), l'utiliser
                if datetime.utcnow() < cached_quota.reset_time:
                    return cached_quota
            
            # Faire un appel de test pour récupérer les headers de quota
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            response = self.session.get(
                'https://api.linkedin.com/rest/me',
                headers=headers,
                timeout=10
            )
            
            # Extraire les informations de quota des headers
            daily_limit = int(response.headers.get('X-RateLimit-Limit', '1000'))
            remaining = int(response.headers.get('X-RateLimit-Remaining', '999'))
            reset_time_header = response.headers.get('X-RateLimit-Reset')
            
            # Calculer le temps de reset
            if reset_time_header:
                try:
                    reset_time = datetime.fromtimestamp(int(reset_time_header))
                except:
                    reset_time = datetime.utcnow() + timedelta(hours=1)
            else:
                reset_time = datetime.utcnow() + timedelta(hours=1)
            
            quota = LinkedinQuota(
                app_type=app_type,
                daily_limit=daily_limit,
                requests_made=daily_limit - remaining,
                reset_time=reset_time,
                remaining=remaining
            )
            
            # Mettre en cache
            self._quota_cache[cache_key] = quota
            
            logger.debug(f"✅ Quota LinkedIn récupéré: {remaining}/{daily_limit} pour {app_type.value}")
            return quota
            
        except Exception as e:
            logger.warning(f"⚠️  Impossible de récupérer le quota LinkedIn: {e}")
            # Retourner un quota par défaut conservateur
            return LinkedinQuota(
                app_type=app_type,
                daily_limit=500,
                requests_made=0,
                reset_time=datetime.utcnow() + timedelta(hours=1),
                remaining=500
            )
    
    def _check_quota_available(self, app_type: LinkedinAppType, access_token: str, 
                              requests_needed: int = 1) -> Tuple[bool, LinkedinQuota]:
        """Vérifier si le quota est suffisant pour les requêtes demandées"""
        
        quota = self._get_quota_info(app_type, access_token)
        
        # Appliquer la marge de sécurité
        margin = int(quota.remaining * (self.config['rate_limit_margin'] / 100))
        available_with_margin = quota.remaining - margin
        
        is_available = available_with_margin >= requests_needed
        
        if not is_available:
            logger.warning(f"⚠️  Quota insuffisant: {available_with_margin} disponible, {requests_needed} nécessaire")
        
        return is_available, quota
    
    def _update_quota_cache(self, app_type: LinkedinAppType, access_token: str, 
                           requests_made: int):
        """Mettre à jour le cache de quota après des requêtes"""
        
        cache_key = f"{app_type.value}_{access_token[-10:]}"
        
        if cache_key in self._quota_cache:
            cached_quota = self._quota_cache[cache_key]
            cached_quota.requests_made += requests_made
            cached_quota.remaining = max(0, cached_quota.remaining - requests_made)
    
    # ========================================
    # COLLECTE DES DONNÉES D'ORGANISATION
    # ========================================
    
    def collect_organization_info(self, user_id: int, organization_id: str) -> CollectionResult:
        """Collecter les informations de base d'une organisation"""
        
        start_time = time.time()
        result = CollectionResult(
            status=CollectionStatus.PENDING,
            data_type=DataType.ORGANIZATION_INFO,
            organization_id=organization_id
        )
        
        try:
            # Récupérer le token Community Management
            token = self.oauth_manager.get_user_token(user_id, LinkedinAppType.COMMUNITY)
            if not token:
                raise LinkedinCollectionError(
                    "Token Community Management non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    organization_id=organization_id
                )
            
            # Vérifier le quota
            quota_ok, quota = self._check_quota_available(LinkedinAppType.COMMUNITY, token.access_token, 2)
            if not quota_ok:
                result.status = CollectionStatus.QUOTA_EXCEEDED
                result.quota_remaining = quota.remaining
                result.next_collection_allowed = quota.reset_time
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les informations de l'organisation
            org_info = self._fetch_organization_details(token.access_token, organization_id)
            result.api_calls_made += 1
            
            # Récupérer le nombre de followers
            follower_count = self._fetch_follower_count(token.access_token, organization_id)
            result.api_calls_made += 1
            
            # Combiner les informations
            org_data = {
                **org_info,
                'follower_count': follower_count,
                'last_updated': datetime.utcnow()
            }
            
            # Stocker en base de données
            self._store_organization_info(organization_id, org_data)
            result.records_collected = 1
            
            # Mettre à jour le quota
            self._update_quota_cache(LinkedinAppType.COMMUNITY, token.access_token, result.api_calls_made)
            
            result.status = CollectionStatus.SUCCESS
            logger.info(f"✅ Informations organisation collectées: {organization_id}")
            
        except LinkedinCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            logger.error(f"❌ Erreur collecte organisation {organization_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte organisation {organization_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_organization_details(self, access_token: str, organization_id: str) -> Dict[str, Any]:
        """Récupérer les détails d'une organisation"""
        
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            # Construire l'URN de l'organisation
            org_urn = f"urn:li:organization:{organization_id}"
            
            params = {
                'q': 'organizationalEntity',
                'organizationalEntity': org_urn,
                'projection': '(elements*(name,description,website,industry,companySize,headquarters,founded,logoV2))'
            }
            
            response = self.session.get(
                'https://api.linkedin.com/rest/organizations',
                headers=headers,
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('elements', [])
                
                if elements:
                    org = elements[0]
                    return {
                        'name': org.get('name', {}).get('localized', {}).get('en_US', 'Organisation inconnue'),
                        'description': org.get('description', {}).get('localized', {}).get('en_US', ''),
                        'website': org.get('website'),
                        'industry': org.get('industry'),
                        'company_size': org.get('companySize'),
                        'headquarters': org.get('headquarters', {}).get('localized', {}).get('en_US', ''),
                        'founded': org.get('founded'),
                        'logo_url': self._extract_logo_url(org.get('logoV2', {}))
                    }
            
            logger.warning(f"⚠️  Impossible de récupérer les détails de l'organisation {organization_id}")
            return {}
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des détails organisation: {e}")
            return {}
    
    def _fetch_follower_count(self, access_token: str, organization_id: str) -> int:
        """Récupérer le nombre total de followers"""
        
        try:
            headers = {
                'Authorization': f'Bearer {access_token}',
                'LinkedIn-Version': '202408'
            }
            
            org_urn = f"urn%3Ali%3Aorganization%3A{organization_id}"
            
            response = self.session.get(
                f'https://api.linkedin.com/rest/networkSizes/{org_urn}',
                headers=headers,
                params={'edgeType': 'COMPANY_FOLLOWED_BY_MEMBER'},
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('firstDegreeSize', 0)
            
            logger.warning(f"⚠️  Impossible de récupérer le nombre de followers pour {organization_id}")
            return 0
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du nombre de followers: {e}")
            return 0
    
    # ========================================
    # COLLECTE DES FOLLOWERS ET SEGMENTATION
    # ========================================
    
    def collect_followers_breakdown(self, user_id: int, organization_id: str) -> CollectionResult:
        """Collecter la segmentation des followers"""
        
        start_time = time.time()
        result = CollectionResult(
            status=CollectionStatus.PENDING,
            data_type=DataType.FOLLOWERS_BREAKDOWN,
            organization_id=organization_id
        )
        
        try:
            # Récupérer le token Community Management
            token = self.oauth_manager.get_user_token(user_id, LinkedinAppType.COMMUNITY)
            if not token:
                raise LinkedinCollectionError(
                    "Token Community Management non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    organization_id=organization_id
                )
            
            # Vérifier le quota (on va faire plusieurs requêtes)
            quota_ok, quota = self._check_quota_available(LinkedinAppType.COMMUNITY, token.access_token, 5)
            if not quota_ok:
                result.status = CollectionStatus.QUOTA_EXCEEDED
                result.quota_remaining = quota.remaining
                result.next_collection_allowed = quota.reset_time
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les différents breakdowns
            breakdowns = self._fetch_follower_statistics(token.access_token, organization_id)
            result.api_calls_made += len(breakdowns)
            
            # Stocker chaque breakdown
            records_stored = 0
            for breakdown_type, breakdown_data in breakdowns.items():
                try:
                    stored_count = self._store_follower_breakdown(organization_id, breakdown_type, breakdown_data)
                    records_stored += stored_count
                except Exception as e:
                    result.warnings.append(f"Erreur stockage {breakdown_type}: {e}")
            
            result.records_collected = records_stored
            
            # Mettre à jour le quota
            self._update_quota_cache(LinkedinAppType.COMMUNITY, token.access_token, result.api_calls_made)
            
            result.status = CollectionStatus.SUCCESS if records_stored > 0 else CollectionStatus.PARTIAL_SUCCESS
            logger.info(f"✅ Breakdown followers collecté: {organization_id} ({records_stored} records)")
            
        except LinkedinCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            logger.error(f"❌ Erreur collecte breakdown {organization_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte breakdown {organization_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_follower_statistics(self, access_token: str, organization_id: str) -> Dict[str, Any]:
        """Récupérer les statistiques de followers segmentées"""
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'LinkedIn-Version': '202408'
        }
        
        org_urn = f"urn:li:organization:{organization_id}"
        
        params = {
            'q': 'organizationalEntity',
            'organizationalEntity': org_urn
        }
        
        breakdowns = {}
        
        try:
            response = self.session.get(
                'https://api.linkedin.com/rest/organizationalEntityFollowerStatistics',
                headers=headers,
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('elements', [])
                
                for element in elements:
                    follower_counts = element.get('followerCounts', {})
                    
                    # Segmentation par taille d'entreprise
                    if 'companySizes' in follower_counts:
                        breakdowns['company_size'] = follower_counts['companySizes']
                    
                    # Segmentation par fonction
                    if 'functions' in follower_counts:
                        breakdowns['function'] = follower_counts['functions']
                    
                    # Segmentation par industrie
                    if 'industries' in follower_counts:
                        breakdowns['industry'] = follower_counts['industries']
                    
                    # Segmentation par séniorité
                    if 'seniorities' in follower_counts:
                        breakdowns['seniority'] = follower_counts['seniorities']
                    
                    # Segmentation par pays (dans les vues de page)
                    if 'countries' in follower_counts:
                        breakdowns['country'] = follower_counts['countries']
            
            logger.info(f"✅ {len(breakdowns)} types de breakdown récupérés pour {organization_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des statistiques followers: {e}")
        
        return breakdowns
    
    # ========================================
    # COLLECTE DES POSTS
    # ========================================
    
    def collect_posts(self, user_id: int, organization_id: str, 
                     max_posts: int = None, since_date: date = None) -> CollectionResult:
        """Collecter les posts d'une organisation"""
        
        start_time = time.time()
        result = CollectionResult(
            status=CollectionStatus.PENDING,
            data_type=DataType.POSTS,
            organization_id=organization_id
        )
        
        try:
            # Récupérer le token Community Management
            token = self.oauth_manager.get_user_token(user_id, LinkedinAppType.COMMUNITY)
            if not token:
                raise LinkedinCollectionError(
                    "Token Community Management non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    organization_id=organization_id
                )
            
            # Configuration de la collecte
            max_posts = max_posts or self.config['max_posts_per_collection']
            since_date = since_date or (date.today() - timedelta(days=self.config['skip_old_posts_days']))
            
            # Estimer le nombre de requêtes nécessaires
            estimated_requests = min(10, (max_posts // 100) + 3)  # Estimation conservatrice
            
            quota_ok, quota = self._check_quota_available(LinkedinAppType.COMMUNITY, token.access_token, estimated_requests)
            if not quota_ok:
                result.status = CollectionStatus.QUOTA_EXCEEDED
                result.quota_remaining = quota.remaining
                result.next_collection_allowed = quota.reset_time
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les posts
            posts_data = self._fetch_organization_posts(
                token.access_token, 
                organization_id, 
                max_posts, 
                since_date
            )
            
            result.api_calls_made += posts_data['api_calls_made']
            
            # Stocker les posts
            stored_count = self._store_posts_metadata(organization_id, posts_data['posts'])
            result.records_collected = stored_count
            
            # Mettre à jour le quota
            self._update_quota_cache(LinkedinAppType.COMMUNITY, token.access_token, result.api_calls_made)
            
            result.status = CollectionStatus.SUCCESS
            logger.info(f"✅ Posts collectés: {organization_id} ({stored_count} posts)")
            
        except LinkedinCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            logger.error(f"❌ Erreur collecte posts {organization_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte posts {organization_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_organization_posts(self, access_token: str, organization_id: str,
                                 max_posts: int, since_date: date) -> Dict[str, Any]:
        """Récupérer les posts d'une organisation"""
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'LinkedIn-Version': '202408'
        }
        
        org_urn = f"urn:li:organization:{organization_id}"
        since_timestamp = int(since_date.strftime('%s')) * 1000  # Convertir en millisecondes
        
        posts = []
        api_calls_made = 0
        start = 0
        count = min(100, max_posts)  # LinkedIn limite à 100 par requête
        
        try:
            while len(posts) < max_posts:
                params = {
                    'q': 'author',
                    'author': org_urn,
                    'count': count,
                    'start': start,
                    'sortBy': 'CREATED'
                }
                
                response = self.session.get(
                    'https://api.linkedin.com/rest/posts',
                    headers=headers,
                    params=params,
                    timeout=self.config['api_timeout_seconds']
                )
                
                api_calls_made += 1
                
                if response.status_code != 200:
                    logger.warning(f"⚠️  Erreur récupération posts: {response.status_code}")
                    break
                
                data = response.json()
                elements = data.get('elements', [])
                
                if not elements:
                    logger.info("ℹ️  Plus de posts à récupérer")
                    break
                
                # Filtrer les posts par date
                filtered_posts = []
                for post in elements:
                    created_time = post.get('createdAt', 0)
                    if created_time >= since_timestamp:
                        filtered_posts.append(self._normalize_post_data(post, organization_id))
                    else:
                        # Si on trouve un post trop ancien, arrêter
                        logger.info(f"ℹ️  Post trop ancien trouvé, arrêt de la collecte")
                        break
                
                posts.extend(filtered_posts)
                
                # Si on a moins d'éléments que demandé, on a atteint la fin
                if len(elements) < count:
                    break
                
                start += count
                
                # Pause pour respecter les rate limits
                time.sleep(0.1)
            
            # Récupérer aussi les reposts instantanés si possible
            try:
                instant_reposts = self._fetch_instant_reposts(access_token, organization_id, since_date)
                posts.extend(instant_reposts['posts'])
                api_calls_made += instant_reposts['api_calls_made']
            except Exception as e:
                logger.warning(f"⚠️  Impossible de récupérer les reposts instantanés: {e}")
            
            logger.info(f"✅ {len(posts)} posts récupérés pour {organization_id} ({api_calls_made} API calls)")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des posts: {e}")
        
        return {
            'posts': posts[:max_posts],  # Limiter au maximum demandé
            'api_calls_made': api_calls_made
        }
    
    def _fetch_instant_reposts(self, access_token: str, organization_id: str, 
                              since_date: date) -> Dict[str, Any]:
        """Récupérer les reposts instantanés (Portability API)"""
        
        # Essayer d'utiliser le token Portability si disponible
        portability_token = None
        try:
            # Cette méthode nécessiterait une modification de oauth_manager
            # Pour l'instant, on utilise le même token
            portability_token = access_token
        except:
            pass
        
        if not portability_token:
            return {'posts': [], 'api_calls_made': 0}
        
        headers = {
            'Authorization': f'Bearer {portability_token}',
            'LinkedIn-Version': '202408'
        }
        
        org_urn = f"urn:li:organization:{organization_id}"
        posts = []
        api_calls_made = 0
        
        try:
            params = {
                'q': 'author',
                'author': org_urn,
                'count': 100
            }
            
            response = self.session.get(
                'https://api.linkedin.com/rest/dmaInstantReposts',
                headers=headers,
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            api_calls_made += 1
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('elements', [])
                
                for repost in elements:
                    normalized_repost = self._normalize_repost_data(repost, organization_id)
                    posts.append(normalized_repost)
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur récupération reposts instantanés: {e}")
        
        return {
            'posts': posts,
            'api_calls_made': api_calls_made
        }
    
    def _normalize_post_data(self, post_data: Dict[str, Any], organization_id: str) -> Dict[str, Any]:
        """Normaliser les données d'un post LinkedIn"""
        
        try:
            # Extraire l'ID du post
            post_urn = post_data.get('id', '')
            post_id = post_urn.split(':')[-1] if ':' in post_urn else post_urn
            
            # Déterminer le type de post
            post_type = 'post'
            post_subtype = 'original'
            
            if post_data.get('reshareContext'):
                post_type = 'share'
                post_subtype = 'repost_with_comment'
            
            # Extraire le contenu textuel
            text_content = ''
            if 'commentary' in post_data:
                text_content = post_data['commentary']
            elif 'content' in post_data:
                content = post_data['content']
                if isinstance(content, dict) and 'text' in content:
                    text_content = content['text']
            
            # Limiter le texte à 1000 caractères
            text_content = text_content[:1000] if text_content else ''
            
            # Extraire les informations média
            media_type = None
            media_url = None
            
            if 'content' in post_data:
                content = post_data['content']
                if isinstance(content, dict):
                    if 'media' in content:
                        media_type = 'media'
                        # Extraire l'URL si disponible
                    elif 'article' in content:
                        media_type = 'article'
                    elif 'poll' in content:
                        media_type = 'poll'
            
            # Date de création
            created_time = None
            if 'createdAt' in post_data:
                try:
                    # LinkedIn utilise des timestamps en millisecondes
                    timestamp = post_data['createdAt'] / 1000
                    created_time = datetime.fromtimestamp(timestamp)
                except:
                    created_time = datetime.utcnow()
            
            return {
                'post_urn': post_urn,
                'organization_id': organization_id,
                'post_type': post_type,
                'post_subtype': post_subtype,
                'author_urn': post_data.get('author', f'urn:li:organization:{organization_id}'),
                'created_time': created_time,
                'text_content': text_content,
                'media_type': media_type,
                'media_url': media_url
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur normalisation post: {e}")
            return {}
    
    def _normalize_repost_data(self, repost_data: Dict[str, Any], organization_id: str) -> Dict[str, Any]:
        """Normaliser les données d'un repost instantané"""
        
        try:
            return {
                'post_urn': repost_data.get('id', ''),
                'organization_id': organization_id,
                'post_type': 'instantRepost',
                'post_subtype': 'instant_repost',
                'author_urn': f'urn:li:organization:{organization_id}',
                'created_time': datetime.fromtimestamp(repost_data.get('createdAt', 0) / 1000),
                'text_content': '',  # Les reposts instantanés n'ont pas de texte
                'media_type': None,
                'media_url': None,
                'original_post_urn': repost_data.get('originalPost', '')
            }
        except Exception as e:
            logger.error(f"❌ Erreur normalisation repost: {e}")
            return {}
    
    # ========================================
    # COLLECTE DES MÉTRIQUES DE POSTS
    # ========================================
    
    def collect_posts_metrics(self, user_id: int, organization_id: str,
                             post_urns: List[str] = None) -> CollectionResult:
        """Collecter les métriques des posts"""
        
        start_time = time.time()
        result = CollectionResult(
            status=CollectionStatus.PENDING,
            data_type=DataType.POSTS_METRICS,
            organization_id=organization_id
        )
        
        try:
            # Récupérer le token Community Management
            token = self.oauth_manager.get_user_token(user_id, LinkedinAppType.COMMUNITY)
            if not token:
                raise LinkedinCollectionError(
                    "Token Community Management non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    organization_id=organization_id
                )
            
            # Si pas de posts spécifiés, récupérer les posts récents de la DB
            if not post_urns:
                post_urns = self._get_recent_posts_from_db(organization_id)
            
            if not post_urns:
                result.status = CollectionStatus.SUCCESS
                result.records_collected = 0
                logger.info(f"ℹ️  Aucun post à traiter pour {organization_id}")
                return result
            
            # Estimer le nombre de requêtes (3 requêtes par post environ)
            estimated_requests = len(post_urns) * 3
            
            quota_ok, quota = self._check_quota_available(LinkedinAppType.COMMUNITY, token.access_token, estimated_requests)
            if not quota_ok:
                result.status = CollectionStatus.QUOTA_EXCEEDED
                result.quota_remaining = quota.remaining
                result.next_collection_allowed = quota.reset_time
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Collecter les métriques par batch
            batch_size = min(10, len(post_urns))  # Traiter par lots de 10
            total_metrics_collected = 0
            
            for i in range(0, len(post_urns), batch_size):
                batch_urns = post_urns[i:i + batch_size]
                
                try:
                    batch_metrics = self._fetch_posts_metrics_batch(
                        token.access_token,
                        organization_id,
                        batch_urns
                    )
                    
                    result.api_calls_made += batch_metrics['api_calls_made']
                    
                    # Stocker les métriques
                    stored_count = self._store_posts_metrics(batch_metrics['metrics'])
                    total_metrics_collected += stored_count
                    
                    # Pause entre les batches
                    if i + batch_size < len(post_urns):
                        time.sleep(1)
                    
                except Exception as e:
                    result.warnings.append(f"Erreur batch {i}-{i+batch_size}: {e}")
                    continue
            
            result.records_collected = total_metrics_collected
            
            # Mettre à jour le quota
            self._update_quota_cache(LinkedinAppType.COMMUNITY, token.access_token, result.api_calls_made)
            
            result.status = CollectionStatus.SUCCESS if total_metrics_collected > 0 else CollectionStatus.PARTIAL_SUCCESS
            logger.info(f"✅ Métriques posts collectées: {organization_id} ({total_metrics_collected} records)")
            
        except LinkedinCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            logger.error(f"❌ Erreur collecte métriques posts {organization_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte métriques posts {organization_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_posts_metrics_batch(self, access_token: str, organization_id: str,
                                  post_urns: List[str]) -> Dict[str, Any]:
        """Récupérer les métriques d'un batch de posts"""
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'LinkedIn-Version': '202408'
        }
        
        org_urn = f"urn:li:organization:{organization_id}"
        metrics = []
        api_calls_made = 0
        
        for post_urn in post_urns:
            try:
                post_metrics = {}
                
                # 1. Statistiques de partage (impressions, clics, etc.)
                share_stats = self._fetch_post_share_statistics(headers, org_urn, post_urn)
                post_metrics.update(share_stats)
                api_calls_made += 1
                
                # 2. Actions sociales (likes, commentaires, partages)
                social_actions = self._fetch_post_social_actions(headers, post_urn)
                post_metrics.update(social_actions)
                api_calls_made += 1
                
                # 3. Métadonnées sociales (réactions détaillées)
                social_metadata = self._fetch_post_social_metadata(headers, post_urn)
                post_metrics.update(social_metadata)
                api_calls_made += 1
                
                # Ajouter les identifiants
                post_metrics['post_urn'] = post_urn
                post_metrics['organization_id'] = organization_id
                post_metrics['date'] = date.today()
                
                # Calculer les métriques dérivées
                post_metrics = self._calculate_derived_metrics(post_metrics)
                
                metrics.append(post_metrics)
                
                # Pause courte entre les posts
                time.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"⚠️  Erreur métriques post {post_urn}: {e}")
                continue
        
        return {
            'metrics': metrics,
            'api_calls_made': api_calls_made
        }
    
    def _fetch_post_share_statistics(self, headers: Dict[str, str], org_urn: str, post_urn: str) -> Dict[str, Any]:
        """Récupérer les statistiques de partage d'un post"""
        
        try:
            params = {
                'q': 'organizationalEntity',
                'organizationalEntity': org_urn,
                'shares': f'List({post_urn})'
            }
            
            response = self.session.get(
                'https://api.linkedin.com/rest/organizationalEntityShareStatistics',
                headers=headers,
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                elements = data.get('elements', [])
                
                if elements:
                    stats = elements[0]
                    return {
                        'impressions': stats.get('impressionCount', 0),
                        'unique_impressions': stats.get('uniqueImpressionCount', 0),
                        'clicks': stats.get('clickCount', 0),
                        'shares': stats.get('shareCount', 0),
                        'comments': stats.get('commentCount', 0),
                        'engagement_rate': stats.get('engagement', 0.0)
                    }
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur statistiques partage: {e}")
        
        return {}
    
    def _fetch_post_social_actions(self, headers: Dict[str, str], post_urn: str) -> Dict[str, Any]:
        """Récupérer les actions sociales d'un post"""
        
        try:
            response = self.session.get(
                f'https://api.linkedin.com/rest/socialActions/{post_urn}',
                headers=headers,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'total_reactions': data.get('likesSummary', {}).get('totalLikes', 0),
                    'comments_count': data.get('commentsSummary', {}).get('totalComments', 0)
                }
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur actions sociales: {e}")
        
        return {}
    
    def _fetch_post_social_metadata(self, headers: Dict[str, str], post_urn: str) -> Dict[str, Any]:
        """Récupérer les métadonnées sociales d'un post (réactions détaillées)"""
        
        try:
            response = self.session.get(
                f'https://api.linkedin.com/rest/socialMetadata/{post_urn}',
                headers=headers,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                reactions = data.get('reactions', {})
                
                # Extraire les réactions par type
                reaction_counts = {
                    'likes': 0,
                    'celebrates': 0,
                    'loves': 0,
                    'insights': 0,
                    'supports': 0,
                    'funnies': 0
                }
                
                for reaction_type, reaction_data in reactions.items():
                    count = reaction_data.get('count', 0)
                    if reaction_type.lower() == 'like':
                        reaction_counts['likes'] = count
                    elif reaction_type.lower() == 'celebrate':
                        reaction_counts['celebrates'] = count
                    elif reaction_type.lower() == 'love':
                        reaction_counts['loves'] = count
                    elif reaction_type.lower() == 'insightful':
                        reaction_counts['insights'] = count
                    elif reaction_type.lower() == 'support':
                        reaction_counts['supports'] = count
                    elif reaction_type.lower() == 'funny':
                        reaction_counts['funnies'] = count
                
                return reaction_counts
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur métadonnées sociales: {e}")
        
        return {}
    
    def _calculate_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculer les métriques dérivées"""
        
        try:
            impressions = metrics.get('impressions', 0)
            clicks = metrics.get('clicks', 0)
            total_reactions = metrics.get('total_reactions', 0)
            shares = metrics.get('shares', 0)
            comments = metrics.get('comments', 0)
            
            # Taux d'engagement
            if impressions > 0:
                engagement_rate = (total_reactions + shares + comments) / impressions
                metrics['engagement_rate'] = round(engagement_rate, 4)
            else:
                metrics['engagement_rate'] = 0.0
            
            # Total interactions
            metrics['total_interactions'] = clicks + shares + comments + total_reactions
            
            # Pourcentages de réactions
            if total_reactions > 0:
                for reaction_type in ['likes', 'celebrates', 'loves', 'insights', 'supports', 'funnies']:
                    count = metrics.get(reaction_type, 0)
                    percentage = count / total_reactions
                    metrics[f'{reaction_type[:-1]}_percentage'] = round(percentage, 4)
            
        except Exception as e:
            logger.error(f"❌ Erreur calcul métriques dérivées: {e}")
        
        return metrics
    
    # ========================================
    # STOCKAGE EN BASE DE DONNÉES
    # ========================================
    
    def _store_organization_info(self, organization_id: str, org_data: Dict[str, Any]):
        """Stocker les informations d'organisation en base"""
        
        try:
            with db_manager.get_session() as session:
                # Vérifier si l'organisation existe déjà
                existing = session.query(LinkedinPagesMetadata).filter(
                    LinkedinPagesMetadata.organization_id == organization_id
                ).first()
                
                if existing:
                    # Mettre à jour
                    for key, value in org_data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    existing.updated_at = datetime.utcnow()
                else:
                    # Créer nouveau
                    new_org = LinkedinPagesMetadata(
                        organization_id=organization_id,
                        **org_data
                    )
                    session.add(new_org)
                
                session.commit()
                logger.debug(f"✅ Informations organisation stockées: {organization_id}")
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage organisation: {e}")
            raise
    
    def _store_follower_breakdown(self, organization_id: str, breakdown_type: str, 
                                 breakdown_data: List[Dict]) -> int:
        """Stocker la segmentation des followers"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                today = date.today()
                
                for item in breakdown_data:
                    try:
                        # Déterminer la table selon le type
                        if breakdown_type == 'company_size':
                            # Supprimer les anciens records du jour
                            session.query(LinkedinFollowerByCompanySize).filter(
                                LinkedinFollowerByCompanySize.organization_id == organization_id,
                                LinkedinFollowerByCompanySize.date_collected == today
                            ).delete()
                            
                            record = LinkedinFollowerByCompanySize(
                                organization_id=organization_id,
                                company_size=item.get('companySize', 'Unknown'),
                                follower_count=item.get('followerCounts', 0),
                                percentage=item.get('followerCounts', 0) / max(1, sum(i.get('followerCounts', 0) for i in breakdown_data)),
                                date_collected=today
                            )
                            
                        elif breakdown_type == 'function':
                            session.query(LinkedinFollowerByFunction).filter(
                                LinkedinFollowerByFunction.organization_id == organization_id,
                                LinkedinFollowerByFunction.date_collected == today
                            ).delete()
                            
                            record = LinkedinFollowerByFunction(
                                organization_id=organization_id,
                                function_name=item.get('function', 'Unknown'),
                                follower_count=item.get('followerCounts', 0),
                                percentage=item.get('followerCounts', 0) / max(1, sum(i.get('followerCounts', 0) for i in breakdown_data)),
                                date_collected=today
                            )
                            
                        elif breakdown_type == 'industry':
                            session.query(LinkedinFollowerByIndustry).filter(
                                LinkedinFollowerByIndustry.organization_id == organization_id,
                                LinkedinFollowerByIndustry.date_collected == today
                            ).delete()
                            
                            record = LinkedinFollowerByIndustry(
                                organization_id=organization_id,
                                industry_name=item.get('industry', 'Unknown'),
                                follower_count=item.get('followerCounts', 0),
                                percentage=item.get('followerCounts', 0) / max(1, sum(i.get('followerCounts', 0) for i in breakdown_data)),
                                date_collected=today
                            )
                            
                        elif breakdown_type == 'seniority':
                            session.query(LinkedinFollowerBySeniority).filter(
                                LinkedinFollowerBySeniority.organization_id == organization_id,
                                LinkedinFollowerBySeniority.date_collected == today
                            ).delete()
                            
                            record = LinkedinFollowerBySeniority(
                                organization_id=organization_id,
                                seniority_level=item.get('seniority', 'Unknown'),
                                follower_count=item.get('followerCounts', 0),
                                percentage=item.get('followerCounts', 0) / max(1, sum(i.get('followerCounts', 0) for i in breakdown_data)),
                                date_collected=today
                            )
                            
                        else:
                            continue  # Type non supporté
                        
                        session.add(record)
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage item breakdown: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Breakdown {breakdown_type} stocké: {stored_count} records")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage breakdown: {e}")
            return 0
    
    def _store_posts_metadata(self, organization_id: str, posts_data: List[Dict]) -> int:
        """Stocker les métadonnées des posts"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                
                for post_data in posts_data:
                    try:
                        # Vérifier si le post existe déjà
                        existing = session.query(LinkedinPostsMetadata).filter(
                            LinkedinPostsMetadata.post_urn == post_data['post_urn']
                        ).first()
                        
                        if existing:
                            # Mettre à jour si nécessaire
                            continue
                        
                        # Créer nouveau post
                        new_post = LinkedinPostsMetadata(
                            post_urn=post_data['post_urn'],
                            organization_id=organization_id,
                            post_type=post_data.get('post_type'),
                            post_subtype=post_data.get('post_subtype'),
                            author_urn=post_data.get('author_urn'),
                            created_time=post_data.get('created_time'),
                            text_content=post_data.get('text_content', ''),
                            media_type=post_data.get('media_type'),
                            media_url=post_data.get('media_url')
                        )
                        
                        session.add(new_post)
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage post {post_data.get('post_urn')}: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Posts stockés: {stored_count} nouveaux posts")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage posts: {e}")
            return 0
    
    def _store_posts_metrics(self, metrics_data: List[Dict]) -> int:
        """Stocker les métriques des posts"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                
                for metrics in metrics_data:
                    try:
                        # Vérifier si les métriques existent déjà pour ce jour
                        existing = session.query(LinkedinPostsDaily).filter(
                            LinkedinPostsDaily.post_urn == metrics['post_urn'],
                            LinkedinPostsDaily.date == metrics['date']
                        ).first()
                        
                        if existing:
                            # Mettre à jour
                            for key, value in metrics.items():
                                if hasattr(existing, key) and key not in ['post_urn', 'organization_id', 'date']:
                                    setattr(existing, key, value)
                        else:
                            # Créer nouveau
                            new_metrics = LinkedinPostsDaily(
                                post_urn=metrics['post_urn'],
                                organization_id=metrics['organization_id'],
                                date=metrics['date'],
                                impressions=metrics.get('impressions', 0),
                                unique_impressions=metrics.get('unique_impressions', 0),
                                clicks=metrics.get('clicks', 0),
                                shares=metrics.get('shares', 0),
                                comments=metrics.get('comments', 0),
                                engagement_rate=metrics.get('engagement_rate', 0.0),
                                total_reactions=metrics.get('total_reactions', 0),
                                likes=metrics.get('likes', 0),
                                celebrates=metrics.get('celebrates', 0),
                                loves=metrics.get('loves', 0),
                                insights=metrics.get('insights', 0),
                                supports=metrics.get('supports', 0),
                                funnies=metrics.get('funnies', 0),
                                like_percentage=metrics.get('like_percentage', 0.0),
                                celebrate_percentage=metrics.get('celebrate_percentage', 0.0),
                                love_percentage=metrics.get('love_percentage', 0.0),
                                insight_percentage=metrics.get('insight_percentage', 0.0),
                                support_percentage=metrics.get('support_percentage', 0.0),
                                funny_percentage=metrics.get('funny_percentage', 0.0),
                                total_interactions=metrics.get('total_interactions', 0)
                            )
                            session.add(new_metrics)
                        
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage métriques post {metrics.get('post_urn')}: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Métriques posts stockées: {stored_count} records")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage métriques posts: {e}")
            return 0
    
    def _get_recent_posts_from_db(self, organization_id: str, days: int = 30) -> List[str]:
        """Récupérer les URNs des posts récents depuis la base"""
        
        try:
            with db_manager.get_session() as session:
                cutoff_date = datetime.utcnow() - timedelta(days=days)
                
                posts = session.query(LinkedinPostsMetadata.post_urn).filter(
                    LinkedinPostsMetadata.organization_id == organization_id,
                    LinkedinPostsMetadata.created_time >= cutoff_date
                ).limit(self.config['max_posts_per_org']).all()
                
                return [post.post_urn for post in posts]
                
        except Exception as e:
            logger.error(f"❌ Erreur récupération posts récents: {e}")
            return []
    
    # ========================================
    # COLLECTE COMPLÈTE PAR ORGANISATION
    # ========================================
    
    def collect_organization_data(self, user_id: int, organization_id: str,
                                 data_types: List[DataType] = None) -> Dict[str, CollectionResult]:
        """Collecter toutes les données d'une organisation"""
        
        if data_types is None:
            data_types = [
                DataType.ORGANIZATION_INFO,
                DataType.FOLLOWERS_BREAKDOWN,
                DataType.POSTS,
                DataType.POSTS_METRICS
            ]
        
        # Verrou pour éviter les collectes concurrentes
        lock_key = f"collect_{organization_id}"
        if lock_key in self._collection_locks:
            logger.warning(f"⚠️  Collecte déjà en cours pour {organization_id}")
            return {}
        
        self._collection_locks[lock_key] = threading.Lock()
        
        try:
            with self._collection_locks[lock_key]:
                self.session_stats['collections_started'] += 1
                start_time = time.time()
                
                logger.info(f"🚀 Début collecte complète: {organization_id}")
                
                results = {}
                
                # Collecter chaque type de données
                for data_type in data_types:
                    try:
                        if data_type == DataType.ORGANIZATION_INFO:
                            result = self.collect_organization_info(user_id, organization_id)
                        elif data_type == DataType.FOLLOWERS_BREAKDOWN:
                            result = self.collect_followers_breakdown(user_id, organization_id)
                        elif data_type == DataType.POSTS:
                            result = self.collect_posts(user_id, organization_id)
                        elif data_type == DataType.POSTS_METRICS:
                            result = self.collect_posts_metrics(user_id, organization_id)
                        else:
                            continue
                        
                        results[data_type.value] = result
                        
                        # Mettre à jour les statistiques
                        self.session_stats['total_api_calls'] += result.api_calls_made
                        self.session_stats['total_records_collected'] += result.records_collected
                        
                        # Pause entre les types de données
                        time.sleep(1)
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur collecte {data_type.value}: {e}")
                        results[data_type.value] = CollectionResult(
                            status=CollectionStatus.FAILED,
                            data_type=data_type,
                            organization_id=organization_id,
                            errors=[str(e)]
                        )
                
                # Statistiques finales
                execution_time = time.time() - start_time
                successful_collections = sum(1 for r in results.values() if r.status == CollectionStatus.SUCCESS)
                
                if successful_collections > 0:
                    self.session_stats['collections_completed'] += 1
                else:
                    self.session_stats['collections_failed'] += 1
                
                logger.info(f"✅ Collecte terminée: {organization_id} ({successful_collections}/{len(data_types)} réussies, {execution_time:.2f}s)")
                return results
                
        finally:
            # Nettoyer le verrou
            if lock_key in self._collection_locks:
                del self._collection_locks[lock_key]
    
    def collect_user_organizations(self, user_id: int, force_refresh: bool = False) -> Dict[str, Any]:
        """Collecter les données de toutes les organisations d'un utilisateur"""
        
        try:
            # Récupérer les comptes LinkedIn de l'utilisateur
            with db_manager.get_session() as session:
                linkedin_accounts = session.query(LinkedinAccount).filter(
                    LinkedinAccount.user_id == user_id,
                    LinkedinAccount.is_active == True
                ).all()
            
            if not linkedin_accounts:
                logger.info(f"ℹ️  Aucun compte LinkedIn trouvé pour user {user_id}")
                return {
                    'user_id': user_id,
                    'organizations_processed': 0,
                    'total_collections': 0,
                    'successful_collections': 0,
                    'failed_collections': 0,
                    'results': {}
                }
            
            logger.info(f"🚀 Début collecte utilisateur {user_id}: {len(linkedin_accounts)} organisations")
            
            all_results = {}
            total_collections = 0
            successful_collections = 0
            failed_collections = 0
            
            # Traitement séquentiel ou parallèle selon la configuration
            if self.config['enable_concurrent_collection'] and len(linkedin_accounts) > 1:
                # Traitement parallèle
                with ThreadPoolExecutor(max_workers=min(3, len(linkedin_accounts))) as executor:
                    future_to_org = {
                        executor.submit(
                            self.collect_organization_data, 
                            user_id, 
                            account.organization_id
                        ): account.organization_id 
                        for account in linkedin_accounts
                    }
                    
                    for future in as_completed(future_to_org):
                        org_id = future_to_org[future]
                        try:
                            org_results = future.result()
                            all_results[org_id] = org_results
                            
                            # Compter les résultats
                            for result in org_results.values():
                                total_collections += 1
                                if result.status == CollectionStatus.SUCCESS:
                                    successful_collections += 1
                                else:
                                    failed_collections += 1
                                    
                        except Exception as e:
                            logger.error(f"❌ Erreur collecte parallèle {org_id}: {e}")
                            failed_collections += 1
            else:
                # Traitement séquentiel
                for account in linkedin_accounts:
                    try:
                        org_results = self.collect_organization_data(user_id, account.organization_id)
                        all_results[account.organization_id] = org_results
                        
                        # Compter les résultats
                        for result in org_results.values():
                            total_collections += 1
                            if result.status == CollectionStatus.SUCCESS:
                                successful_collections += 1
                            else:
                                failed_collections += 1
                        
                        # Pause entre organisations
                        time.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur collecte séquentielle {account.organization_id}: {e}")
                        failed_collections += 1
            
            summary = {
                'user_id': user_id,
                'organizations_processed': len(linkedin_accounts),
                'total_collections': total_collections,
                'successful_collections': successful_collections,
                'failed_collections': failed_collections,
                'success_rate': successful_collections / max(1, total_collections),
                'results': all_results,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"✅ Collecte utilisateur terminée: {user_id} ({successful_collections}/{total_collections} réussies)")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Erreur collecte utilisateur {user_id}: {e}")
            return {
                'user_id': user_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    # ========================================
    # PLANIFICATION ET AUTOMATISATION
    # ========================================
    
    def schedule_collection(self, user_id: int, organization_id: str = None, 
                           interval_hours: int = None) -> Dict[str, Any]:
        """Planifier une collecte automatique"""
        
        interval_hours = interval_hours or self.config['collection_interval_hours']
        next_collection = datetime.utcnow() + timedelta(hours=interval_hours)
        
        # En production, ceci devrait être géré par Celery, APScheduler, ou équivalent
        schedule_info = {
            'user_id': user_id,
            'organization_id': organization_id,
            'interval_hours': interval_hours,
            'next_collection': next_collection.isoformat(),
            'status': 'scheduled'
        }
        
        logger.info(f"📅 Collecte planifiée: user {user_id}, prochaine collecte dans {interval_hours}h")
        return schedule_info
    
    def should_collect_now(self, user_id: int, organization_id: str) -> Tuple[bool, str]:
        """Déterminer si une collecte doit avoir lieu maintenant"""
        
        try:
            # Vérifier la dernière collecte en base
            with db_manager.get_session() as session:
                # Chercher la dernière entrée de métriques
                last_metrics = session.query(LinkedinPostsDaily).filter(
                    LinkedinPostsDaily.organization_id == organization_id
                ).order_by(LinkedinPostsDaily.created_at.desc()).first()
                
                if not last_metrics:
                    return True, "Aucune collecte précédente trouvée"
                
                # Calculer le temps écoulé
                time_since_last = datetime.utcnow() - last_metrics.created_at
                hours_since_last = time_since_last.total_seconds() / 3600
                
                if hours_since_last >= self.config['collection_interval_hours']:
                    return True, f"Dernière collecte il y a {hours_since_last:.1f}h"
                else:
                    return False, f"Collecte récente il y a {hours_since_last:.1f}h"
                    
        except Exception as e:
            logger.error(f"❌ Erreur vérification timing collecte: {e}")
            return True, "Erreur vérification, collecte par défaut"
    
    # ========================================
    # UTILITAIRES ET HELPERS
    # ========================================
    
    def _extract_logo_url(self, logo_data: Dict) -> Optional[str]:
        """Extraire l'URL du logo depuis les données LinkedIn"""
        
        try:
            original_elements = logo_data.get('originalElements', [])
            if not original_elements:
                return None
            
            # Prendre la plus grande image disponible
            elements = sorted(
                original_elements,
                key=lambda x: x.get('data', {}).get('com.linkedin.digitalmedia.mediaartifact.StillImage', {}).get('storageSize', {}).get('width', 0),
                reverse=True
            )
            
            if elements:
                identifiers = elements[0].get('identifiers', [])
                if identifiers:
                    return identifiers[0].get('identifier')
            
        except Exception as e:
            logger.debug(f"Impossible d'extraire l'URL du logo: {e}")
        
        return None
    
    def get_collection_statistics(self) -> Dict[str, Any]:
        """Obtenir les statistiques de collecte de la session"""
        
        uptime = datetime.utcnow() - self.session_stats['start_time']
        
        stats = {
            **self.session_stats,
            'uptime_seconds': uptime.total_seconds(),
            'uptime_hours': uptime.total_seconds() / 3600,
            'average_api_calls_per_collection': (
                self.session_stats['total_api_calls'] / max(1, self.session_stats['collections_completed'])
            ),
            'average_records_per_collection': (
                self.session_stats['total_records_collected'] / max(1, self.session_stats['collections_completed'])
            ),
            'success_rate': (
                self.session_stats['collections_completed'] / 
                max(1, self.session_stats['collections_started'])
            ),
            'quota_cache_size': len(self._quota_cache),
            'active_locks': len(self._collection_locks)
        }
        
        return stats
    
    def clear_quota_cache(self):
        """Vider le cache des quotas"""
        self._quota_cache.clear()
        logger.info("🧹 Cache des quotas LinkedIn vidé")
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du collecteur"""
        
        health = {
            'collector_status': 'ok',
            'configuration': self.config,
            'session_statistics': self.get_collection_statistics(),
            'oauth_manager_status': 'ok',
            'database_connection': 'unknown',
            'linkedin_api_reachable': 'unknown',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Test de la base de données
        try:
            with db_manager.get_session() as session:
                session.execute('SELECT 1').fetchone()
            health['database_connection'] = 'ok'
        except Exception as e:
            health['database_connection'] = f'error: {e}'
        
        # Test de l'API LinkedIn
        try:
            response = self.session.get('https://api.linkedin.com', timeout=5)
            health['linkedin_api_reachable'] = 'ok' if response.status_code in [200, 401, 403] else 'error'
        except Exception as e:
            health['linkedin_api_reachable'] = f'error: {e}'
        
        # Test du gestionnaire OAuth
        try:
            oauth_health = self.oauth_manager.health_check()
            health['oauth_manager_status'] = 'ok' if oauth_health['oauth_manager'] == 'ok' else 'error'
        except Exception as e:
            health['oauth_manager_status'] = f'error: {e}'
        
        return health
    
    def cleanup_old_data(self, days: int = 90) -> Dict[str, int]:
        """Nettoyer les anciennes données de collecte"""
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            deleted_counts = {}
            
            with db_manager.get_session() as session:
                # Nettoyer les métriques quotidiennes anciennes
                deleted_posts_daily = session.query(LinkedinPostsDaily).filter(
                    LinkedinPostsDaily.created_at < cutoff_date
                ).count()
                
                session.query(LinkedinPostsDaily).filter(
                    LinkedinPostsDaily.created_at < cutoff_date
                ).delete()
                
                deleted_counts['posts_daily'] = deleted_posts_daily
                
                # Nettoyer les breakdowns anciens
                deleted_followers = session.query(LinkedinFollowerByCompanySize).filter(
                    LinkedinFollowerByCompanySize.created_at < cutoff_date
                ).count()
                
                session.query(LinkedinFollowerByCompanySize).filter(
                    LinkedinFollowerByCompanySize.created_at < cutoff_date
                ).delete()
                
                # Faire de même pour les autres tables de breakdown
                for table in [LinkedinFollowerByFunction, LinkedinFollowerByIndustry, LinkedinFollowerBySeniority]:
                    count = session.query(table).filter(
                        table.created_at < cutoff_date
                    ).count()
                    
                    session.query(table).filter(
                        table.created_at < cutoff_date
                    ).delete()
                    
                    deleted_counts[table.__tablename__] = count
                
                session.commit()
            
            total_deleted = sum(deleted_counts.values())
            logger.info(f"🧹 Nettoyage terminé: {total_deleted} enregistrements supprimés")
            
            return deleted_counts
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du nettoyage: {e}")
            return {}

# ========================================
# INSTANCE GLOBALE
# ========================================

linkedin_collector = LinkedinCollector()

# ========================================
# FONCTIONS HELPER
# ========================================

def collect_user_linkedin_data(user_id: int, force_refresh: bool = False) -> Dict[str, Any]:
    """Fonction helper pour collecter les données LinkedIn d'un utilisateur"""
    return linkedin_collector.collect_user_organizations(user_id, force_refresh)

def collect_organization_linkedin_data(user_id: int, organization_id: str, 
                                     data_types: List[str] = None) -> Dict[str, Any]:
    """Fonction helper pour collecter les données d'une organisation spécifique"""
    
    if data_types:
        data_types_enum = [DataType(dt) for dt in data_types if dt in [e.value for e in DataType]]
    else:
        data_types_enum = None
    
    return linkedin_collector.collect_organization_data(user_id, organization_id, data_types_enum)

def should_collect_linkedin_data(user_id: int, organization_id: str) -> Dict[str, Any]:
    """Fonction helper pour vérifier si une collecte est nécessaire"""
    
    should_collect, reason = linkedin_collector.should_collect_now(user_id, organization_id)
    
    return {
        'should_collect': should_collect,
        'reason': reason,
        'user_id': user_id,
        'organization_id': organization_id,
        'timestamp': datetime.utcnow().isoformat()
    }

def get_linkedin_collection_stats() -> Dict[str, Any]:
    """Fonction helper pour obtenir les statistiques de collecte"""
    return linkedin_collector.get_collection_statistics()

def cleanup_linkedin_data(days: int = 90) -> Dict[str, Any]:
    """Fonction helper pour nettoyer les anciennes données"""
    return linkedin_collector.cleanup_old_data(days)

# ========================================
# COLLECTEUR AUTOMATIQUE (POUR SCHEDULER)
# ========================================

class LinkedinCollectionScheduler:
    """Planificateur de collectes automatiques LinkedIn"""
    
    def __init__(self):
        self.collector = linkedin_collector
        self.is_running = False
        self._stop_event = threading.Event()
    
    def start_automatic_collection(self, interval_minutes: int = 60):
        """Démarrer la collecte automatique"""
        
        if self.is_running:
            logger.warning("⚠️  Collecte automatique déjà en cours")
            return
        
        self.is_running = True
        self._stop_event.clear()
        
        def collection_loop():
            while not self._stop_event.is_set():
                try:
                    self._run_scheduled_collections()
                except Exception as e:
                    logger.error(f"❌ Erreur dans la boucle de collecte: {e}")
                
                # Attendre avant la prochaine vérification
                self._stop_event.wait(interval_minutes * 60)
        
        # Démarrer dans un thread séparé
        collection_thread = threading.Thread(target=collection_loop, daemon=True)
        collection_thread.start()
        
        logger.info(f"🚀 Collecte automatique démarrée (intervalle: {interval_minutes} min)")
    
    def stop_automatic_collection(self):
        """Arrêter la collecte automatique"""
        
        if not self.is_running:
            return
        
        self._stop_event.set()
        self.is_running = False
        logger.info("🛑 Collecte automatique arrêtée")
    
    def _run_scheduled_collections(self):
        """Exécuter les collectes programmées"""
        
        try:
            # Récupérer tous les utilisateurs actifs avec des comptes LinkedIn
            with db_manager.get_session() as session:
                users_with_linkedin = session.query(User.id).join(LinkedinAccount).filter(
                    User.is_active == True,
                    LinkedinAccount.is_active == True
                ).distinct().all()
            
            logger.info(f"🔄 Vérification collectes programmées: {len(users_with_linkedin)} utilisateurs")
            
            for (user_id,) in users_with_linkedin:
                try:
                    # Récupérer les organisations de l'utilisateur
                    with db_manager.get_session() as session:
                        organizations = session.query(LinkedinAccount.organization_id).filter(
                            LinkedinAccount.user_id == user_id,
                            LinkedinAccount.is_active == True
                        ).all()
                    
                    for (org_id,) in organizations:
                        # Vérifier si une collecte est nécessaire
                        should_collect, reason = self.collector.should_collect_now(user_id, org_id)
                        
                        if should_collect:
                            logger.info(f"📊 Démarrage collecte programmée: user {user_id}, org {org_id} ({reason})")
                            
                            # Lancer la collecte en arrière-plan
                            self._collect_async(user_id, org_id)
                        
                        # Pause courte entre les vérifications
                        time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"❌ Erreur vérification utilisateur {user_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Erreur dans la vérification des collectes programmées: {e}")
    
    def _collect_async(self, user_id: int, organization_id: str):
        """Lancer une collecte en arrière-plan"""
        
        def collect():
            try:
                self.collector.collect_organization_data(user_id, organization_id)
            except Exception as e:
                logger.error(f"❌ Erreur collecte async {organization_id}: {e}")
        
        # Lancer dans un thread séparé
        thread = threading.Thread(target=collect, daemon=True)
        thread.start()

# Instance globale du scheduler
linkedin_scheduler = LinkedinCollectionScheduler()

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test LinkedinCollector...")
    
    try:
        # Test de configuration
        collector = LinkedinCollector()
        health = collector.health_check()
        print(f"Health check: {json.dumps(health, indent=2)}")
        
        # Test des statistiques
        stats = collector.get_collection_statistics()
        print(f"Statistiques: {json.dumps(stats, indent=2)}")
        
        # Test de vérification de timing
        should_collect, reason = collector.should_collect_now(1, "12345")
        print(f"Should collect: {should_collect} ({reason})")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")
        import traceback
        traceback.print_exc()