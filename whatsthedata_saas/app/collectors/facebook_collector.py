"""
Collecteur Facebook complet et robuste
Récupère toutes les métriques Facebook via Graph API v21.0
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

from ..auth.facebook_oauth import facebook_oauth_manager, FacebookOAuthError, FacebookAPIError
from ..auth.user_manager import user_manager
from ..database.connection import db_manager
from ..database.models import (
    User, FacebookAccount, SocialAccessToken, FacebookPageMetadata,
    FacebookPageDaily, FacebookPostsMetadata, FacebookPostsLifetime
)
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

class CollectionStatus(Enum):
    """Statuts de collecte Facebook"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILED = "failed"
    QUOTA_EXCEEDED = "quota_exceeded"
    TOKEN_EXPIRED = "token_expired"
    PERMISSION_DENIED = "permission_denied"
    PAGE_NOT_FOUND = "page_not_found"
    RATE_LIMITED = "rate_limited"

class FacebookDataType(Enum):
    """Types de données Facebook collectées"""
    PAGE_INFO = "page_info"
    PAGE_METRICS = "page_metrics"
    POSTS = "posts"
    POSTS_METRICS = "posts_metrics"
    INSIGHTS = "insights"

@dataclass
class FacebookCollectionResult:
    """Résultat d'une collecte Facebook"""
    status: CollectionStatus
    data_type: FacebookDataType
    page_id: str
    records_collected: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    execution_time: float = 0.0
    api_calls_made: int = 0
    quota_remaining: Optional[int] = None
    next_collection_allowed: Optional[datetime] = None
    facebook_error_code: Optional[int] = None
    facebook_error_subcode: Optional[int] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []

@dataclass
class FacebookQuota:
    """Informations sur les quotas Facebook"""
    app_id: str
    calls_made: int
    total_time: int
    total_cputime: int
    type: str  # 'application' ou 'business'
    call_count: int
    cpu_time: int
    estimated_time_to_regain_access: int = 0
    
    @property
    def is_throttled(self) -> bool:
        return self.estimated_time_to_regain_access > 0

class FacebookCollectionError(Exception):
    """Erreur de collecte Facebook"""
    
    def __init__(self, message: str, error_code: str = None, 
                 page_id: str = None, data_type: FacebookDataType = None,
                 facebook_error_code: int = None, facebook_error_subcode: int = None):
        super().__init__(message)
        self.error_code = error_code
        self.page_id = page_id
        self.data_type = data_type
        self.facebook_error_code = facebook_error_code
        self.facebook_error_subcode = facebook_error_subcode
        self.timestamp = datetime.utcnow()

class FacebookCollector:
    """Collecteur principal Facebook"""
    
    def __init__(self):
        self.oauth_manager = facebook_oauth_manager
        self.session = self._create_session()
        
        # Configuration de collecte
        self.config = self._load_collection_config()
        
        # URLs API Facebook
        self.graph_url = "https://graph.facebook.com/v21.0"
        
        # Cache des quotas et informations de rate limiting
        self._quota_cache = {}
        self._rate_limit_cache = {}
        
        # Verrous pour éviter les collectes concurrentes par page
        self._collection_locks = {}
        
        # Statistiques de session
        self.session_stats = {
            'collections_started': 0,
            'collections_completed': 0,
            'collections_failed': 0,
            'total_api_calls': 0,
            'total_records_collected': 0,
            'rate_limit_hits': 0,
            'start_time': datetime.utcnow()
        }
        
        # Métriques Facebook supportées par type
        self.page_metrics = [
            'page_impressions', 'page_impressions_unique', 'page_impressions_nonviral', 'page_impressions_viral',
            'page_posts_impressions', 'page_posts_impressions_unique', 'page_posts_impressions_paid',
            'page_posts_impressions_organic', 'page_posts_impressions_organic_unique',
            'page_views_total', 'page_fans', 'page_fan_adds', 'page_fan_removes',
            'page_fan_adds_by_paid_non_paid_unique', 'page_follows', 'page_daily_follows',
            'page_daily_unfollows', 'page_daily_follows_unique',
            'page_video_views', 'page_video_views_unique', 'page_video_views_paid', 'page_video_views_organic',
            'page_video_views_repeat', 'page_video_view_time', 'page_video_complete_views_30s',
            'page_video_complete_views_30s_unique', 'page_video_complete_views_30s_paid',
            'page_video_complete_views_30s_organic', 'page_video_complete_views_30s_autoplayed',
            'page_video_complete_views_30s_repeated_views',
            'page_post_engagements', 'page_total_actions',
            'page_actions_post_reactions_like_total', 'page_actions_post_reactions_love_total',
            'page_actions_post_reactions_wow_total', 'page_actions_post_reactions_haha_total',
            'page_actions_post_reactions_sorry_total', 'page_actions_post_reactions_anger_total'
        ]
        
        self.post_metrics = [
            'post_impressions', 'post_impressions_unique', 'post_impressions_organic',
            'post_impressions_organic_unique', 'post_impressions_paid', 'post_impressions_paid_unique',
            'post_impressions_viral', 'post_impressions_viral_unique', 'post_impressions_fan',
            'post_impressions_nonviral', 'post_impressions_nonviral_unique',
            'post_reactions_like_total', 'post_reactions_love_total', 'post_reactions_wow_total',
            'post_reactions_haha_total', 'post_reactions_sorry_total', 'post_reactions_anger_total',
            'post_clicks', 'post_consumptions',
            'post_video_views', 'post_video_views_unique', 'post_video_views_organic',
            'post_video_views_organic_unique', 'post_video_views_paid', 'post_video_views_paid_unique',
            'post_video_views_sound_on', 'post_video_complete_views_30s', 'post_video_avg_time_watched',
            'post_video_view_time', 'post_fan_reach'
        ]
        
    def _create_session(self) -> requests.Session:
        """Créer une session HTTP optimisée pour Facebook Graph API"""
        
        session = requests.Session()
        
        # Configuration du retry spécifique à Facebook
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "POST"],
            backoff_factor=3,  # Plus agressif pour Facebook
            respect_retry_after_header=True,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Headers optimisés pour Facebook Graph API
        session.headers.update({
            'User-Agent': f'{get_env_var("APP_NAME", "WhatsTheData")}/{get_env_var("APP_VERSION", "1.0.0")} (+{get_env_var("BASE_URL", "http://localhost:8501")})',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate'
        })
        
        return session
    
    def _load_collection_config(self) -> Dict[str, Any]:
        """Charger la configuration de collecte Facebook"""
        
        config = {
            'collection_interval_hours': int(get_env_var('FB_COLLECTION_INTERVAL_HOURS', '6')),
            'max_posts_per_collection': int(get_env_var('FB_MAX_POSTS_PER_COLLECTION', '100')),
            'api_timeout_seconds': int(get_env_var('FB_API_TIMEOUT_SECONDS', '30')),
            'api_retry_attempts': int(get_env_var('FB_API_RETRY_ATTEMPTS', '3')),
            'parallel_requests': int(get_env_var('FB_PARALLEL_REQUESTS', '2')),  # Plus conservateur pour Facebook
            'skip_old_posts_days': int(get_env_var('FB_SKIP_OLD_POSTS_DAYS', '90')),
            'max_posts_per_page': int(get_env_var('FB_MAX_POSTS_PER_PAGE', '500')),
            'rate_limit_margin': int(get_env_var('FB_RATE_LIMIT_MARGIN', '20')),  # Plus de marge pour Facebook
            'enable_concurrent_collection': get_env_var('FB_ENABLE_CONCURRENT_COLLECTION', 'true').lower() == 'true',
            'debug_mode': get_env_var('FB_DEBUG_MODE', 'false').lower() == 'true',
            'batch_size': int(get_env_var('FB_BATCH_SIZE', '50')),  # Taille des batches pour Graph API
            'insights_period': get_env_var('FB_INSIGHTS_PERIOD', 'day'),  # day, week, days_28
            'date_range_days': int(get_env_var('FB_DATE_RANGE_DAYS', '30'))
        }
        
        logger.info(f"✅ Configuration collecte Facebook chargée: {config}")
        return config
    
    # ========================================
    # GESTION DES QUOTAS ET RATE LIMITS FACEBOOK
    # ========================================
    
    def _get_quota_info(self, access_token: str) -> FacebookQuota:
        """Récupérer les informations de quota Facebook depuis les headers de réponse"""
        
        try:
            # Faire un appel de test pour récupérer les headers de quota
            response = self.session.get(
                f"{self.graph_url}/me",
                params={'access_token': access_token},
                timeout=10
            )
            
            # Facebook renvoie les quotas dans les headers de réponse
            app_usage = response.headers.get('X-App-Usage', '{}')
            business_usage = response.headers.get('X-Business-Use-Case-Usage', '{}')
            
            try:
                app_usage_data = json.loads(app_usage)
                business_usage_data = json.loads(business_usage)
            except:
                app_usage_data = {}
                business_usage_data = {}
            
            # Analyser les quotas d'application
            quota = FacebookQuota(
                app_id=get_env_var('FB_CLIENT_ID', 'unknown'),
                calls_made=app_usage_data.get('call_count', 0),
                total_time=app_usage_data.get('total_time', 0),
                total_cputime=app_usage_data.get('total_cputime', 0),
                type='application',
                call_count=app_usage_data.get('call_count', 0),
                cpu_time=app_usage_data.get('total_cputime', 0),
                estimated_time_to_regain_access=app_usage_data.get('estimated_time_to_regain_access', 0)
            )
            
            # Mettre en cache
            cache_key = f"quota_{access_token[-10:]}"
            self._quota_cache[cache_key] = {
                'quota': quota,
                'timestamp': datetime.utcnow(),
                'expires_at': datetime.utcnow() + timedelta(minutes=5)
            }
            
            logger.debug(f"✅ Quota Facebook récupéré: {quota.call_count}% utilisé")
            return quota
            
        except Exception as e:
            logger.warning(f"⚠️  Impossible de récupérer le quota Facebook: {e}")
            # Retourner un quota par défaut conservateur
            return FacebookQuota(
                app_id=get_env_var('FB_CLIENT_ID', 'unknown'),
                calls_made=0,
                total_time=0,
                total_cputime=0,
                type='application',
                call_count=0,
                cpu_time=0
            )
    
    def _check_rate_limits(self, access_token: str, calls_needed: int = 1) -> Tuple[bool, FacebookQuota]:
        """Vérifier les rate limits Facebook avant de faire des appels"""
        
        quota = self._get_quota_info(access_token)
        
        # Facebook limite à 200 appels par heure par défaut pour les apps
        # et applique aussi des limites CPU
        rate_limit_threshold = 100 - self.config['rate_limit_margin']  # 80% par défaut
        
        is_safe = (
            quota.call_count < rate_limit_threshold and 
            quota.estimated_time_to_regain_access == 0
        )
        
        if not is_safe:
            if quota.is_throttled:
                logger.warning(f"⚠️  Facebook rate limit atteint, attente requise: {quota.estimated_time_to_regain_access}s")
            else:
                logger.warning(f"⚠️  Quota Facebook proche de la limite: {quota.call_count}%")
        
        return is_safe, quota
    
    def _handle_rate_limit_response(self, response: requests.Response, access_token: str):
        """Gérer les réponses de rate limiting Facebook"""
        
        if response.status_code == 429:
            self.session_stats['rate_limit_hits'] += 1
            
            # Analyser les headers de rate limiting
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                wait_time = int(retry_after)
                logger.warning(f"⚠️  Rate limit Facebook hit, attente {wait_time}s")
                time.sleep(wait_time)
            else:
                # Attente par défaut
                logger.warning("⚠️  Rate limit Facebook hit, attente 60s par défaut")
                time.sleep(60)
        
        # Mettre à jour le cache de quota avec les nouvelles informations
        self._get_quota_info(access_token)
    
    # ========================================
    # COLLECTE DES INFORMATIONS DE PAGE
    # ========================================
    
    def collect_page_info(self, user_id: int, page_id: str) -> FacebookCollectionResult:
        """Collecter les informations de base d'une page Facebook"""
        
        start_time = time.time()
        result = FacebookCollectionResult(
            status=CollectionStatus.PENDING,
            data_type=FacebookDataType.PAGE_INFO,
            page_id=page_id
        )
        
        try:
            # Récupérer le token Facebook de l'utilisateur
            token = user_manager.get_social_token(user_id, "facebook")
            if not token:
                raise FacebookCollectionError(
                    "Token Facebook non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    page_id=page_id
                )
            
            # Vérifier les rate limits
            rate_ok, quota = self._check_rate_limits(token.access_token, 2)
            if not rate_ok:
                result.status = CollectionStatus.RATE_LIMITED
                result.quota_remaining = 100 - quota.call_count
                result.next_collection_allowed = datetime.utcnow() + timedelta(seconds=quota.estimated_time_to_regain_access)
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les informations de la page
            page_info = self._fetch_page_details(token.access_token, page_id)
            result.api_calls_made += 1
            
            # Récupérer le token de page si disponible
            page_token = self._get_page_access_token(token.access_token, page_id)
            if page_token:
                page_info['page_access_token'] = page_token
                result.api_calls_made += 1
            
            # Stocker en base de données
            self._store_page_info(page_id, page_info)
            result.records_collected = 1
            
            result.status = CollectionStatus.SUCCESS
            logger.info(f"✅ Informations page Facebook collectées: {page_id}")
            
        except FacebookCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            result.facebook_error_code = e.facebook_error_code
            result.facebook_error_subcode = e.facebook_error_subcode
            logger.error(f"❌ Erreur collecte page Facebook {page_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte page Facebook {page_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_page_details(self, access_token: str, page_id: str) -> Dict[str, Any]:
        """Récupérer les détails d'une page Facebook"""
        
        try:
            fields = [
                'id', 'name', 'username', 'category', 'about', 'description',
                'website', 'link', 'picture.type(large)', 'cover',
                'talking_about_count', 'fan_count', 'followers_count'
            ]
            
            params = {
                'fields': ','.join(fields),
                'access_token': access_token
            }
            
            response = self.session.get(
                f"{self.graph_url}/{page_id}",
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                
                return {
                    'name': data.get('name'),
                    'username': data.get('username'),
                    'category': data.get('category'),
                    'about': data.get('about', ''),
                    'description': data.get('description', ''),
                    'website': data.get('website'),
                    'link': data.get('link'),
                    'picture_url': data.get('picture', {}).get('data', {}).get('url'),
                    'cover_url': data.get('cover', {}).get('source'),
                    'talking_about_count': data.get('talking_about_count', 0),
                    'fan_count': data.get('fan_count', 0),
                    'followers_count': data.get('followers_count', 0)
                }
            
            elif response.status_code == 429:
                self._handle_rate_limit_response(response, access_token)
                raise FacebookCollectionError(
                    "Rate limit atteint",
                    error_code="RATE_LIMITED",
                    page_id=page_id
                )
            
            else:
                error_data = self._parse_facebook_error(response)
                raise FacebookCollectionError(
                    f"Erreur API Facebook: {error_data.get('message', 'Erreur inconnue')}",
                    error_code="API_ERROR",
                    page_id=page_id,
                    facebook_error_code=error_data.get('code'),
                    facebook_error_subcode=error_data.get('error_subcode')
                )
                
        except FacebookCollectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des détails de page: {e}")
            raise FacebookCollectionError(
                f"Erreur récupération page: {e}",
                error_code="FETCH_ERROR",
                page_id=page_id
            )
    
    def _get_page_access_token(self, user_token: str, page_id: str) -> Optional[str]:
        """Récupérer le token d'accès spécifique à une page"""
        
        try:
            params = {
                'access_token': user_token
            }
            
            response = self.session.get(
                f"{self.graph_url}/me/accounts",
                params=params,
                timeout=self.config['api_timeout_seconds']
            )
            
            if response.status_code == 200:
                data = response.json()
                pages = data.get('data', [])
                
                for page in pages:
                    if page.get('id') == page_id:
                        return page.get('access_token')
            
            logger.warning(f"⚠️  Token de page non trouvé pour {page_id}")
            return None
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur récupération token de page: {e}")
            return None
    
    # ========================================
    # COLLECTE DES MÉTRIQUES DE PAGE
    # ========================================
    
    def collect_page_metrics(self, user_id: int, page_id: str, 
                           since_date: date = None, until_date: date = None) -> FacebookCollectionResult:
        """Collecter les métriques quotidiennes d'une page Facebook"""
        
        start_time = time.time()
        result = FacebookCollectionResult(
            status=CollectionStatus.PENDING,
            data_type=FacebookDataType.PAGE_METRICS,
            page_id=page_id
        )
        
        try:
            # Récupérer le token Facebook
            token = user_manager.get_social_token(user_id, "facebook")
            if not token:
                raise FacebookCollectionError(
                    "Token Facebook non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    page_id=page_id
                )
            
            # Configuration des dates
            if not since_date:
                since_date = date.today() - timedelta(days=self.config['date_range_days'])
            if not until_date:
                until_date = date.today()
            
            # Estimer le nombre d'appels nécessaires
            days_range = (until_date - since_date).days
            estimated_calls = len(self.page_metrics) * max(1, days_range // 30)  # Facebook groupe par ~30 jours
            
            rate_ok, quota = self._check_rate_limits(token.access_token, estimated_calls)
            if not rate_ok:
                result.status = CollectionStatus.RATE_LIMITED
                result.quota_remaining = 100 - quota.call_count
                result.next_collection_allowed = datetime.utcnow() + timedelta(seconds=quota.estimated_time_to_regain_access)
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les métriques par batch
            metrics_data = self._fetch_page_insights_batch(
                token.access_token,
                page_id,
                self.page_metrics,
                since_date,
                until_date
            )
            
            result.api_calls_made = metrics_data['api_calls_made']
            
            # Transformer et stocker les données
            daily_metrics = self._transform_page_metrics_to_daily(metrics_data['metrics'], page_id)
            stored_count = self._store_page_daily_metrics(daily_metrics)
            
            result.records_collected = stored_count
            result.status = CollectionStatus.SUCCESS if stored_count > 0 else CollectionStatus.PARTIAL_SUCCESS
            
            logger.info(f"✅ Métriques page Facebook collectées: {page_id} ({stored_count} jours)")
            
        except FacebookCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            result.facebook_error_code = e.facebook_error_code
            result.facebook_error_subcode = e.facebook_error_subcode
            logger.error(f"❌ Erreur collecte métriques page {page_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte métriques page {page_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_page_insights_batch(self, access_token: str, page_id: str,
                                  metrics: List[str], since_date: date, until_date: date) -> Dict[str, Any]:
        """Récupérer les insights de page par batch pour optimiser les appels API"""
        
        all_metrics = {}
        api_calls_made = 0
        
        # Diviser les métriques en groupes pour éviter les URLs trop longues
        batch_size = self.config['batch_size']
        
        for i in range(0, len(metrics), batch_size):
            batch_metrics = metrics[i:i + batch_size]
            
            try:
                params = {
                    'metric': ','.join(batch_metrics),
                    'period': self.config['insights_period'],
                    'since': since_date.strftime('%Y-%m-%d'),
                    'until': until_date.strftime('%Y-%m-%d'),
                    'access_token': access_token
                }
                
                response = self.session.get(
                    f"{self.graph_url}/{page_id}/insights",
                    params=params,
                    timeout=self.config['api_timeout_seconds']
                )
                
                api_calls_made += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for metric_data in data.get('data', []):
                        metric_name = metric_data.get('name')
                        values = metric_data.get('values', [])
                        
                        for value_entry in values:
                            end_time = value_entry.get('end_time')
                            value = value_entry.get('value')
                            
                            # Convertir la date
                            try:
                                metric_date = datetime.strptime(end_time, '%Y-%m-%dT%H:%M:%S%z').date()
                            except:
                                metric_date = datetime.strptime(end_time[:10], '%Y-%m-%d').date()
                            
                            # Stocker la métrique
                            if metric_date not in all_metrics:
                                all_metrics[metric_date] = {}
                            
                            # Gérer les valeurs complexes (dictionnaires)
                            if isinstance(value, dict):
                                for sub_key, sub_value in value.items():
                                    all_metrics[metric_date][f"{metric_name}_{sub_key}"] = sub_value
                            else:
                                all_metrics[metric_date][metric_name] = value
                
                elif response.status_code == 429:
                    self._handle_rate_limit_response(response, access_token)
                    # Réessayer après la pause
                    continue
                
                else:
                    error_data = self._parse_facebook_error(response)
                    logger.warning(f"⚠️  Erreur batch métriques {', '.join(batch_metrics)}: {error_data.get('message')}")
                    continue
                
                # Pause entre les batches pour respecter les rate limits
                time.sleep(0.5)
                
            except Exception as e:
                logger.warning(f"⚠️  Erreur lors du traitement du batch {i}: {e}")
                continue
        
        return {
            'metrics': all_metrics,
            'api_calls_made': api_calls_made
        }
    
    def _transform_page_metrics_to_daily(self, metrics_data: Dict[date, Dict[str, Any]], 
                                       page_id: str) -> List[Dict[str, Any]]:
        """Transformer les métriques en format quotidien pour la base de données"""
        
        daily_metrics = []
        
        for metric_date, metrics in metrics_data.items():
            daily_record = {
                'page_id': page_id,
                'date': metric_date,
                'created_at': datetime.utcnow()
            }
            
            # Mapper les métriques Facebook vers les colonnes de la base
            metric_mapping = {
                'page_impressions': 'page_impressions',
                'page_impressions_unique': 'page_impressions_unique',
                'page_impressions_nonviral': 'page_impressions_non_viral',
                'page_impressions_viral': 'page_impressions_viral',
                'page_posts_impressions': 'page_posts_impressions',
                'page_posts_impressions_unique': 'page_posts_impressions_unique',
                'page_posts_impressions_paid': 'page_posts_impressions_paid',
                'page_posts_impressions_organic': 'page_posts_impressions_organic',
                'page_posts_impressions_organic_unique': 'page_posts_impressions_organic_unique',
                'page_views_total': 'page_views_total',
                'page_fans': 'page_fans',
                'page_fan_adds': 'page_fan_adds',
                'page_fan_removes': 'page_fan_removes',
                'page_fan_adds_by_paid_non_paid_unique_total': 'page_fan_adds_by_paid_non_paid_unique_total',
                'page_fan_adds_by_paid_non_paid_unique_paid': 'page_fan_adds_by_paid_non_paid_unique_paid',
                'page_fan_adds_by_paid_non_paid_unique_unpaid': 'page_fan_adds_by_paid_non_paid_unique_unpaid',
                'page_follows': 'page_follows',
                'page_daily_follows': 'page_daily_follows',
                'page_daily_unfollows': 'page_daily_unfollows',
                'page_daily_follows_unique': 'page_daily_follows_unique',
                'page_video_views': 'page_video_views',
                'page_video_views_unique': 'page_video_views_unique',
                'page_video_views_paid': 'page_video_views_paid',
                'page_video_views_organic': 'page_video_views_organic',
                'page_video_views_repeat': 'page_video_views_repeat',
                'page_video_view_time': 'page_video_view_time',
                'page_video_complete_views_30s': 'page_video_complete_views_30s',
                'page_video_complete_views_30s_unique': 'page_video_complete_views_30s_unique',
                'page_video_complete_views_30s_paid': 'page_video_complete_views_30s_paid',
                'page_video_complete_views_30s_organic': 'page_video_complete_views_30s_organic',
                'page_video_complete_views_30s_autoplayed': 'page_video_complete_views_30s_autoplayed',
                'page_video_complete_views_30s_repeated_views': 'page_video_complete_views_30s_repeated_views',
                'page_post_engagements': 'page_post_engagements',
                'page_total_actions': 'page_total_actions',
                'page_actions_post_reactions_like_total': 'page_actions_post_reactions_like_total',
                'page_actions_post_reactions_love_total': 'page_actions_post_reactions_love_total',
                'page_actions_post_reactions_wow_total': 'page_actions_post_reactions_wow_total',
                'page_actions_post_reactions_haha_total': 'page_actions_post_reactions_haha_total',
                'page_actions_post_reactions_sorry_total': 'page_actions_post_reactions_sorry_total',
                'page_actions_post_reactions_anger_total': 'page_actions_post_reactions_anger_total'
            }
            
            # Appliquer le mapping
            for fb_metric, db_column in metric_mapping.items():
                if fb_metric in metrics:
                    daily_record[db_column] = metrics[fb_metric]
                else:
                    daily_record[db_column] = 0  # Valeur par défaut
            
            daily_metrics.append(daily_record)
        
        return daily_metrics
    
    # ========================================
    # COLLECTE DES POSTS
    # ========================================
    
    def collect_posts(self, user_id: int, page_id: str,
                     max_posts: int = None, since_date: date = None) -> FacebookCollectionResult:
        """Collecter les posts d'une page Facebook"""
        
        start_time = time.time()
        result = FacebookCollectionResult(
            status=CollectionStatus.PENDING,
            data_type=FacebookDataType.POSTS,
            page_id=page_id
        )
        
        try:
            # Récupérer le token Facebook
            token = user_manager.get_social_token(user_id, "facebook")
            if not token:
                raise FacebookCollectionError(
                    "Token Facebook non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    page_id=page_id
                )
            
            # Configuration
            max_posts = max_posts or self.config['max_posts_per_collection']
            since_date = since_date or (date.today() - timedelta(days=self.config['skip_old_posts_days']))
            
            # Estimer les appels API nécessaires
            estimated_calls = max(1, max_posts // 25) + 2  # Facebook retourne ~25 posts par page
            
            rate_ok, quota = self._check_rate_limits(token.access_token, estimated_calls)
            if not rate_ok:
                result.status = CollectionStatus.RATE_LIMITED
                result.quota_remaining = 100 - quota.call_count
                result.next_collection_allowed = datetime.utcnow() + timedelta(seconds=quota.estimated_time_to_regain_access)
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Récupérer les posts
            posts_data = self._fetch_page_posts(
                token.access_token,
                page_id,
                max_posts,
                since_date
            )
            
            result.api_calls_made = posts_data['api_calls_made']
            
            # Stocker les posts
            stored_count = self._store_posts_metadata(page_id, posts_data['posts'])
            result.records_collected = stored_count
            
            result.status = CollectionStatus.SUCCESS
            logger.info(f"✅ Posts Facebook collectés: {page_id} ({stored_count} posts)")
            
        except FacebookCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            result.facebook_error_code = e.facebook_error_code
            result.facebook_error_subcode = e.facebook_error_subcode
            logger.error(f"❌ Erreur collecte posts Facebook {page_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte posts Facebook {page_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_page_posts(self, access_token: str, page_id: str,
                         max_posts: int, since_date: date) -> Dict[str, Any]:
        """Récupérer les posts d'une page Facebook"""
        
        posts = []
        api_calls_made = 0
        next_url = None
        
        # Convertir since_date en timestamp
        since_timestamp = int(since_date.strftime('%s'))
        
        try:
            # Configuration des champs à récupérer
            fields = [
                'id', 'created_time', 'message', 'story', 'status_type',
                'permalink_url', 'full_picture', 'from', 'type',
                'attachments{type,media_type,url,subattachments}',
                'comments.summary(true)', 'likes.summary(true)', 'shares'
            ]
            
            params = {
                'fields': ','.join(fields),
                'limit': min(100, max_posts),  # Facebook limite à 100
                'since': since_timestamp,
                'access_token': access_token
            }
            
            url = f"{self.graph_url}/{page_id}/posts"
            
            while len(posts) < max_posts:
                response = self.session.get(
                    url,
                    params=params if not next_url else None,
                    timeout=self.config['api_timeout_seconds']
                )
                
                api_calls_made += 1
                
                if response.status_code == 200:
                    data = response.json()
                    page_posts = data.get('data', [])
                    
                    if not page_posts:
                        logger.info("ℹ️  Plus de posts à récupérer")
                        break
                    
                    # Traiter chaque post
                    for post in page_posts:
                        try:
                            normalized_post = self._normalize_facebook_post(post, page_id)
                            if normalized_post:
                                posts.append(normalized_post)
                                
                                if len(posts) >= max_posts:
                                    break
                        except Exception as e:
                            logger.warning(f"⚠️  Erreur normalisation post {post.get('id')}: {e}")
                            continue
                    
                    # Vérifier s'il y a une page suivante
                    paging = data.get('paging', {})
                    next_url = paging.get('next')
                    
                    if not next_url or len(posts) >= max_posts:
                        break
                    
                    # Préparer pour la page suivante
                    url = next_url
                    params = None  # next_url contient déjà tous les paramètres
                    
                    # Pause pour respecter les rate limits
                    time.sleep(0.2)
                
                elif response.status_code == 429:
                    self._handle_rate_limit_response(response, access_token)
                    continue
                
                else:
                    error_data = self._parse_facebook_error(response)
                    logger.warning(f"⚠️  Erreur récupération posts: {error_data.get('message')}")
                    break
            
            logger.info(f"✅ {len(posts)} posts Facebook récupérés pour {page_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des posts Facebook: {e}")
        
        return {
            'posts': posts,
            'api_calls_made': api_calls_made
        }
    
    def _normalize_facebook_post(self, post_data: Dict[str, Any], page_id: str) -> Dict[str, Any]:
        """Normaliser les données d'un post Facebook"""
        
        try:
            # Informations de base
            post_id = post_data.get('id', '')
            created_time = post_data.get('created_time')
            
            # Convertir la date
            try:
                created_datetime = datetime.strptime(created_time, '%Y-%m-%dT%H:%M:%S%z')
            except:
                created_datetime = datetime.utcnow()
            
            # Contenu du post
            message = post_data.get('message', '')
            story = post_data.get('story', '')
            status_type = post_data.get('status_type', 'unknown')
            
            # Traduire le status_type en français
            status_translations = {
                'mobile_status_update': 'Statut mobile',
                'created_note': 'Note créée',
                'added_photos': 'Photo publiée',
                'added_video': 'Vidéo publiée',
                'shared_story': 'Contenu partagé',
                'created_group': 'Groupe créé',
                'created_event': 'Événement créé',
                'wall_post': 'Publication',
                'app_created_story': 'Contenu automatique',
                'published_story': 'Article publié',
                'tagged_in_photo': 'Identifié dans une photo',
                'approved_friend': 'Ami approuvé'
            }
            
            status_type_fr = status_translations.get(status_type, status_type)
            
            # Informations de l'auteur
            author_info = post_data.get('from', {})
            author_name = author_info.get('name', 'Page inconnue')
            author_id = author_info.get('id', page_id)
            
            # URLs et images
            permalink_url = post_data.get('permalink_url', '')
            full_picture = post_data.get('full_picture', '')
            
            # Compteurs basiques
            comments_data = post_data.get('comments', {}).get('summary', {})
            likes_data = post_data.get('likes', {}).get('summary', {})
            shares_data = post_data.get('shares', {})
            
            comments_count = comments_data.get('total_count', 0)
            likes_count = likes_data.get('total_count', 0)
            shares_count = shares_data.get('count', 0)
            
            # Utiliser le message ou l'histoire comme texte principal
            text_content = message or story or 'Contenu sans texte'
            
            return {
                'post_id': post_id,
                'page_id': page_id,
                'created_time': created_datetime,
                'status_type': status_type_fr,
                'message': text_content[:1000],  # Limiter à 1000 caractères
                'permalink_url': permalink_url,
                'full_picture': full_picture,
                'author_name': author_name,
                'author_id': author_id,
                'comments_count': comments_count,
                'likes_count': likes_count,
                'shares_count': shares_count
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur normalisation post Facebook: {e}")
            return None
    
    # ========================================
    # COLLECTE DES MÉTRIQUES DE POSTS
    # ========================================
    
    def collect_posts_metrics(self, user_id: int, page_id: str,
                             post_ids: List[str] = None) -> FacebookCollectionResult:
        """Collecter les métriques lifetime des posts Facebook"""
        
        start_time = time.time()
        result = FacebookCollectionResult(
            status=CollectionStatus.PENDING,
            data_type=FacebookDataType.POSTS_METRICS,
            page_id=page_id
        )
        
        try:
            # Récupérer le token Facebook
            token = user_manager.get_social_token(user_id, "facebook")
            if not token:
                raise FacebookCollectionError(
                    "Token Facebook non trouvé",
                    error_code="TOKEN_NOT_FOUND",
                    page_id=page_id
                )
            
            # Si pas de posts spécifiés, récupérer les posts récents de la DB
            if not post_ids:
                post_ids = self._get_recent_posts_from_db(page_id)
            
            if not post_ids:
                result.status = CollectionStatus.SUCCESS
                result.records_collected = 0
                logger.info(f"ℹ️  Aucun post à traiter pour {page_id}")
                return result
            
            # Estimer les appels API (1 appel par post pour les insights)
            estimated_calls = len(post_ids)
            
            rate_ok, quota = self._check_rate_limits(token.access_token, estimated_calls)
            if not rate_ok:
                result.status = CollectionStatus.RATE_LIMITED
                result.quota_remaining = 100 - quota.call_count
                result.next_collection_allowed = datetime.utcnow() + timedelta(seconds=quota.estimated_time_to_regain_access)
                return result
            
            result.status = CollectionStatus.RUNNING
            
            # Collecter les métriques par batch
            batch_size = min(10, len(post_ids))  # Traiter par lots de 10
            total_metrics_collected = 0
            
            for i in range(0, len(post_ids), batch_size):
                batch_ids = post_ids[i:i + batch_size]
                
                try:
                    batch_metrics = self._fetch_posts_insights_batch(
                        token.access_token,
                        batch_ids
                    )
                    
                    result.api_calls_made += batch_metrics['api_calls_made']
                    
                    # Stocker les métriques
                    stored_count = self._store_posts_lifetime_metrics(page_id, batch_metrics['metrics'])
                    total_metrics_collected += stored_count
                    
                    # Pause entre les batches
                    if i + batch_size < len(post_ids):
                        time.sleep(1)
                    
                except Exception as e:
                    result.warnings.append(f"Erreur batch {i}-{i+batch_size}: {e}")
                    continue
            
            result.records_collected = total_metrics_collected
            result.status = CollectionStatus.SUCCESS if total_metrics_collected > 0 else CollectionStatus.PARTIAL_SUCCESS
            
            logger.info(f"✅ Métriques posts Facebook collectées: {page_id} ({total_metrics_collected} posts)")
            
        except FacebookCollectionError as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(str(e))
            result.facebook_error_code = e.facebook_error_code
            result.facebook_error_subcode = e.facebook_error_subcode
            logger.error(f"❌ Erreur collecte métriques posts Facebook {page_id}: {e}")
            
        except Exception as e:
            result.status = CollectionStatus.FAILED
            result.errors.append(f"Erreur inattendue: {e}")
            logger.error(f"❌ Erreur inattendue collecte métriques posts Facebook {page_id}: {e}")
        
        finally:
            result.execution_time = time.time() - start_time
            
        return result
    
    def _fetch_posts_insights_batch(self, access_token: str, post_ids: List[str]) -> Dict[str, Any]:
        """Récupérer les insights d'un batch de posts Facebook"""
        
        metrics = []
        api_calls_made = 0
        
        for post_id in post_ids:
            try:
                post_metrics = {}
                
                # Récupérer les insights du post
                params = {
                    'metric': ','.join(self.post_metrics),
                    'period': 'lifetime',
                    'access_token': access_token
                }
                
                response = self.session.get(
                    f"{self.graph_url}/{post_id}/insights",
                    params=params,
                    timeout=self.config['api_timeout_seconds']
                )
                
                api_calls_made += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Traiter chaque métrique
                    for insight in data.get('data', []):
                        metric_name = insight.get('name')
                        values = insight.get('values', [])
                        
                        if values:
                            # Prendre la dernière valeur (lifetime)
                            latest_value = values[-1].get('value', 0)
                            post_metrics[metric_name] = latest_value
                
                elif response.status_code == 429:
                    self._handle_rate_limit_response(response, access_token)
                    continue
                
                else:
                    error_data = self._parse_facebook_error(response)
                    logger.warning(f"⚠️  Erreur insights post {post_id}: {error_data.get('message')}")
                    continue
                
                # Ajouter les identifiants
                post_metrics['post_id'] = post_id
                post_metrics['collected_at'] = datetime.utcnow()
                
                # Calculer les métriques dérivées
                post_metrics = self._calculate_facebook_derived_metrics(post_metrics)
                
                metrics.append(post_metrics)
                
                # Pause courte entre les posts
                time.sleep(0.1)
                
            except Exception as e:
                logger.warning(f"⚠️  Erreur métriques post Facebook {post_id}: {e}")
                continue
        
        return {
            'metrics': metrics,
            'api_calls_made': api_calls_made
        }
    
    def _calculate_facebook_derived_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Calculer les métriques dérivées Facebook"""
        
        try:
            # Récupérer les valeurs de base
            impressions = metrics.get('post_impressions', 0)
            clicks = metrics.get('post_clicks', 0)
            
            # Calculer les totaux de réactions
            reactions_total = sum([
                metrics.get('post_reactions_like_total', 0),
                metrics.get('post_reactions_love_total', 0),
                metrics.get('post_reactions_wow_total', 0),
                metrics.get('post_reactions_haha_total', 0),
                metrics.get('post_reactions_sorry_total', 0),
                metrics.get('post_reactions_anger_total', 0)
            ])
            
            # Réactions positives vs négatives
            reactions_positives = sum([
                metrics.get('post_reactions_like_total', 0),
                metrics.get('post_reactions_love_total', 0),
                metrics.get('post_reactions_wow_total', 0),
                metrics.get('post_reactions_haha_total', 0)
            ])
            
            reactions_negatives = sum([
                metrics.get('post_reactions_sorry_total', 0),
                metrics.get('post_reactions_anger_total', 0)
            ])
            
            # Taux de clic
            if impressions > 0:
                taux_de_clic = clicks / impressions
                metrics['taux_de_clic'] = round(taux_de_clic, 4)
            else:
                metrics['taux_de_clic'] = 0.0
            
            # Taux d'engagement complet
            if impressions > 0:
                taux_engagement_complet = (reactions_total + clicks) / impressions
                metrics['taux_engagement_complet'] = round(taux_engagement_complet, 4)
            else:
                metrics['taux_engagement_complet'] = 0.0
            
            # Ajouter les totaux calculés
            metrics['reactions_positives'] = reactions_positives
            metrics['reactions_negatives'] = reactions_negatives
            metrics['total_reactions'] = reactions_total
            
        except Exception as e:
            logger.error(f"❌ Erreur calcul métriques dérivées Facebook: {e}")
        
        return metrics
    
    # ========================================
    # STOCKAGE EN BASE DE DONNÉES
    # ========================================
    
    def _store_page_info(self, page_id: str, page_data: Dict[str, Any]):
        """Stocker les informations de page en base"""
        
        try:
            with db_manager.get_session() as session:
                # Vérifier si la page existe déjà
                existing = session.query(FacebookPageMetadata).filter(
                    FacebookPageMetadata.page_id == page_id
                ).first()
                
                if existing:
                    # Mettre à jour
                    for key, value in page_data.items():
                        if hasattr(existing, key) and key != 'page_access_token':  # Ne pas stocker le token en base
                            setattr(existing, key, value)
                else:
                    # Créer nouveau
                    new_page = FacebookPageMetadata(
                        page_id=page_id,
                        name=page_data.get('name'),
                        username=page_data.get('username'),
                        category=page_data.get('category'),
                        about=page_data.get('about'),
                        website=page_data.get('website'),
                        link=page_data.get('link'),
                        picture_url=page_data.get('picture_url'),
                        cover_url=page_data.get('cover_url'),
                        talking_about_count=page_data.get('talking_about_count', 0)
                    )
                    session.add(new_page)
                
                session.commit()
                logger.debug(f"✅ Informations page Facebook stockées: {page_id}")
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage page Facebook: {e}")
            raise
    
    def _store_page_daily_metrics(self, daily_metrics: List[Dict[str, Any]]) -> int:
        """Stocker les métriques quotidiennes de page"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                
                for metrics in daily_metrics:
                    try:
                        # Vérifier si les métriques existent déjà pour ce jour
                        existing = session.query(FacebookPageDaily).filter(
                            FacebookPageDaily.page_id == metrics['page_id'],
                            FacebookPageDaily.date == metrics['date']
                        ).first()
                        
                        if existing:
                            # Mettre à jour
                            for key, value in metrics.items():
                                if hasattr(existing, key) and key not in ['page_id', 'date']:
                                    setattr(existing, key, value)
                        else:
                            # Créer nouveau
                            new_metrics = FacebookPageDaily(**metrics)
                            session.add(new_metrics)
                        
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage métriques quotidiennes {metrics.get('date')}: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Métriques quotidiennes Facebook stockées: {stored_count} records")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage métriques quotidiennes Facebook: {e}")
            return 0
    
    def _store_posts_metadata(self, page_id: str, posts_data: List[Dict]) -> int:
        """Stocker les métadonnées des posts Facebook"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                
                for post_data in posts_data:
                    try:
                        # Vérifier si le post existe déjà
                        existing = session.query(FacebookPostsMetadata).filter(
                            FacebookPostsMetadata.post_id == post_data['post_id']
                        ).first()
                        
                        if existing:
                            # Mettre à jour si nécessaire
                            continue
                        
                        # Créer nouveau post
                        new_post = FacebookPostsMetadata(
                            post_id=post_data['post_id'],
                            page_id=page_id,
                            created_time=post_data['created_time'],
                            status_type=post_data['status_type'],
                            message=post_data['message'],
                            permalink_url=post_data['permalink_url'],
                            full_picture=post_data['full_picture'],
                            author_name=post_data['author_name'],
                            author_id=post_data['author_id'],
                            comments_count=post_data['comments_count'],
                            likes_count=post_data['likes_count'],
                            shares_count=post_data['shares_count']
                        )
                        
                        session.add(new_post)
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage post Facebook {post_data.get('post_id')}: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Posts Facebook stockés: {stored_count} nouveaux posts")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage posts Facebook: {e}")
            return 0
    
    def _store_posts_lifetime_metrics(self, page_id: str, metrics_data: List[Dict]) -> int:
        """Stocker les métriques lifetime des posts"""
        
        try:
            with db_manager.get_session() as session:
                stored_count = 0
                
                for metrics in metrics_data:
                    try:
                        # Vérifier si les métriques existent déjà
                        existing = session.query(FacebookPostsLifetime).filter(
                            FacebookPostsLifetime.post_id == metrics['post_id']
                        ).first()
                        
                        if existing:
                            # Mettre à jour
                            for key, value in metrics.items():
                                if hasattr(existing, key) and key not in ['post_id', 'page_id']:
                                    setattr(existing, key, value)
                        else:
                            # Créer nouveau
                            new_metrics = FacebookPostsLifetime(
                                post_id=metrics['post_id'],
                                page_id=page_id,
                                # Mapper toutes les métriques Facebook
                                post_impressions=metrics.get('post_impressions', 0),
                                post_impressions_unique=metrics.get('post_impressions_unique', 0),
                                post_impressions_organic=metrics.get('post_impressions_organic', 0),
                                post_impressions_organic_unique=metrics.get('post_impressions_organic_unique', 0),
                                post_impressions_paid=metrics.get('post_impressions_paid', 0),
                                post_impressions_paid_unique=metrics.get('post_impressions_paid_unique', 0),
                                post_impressions_viral=metrics.get('post_impressions_viral', 0),
                                post_impressions_viral_unique=metrics.get('post_impressions_viral_unique', 0),
                                post_impressions_fan=metrics.get('post_impressions_fan', 0),
                                post_impressions_nonviral=metrics.get('post_impressions_nonviral', 0),
                                post_impressions_nonviral_unique=metrics.get('post_impressions_nonviral_unique', 0),
                                post_reactions_like_total=metrics.get('post_reactions_like_total', 0),
                                post_reactions_love_total=metrics.get('post_reactions_love_total', 0),
                                post_reactions_wow_total=metrics.get('post_reactions_wow_total', 0),
                                post_reactions_haha_total=metrics.get('post_reactions_haha_total', 0),
                                post_reactions_sorry_total=metrics.get('post_reactions_sorry_total', 0),
                                post_reactions_anger_total=metrics.get('post_reactions_anger_total', 0),
                                post_clicks=metrics.get('post_clicks', 0),
                                post_consumptions=metrics.get('post_consumptions', 0),
                                post_video_views=metrics.get('post_video_views', 0),
                                post_video_views_unique=metrics.get('post_video_views_unique', 0),
                                post_video_views_organic=metrics.get('post_video_views_organic', 0),
                                post_video_views_organic_unique=metrics.get('post_video_views_organic_unique', 0),
                                post_video_views_paid=metrics.get('post_video_views_paid', 0),
                                post_video_views_paid_unique=metrics.get('post_video_views_paid_unique', 0),
                                post_video_views_sound_on=metrics.get('post_video_views_sound_on', 0),
                                post_video_complete_views_30s=metrics.get('post_video_complete_views_30s', 0),
                                post_video_avg_time_watched=metrics.get('post_video_avg_time_watched', 0),
                                post_video_view_time=metrics.get('post_video_view_time', 0),
                                post_fan_reach=metrics.get('post_fan_reach', 0)
                            )
                            session.add(new_metrics)
                        
                        stored_count += 1
                        
                    except Exception as e:
                        logger.warning(f"⚠️  Erreur stockage métriques lifetime {metrics.get('post_id')}: {e}")
                        continue
                
                session.commit()
                logger.debug(f"✅ Métriques lifetime Facebook stockées: {stored_count} records")
                return stored_count
                
        except Exception as e:
            logger.error(f"❌ Erreur stockage métriques lifetime Facebook: {e}")
            return 0
    
    def _get_recent_posts_from_db(self, page_id: str, days: int = 30) -> List[str]:
        """Récupérer les IDs des posts récents depuis la base"""
        
        try:
            with db_manager.get_session() as session:
                cutoff_date = datetime.utcnow() - timedelta(days=days)
                
                posts = session.query(FacebookPostsMetadata.post_id).filter(
                    FacebookPostsMetadata.page_id == page_id,
                    FacebookPostsMetadata.created_time >= cutoff_date
                ).limit(self.config['max_posts_per_page']).all()
                
                return [post.post_id for post in posts]
                
        except Exception as e:
            logger.error(f"❌ Erreur récupération posts récents Facebook: {e}")
            return []
    
    # ========================================
    # COLLECTE COMPLÈTE PAR PAGE
    # ========================================
    
    def collect_page_data(self, user_id: int, page_id: str,
                         data_types: List[FacebookDataType] = None) -> Dict[str, FacebookCollectionResult]:
        """Collecter toutes les données d'une page Facebook"""
        
        if data_types is None:
            data_types = [
                FacebookDataType.PAGE_INFO,
                FacebookDataType.PAGE_METRICS,
                FacebookDataType.POSTS,
                FacebookDataType.POSTS_METRICS
            ]
        
        # Verrou pour éviter les collectes concurrentes
        lock_key = f"collect_fb_{page_id}"
        if lock_key in self._collection_locks:
            logger.warning(f"⚠️  Collecte Facebook déjà en cours pour {page_id}")
            return {}
        
        self._collection_locks[lock_key] = threading.Lock()
        
        try:
            with self._collection_locks[lock_key]:
                self.session_stats['collections_started'] += 1
                start_time = time.time()
                
                logger.info(f"🚀 Début collecte complète Facebook: {page_id}")
                
                results = {}
                
                # Collecter chaque type de données
                for data_type in data_types:
                    try:
                        if data_type == FacebookDataType.PAGE_INFO:
                            result = self.collect_page_info(user_id, page_id)
                        elif data_type == FacebookDataType.PAGE_METRICS:
                            result = self.collect_page_metrics(user_id, page_id)
                        elif data_type == FacebookDataType.POSTS:
                            result = self.collect_posts(user_id, page_id)
                        elif data_type == FacebookDataType.POSTS_METRICS:
                            result = self.collect_posts_metrics(user_id, page_id)
                        else:
                            continue
                        
                        results[data_type.value] = result
                        
                        # Mettre à jour les statistiques
                        self.session_stats['total_api_calls'] += result.api_calls_made
                        self.session_stats['total_records_collected'] += result.records_collected
                        
                        # Pause entre les types de données pour respecter les rate limits
                        time.sleep(2)
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur collecte Facebook {data_type.value}: {e}")
                        results[data_type.value] = FacebookCollectionResult(
                            status=CollectionStatus.FAILED,
                            data_type=data_type,
                            page_id=page_id,
                            errors=[str(e)]
                        )
                
                # Statistiques finales
                execution_time = time.time() - start_time
                successful_collections = sum(1 for r in results.values() if r.status == CollectionStatus.SUCCESS)
                
                if successful_collections > 0:
                    self.session_stats['collections_completed'] += 1
                else:
                    self.session_stats['collections_failed'] += 1
                
                logger.info(f"✅ Collecte Facebook terminée: {page_id} ({successful_collections}/{len(data_types)} réussies, {execution_time:.2f}s)")
                return results
                
        finally:
            # Nettoyer le verrou
            if lock_key in self._collection_locks:
                del self._collection_locks[lock_key]
    
    def collect_user_pages(self, user_id: int, force_refresh: bool = False) -> Dict[str, Any]:
        """Collecter les données de toutes les pages Facebook d'un utilisateur"""
        
        try:
            # Récupérer les comptes Facebook de l'utilisateur
            with db_manager.get_session() as session:
                facebook_accounts = session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user_id,
                    FacebookAccount.is_active == True
                ).all()
            
            if not facebook_accounts:
                logger.info(f"ℹ️  Aucun compte Facebook trouvé pour user {user_id}")
                return {
                    'user_id': user_id,
                    'pages_processed': 0,
                    'total_collections': 0,
                    'successful_collections': 0,
                    'failed_collections': 0,
                    'results': {}
                }
            
            logger.info(f"🚀 Début collecte utilisateur Facebook {user_id}: {len(facebook_accounts)} pages")
            
            all_results = {}
            total_collections = 0
            successful_collections = 0
            failed_collections = 0
            
            # Traitement séquentiel ou parallèle selon la configuration
            if self.config['enable_concurrent_collection'] and len(facebook_accounts) > 1:
                # Traitement parallèle (limité à 2 threads pour Facebook)
                with ThreadPoolExecutor(max_workers=min(2, len(facebook_accounts))) as executor:
                    future_to_page = {
                        executor.submit(
                            self.collect_page_data, 
                            user_id, 
                            account.page_id
                        ): account.page_id 
                        for account in facebook_accounts
                    }
                    
                    for future in as_completed(future_to_page):
                        page_id = future_to_page[future]
                        try:
                            page_results = future.result()
                            all_results[page_id] = page_results
                            
                            # Compter les résultats
                            for result in page_results.values():
                                total_collections += 1
                                if result.status == CollectionStatus.SUCCESS:
                                    successful_collections += 1
                                else:
                                    failed_collections += 1
                                    
                        except Exception as e:
                            logger.error(f"❌ Erreur collecte parallèle Facebook {page_id}: {e}")
                            failed_collections += 1
            else:
                # Traitement séquentiel
                for account in facebook_accounts:
                    try:
                        page_results = self.collect_page_data(user_id, account.page_id)
                        all_results[account.page_id] = page_results
                        
                        # Compter les résultats
                        for result in page_results.values():
                            total_collections += 1
                            if result.status == CollectionStatus.SUCCESS:
                                successful_collections += 1
                            else:
                                failed_collections += 1
                        
                        # Pause entre pages pour respecter les rate limits Facebook
                        time.sleep(3)
                        
                    except Exception as e:
                        logger.error(f"❌ Erreur collecte séquentielle Facebook {account.page_id}: {e}")
                        failed_collections += 1
            
            summary = {
                'user_id': user_id,
                'pages_processed': len(facebook_accounts),
                'total_collections': total_collections,
                'successful_collections': successful_collections,
                'failed_collections': failed_collections,
                'success_rate': successful_collections / max(1, total_collections),
                'results': all_results,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            logger.info(f"✅ Collecte utilisateur Facebook terminée: {user_id} ({successful_collections}/{total_collections} réussies)")
            return summary
            
        except Exception as e:
            logger.error(f"❌ Erreur collecte utilisateur Facebook {user_id}: {e}")
            return {
                'user_id': user_id,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    # ========================================
    # PLANIFICATION ET AUTOMATISATION
    # ========================================
    
    def should_collect_now(self, user_id: int, page_id: str) -> Tuple[bool, str]:
        """Déterminer si une collecte Facebook doit avoir lieu maintenant"""
        
        try:
            # Vérifier la dernière collecte en base
            with db_manager.get_session() as session:
                # Chercher la dernière entrée de métriques
                last_metrics = session.query(FacebookPageDaily).filter(
                    FacebookPageDaily.page_id == page_id
                ).order_by(FacebookPageDaily.created_at.desc()).first()
                
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
            logger.error(f"❌ Erreur vérification timing collecte Facebook: {e}")
            return True, "Erreur vérification, collecte par défaut"
    
    # ========================================
    # UTILITAIRES ET HELPERS
    # ========================================
    
    def _parse_facebook_error(self, response: requests.Response) -> Dict[str, Any]:
        """Parser une réponse d'erreur Facebook"""
        
        try:
            error_data = response.json()
            if 'error' in error_data:
                error_info = error_data['error']
                return {
                    'message': error_info.get('message', 'Erreur Facebook inconnue'),
                    'type': error_info.get('type', 'unknown'),
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
                'message': response.text or 'Réponse Facebook invalide',
                'status_code': response.status_code
            }
    
    def get_collection_statistics(self) -> Dict[str, Any]:
        """Obtenir les statistiques de collecte Facebook de la session"""
        
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
            'rate_limit_cache_size': len(self._rate_limit_cache),
            'active_locks': len(self._collection_locks)
        }
        
        return stats
    
    def clear_quota_cache(self):
        """Vider le cache des quotas Facebook"""
        self._quota_cache.clear()
        self._rate_limit_cache.clear()
        logger.info("🧹 Cache des quotas Facebook vidé")
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du collecteur Facebook"""
        
        health = {
            'collector_status': 'ok',
            'configuration': self.config,
            'session_statistics': self.get_collection_statistics(),
            'oauth_manager_status': 'ok',
            'database_connection': 'unknown',
            'facebook_api_reachable': 'unknown',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Test de la base de données
        try:
            with db_manager.get_session() as session:
                session.execute('SELECT 1').fetchone()
            health['database_connection'] = 'ok'
        except Exception as e:
            health['database_connection'] = f'error: {e}'
        
        # Test de l'API Facebook
        try:
            response = self.session.get('https://graph.facebook.com', timeout=5)
            health['facebook_api_reachable'] = 'ok' if response.status_code in [200, 400, 401] else 'error'
        except Exception as e:
            health['facebook_api_reachable'] = f'error: {e}'
        
        # Test du gestionnaire OAuth
        try:
            oauth_health = self.oauth_manager.health_check()
            health['oauth_manager_status'] = 'ok' if oauth_health['oauth_manager'] == 'ok' else 'error'
        except Exception as e:
            health['oauth_manager_status'] = f'error: {e}'
        
        return health
    
    def cleanup_old_data(self, days: int = 90) -> Dict[str, int]:
        """Nettoyer les anciennes données de collecte Facebook"""
        
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            deleted_counts = {}
            
            with db_manager.get_session() as session:
                # Nettoyer les métriques quotidiennes anciennes
                deleted_page_daily = session.query(FacebookPageDaily).filter(
                    FacebookPageDaily.created_at < cutoff_date
                ).count()
                
                session.query(FacebookPageDaily).filter(
                    FacebookPageDaily.created_at < cutoff_date
                ).delete()
                
                deleted_counts['page_daily'] = deleted_page_daily
                
                # Nettoyer les posts très anciens (garde plus longtemps car lifetime)
                very_old_cutoff = datetime.utcnow() - timedelta(days=days * 2)
                
                deleted_posts = session.query(FacebookPostsMetadata).filter(
                    FacebookPostsMetadata.created_at < very_old_cutoff
                ).count()
                
                session.query(FacebookPostsMetadata).filter(
                    FacebookPostsMetadata.created_at < very_old_cutoff
                ).delete()
                
                deleted_counts['posts_metadata'] = deleted_posts
                
                session.commit()
            
            total_deleted = sum(deleted_counts.values())
            logger.info(f"🧹 Nettoyage Facebook terminé: {total_deleted} enregistrements supprimés")
            
            return deleted_counts
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du nettoyage Facebook: {e}")
            return {}

# ========================================
# INSTANCE GLOBALE
# ========================================

facebook_collector = FacebookCollector()

# ========================================
# FONCTIONS HELPER
# ========================================

def collect_user_facebook_data(user_id: int, force_refresh: bool = False) -> Dict[str, Any]:
    """Fonction helper pour collecter les données Facebook d'un utilisateur"""
    return facebook_collector.collect_user_pages(user_id, force_refresh)

def collect_page_facebook_data(user_id: int, page_id: str, 
                              data_types: List[str] = None) -> Dict[str, Any]:
    """Fonction helper pour collecter les données d'une page spécifique"""
    
    if data_types:
        data_types_enum = [FacebookDataType(dt) for dt in data_types if dt in [e.value for e in FacebookDataType]]
    else:
        data_types_enum = None
    
    return facebook_collector.collect_page_data(user_id, page_id, data_types_enum)

def should_collect_facebook_data(user_id: int, page_id: str) -> Dict[str, Any]:
    """Fonction helper pour vérifier si une collecte Facebook est nécessaire"""
    
    should_collect, reason = facebook_collector.should_collect_now(user_id, page_id)
    
    return {
        'should_collect': should_collect,
        'reason': reason,
        'user_id': user_id,
        'page_id': page_id,
        'timestamp': datetime.utcnow().isoformat()
    }

def get_facebook_collection_stats() -> Dict[str, Any]:
    """Fonction helper pour obtenir les statistiques de collecte Facebook"""
    return facebook_collector.get_collection_statistics()

def cleanup_facebook_data(days: int = 90) -> Dict[str, Any]:
    """Fonction helper pour nettoyer les anciennes données Facebook"""
    return facebook_collector.cleanup_old_data(days)

# ========================================
# COLLECTEUR AUTOMATIQUE FACEBOOK (POUR SCHEDULER)
# ========================================

class FacebookCollectionScheduler:
    """Planificateur de collectes automatiques Facebook"""
    
    def __init__(self):
        self.collector = facebook_collector
        self.is_running = False
        self._stop_event = threading.Event()
    
    def start_automatic_collection(self, interval_minutes: int = 90):  # Plus long que LinkedIn
        """Démarrer la collecte automatique Facebook"""
        
        if self.is_running:
            logger.warning("⚠️  Collecte automatique Facebook déjà en cours")
            return
        
        self.is_running = True
        self._stop_event.clear()
        
        def collection_loop():
            while not self._stop_event.is_set():
                try:
                    self._run_scheduled_collections()
                except Exception as e:
                    logger.error(f"❌ Erreur dans la boucle de collecte Facebook: {e}")
                
                # Attendre avant la prochaine vérification
                self._stop_event.wait(interval_minutes * 60)
        
        # Démarrer dans un thread séparé
        collection_thread = threading.Thread(target=collection_loop, daemon=True)
        collection_thread.start()
        
        logger.info(f"🚀 Collecte automatique Facebook démarrée (intervalle: {interval_minutes} min)")
    
    def stop_automatic_collection(self):
        """Arrêter la collecte automatique Facebook"""
        
        if not self.is_running:
            return
        
        self._stop_event.set()
        self.is_running = False
        logger.info("🛑 Collecte automatique Facebook arrêtée")
    
    def _run_scheduled_collections(self):
        """Exécuter les collectes programmées Facebook"""
        
        try:
            # Récupérer tous les utilisateurs actifs avec des comptes Facebook
            with db_manager.get_session() as session:
                users_with_facebook = session.query(User.id).join(FacebookAccount).filter(
                    User.is_active == True,
                    FacebookAccount.is_active == True
                ).distinct().all()
            
            logger.info(f"🔄 Vérification collectes Facebook programmées: {len(users_with_facebook)} utilisateurs")
            
            for (user_id,) in users_with_facebook:
                try:
                    # Récupérer les pages de l'utilisateur
                    with db_manager.get_session() as session:
                        pages = session.query(FacebookAccount.page_id).filter(
                            FacebookAccount.user_id == user_id,
                            FacebookAccount.is_active == True
                        ).all()
                    
                    for (page_id,) in pages:
                        # Vérifier si une collecte est nécessaire
                        should_collect, reason = self.collector.should_collect_now(user_id, page_id)
                        
                        if should_collect:
                            logger.info(f"📊 Démarrage collecte Facebook programmée: user {user_id}, page {page_id} ({reason})")
                            
                            # Lancer la collecte en arrière-plan
                            self._collect_async(user_id, page_id)
                        
                        # Pause plus longue entre les vérifications (Facebook rate limits)
                        time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Erreur vérification utilisateur Facebook {user_id}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"❌ Erreur dans la vérification des collectes Facebook programmées: {e}")
    
    def _collect_async(self, user_id: int, page_id: str):
        """Lancer une collecte Facebook en arrière-plan"""
        
        def collect():
            try:
                self.collector.collect_page_data(user_id, page_id)
            except Exception as e:
                logger.error(f"❌ Erreur collecte Facebook async {page_id}: {e}")
        
        # Lancer dans un thread séparé
        thread = threading.Thread(target=collect, daemon=True)
        thread.start()

# Instance globale du scheduler Facebook
facebook_scheduler = FacebookCollectionScheduler()

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test FacebookCollector...")
    
    try:
        # Test de configuration
        collector = FacebookCollector()
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