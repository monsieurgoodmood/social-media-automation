"""
Gestionnaire de configuration pour WhatsTheData
Charge et valide toutes les variables d'environnement
"""

import os
import logging
from typing import Optional, Union, List, Dict, Any
from pathlib import Path
from dotenv import load_dotenv

# Configuration du logging
logger = logging.getLogger(__name__)

# Charger le fichier .env
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(env_path)

def get_env_var(key: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """
    Récupérer une variable d'environnement avec validation
    
    Args:
        key: Nom de la variable d'environnement
        default: Valeur par défaut si la variable n'existe pas
        required: Si True, lève une exception si la variable n'existe pas
    
    Returns:
        Valeur de la variable d'environnement
    
    Raises:
        ValueError: Si required=True et que la variable n'existe pas
    """
    value = os.getenv(key, default)
    
    if required and value is None:
        raise ValueError(f"Variable d'environnement requise manquante: {key}")
    
    return value

def get_env_bool(key: str, default: bool = False) -> bool:
    """Récupérer une variable d'environnement booléenne"""
    value = get_env_var(key, str(default).lower())
    return value.lower() in ('true', '1', 'yes', 'on', 'enabled')

def get_env_int(key: str, default: int = 0) -> int:
    """Récupérer une variable d'environnement entière"""
    value = get_env_var(key, str(default))
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Impossible de convertir {key}='{value}' en entier, utilisation de {default}")
        return default

def get_env_float(key: str, default: float = 0.0) -> float:
    """Récupérer une variable d'environnement flottante"""
    value = get_env_var(key, str(default))
    try:
        return float(value)
    except ValueError:
        logger.warning(f"Impossible de convertir {key}='{value}' en float, utilisation de {default}")
        return default

def get_env_list(key: str, default: List[str] = None, separator: str = ',') -> List[str]:
    """Récupérer une variable d'environnement sous forme de liste"""
    if default is None:
        default = []
    
    value = get_env_var(key)
    if not value:
        return default
    
    return [item.strip() for item in value.split(separator) if item.strip()]

class ConfigManager:
    """Gestionnaire centralisé de configuration"""
    
    def __init__(self):
        self._config_cache = {}
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Charger toutes les configurations"""
        
        # Configuration de l'application
        self.app = {
            'name': get_env_var('APP_NAME', 'WhatsTheData'),
            'version': get_env_var('APP_VERSION', '1.0.0'),
            'base_url': get_env_var('BASE_URL', 'http://localhost:8501'),
            'environment': get_env_var('ENVIRONMENT', 'development'),
            'debug': get_env_bool('DEBUG', True),
            'testing': get_env_bool('TESTING', False)
        }
        
        # 🆕 AJOUT - Configuration Google OAuth
        self.google_oauth = {
            'client_id': get_env_var('GOOGLE_CLIENT_ID', required=True),
            'client_secret': get_env_var('GOOGLE_CLIENT_SECRET', required=True),
            'redirect_uri': get_env_var('GOOGLE_REDIRECT_URI', required=True),
            'scopes': get_env_list('GOOGLE_OAUTH_SCOPES', ['openid', 'email', 'profile']),
            'urls': {
                'auth': get_env_var('GOOGLE_AUTH_URL', 'https://accounts.google.com/o/oauth2/v2/auth'),
                'token': get_env_var('GOOGLE_TOKEN_URL', 'https://oauth2.googleapis.com/token'),
                'userinfo': get_env_var('GOOGLE_USERINFO_URL', 'https://www.googleapis.com/oauth2/v2/userinfo')
            }
        }
    
        # Configuration JWT
        self.jwt = {
            'secret_key': get_env_var('JWT_SECRET_KEY', required=True),
            'algorithm': get_env_var('JWT_ALGORITHM', 'HS256'),
            'expiration_hours': get_env_int('JWT_EXPIRATION_HOURS', 24)
        }
        
        # Configuration Stripe
        self.stripe = {
            'publishable_key': get_env_var('STRIPE_PUBLISHABLE_KEY', required=True),
            'secret_key': get_env_var('STRIPE_SECRET_KEY', required=True),
            'webhook_secret': get_env_var('STRIPE_WEBHOOK_SECRET'),
            'prices': {
                'linkedin_basic': get_env_var('STRIPE_PRICE_LINKEDIN_BASIC'),
                'facebook_basic': get_env_var('STRIPE_PRICE_FACEBOOK_BASIC'),
                'premium': get_env_var('STRIPE_PRICE_PREMIUM')
            }
        }
        
        # Configuration LinkedIn
        self.linkedin = {
            'community': {
                'client_id': get_env_var('COMMUNITY_CLIENT_ID', required=True),
                'client_secret': get_env_var('COMMUNITY_CLIENT_SECRET', required=True),
                'access_token': get_env_var('COMMUNITY_ACCESS_TOKEN'),
                'refresh_token': get_env_var('COMMUNITY_REFRESH_TOKEN')
            },
            'portability': {
                'client_id': get_env_var('PORTABILITY_CLIENT_ID', required=True),
                'client_secret': get_env_var('PORTABILITY_CLIENT_SECRET', required=True),
                'access_token': get_env_var('PORTABILITY_ACCESS_TOKEN'),
                'refresh_token': get_env_var('PORTABILITY_REFRESH_TOKEN')
            },
            'signin': {
                'client_id': get_env_var('SIGNIN_CLIENT_ID', required=True),
                'client_secret': get_env_var('SIGNIN_CLIENT_SECRET', required=True)
            },
            'urls': {
                'base': get_env_var('LINKEDIN_BASE_URL', 'https://www.linkedin.com/oauth/v2'),
                'api_base': get_env_var('LINKEDIN_API_BASE_URL', 'https://api.linkedin.com/rest'),
                'redirect': get_env_var('LINKEDIN_REDIRECT_URI', 'http://localhost:8501/auth/linkedin/callback')
            },
            'collection': {
                'interval_hours': get_env_int('LINKEDIN_COLLECTION_INTERVAL_HOURS', 6),
                'max_posts': get_env_int('LINKEDIN_MAX_POSTS_PER_COLLECTION', 100),
                'timeout': get_env_int('LINKEDIN_API_TIMEOUT_SECONDS', 30),
                'retry_attempts': get_env_int('LINKEDIN_API_RETRY_ATTEMPTS', 3),
                'parallel_requests': get_env_int('LINKEDIN_PARALLEL_REQUESTS', 5),
                'rate_limit_margin': get_env_int('LINKEDIN_RATE_LIMIT_MARGIN', 10),
                'enable_concurrent': get_env_bool('LINKEDIN_ENABLE_CONCURRENT_COLLECTION', True),
                'debug_mode': get_env_bool('LINKEDIN_DEBUG_MODE', False)
            }
        }
        
        # Configuration Facebook
        self.facebook = {
            'client_id': get_env_var('FB_CLIENT_ID', required=True),
            'client_secret': get_env_var('FB_CLIENT_SECRET', required=True),
            'permissions': get_env_list('FB_PERMISSIONS', ['pages_read_engagement', 'pages_show_list', 'read_insights']),
            'urls': {
                'base': get_env_var('FACEBOOK_BASE_URL', 'https://www.facebook.com'),
                'graph': get_env_var('FACEBOOK_GRAPH_URL', 'https://graph.facebook.com/v21.0'),
                'redirect': get_env_var('FACEBOOK_REDIRECT_URI', 'http://localhost:8501/auth/facebook/callback')
            },
            'collection': {
                'interval_hours': get_env_int('FB_COLLECTION_INTERVAL_HOURS', 6),
                'max_posts': get_env_int('FB_MAX_POSTS_PER_COLLECTION', 100),
                'timeout': get_env_int('FB_API_TIMEOUT_SECONDS', 30),
                'retry_attempts': get_env_int('FB_API_RETRY_ATTEMPTS', 3),
                'parallel_requests': get_env_int('FB_PARALLEL_REQUESTS', 2),
                'rate_limit_margin': get_env_int('FB_RATE_LIMIT_MARGIN', 20),
                'enable_concurrent': get_env_bool('FB_ENABLE_CONCURRENT_COLLECTION', True),
                'debug_mode': get_env_bool('FB_DEBUG_MODE', False)
            }
        }
        
        # Configuration base de données
        self.database = {
            'url': get_env_var('DATABASE_URL', required=True),
            'pool_size': get_env_int('DB_POOL_SIZE', 10),
            'max_overflow': get_env_int('DB_MAX_OVERFLOW', 20),
            'echo': get_env_bool('DB_ECHO', False)
        }
        
        # Configuration Redis
        self.redis = {
            'url': get_env_var('REDIS_URL', 'redis://localhost:6379/0'),
            'password': get_env_var('REDIS_PASSWORD'),
            'db': get_env_int('REDIS_DB', 0)
        }
        
        # Configuration sécurité
        self.security = {
            'allowed_hosts': get_env_list('ALLOWED_HOSTS', ['localhost', '127.0.0.1']),
            'cors_origins': get_env_list('CORS_ORIGINS', ['http://localhost:8501']),
            'secure_cookies': get_env_bool('SECURE_COOKIES', False),
            'session_cookie_secure': get_env_bool('SESSION_COOKIE_SECURE', False),
            'max_requests_per_minute': get_env_int('MAX_REQUESTS_PER_MINUTE', 60)
        }
        
        # Configuration logging
        self.logging = {
            'level': get_env_var('LOG_LEVEL', 'INFO'),
            'file_path': get_env_var('LOG_FILE_PATH', 'logs/whatsthedata.log'),
            'enable_file_logging': get_env_bool('ENABLE_FILE_LOGGING', True),
            'format': get_env_var('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        }
        
        # Configuration Looker Studio
        self.looker = {
            'connector_url': get_env_var('LOOKER_CONNECTOR_URL'),
            'timeout': get_env_int('LOOKER_CONNECTOR_TIMEOUT', 30),
            'enabled': get_env_bool('ENABLE_LOOKER_CONNECTOR', True)
        }
        
        # Configuration Streamlit
        self.streamlit = {
            'port': get_env_int('STREAMLIT_SERVER_PORT', 8501),
            'address': get_env_var('STREAMLIT_SERVER_ADDRESS', 'localhost'),
            'theme': {
                'primary_color': get_env_var('STREAMLIT_THEME_PRIMARY_COLOR', '#1f77b4'),
                'background_color': get_env_var('STREAMLIT_THEME_BACKGROUND_COLOR', '#ffffff'),
                'secondary_background_color': get_env_var('STREAMLIT_THEME_SECONDARY_BACKGROUND_COLOR', '#f0f2f6')
            }
        }
        
        # Configuration email
        self.email = {
            'smtp_host': get_env_var('SMTP_HOST', 'smtp.gmail.com'),
            'smtp_port': get_env_int('SMTP_PORT', 587),
            'use_tls': get_env_bool('SMTP_USE_TLS', True),
            'username': get_env_var('SMTP_USERNAME'),
            'password': get_env_var('SMTP_PASSWORD'),
            'from_email': get_env_var('FROM_EMAIL', 'noreply@whatsthedata.com')
        }
    
    def validate_config(self) -> Dict[str, Any]:
        """Valider toutes les configurations"""
        
        validation_results = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Vérifier les configurations critiques
        try:
            # 🆕 AJOUT - Validation Google OAuth
            if not self.google_oauth['client_id']:
                validation_results['errors'].append("GOOGLE_CLIENT_ID manquant")
            elif not self.google_oauth['client_id'].endswith('.apps.googleusercontent.com'):
                validation_results['warnings'].append("GOOGLE_CLIENT_ID ne semble pas valide")
            
            if not self.google_oauth['client_secret']:
                validation_results['errors'].append("GOOGLE_CLIENT_SECRET manquant")
            elif not self.google_oauth['client_secret'].startswith('GOCSPX-'):
                validation_results['warnings'].append("GOOGLE_CLIENT_SECRET ne semble pas valide")
            
            if not self.google_oauth['redirect_uri']:
                validation_results['errors'].append("GOOGLE_REDIRECT_URI manquant")
            
            # Validation HTTPS en production pour Google OAuth
            if self.app['environment'] == 'production':
                if not self.google_oauth['redirect_uri'].startswith('https://'):
                    validation_results['errors'].append("GOOGLE_REDIRECT_URI doit utiliser HTTPS en production")

            # JWT
            if len(self.jwt['secret_key']) < 32:
                validation_results['errors'].append("JWT_SECRET_KEY doit faire au moins 32 caractères")
            
            # Stripe
            if self.app['environment'] == 'production':
                if self.stripe['publishable_key'].startswith('pk_test_'):
                    validation_results['warnings'].append("Clés Stripe de test utilisées en production")
            
            # HTTPS en production
            if self.app['environment'] == 'production':
                if not self.app['base_url'].startswith('https://'):
                    validation_results['errors'].append("BASE_URL doit utiliser HTTPS en production")
                
                if not all(url.startswith('https://') for url in [
                    self.linkedin['urls']['redirect'],
                    self.facebook['urls']['redirect']
                ]):
                    validation_results['errors'].append("URLs de redirection doivent utiliser HTTPS en production")
            
            # Base de données
            if 'postgresql://' not in self.database['url']:
                validation_results['warnings'].append("URL de base de données ne semble pas être PostgreSQL")
            
        except Exception as e:
            validation_results['errors'].append(f"Erreur lors de la validation: {e}")
        
        validation_results['valid'] = len(validation_results['errors']) == 0
        return validation_results
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Obtenir un résumé des configurations (sans données sensibles)"""
        
        return {
            'google_oauth': {
                'configured': bool(self.google_oauth['client_id']),
                'scopes_count': len(self.google_oauth['scopes']),
                'redirect_uri_https': self.google_oauth['redirect_uri'].startswith('https://')
            },
            'app': {
                'name': self.app['name'],
                'version': self.app['version'],
                'environment': self.app['environment'],
                'debug': self.app['debug']
            },
            'database': {
                'configured': bool(self.database['url']),
                'pool_size': self.database['pool_size']
            },
            'linkedin': {
                'apps_configured': 3,
                'collection_enabled': True,
                'interval_hours': self.linkedin['collection']['interval_hours']
            },
            'facebook': {
                'app_configured': bool(self.facebook['client_id']),
                'permissions_count': len(self.facebook['permissions']),
                'collection_enabled': True
            },
            'stripe': {
                'configured': bool(self.stripe['secret_key']),
                'is_test_mode': self.stripe['publishable_key'].startswith('pk_test_')
            },
            'security': {
                'allowed_hosts': len(self.security['allowed_hosts']),
                'cors_origins': len(self.security['cors_origins']),
                'secure_cookies': self.security['secure_cookies']
            }
        }

# Instance globale du gestionnaire de configuration
config_manager = ConfigManager()

# Fonctions de commodité (pour compatibilité avec le code existant)
def get_config() -> ConfigManager:
    """Récupérer l'instance du gestionnaire de configuration"""
    return config_manager

def validate_environment() -> bool:
    """Valider l'environnement complet"""
    validation = config_manager.validate_config()
    
    if not validation['valid']:
        logger.error("❌ Configuration invalide:")
        for error in validation['errors']:
            logger.error(f"  - {error}")
        return False
    
    if validation['warnings']:
        logger.warning("⚠️ Avertissements de configuration:")
        for warning in validation['warnings']:
            logger.warning(f"  - {warning}")
    
    logger.info("✅ Configuration validée avec succès")
    return True

# Test des configurations si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("🧪 Test des configurations...")
    
    try:
        # Valider l'environnement
        is_valid = validate_environment()
        
        # Afficher le résumé
        summary = config_manager.get_config_summary()
        print(f"Résumé de configuration: {summary}")
        
        # Test d'accès aux configurations
        print(f"App: {config_manager.app['name']} v{config_manager.app['version']}")
        print(f"Environnement: {config_manager.app['environment']}")
        print(f"Base de données configurée: {bool(config_manager.database['url'])}")
        print(f"LinkedIn apps: {len([app for app in ['community', 'portability', 'signin']])}")
        print(f"Facebook configuré: {bool(config_manager.facebook['client_id'])}")
        
        if is_valid:
            print("✅ Toutes les configurations sont valides")
        else:
            print("❌ Certaines configurations sont invalides")
            
    except Exception as e:
        print(f"❌ Erreur lors du test: {e}")
        import traceback
        traceback.print_exc()
# ============================================
# ALIASES DE COMPATIBILITÉ POUR MAIN.PY
# ============================================

class ConfigProxy:
    """Proxy pour compatibilité avec main.py"""
    
    def __init__(self, config_manager):
        self._config = config_manager
    
    @property
    def LOG_LEVEL(self):
        return self._config.logging['level']
    
    @property
    def APP_VERSION(self):
        return self._config.app['version']
    
    @property
    def DEBUG(self):
        return self._config.app['debug']
    
    @property
    def ENVIRONMENT(self):
        return self._config.app['environment']
    
    @property
    def LOG_LEVEL(self):
        return self._config.logging['level']
    
    @property
    def APP_VERSION(self):
        return self._config.app['version']
    
    @property
    def DEBUG(self):
        return self._config.app['debug']
    
    @property
    def BASE_URL(self):
        return self._config.app['base_url']
    
    # 🆕 AJOUT - Propriétés Google OAuth
    @property
    def GOOGLE_CLIENT_ID(self):
        return self._config.google_oauth['client_id']
    
    @property
    def GOOGLE_CLIENT_SECRET(self):
        return self._config.google_oauth['client_secret']
    
    @property
    def GOOGLE_REDIRECT_URI(self):
        return self._config.google_oauth['redirect_uri']
    
    # 🆕 AJOUT - Propriétés LinkedIn pour compatibilité avec connect_routes.py
    @property
    def LINKEDIN_CLIENT_ID(self):
        return self._config.linkedin['community']['client_id']
    
    @property
    def LINKEDIN_CLIENT_SECRET(self):
        return self._config.linkedin['community']['client_secret']
    
    # 🆕 AJOUT - Propriétés Facebook pour compatibilité
    @property
    def FB_CLIENT_ID(self):
        return self._config.facebook['client_id']
    
    @property
    def FB_CLIENT_SECRET(self):
        return self._config.facebook['client_secret']
    
    def validate_required_settings(self):
        return self._config.validate_config()
    
    def get_cors_origins(self):
        return self._config.security['cors_origins']
    
    def get_env_summary(self):
        return self._config.get_config_summary()

# Instance de compatibilité
Config = ConfigProxy(config_manager)
settings = config_manager
