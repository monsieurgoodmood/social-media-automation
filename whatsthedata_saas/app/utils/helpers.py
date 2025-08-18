"""
Fonctions utilitaires complètes pour l'application WhatTheData
Validation, formatage, retry, rate limiting, cache, sécurité, etc.
"""

import os
import re
import json
import time
import hashlib
import hmac
import base64
import uuid
import logging
import functools
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse, parse_qs, urlencode
from email.utils import parseaddr
import secrets
import string

# Imports pour rate limiting et cache
from collections import defaultdict, OrderedDict
from dataclasses import dataclass
from enum import Enum

# Configuration du logging
logger = logging.getLogger(__name__)

# ========================================
# VALIDATION ET FORMATAGE
# ========================================

def validate_email(email: str) -> bool:
    """Valider une adresse email"""
    if not email or not isinstance(email, str):
        return False
    
    # Pattern regex pour email (version simplifiée mais robuste)
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Vérifications de base
    if len(email) > 254:  # RFC 5321
        return False
    
    if not re.match(pattern, email):
        return False
    
    # Vérifications supplémentaires
    local, domain = email.rsplit('@', 1)
    if len(local) > 64:  # RFC 5321
        return False
    
    # Domaines interdits (liste basique)
    blocked_domains = {
        'example.com', 'test.com', 'localhost', 
        'temp-mail.org', '10minutemail.com'
    }
    
    if domain.lower() in blocked_domains:
        return False
    
    return True

def validate_url(url: str, allowed_schemes: List[str] = None) -> bool:
    """Valider une URL"""
    if not url or not isinstance(url, str):
        return False
    
    try:
        parsed = urlparse(url)
        
        # Vérifier le schéma
        if allowed_schemes is None:
            allowed_schemes = ['http', 'https']
        
        if parsed.scheme not in allowed_schemes:
            return False
        
        # Vérifier qu'il y a un nom de domaine
        if not parsed.netloc:
            return False
        
        return True
        
    except Exception:
        return False

def validate_phone(phone: str, country_code: str = None) -> bool:
    """Valider un numéro de téléphone (version basique)"""
    if not phone or not isinstance(phone, str):
        return False
    
    # Nettoyer le numéro
    cleaned = re.sub(r'[^\d+]', '', phone)
    
    # Vérifications de base
    if len(cleaned) < 10 or len(cleaned) > 15:
        return False
    
    # Doit commencer par + ou un chiffre
    if not (cleaned.startswith('+') or cleaned[0].isdigit()):
        return False
    
    return True

def validate_password_strength(password: str) -> Dict[str, Any]:
    """Valider la force d'un mot de passe"""
    if not password:
        return {'valid': False, 'score': 0, 'errors': ['Mot de passe requis']}
    
    errors = []
    score = 0
    
    # Longueur minimum
    if len(password) < 8:
        errors.append('Au moins 8 caractères requis')
    else:
        score += 1
    
    # Complexité
    if re.search(r'[a-z]', password):
        score += 1
    else:
        errors.append('Au moins une lettre minuscule requise')
    
    if re.search(r'[A-Z]', password):
        score += 1
    else:
        errors.append('Au moins une lettre majuscule requise')
    
    if re.search(r'\d', password):
        score += 1
    else:
        errors.append('Au moins un chiffre requis')
    
    if re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        score += 1
    else:
        errors.append('Au moins un caractère spécial requis')
    
    # Longueur bonus
    if len(password) >= 12:
        score += 1
    
    # Mots de passe communs (liste basique)
    common_passwords = {
        'password', '123456', 'password123', 'admin', 'qwerty',
        'letmein', 'welcome', 'monkey', '1234567890'
    }
    
    if password.lower() in common_passwords:
        errors.append('Mot de passe trop commun')
        score = max(0, score - 2)
    
    is_valid = len(errors) == 0 and score >= 4
    
    return {
        'valid': is_valid,
        'score': score,
        'max_score': 6,
        'errors': errors,
        'strength': ['Très faible', 'Faible', 'Moyen', 'Bon', 'Fort', 'Très fort'][min(score, 5)]
    }

def validate_plan_type(plan_type: str) -> bool:
    """Valider un type de plan"""
    valid_plans = {'free', 'linkedin_basic', 'facebook_basic', 'premium'}
    return plan_type and plan_type.lower() in valid_plans

# ========================================
# FORMATAGE ET CONVERSION
# ========================================

def format_datetime(dt: datetime, format_type: str = 'default', timezone_str: str = 'UTC') -> str:
    """Formater une datetime avec gestion des timezones"""
    if not dt:
        return ''
    
    # Formats disponibles
    formats = {
        'default': '%Y-%m-%d %H:%M:%S',
        'date_only': '%Y-%m-%d',
        'time_only': '%H:%M:%S',
        'iso': '%Y-%m-%dT%H:%M:%S',
        'human': '%d/%m/%Y à %H:%M',
        'compact': '%Y%m%d_%H%M%S',
        'api': '%Y-%m-%dT%H:%M:%S.%fZ'
    }
    
    format_str = formats.get(format_type, formats['default'])
    
    try:
        # Gestion des timezones
        if dt.tzinfo is None and timezone_str != 'UTC':
            # Assumé UTC si pas de timezone
            dt = dt.replace(tzinfo=timezone.utc)
        
        return dt.strftime(format_str)
    except Exception as e:
        logger.error(f"❌ Erreur formatage datetime: {e}")
        return str(dt)

def parse_datetime(date_str: str, format_type: str = 'auto') -> Optional[datetime]:
    """Parser une string en datetime avec détection automatique"""
    if not date_str:
        return None
    
    # Formats à essayer
    formats_to_try = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%d',
        '%d/%m/%Y',
        '%d/%m/%Y %H:%M:%S',
        '%Y%m%d_%H%M%S'
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_str.replace('Z', ''), fmt.replace('Z', ''))
        except ValueError:
            continue
    
    logger.warning(f"⚠️  Impossible de parser la date: {date_str}")
    return None

def safe_int(value: Any, default: int = 0) -> int:
    """Conversion sécurisée vers int"""
    if value is None:
        return default
    
    try:
        if isinstance(value, str):
            # Nettoyer la string
            cleaned = re.sub(r'[^\d.-]', '', value)
            if not cleaned:
                return default
            return int(float(cleaned))
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value: Any, default: float = 0.0) -> float:
    """Conversion sécurisée vers float"""
    if value is None:
        return default
    
    try:
        if isinstance(value, str):
            cleaned = re.sub(r'[^\d.-]', '', value)
            if not cleaned:
                return default
            return float(cleaned)
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_str(value: Any, default: str = '') -> str:
    """Conversion sécurisée vers string"""
    if value is None:
        return default
    
    try:
        return str(value).strip()
    except:
        return default

def safe_bool(value: Any, default: bool = False) -> bool:
    """Conversion sécurisée vers bool"""
    if value is None:
        return default
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        return value.lower() in ('true', '1', 'yes', 'on', 'oui')
    
    if isinstance(value, (int, float)):
        return value != 0
    
    return default

def format_number(value: Union[int, float], decimal_places: int = 2, 
                  thousands_separator: str = ' ') -> str:
    """Formater un nombre avec séparateurs"""
    
    try:
        if isinstance(value, (int, float)):
            if decimal_places == 0:
                return f"{int(value):,}".replace(',', thousands_separator)
            else:
                return f"{value:,.{decimal_places}f}".replace(',', thousands_separator)
        return str(value)
    except:
        return str(value)

def format_percentage(value: Union[int, float], decimal_places: int = 2) -> str:
    """Formater un pourcentage"""
    
    try:
        if isinstance(value, (int, float)):
            return f"{value:.{decimal_places}f}%"
        return "0.00%"
    except:
        return "0.00%"

def format_currency(value: Union[int, float], currency: str = '€', 
                   decimal_places: int = 2) -> str:
    """Formater une devise"""
    
    try:
        if isinstance(value, (int, float)):
            formatted = f"{value:,.{decimal_places}f}".replace(',', ' ')
            return f"{formatted} {currency}"
        return f"0.00 {currency}"
    except:
        return f"0.00 {currency}"

def truncate_text(text: str, max_length: int = 100, suffix: str = '...') -> str:
    """Tronquer un texte avec ellipses"""
    
    if not text or not isinstance(text, str):
        return ''
    
    if len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix

def clean_html(text: str) -> str:
    """Nettoyer le HTML d'un texte (version basique)"""
    
    if not text:
        return ''
    
    import re
    
    # Supprimer les balises HTML
    text = re.sub(r'<[^>]+>', '', text)
    
    # Décoder les entités HTML courantes
    html_entities = {
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
        '&nbsp;': ' '
    }
    
    for entity, char in html_entities.items():
        text = text.replace(entity, char)
    
    return text.strip()

def slugify(text: str) -> str:
    """Convertir un texte en slug URL-friendly"""
    
    if not text:
        return ''
    
    import re
    import unicodedata
    
    # Normaliser les caractères Unicode
    text = unicodedata.normalize('NFD', text)
    text = text.encode('ascii', 'ignore').decode('ascii')
    
    # Convertir en minuscules et remplacer les espaces/caractères spéciaux
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    text = text.strip('-')
    
    return text

# ========================================
# CRYPTOGRAPHIE ET SÉCURITÉ
# ========================================

def generate_secure_token(length: int = 32) -> str:
    """Générer un token sécurisé"""
    
    return secrets.token_urlsafe(length)

def generate_api_key(prefix: str = 'wtd', length: int = 32) -> str:
    """Générer une clé API avec préfixe"""
    
    token = secrets.token_urlsafe(length)
    return f"{prefix}_{token}"

def hash_string(text: str, salt: str = None) -> str:
    """Hasher une chaîne avec SHA-256"""
    
    if salt is None:
        salt = secrets.token_hex(16)
    
    hash_object = hashlib.sha256((text + salt).encode())
    return hash_object.hexdigest()

def verify_hash(text: str, hashed: str, salt: str) -> bool:
    """Vérifier un hash"""
    
    try:
        return hash_string(text, salt) == hashed
    except:
        return False

def encrypt_text(text: str, key: str = None) -> str:
    """Chiffrement simple (base64 + obfuscation)"""
    
    if key is None:
        key = get_env_var('ENCRYPTION_KEY', 'default-key-change-me')
    
    try:
        # Simple obfuscation avec XOR
        key_bytes = key.encode() * (len(text) // len(key) + 1)
        encrypted_bytes = bytes(a ^ b for a, b in zip(text.encode(), key_bytes))
        
        # Encoder en base64
        return base64.b64encode(encrypted_bytes).decode()
    except:
        return text

def decrypt_text(encrypted_text: str, key: str = None) -> str:
    """Déchiffrement simple"""
    
    if key is None:
        key = get_env_var('ENCRYPTION_KEY', 'default-key-change-me')
    
    try:
        # Décoder base64
        encrypted_bytes = base64.b64decode(encrypted_text.encode())
        
        # Déchiffrer avec XOR
        key_bytes = key.encode() * (len(encrypted_bytes) // len(key) + 1)
        decrypted_bytes = bytes(a ^ b for a, b in zip(encrypted_bytes, key_bytes))
        
        return decrypted_bytes.decode()
    except:
        return encrypted_text

# ========================================
# RETRY ET RATE LIMITING
# ========================================

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, 
                      exponential: bool = True, jitter: bool = True):
    """Décorateur de retry avec backoff exponentiel"""
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        logger.error(f"❌ Échec définitif après {max_retries + 1} tentatives: {e}")
                        raise
                    
                    # Calculer le délai
                    if exponential:
                        delay = base_delay * (2 ** attempt)
                    else:
                        delay = base_delay
                    
                    # Ajouter du jitter pour éviter les thundering herds
                    if jitter:
                        delay *= (0.5 + secrets.randbelow(50) / 100)
                    
                    logger.warning(f"⚠️  Tentative {attempt + 1} échouée, retry dans {delay:.2f}s: {e}")
                    time.sleep(delay)
            
            # Ne devrait jamais arriver
            raise last_exception
        
        return wrapper
    return decorator

class RateLimiter:
    """Rate limiter simple basé sur token bucket"""
    
    def __init__(self, max_calls: int, window_seconds: int):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.calls = defaultdict(list)
        self.lock = threading.Lock()
    
    def is_allowed(self, key: str) -> bool:
        """Vérifier si l'appel est autorisé"""
        
        with self.lock:
            now = time.time()
            window_start = now - self.window_seconds
            
            # Nettoyer les anciens appels
            self.calls[key] = [call_time for call_time in self.calls[key] if call_time > window_start]
            
            # Vérifier la limite
            if len(self.calls[key]) >= self.max_calls:
                return False
            
            # Enregistrer l'appel
            self.calls[key].append(now)
            return True
    
    def get_reset_time(self, key: str) -> float:
        """Obtenir le temps jusqu'au reset"""
        
        with self.lock:
            if key not in self.calls or not self.calls[key]:
                return 0
            
            oldest_call = min(self.calls[key])
            reset_time = oldest_call + self.window_seconds
            return max(0, reset_time - time.time())

# Instance globale de rate limiter
rate_limiter = RateLimiter(max_calls=100, window_seconds=60)

def rate_limit(key_func: Callable = None, max_calls: int = 60, window_seconds: int = 60):
    """Décorateur de rate limiting"""
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Déterminer la clé
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key = f"{func.__name__}:default"
            
            # Vérifier la limite
            limiter = RateLimiter(max_calls, window_seconds)
            if not limiter.is_allowed(key):
                reset_time = limiter.get_reset_time(key)
                raise Exception(f"Rate limit dépassé. Reset dans {reset_time:.0f}s")
            
            return func(*args, **kwargs)
        
        return wrapper
    return decorator

# ========================================
# CACHE SIMPLE
# ========================================

class SimpleCache:
    """Cache simple en mémoire avec TTL"""
    
    def __init__(self, default_ttl: int = 300):
        self.cache = OrderedDict()
        self.default_ttl = default_ttl
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Any:
        """Récupérer une valeur du cache"""
        
        with self.lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if time.time() < expiry:
                    # Déplacer vers la fin (LRU)
                    self.cache.move_to_end(key)
                    return value
                else:
                    # Expiré
                    del self.cache[key]
            
            return None
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Stocker une valeur dans le cache"""
        
        if ttl is None:
            ttl = self.default_ttl
        
        expiry = time.time() + ttl
        
        with self.lock:
            self.cache[key] = (value, expiry)
            self.cache.move_to_end(key)
            
            # Limiter la taille du cache
            if len(self.cache) > 1000:
                self.cache.popitem(last=False)
    
    def delete(self, key: str) -> None:
        """Supprimer une clé du cache"""
        
        with self.lock:
            if key in self.cache:
                del self.cache[key]
    
    def clear(self) -> None:
        """Vider le cache"""
        
        with self.lock:
            self.cache.clear()
    
    def cleanup_expired(self) -> int:
        """Nettoyer les entrées expirées"""
        
        removed = 0
        current_time = time.time()
        
        with self.lock:
            expired_keys = [
                key for key, (value, expiry) in self.cache.items()
                if current_time >= expiry
            ]
            
            for key in expired_keys:
                del self.cache[key]
                removed += 1
        
        return removed

# Instance globale de cache
cache = SimpleCache()

def cached(ttl: int = 300, key_func: Callable = None):
    """Décorateur de cache"""
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Générer la clé de cache
            if key_func:
                cache_key = key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"
            
            # Vérifier le cache
            cached_result = cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit pour {cache_key}")
                return cached_result
            
            # Exécuter la fonction
            result = func(*args, **kwargs)
            
            # Stocker en cache
            cache.set(cache_key, result, ttl)
            logger.debug(f"Résultat mis en cache pour {cache_key}")
            
            return result
        
        return wrapper
    return decorator

# ========================================
# GESTION DES ERREURS ET LOGGING
# ========================================

def log_execution_time(func):
    """Décorateur pour logger le temps d'exécution"""
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"⏱️  {func.__name__} exécuté en {execution_time:.3f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"❌ {func.__name__} échoué après {execution_time:.3f}s: {e}")
            raise
    
    return wrapper

def safe_execute(func, default_value=None, log_errors=True):
    """Exécuter une fonction de manière sécurisée"""
    
    try:
        return func()
    except Exception as e:
        if log_errors:
            logger.error(f"❌ Erreur dans safe_execute: {e}")
        return default_value

def log_function_call(include_args=False, include_result=False):
    """Décorateur pour logger les appels de fonction"""
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log_msg = f"📞 Appel de {func.__name__}"
            
            if include_args:
                log_msg += f" avec args={args}, kwargs={kwargs}"
            
            logger.debug(log_msg)
            
            try:
                result = func(*args, **kwargs)
                
                if include_result:
                    logger.debug(f"✅ {func.__name__} retourne: {result}")
                
                return result
            except Exception as e:
                logger.error(f"❌ {func.__name__} a échoué: {e}")
                raise
        
        return wrapper
    return decorator

# ========================================
# UTILITAIRES SPÉCIFIQUES AU PROJET
# ========================================

def get_env_var(key: str, default: Any = None, required: bool = False) -> str:
    """Récupérer une variable d'environnement avec gestion d'erreurs"""
    
    value = os.getenv(key, default)
    
    if required and not value:
        raise ValueError(f"Variable d'environnement requise manquante: {key}")
    
    return value

def is_production() -> bool:
    """Vérifier si on est en environnement de production"""
    
    env = get_env_var('ENVIRONMENT', 'development').lower()
    return env in ('production', 'prod')

def is_development() -> bool:
    """Vérifier si on est en environnement de développement"""
    
    env = get_env_var('ENVIRONMENT', 'development').lower()
    return env in ('development', 'dev', 'local')

def get_user_ip(request) -> str:
    """Extraire l'IP utilisateur d'une requête (FastAPI/Flask compatible)"""
    
    # Vérifier les headers de proxy
    forwarded_for = getattr(request, 'headers', {}).get('X-Forwarded-For')
    if forwarded_for:
        return forwarded_for.split(',')[0].strip()
    
    real_ip = getattr(request, 'headers', {}).get('X-Real-IP')
    if real_ip:
        return real_ip
    
    # IP directe
    if hasattr(request, 'client') and hasattr(request.client, 'host'):
        return request.client.host
    
    return 'unknown'

def generate_unique_filename(original_filename: str, prefix: str = '') -> str:
    """Générer un nom de fichier unique"""
    
    timestamp = int(time.time())
    random_suffix = secrets.token_hex(8)
    
    if original_filename:
        name, ext = os.path.splitext(original_filename)
        clean_name = slugify(name)[:50]  # Limiter la longueur
        return f"{prefix}{clean_name}_{timestamp}_{random_suffix}{ext}"
    else:
        return f"{prefix}file_{timestamp}_{random_suffix}"

def chunk_list(lst: List, chunk_size: int) -> List[List]:
    """Diviser une liste en chunks"""
    
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def merge_dicts(*dicts) -> Dict:
    """Fusionner plusieurs dictionnaires"""
    
    result = {}
    for d in dicts:
        if d:
            result.update(d)
    return result

def extract_domain_from_url(url: str) -> str:
    """Extraire le domaine d'une URL"""
    
    try:
        parsed = urlparse(url)
        return parsed.netloc.lower()
    except:
        return ''

def is_valid_json(text: str) -> bool:
    """Vérifier si une chaîne est du JSON valide"""
    
    try:
        json.loads(text)
        return True
    except:
        return False

def parse_json_safe(text: str, default=None):
    """Parser du JSON de manière sécurisée"""
    
    try:
        return json.loads(text)
    except:
        return default

def format_file_size(size_bytes: int) -> str:
    """Formater une taille de fichier"""
    
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

# ========================================
# UTILITAIRES SPÉCIFIQUES WHATSTHEDATA
# ========================================

def normalize_platform_name(platform: str) -> str:
    """Normaliser le nom d'une plateforme"""
    
    platform_mapping = {
        'fb': 'facebook',
        'li': 'linkedin',
        'ig': 'instagram',
        'tw': 'twitter',
        'yt': 'youtube'
    }
    
    platform = platform.lower().strip()
    return platform_mapping.get(platform, platform)

def get_platform_color(platform: str) -> str:
    """Obtenir la couleur associée à une plateforme"""
    
    colors = {
        'facebook': '#1877f2',
        'linkedin': '#0a66c2',
        'instagram': '#e4405f',
        'twitter': '#1da1f2',
        'youtube': '#ff0000',
        'tiktok': '#000000'
    }
    
    return colors.get(normalize_platform_name(platform), '#666666')

def calculate_engagement_rate(interactions: int, impressions: int) -> float:
    """Calculer un taux d'engagement"""
    
    if impressions <= 0:
        return 0.0
    
    return (interactions / impressions) * 100

def format_metric_name(metric: str) -> str:
    """Formater le nom d'une métrique pour affichage"""
    
    metric_names = {
        'impressions': 'Impressions',
        'unique_impressions': 'Impressions uniques',
        'clicks': 'Clics',
        'likes': 'J\'aime',
        'comments': 'Commentaires',
        'shares': 'Partages',
        'followers': 'Abonnés',
        'engagement_rate': 'Taux d\'engagement',
        'reach': 'Portée',
        'video_views': 'Vues vidéo'
    }
    
    return metric_names.get(metric, metric.replace('_', ' ').title())

def detect_content_type(text: str) -> str:
    """Détecter le type de contenu d'un post"""
    
    if not text:
        return 'unknown'
    
    text = text.lower()
    
    # Mots-clés pour détecter le type
    if any(word in text for word in ['photo', 'image', '📷', '📸']):
        return 'photo'
    elif any(word in text for word in ['vidéo', 'video', '🎥', '📹']):
        return 'video'
    elif any(word in text for word in ['lien', 'link', 'article', 'http']):
        return 'link'
    elif any(word in text for word in ['événement', 'event', '📅']):
        return 'event'
    else:
        return 'text'

def extract_hashtags(text: str) -> List[str]:
    """Extraire les hashtags d'un texte"""
    
    if not text:
        return []
    
    import re
    hashtags = re.findall(r'#(\w+)', text)
    return [tag.lower() for tag in hashtags]

def extract_mentions(text: str) -> List[str]:
    """Extraire les mentions d'un texte"""
    
    if not text:
        return []
    
    import re
    mentions = re.findall(r'@(\w+)', text)
    return [mention.lower() for mention in mentions]

def calculate_best_posting_time(posts_data: List[Dict]) -> Dict[str, Any]:
    """Calculer le meilleur moment pour poster (analyse basique)"""
    
    if not posts_data:
        return {'hour': 12, 'day': 'Tuesday', 'confidence': 0}
    
    # Analyser les heures et jours avec le plus d'engagement
    hourly_engagement = defaultdict(list)
    daily_engagement = defaultdict(list)
    
    for post in posts_data:
        if 'created_time' in post and 'engagement_rate' in post:
            created_time = post['created_time']
            engagement = post['engagement_rate'] or 0
            
            if isinstance(created_time, str):
                try:
                    dt = datetime.fromisoformat(created_time.replace('Z', '+00:00'))
                    hour = dt.hour
                    day = dt.strftime('%A')
                    
                    hourly_engagement[hour].append(engagement)
                    daily_engagement[day].append(engagement)
                except:
                    continue
    
    # Calculer les moyennes
    best_hour = 12
    best_day = 'Tuesday'
    
    if hourly_engagement:
        hour_averages = {hour: sum(rates) / len(rates) for hour, rates in hourly_engagement.items()}
        best_hour = max(hour_averages, key=hour_averages.get)
    
    if daily_engagement:
        day_averages = {day: sum(rates) / len(rates) for day, rates in daily_engagement.items()}
        best_day = max(day_averages, key=day_averages.get)
    
    confidence = min(len(posts_data) / 100, 1.0)  # Confiance basée sur la quantité de données
    
    return {
        'hour': best_hour,
        'day': best_day,
        'confidence': confidence,
        'sample_size': len(posts_data)
    }

# ========================================
# HEALTH CHECK ET MONITORING
# ========================================

def system_health_check() -> Dict[str, Any]:
    """Vérification de santé système"""
    
    health = {
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'healthy',
        'checks': {}
    }
    
    # Vérifier la mémoire
    try:
        import psutil
        memory = psutil.virtual_memory()
        health['checks']['memory'] = {
            'status': 'ok' if memory.percent < 85 else 'warning',
            'usage_percent': memory.percent,
            'available_gb': round(memory.available / (1024**3), 2)
        }
    except ImportError:
        health['checks']['memory'] = {'status': 'unknown', 'error': 'psutil not available'}
    
    # Vérifier le disque
    try:
        import psutil
        disk = psutil.disk_usage('/')
        health['checks']['disk'] = {
            'status': 'ok' if disk.percent < 80 else 'warning',
            'usage_percent': disk.percent,
            'free_gb': round(disk.free / (1024**3), 2)
        }
    except:
        health['checks']['disk'] = {'status': 'unknown'}
    
    # Vérifier le cache
    health['checks']['cache'] = {
        'status': 'ok',
        'size': len(cache.cache),
        'expired_cleaned': cache.cleanup_expired()
    }
    
    # Déterminer le statut global
    if any(check.get('status') == 'error' for check in health['checks'].values()):
        health['status'] = 'unhealthy'
    elif any(check.get('status') == 'warning' for check in health['checks'].values()):
        health['status'] = 'degraded'
    
    return health

# ========================================
# TESTS UTILITAIRES
# ========================================

def run_utility_tests():
    """Lancer des tests sur les fonctions utilitaires"""
    
    print("🧪 Tests des utilitaires...")
    
    # Test validation email
    assert validate_email("test@example.com") == True
    assert validate_email("invalid-email") == False
    print("✅ Validation email OK")
    
    # Test formatage
    assert format_number(1234.56, 2) == "1 234.56"
    assert format_percentage(0.1234, 2) == "0.12%"
    print("✅ Formatage OK")
    
    # Test cache
    cache.set("test", "value", 1)
    assert cache.get("test") == "value"
    time.sleep(2)
    assert cache.get("test") is None
    print("✅ Cache OK")
    
    # Test conversions sécurisées
    assert safe_int("123") == 123
    assert safe_int("invalid", 0) == 0
    assert safe_bool("true") == True
    assert safe_bool("false") == False
    print("✅ Conversions sécurisées OK")
    
    # Test slugify
    assert slugify("Hello World!") == "hello-world"
    print("✅ Slugify OK")
    
    print("🎉 Tous les tests utilitaires passent!")

# ========================================
# EXPORT DES FONCTIONS PRINCIPALES
# ========================================

__all__ = [
    # Validation
    'validate_email', 'validate_url', 'validate_phone', 'validate_password_strength',
    'validate_plan_type',
    
    # Formatage
    'format_datetime', 'parse_datetime', 'safe_int', 'safe_float', 'safe_str', 'safe_bool',
    'format_number', 'format_percentage', 'format_currency', 'truncate_text', 'clean_html',
    'slugify',
    
    # Sécurité
    'generate_secure_token', 'generate_api_key', 'hash_string', 'verify_hash',
    'encrypt_text', 'decrypt_text',
    
    # Retry et cache
    'retry_with_backoff', 'RateLimiter', 'rate_limiter', 'rate_limit',
    'SimpleCache', 'cache', 'cached',
    
    # Logging
    'log_execution_time', 'safe_execute', 'log_function_call',
    
    # Utilitaires généraux
    'get_env_var', 'is_production', 'is_development', 'get_user_ip',
    'generate_unique_filename', 'chunk_list', 'merge_dicts', 'extract_domain_from_url',
    'is_valid_json', 'parse_json_safe', 'format_file_size',
    
    # WhatsTheData spécifique
    'normalize_platform_name', 'get_platform_color', 'calculate_engagement_rate',
    'format_metric_name', 'detect_content_type', 'extract_hashtags', 'extract_mentions',
    'calculate_best_posting_time',
    
    # Monitoring
    'system_health_check', 'run_utility_tests'
]

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    run_utility_tests()