# ========================================
# app/utils/metrics/base_metrics.py
# ========================================

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class MetricType(Enum):
    """Types de métriques pour une classification cohérente"""
    NUMBER = "NUMBER"
    PERCENT = "PERCENT"
    TEXT = "TEXT"
    DATE = "DATE"
    DATETIME = "DATETIME"
    URL = "URL"
    BOOLEAN = "BOOLEAN"

class MetricCategory(Enum):
    """Catégories de métriques universelles"""
    PAGE_STATISTICS = "page_statistics"
    POST_METRICS = "post_metrics"
    FOLLOWER_METRICS = "follower_metrics"
    ENGAGEMENT_METRICS = "engagement_metrics"
    VIDEO_METRICS = "video_metrics"
    BREAKDOWN_METRICS = "breakdown_metrics"
    CALCULATED_METRICS = "calculated_metrics"

class BaseMetrics(ABC):
    """Classe de base optimisée pour toutes les métriques de plateformes sociales"""
    
    def __init__(self, platform_name: str, api_version: str):
        self.platform_name = platform_name.lower()
        self.api_version = api_version
        self.last_updated = datetime.now()
        self._metrics_cache = {}
        self._schema_cache = None
        self._validation_cache = {}
        
        # Configuration spécifique par plateforme
        self.platform_config = self._get_platform_config()
    
    def _get_platform_config(self) -> Dict[str, Any]:
        """Configuration spécifique par plateforme"""
        configs = {
            'linkedin': {
                'api_base_url': 'https://api.linkedin.com/rest',
                'rate_limits': {'requests_per_minute': 500},
                'supported_versions': ['202505', '202312', '202309'],
                'default_aggregations': {
                    'followers': 'MAX',
                    'impressions': 'SUM',
                    'engagement_rate': 'AVG'
                }
            },
            'facebook': {
                'api_base_url': 'https://graph.facebook.com',
                'rate_limits': {'requests_per_minute': 200},
                'supported_versions': ['v19.0', 'v18.0', 'v17.0'],
                'default_aggregations': {
                    'page_fans': 'MAX',
                    'page_impressions': 'SUM',
                    'engagement_rate': 'AVG'
                }
            }
        }
        return configs.get(self.platform_name, {})
    
    @abstractmethod
    def get_page_metrics(self) -> List[str]:
        """Retourne les métriques de page de la plateforme"""
        pass
    
    @abstractmethod
    def get_post_metrics(self) -> List[str]:
        """Retourne les métriques de posts de la plateforme"""
        pass
    
    @abstractmethod
    def get_column_mapping(self) -> Dict[str, str]:
        """Retourne le mapping API -> Nom d'affichage"""
        pass
    
    @abstractmethod
    def get_calculated_metrics(self) -> Dict[str, str]:
        """Retourne les formules des métriques calculées"""
        pass
    
    def get_all_metrics(self) -> List[str]:
        """Retourne toutes les métriques disponibles avec cache"""
        cache_key = 'all_metrics'
        if cache_key not in self._metrics_cache:
            self._metrics_cache[cache_key] = self.get_page_metrics() + self.get_post_metrics()
        return self._metrics_cache[cache_key]
    
    def get_deprecated_metrics(self) -> List[str]:
        """Retourne les métriques dépréciées (à implémenter dans les sous-classes)"""
        return getattr(self, '_deprecated_metrics', [])
    
    def get_metrics_by_category(self, category: str) -> List[str]:
        """Retourne les métriques par catégorie (à implémenter dans les sous-classes)"""
        return []
    
    def get_api_field_mapping(self) -> Dict[str, str]:
        """Retourne le mapping vers les champs API (à implémenter dans les sous-classes)"""
        return getattr(self, '_api_field_mapping', {})
    
    def validate_api_compatibility(self, api_version: str = None) -> Dict[str, Any]:
        """Valide la compatibilité avec une version d'API"""
        target_version = api_version or self.api_version
        supported_versions = self.platform_config.get('supported_versions', [])
        
        return {
            'compatible': target_version in supported_versions,
            'current_version': self.api_version,
            'target_version': target_version,
            'supported_versions': supported_versions,
            'warnings': self._get_version_warnings(target_version, supported_versions)
        }
    
    def _get_version_warnings(self, target_version: str, supported_versions: List[str]) -> List[str]:
        """Génère des avertissements de compatibilité"""
        warnings = []
        if target_version not in supported_versions:
            warnings.append(f"Version {target_version} non testée pour {self.platform_name}")
        
        if supported_versions and target_version < min(supported_versions):
            warnings.append("Version trop ancienne, certaines métriques peuvent être indisponibles")
        
        return warnings
    
    def get_looker_schema(self) -> Dict[str, Any]:
        """Génère le schéma optimisé pour Looker Studio avec cache"""
        if self._schema_cache is None:
            self._schema_cache = self._build_looker_schema()
        return self._schema_cache
    
    def _build_looker_schema(self) -> Dict[str, Any]:
        """Construit le schéma Looker avec optimisations"""
        schema = {
            'platform': self.platform_name,
            'api_version': self.api_version,
            'last_updated': self.last_updated.isoformat(),
            'dimensions': [],
            'metrics': [],
            'metadata': {
                'total_metrics': 0,
                'calculated_metrics_count': len(self.get_calculated_metrics()),
                'deprecated_metrics_count': len(self.get_deprecated_metrics())
            }
        }
        
        # Dimensions communes optimisées
        common_dimensions = self._get_common_dimensions()
        schema['dimensions'].extend(common_dimensions)
        
        # Dimensions spécifiques à la plateforme
        platform_dimensions = self._get_platform_specific_dimensions()
        schema['dimensions'].extend(platform_dimensions)
        
        # Métriques avec métadonnées enrichies
        mapping = self.get_column_mapping()
        calculated_metrics = self.get_calculated_metrics()
        deprecated_metrics = set(self.get_deprecated_metrics())
        
        for api_name, display_name in mapping.items():
            if not self._is_dimension_field(api_name):
                metric_info = {
                    'id': api_name,
                    'name': display_name,
                    'type': self._determine_metric_type(display_name),
                    'platform': self.platform_name,
                    'is_calculated': api_name in calculated_metrics,
                    'is_deprecated': api_name in deprecated_metrics,
                    'aggregation': self._get_default_aggregation(api_name)
                }
                
                if metric_info['is_calculated']:
                    metric_info['formula'] = calculated_metrics[api_name]
                
                schema['metrics'].append(metric_info)
        
        schema['metadata']['total_metrics'] = len(schema['metrics'])
        return schema
    
    def _get_common_dimensions(self) -> List[Dict[str, str]]:
        """Dimensions communes à toutes les plateformes"""
        return [
            {'id': 'platform', 'name': 'Plateforme', 'type': 'TEXT'},
            {'id': 'date', 'name': 'Date', 'type': 'DATE'},
            {'id': 'account_name', 'name': 'Nom du compte', 'type': 'TEXT'},
            {'id': 'account_id', 'name': 'ID du compte', 'type': 'TEXT'},
            {'id': 'content_type', 'name': 'Type de contenu', 'type': 'TEXT'}
        ]
    
    def _get_platform_specific_dimensions(self) -> List[Dict[str, str]]:
        """Dimensions spécifiques à la plateforme (à implémenter dans les sous-classes)"""
        return []
    
    def _is_dimension_field(self, field_name: str) -> bool:
        """Détermine si un champ est une dimension plutôt qu'une métrique"""
        dimension_indicators = [
            'platform', 'date', 'account_', 'post_id', 'post_type', 'post_text',
            'media_type', 'media_url', 'status_type', 'message', 'permalink_url',
            'breakdown_', 'author_', 'content_type'
        ]
        return any(field_name.startswith(indicator) for indicator in dimension_indicators)
    
    def _determine_metric_type(self, display_name: str) -> str:
        """Détermine le type de métrique basé sur le nom avec logique améliorée"""
        name_lower = display_name.lower()
        
        # Pourcentages et taux
        if any(keyword in name_lower for keyword in ['%', 'tx', 'taux', 'rate', 'pourcentage']):
            return MetricType.PERCENT.value
        
        # URLs et liens
        elif any(keyword in name_lower for keyword in ['url', 'lien', 'link', 'permalink']):
            return MetricType.URL.value
        
        # Dates et temps
        elif any(keyword in name_lower for keyword in ['date', 'temps', 'time', 'heure', 'création']):
            return MetricType.DATETIME.value
        
        # Booléens
        elif any(keyword in name_lower for keyword in ['est un', 'is_', 'has_', 'booléen']):
            return MetricType.BOOLEAN.value
        
        # Texte
        elif any(keyword in name_lower for keyword in ['nom', 'texte', 'message', 'type', 'label']):
            return MetricType.TEXT.value
        
        # Par défaut: numérique
        else:
            return MetricType.NUMBER.value
    
    def _get_default_aggregation(self, metric_name: str) -> str:
        """Détermine l'agrégation par défaut pour une métrique"""
        default_aggregations = self.platform_config.get('default_aggregations', {})
        
        # Recherche exacte
        if metric_name in default_aggregations:
            return default_aggregations[metric_name]
        
        # Recherche par pattern
        name_lower = metric_name.lower()
        
        if any(keyword in name_lower for keyword in ['total_', 'followers', 'fans', 'count']):
            return 'MAX'  # Valeurs cumulatives
        elif any(keyword in name_lower for keyword in ['rate', 'percentage', '%', 'tx', 'taux']):
            return 'AVG'  # Moyennes pour les pourcentages
        else:
            return 'SUM'  # Somme par défaut
    
    def clear_cache(self) -> None:
        """Vide tous les caches"""
        self._metrics_cache.clear()
        self._schema_cache = None
        self._validation_cache.clear()
        logger.debug(f"Cache vidé pour {self.platform_name}")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Retourne un résumé des métriques disponibles"""
        all_metrics = self.get_all_metrics()
        calculated = self.get_calculated_metrics()
        deprecated = self.get_deprecated_metrics()
        
        return {
            'platform': self.platform_name,
            'api_version': self.api_version,
            'total_metrics': len(all_metrics),
            'page_metrics': len(self.get_page_metrics()),
            'post_metrics': len(self.get_post_metrics()),
            'calculated_metrics': len(calculated),
            'deprecated_metrics': len(deprecated),
            'last_updated': self.last_updated.isoformat(),
            'rate_limits': self.platform_config.get('rate_limits', {}),
            'supported_versions': self.platform_config.get('supported_versions', [])
        }
