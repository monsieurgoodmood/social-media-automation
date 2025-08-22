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


# ========================================
# app/utils/metrics/version_manager.py
# ========================================

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class MetricsVersionManager:
    """Gestionnaire de versions optimisé pour les métriques LinkedIn et Facebook"""
    
    def __init__(self, changelog_dir: str = "app/utils/metrics/changelogs"):
        self.changelog_dir = Path(changelog_dir)
        self.changelog_dir.mkdir(parents=True, exist_ok=True)
        
        # Fichiers de changelog par plateforme
        self.changelog_files = {
            'linkedin': self.changelog_dir / 'linkedin_changelog.json',
            'facebook': self.changelog_dir / 'facebook_changelog.json',
            'combined': self.changelog_dir / 'combined_changelog.json'
        }
    
    def create_changelog_entry(
        self, 
        platform: str, 
        old_version: str,
        new_version: str, 
        changes: Dict[str, List[str]],
        migration_notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """Crée une entrée de changelog enrichie"""
        
        entry = {
            'platform': platform.lower(),
            'version_change': {
                'from': old_version,
                'to': new_version
            },
            'date': datetime.now().isoformat(),
            'changes': {
                'added': changes.get('added', []),
                'removed': changes.get('removed', []),
                'deprecated': changes.get('deprecated', []),
                'modified': changes.get('modified', []),
                'renamed': changes.get('renamed', [])
            },
            'impact': self._calculate_change_impact(changes),
            'migration_notes': migration_notes,
            'compatibility': self._assess_compatibility(platform, old_version, new_version),
            'metadata': {
                'total_changes': sum(len(v) for v in changes.values()),
                'breaking_changes': len(changes.get('removed', [])) + len(changes.get('modified', [])),
                'created_by': 'MetricsVersionManager',
                'created_at': datetime.now().isoformat()
            }
        }
        
        return entry
    
    def _calculate_change_impact(self, changes: Dict[str, List[str]]) -> str:
        """Calcule l'impact des changements"""
        total_changes = sum(len(v) for v in changes.values())
        breaking_changes = len(changes.get('removed', [])) + len(changes.get('modified', []))
        
        if breaking_changes > 0:
            return 'MAJOR'  # Changements cassants
        elif len(changes.get('added', [])) > 5:
            return 'MINOR'  # Nouvelles fonctionnalités importantes
        elif total_changes > 0:
            return 'PATCH'  # Corrections mineures
        else:
            return 'NONE'
    
    def _assess_compatibility(self, platform: str, old_version: str, new_version: str) -> Dict[str, Any]:
        """Évalue la compatibilité entre versions"""
        compatibility_matrix = {
            'linkedin': {
                ('202309', '202312'): 'COMPATIBLE',
                ('202312', '202505'): 'MOSTLY_COMPATIBLE',
                # Versions trop différentes
                ('202309', '202505'): 'BREAKING_CHANGES'
            },
            'facebook': {
                ('v17.0', 'v18.0'): 'COMPATIBLE',
                ('v18.0', 'v19.0'): 'MOSTLY_COMPATIBLE',
                # Versions trop différentes  
                ('v17.0', 'v19.0'): 'BREAKING_CHANGES'
            }
        }
        
        platform_matrix = compatibility_matrix.get(platform, {})
        compatibility_level = platform_matrix.get((old_version, new_version), 'UNKNOWN')
        
        return {
            'level': compatibility_level,
            'requires_migration': compatibility_level in ['BREAKING_CHANGES', 'MOSTLY_COMPATIBLE'],
            'automatic_migration': compatibility_level == 'COMPATIBLE'
        }
    
    def save_changelog(self, changelog_entry: Dict[str, Any]) -> bool:
        """Sauvegarde une entrée dans le changelog avec validation"""
        try:
            platform = changelog_entry['platform']
            changelog_file = self.changelog_files.get(platform)
            
            if not changelog_file:
                logger.error(f"Plateforme {platform} non supportée")
                return False
            
            # Charger l'historique existant
            changelog_data = self._load_changelog(changelog_file)
            
            # Validation de l'entrée
            if not self._validate_changelog_entry(changelog_entry):
                logger.error("Entrée de changelog invalide")
                return False
            
            # Ajouter la nouvelle entrée
            changelog_data['entries'].append(changelog_entry)
            changelog_data['metadata']['last_updated'] = datetime.now().isoformat()
            changelog_data['metadata']['total_entries'] = len(changelog_data['entries'])
            
            # Sauvegarder
            with open(changelog_file, 'w', encoding='utf-8') as f:
                json.dump(changelog_data, f, indent=2, ensure_ascii=False)
            
            # Également sauvegarder dans le changelog combiné
            self._update_combined_changelog(changelog_entry)
            
            logger.info(f"Changelog sauvegardé pour {platform}: {changelog_file}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du changelog: {e}")
            return False
    
    def _load_changelog(self, filepath: Path) -> Dict[str, Any]:
        """Charge un fichier de changelog avec structure par défaut"""
        try:
            if filepath.exists():
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"Impossible de charger {filepath}: {e}")
        
        # Structure par défaut
        return {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'total_entries': 0
            },
            'entries': []
        }
    
    def _validate_changelog_entry(self, entry: Dict[str, Any]) -> bool:
        """Valide la structure d'une entrée de changelog"""
        required_fields = ['platform', 'version_change', 'date', 'changes']
        
        for field in required_fields:
            if field not in entry:
                logger.error(f"Champ requis manquant: {field}")
                return False
        
        # Validation des changements
        changes = entry.get('changes', {})
        if not isinstance(changes, dict):
            logger.error("Le champ 'changes' doit être un dictionnaire")
            return False
        
        return True
    
    def _update_combined_changelog(self, entry: Dict[str, Any]) -> None:
        """Met à jour le changelog combiné"""
        try:
            combined_file = self.changelog_files['combined']
            combined_data = self._load_changelog(combined_file)
            
            combined_data['entries'].append(entry)
            combined_data['metadata']['last_updated'] = datetime.now().isoformat()
            combined_data['metadata']['total_entries'] = len(combined_data['entries'])
            
            with open(combined_file, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erreur lors de la mise à jour du changelog combiné: {e}")
    
    def get_version_history(self, platform: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Récupère l'historique des versions pour une plateforme"""
        changelog_file = self.changelog_files.get(platform)
        if not changelog_file:
            return []
        
        changelog_data = self._load_changelog(changelog_file)
        entries = changelog_data.get('entries', [])
        
        # Trier par date (plus récent en premier) et limiter
        sorted_entries = sorted(entries, key=lambda x: x['date'], reverse=True)
        return sorted_entries[:limit]
    
    def check_migration_needed(self, platform: str, current_version: str, target_version: str) -> Dict[str, Any]:
        """Vérifie si une migration est nécessaire entre deux versions"""
        history = self.get_version_history(platform, limit=50)
        
        # Rechercher les changements entre les versions
        relevant_changes = []
        for entry in history:
            version_change = entry.get('version_change', {})
            if (version_change.get('from') == current_version and 
                version_change.get('to') == target_version):
                relevant_changes.append(entry)
        
        if not relevant_changes:
            return {
                'migration_needed': False,
                'reason': 'Aucun changement trouvé entre les versions',
                'changes': []
            }
        
        # Analyser l'impact des changements
        total_breaking_changes = 0
        all_changes = []
        
        for change in relevant_changes:
            metadata = change.get('metadata', {})
            total_breaking_changes += metadata.get('breaking_changes', 0)
            all_changes.extend(change.get('changes', {}).get('removed', []))
            all_changes.extend(change.get('changes', {}).get('modified', []))
        
        return {
            'migration_needed': total_breaking_changes > 0,
            'breaking_changes_count': total_breaking_changes,
            'affected_metrics': list(set(all_changes)),
            'migration_complexity': 'HIGH' if total_breaking_changes > 5 else 'MEDIUM' if total_breaking_changes > 0 else 'LOW',
            'changes': relevant_changes
        }
    
    def generate_migration_guide(self, platform: str, from_version: str, to_version: str) -> str:
        """Génère un guide de migration"""
        migration_info = self.check_migration_needed(platform, from_version, to_version)
        
        if not migration_info['migration_needed']:
            return f"Aucune migration nécessaire de {from_version} vers {to_version}"
        
        guide = f"""
# Guide de Migration {platform.title()}
## De la version {from_version} vers {to_version}

### Résumé
- Changements cassants: {migration_info['breaking_changes_count']}
- Complexité: {migration_info['migration_complexity']}
- Métriques affectées: {len(migration_info['affected_metrics'])}

### Métriques Impactées
"""
        
        for metric in migration_info['affected_metrics']:
            guide += f"- `{metric}`\n"
        
        guide += "\n### Actions Recommandées\n"
        
        if migration_info['migration_complexity'] == 'HIGH':
            guide += "1. Tester en environnement de développement\n"
            guide += "2. Mettre à jour les mappings de colonnes\n"
            guide += "3. Vérifier les connecteurs Looker Studio\n"
            guide += "4. Planifier une maintenance\n"
        else:
            guide += "1. Mettre à jour les mappings\n"
            guide += "2. Tester les connecteurs\n"
        
        return guide