# ========================================
# app/utils/metrics/metrics_manager.py
# ========================================

from typing import Dict, List, Any, Optional, Union
from .facebook_metrics import FacebookMetrics
from .linkedin_metrics import LinkedInMetrics
from .version_manager import MetricsVersionManager
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class MetricsManager:
    """Gestionnaire centralisé optimisé pour LinkedIn et Facebook"""
    
    def __init__(self):
        self.platforms = {
            'facebook': FacebookMetrics(),
            'linkedin': LinkedInMetrics()
        }
        self.version_manager = MetricsVersionManager()
        self._schema_cache = {}
        self._validation_cache = {}
    
    def get_platform_metrics(self, platform: str) -> Optional[object]:
        """Retourne l'objet métriques d'une plateforme avec validation"""
        platform_key = platform.lower()
        
        if platform_key not in self.platforms:
            logger.warning(f"Plateforme {platform} non supportée. Plateformes disponibles: {list(self.platforms.keys())}")
            return None
        
        return self.platforms[platform_key]
    
    def get_all_metrics(self, platform: str) -> List[str]:
        """Retourne toutes les métriques d'une plateforme"""
        platform_obj = self.get_platform_metrics(platform)
        return platform_obj.get_all_metrics() if platform_obj else []
    
    def get_metrics_by_category(self, platform: str, category: str) -> List[str]:
        """Retourne les métriques par catégorie"""
        platform_obj = self.get_platform_metrics(platform)
        if not platform_obj:
            return []
        
        return platform_obj.get_metrics_by_category(category)
    
    def get_looker_schema(self, platforms: Union[str, List[str]], force_refresh: bool = False) -> Dict[str, Any]:
        """Génère un schéma Looker combiné optimisé avec cache intelligent"""
        
        # Normaliser l'entrée
        if isinstance(platforms, str):
            platforms = [platforms]
        
        platforms = [p.lower() for p in platforms]
        cache_key = ','.join(sorted(platforms))
        
        # Utiliser le cache si disponible et pas de force refresh
        if not force_refresh and cache_key in self._schema_cache:
            cached_schema = self._schema_cache[cache_key]
            # Vérifier si le cache n'est pas trop ancien (1 heure)
            cache_age = datetime.now() - datetime.fromisoformat(cached_schema.get('cached_at', '1970-01-01'))
            if cache_age.total_seconds() < 3600:  # 1 heure
                logger.debug(f"Utilisation du cache pour {cache_key}")
                return cached_schema
        
        # Construire le schéma
        combined_schema = self._build_combined_schema(platforms)
        
        # Mettre en cache
        combined_schema['cached_at'] = datetime.now().isoformat()
        self._schema_cache[cache_key] = combined_schema
        
        return combined_schema
    
    def _build_combined_schema(self, platforms: List[str]) -> Dict[str, Any]:
        """Construit un schéma combiné optimisé"""
        combined_schema = {
            'platforms': platforms,
            'dimensions': [],
            'metrics': [],
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_platforms': len(platforms),
                'platform_versions': {},
                'conflicts_resolved': 0,
                'total_metrics': 0,
                'total_dimensions': 0
            }
        }
        
        all_dimensions = {}  # Utiliser dict pour éviter doublons
        all_metrics = {}
        conflicts_count = 0
        
        for platform in platforms:
            platform_obj = self.get_platform_metrics(platform)
            if not platform_obj:
                logger.warning(f"Plateforme {platform} ignorée")
                continue
            
            schema = platform_obj.get_looker_schema()
            combined_schema['metadata']['platform_versions'][platform] = schema['api_version']
            
            # Traiter les dimensions
            for dim in schema['dimensions']:
                dim_id = dim['id']
                if dim_id not in all_dimensions:
                    all_dimensions[dim_id] = dim
                else:
                    # Dimension existe déjà, garder la plus complète
                    existing = all_dimensions[dim_id]
                    if len(dim.get('description', '')) > len(existing.get('description', '')):
                        all_dimensions[dim_id] = dim
            
            # Traiter les métriques avec résolution intelligente des conflits
            for metric in schema['metrics']:
                metric_id = metric['id']
                original_id = metric_id
                
                if metric_id in all_metrics:
                    # Conflit détecté
                    conflicts_count += 1
                    
                    # Stratégies de résolution de conflit
                    if self._are_metrics_equivalent(all_metrics[metric_id], metric):
                        # Métriques équivalentes, on garde la première
                        continue
                    else:
                        # Métriques différentes, préfixer par plateforme
                        metric_id = f"{platform}_{original_id}"
                        metric['id'] = metric_id
                        
                        # Mettre à jour le nom pour clarifier
                        if not metric['name'].startswith(platform.title()):
                            metric['name'] = f"{platform.title()} - {metric['name']}"
                        
                        metric['original_id'] = original_id
                        metric['conflict_resolved'] = True
                
                all_metrics[metric_id] = metric
        
        # Finaliser le schéma
        combined_schema['dimensions'] = list(all_dimensions.values())
        combined_schema['metrics'] = list(all_metrics.values())
        
        # Mettre à jour les métadonnées
        combined_schema['metadata'].update({
            'conflicts_resolved': conflicts_count,
            'total_metrics': len(all_metrics),
            'total_dimensions': len(all_dimensions)
        })
        
        # Trier pour une présentation cohérente
        combined_schema['dimensions'].sort(key=lambda x: x['id'])
        combined_schema['metrics'].sort(key=lambda x: (x.get('platform', ''), x['id']))
        
        return combined_schema
    
    def _are_metrics_equivalent(self, metric1: Dict[str, Any], metric2: Dict[str, Any]) -> bool:
        """Détermine si deux métriques sont équivalentes"""
        # Critères d'équivalence
        return (
            metric1['type'] == metric2['type'] and
            metric1.get('aggregation') == metric2.get('aggregation') and
            metric1.get('is_calculated', False) == metric2.get('is_calculated', False)
        )
    
    def validate_metrics(self, platform: str, metrics_list: List[str]) -> Dict[str, Any]:
        """Valide une liste de métriques avec cache et analyse détaillée"""
        
        platform_obj = self.get_platform_metrics(platform)
        if not platform_obj:
            return {
                'valid': False,
                'error': f'Plateforme {platform} non supportée',
                'supported_platforms': list(self.platforms.keys())
            }
        
        # Utiliser le cache si disponible
        cache_key = f"{platform}:{','.join(sorted(metrics_list))}"
        if cache_key in self._validation_cache:
            return self._validation_cache[cache_key]
        
        # Effectuer la validation
        available_metrics = set(platform_obj.get_all_metrics())
        deprecated_metrics = set(platform_obj.get_deprecated_metrics())
        calculated_metrics = set(platform_obj.get_calculated_metrics().keys())
        
        valid_metrics = [m for m in metrics_list if m in available_metrics]
        invalid_metrics = [m for m in metrics_list if m not in available_metrics]
        deprecated_found = [m for m in metrics_list if m in deprecated_metrics]
        calculated_found = [m for m in metrics_list if m in calculated_metrics]
        
        # Analyse par catégorie
        category_analysis = {}
        for metric in valid_metrics:
            for category in ['page_views', 'followers', 'engagement', 'reactions', 'buttons']:
                category_metrics = platform_obj.get_metrics_by_category(category)
                if metric in category_metrics:
                    if category not in category_analysis:
                        category_analysis[category] = []
                    category_analysis[category].append(metric)
        
        result = {
            'valid': len(invalid_metrics) == 0,
            'platform': platform,
            'validation_timestamp': datetime.now().isoformat(),
            'summary': {
                'total_requested': len(metrics_list),
                'total_valid': len(valid_metrics),
                'total_invalid': len(invalid_metrics),
                'total_deprecated': len(deprecated_found),
                'total_calculated': len(calculated_found)
            },
            'metrics': {
                'valid': valid_metrics,
                'invalid': invalid_metrics,
                'deprecated': deprecated_found,
                'calculated': calculated_found
            },
            'category_analysis': category_analysis,
            'recommendations': self._generate_validation_recommendations(
                platform, invalid_metrics, deprecated_found, available_metrics
            )
        }
        
        # Mettre en cache
        self._validation_cache[cache_key] = result
        
        return result
    
    def _generate_validation_recommendations(
        self, 
        platform: str, 
        invalid_metrics: List[str], 
        deprecated_metrics: List[str],
        available_metrics: set
    ) -> List[str]:
        """Génère des recommandations pour la validation"""
        recommendations = []
        
        if deprecated_metrics:
            recommendations.append(
                f"Remplacer les {len(deprecated_metrics)} métriques dépréciées par leurs équivalents modernes"
            )
        
        if invalid_metrics:
            # Suggérer des métriques similaires
            suggestions = []
            for invalid in invalid_metrics:
                similar = self._find_similar_metrics(invalid, available_metrics)
                if similar:
                    suggestions.append(f"'{invalid}' → '{similar[0]}'")
            
            if suggestions:
                recommendations.append(f"Métriques suggérées: {', '.join(suggestions[:3])}")
        
        return recommendations
    
    def _find_similar_metrics(self, target: str, available: set, max_results: int = 3) -> List[str]:
        """Trouve des métriques similaires basées sur la distance de chaîne"""
        import difflib
        
        # Utiliser difflib pour trouver les correspondances les plus proches
        close_matches = difflib.get_close_matches(
            target, 
            list(available), 
            n=max_results, 
            cutoff=0.6
        )
        
        return close_matches
    
    def export_metrics_config(self, filepath: str = None, include_metadata: bool = True) -> Dict[str, Any]:
        """Exporte la configuration complète avec métadonnées enrichies"""
        config = {
            'version': '2.0.0',
            'generated_at': datetime.now().isoformat(),
            'platforms': {},
            'summary': {
                'total_platforms': len(self.platforms),
                'total_metrics': 0,
                'total_calculated_metrics': 0,
                'total_deprecated_metrics': 0
            }
        }
        
        if include_metadata:
            config['metadata'] = {
                'manager_version': '2.0.0',
                'supported_platforms': list(self.platforms.keys()),
                'cache_size': len(self._schema_cache),
                'validation_cache_size': len(self._validation_cache)
            }
        
        total_metrics = 0
        total_calculated = 0
        total_deprecated = 0
        
        for platform_name, platform_obj in self.platforms.items():
            platform_summary = platform_obj.get_metrics_summary()
            
            config['platforms'][platform_name] = {
                'api_version': platform_obj.api_version,
                'last_updated': platform_obj.last_updated.isoformat(),
                'metrics': {
                    'page_metrics': platform_obj.get_page_metrics(),
                    'post_metrics': platform_obj.get_post_metrics(),
                    'calculated_metrics': platform_obj.get_calculated_metrics(),
                    'deprecated_metrics': platform_obj.get_deprecated_metrics()
                },
                'mappings': {
                    'column_mapping': platform_obj.get_column_mapping(),
                    'api_field_mapping': platform_obj.get_api_field_mapping()
                },
                'summary': platform_summary,
                'compatibility': platform_obj.validate_api_compatibility()
            }
            
            total_metrics += platform_summary['total_metrics']
            total_calculated += platform_summary['calculated_metrics']
            total_deprecated += platform_summary['deprecated_metrics']
        
        # Mettre à jour le résumé global
        config['summary'].update({
            'total_metrics': total_metrics,
            'total_calculated_metrics': total_calculated,
            'total_deprecated_metrics': total_deprecated
        })
        
        # Sauvegarder si un chemin est fourni
        if filepath:
            try:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=2, ensure_ascii=False)
                logger.info(f"Configuration exportée vers {filepath}")
            except Exception as e:
                logger.error(f"Erreur lors de l'export vers {filepath}: {e}")
        
        return config
    
    def clear_all_caches(self) -> None:
        """Vide tous les caches du manager et des plateformes"""
        self._schema_cache.clear()
        self._validation_cache.clear()
        
        for platform_obj in self.platforms.values():
            platform_obj.clear_cache()
        
        logger.info("Tous les caches ont été vidés")
    
    def get_platform_comparison(self) -> Dict[str, Any]:
        """Compare les plateformes disponibles"""
        comparison = {
            'platforms': list(self.platforms.keys()),
            'comparison_matrix': {},
            'common_metrics': [],
            'unique_metrics': {},
            'generated_at': datetime.now().isoformat()
        }
        
        # Récupérer toutes les métriques par plateforme
        platform_metrics = {}
        for platform_name, platform_obj in self.platforms.items():
            platform_metrics[platform_name] = set(platform_obj.get_all_metrics())
        
        # Trouver les métriques communes
        if len(platform_metrics) > 1:
            common_metrics = set.intersection(*platform_metrics.values())
            comparison['common_metrics'] = sorted(list(common_metrics))
        
        # Trouver les métriques uniques par plateforme
        for platform_name, metrics in platform_metrics.items():
            other_metrics = set()
            for other_platform, other_platform_metrics in platform_metrics.items():
                if other_platform != platform_name:
                    other_metrics.update(other_platform_metrics)
            
            unique_metrics = metrics - other_metrics
            comparison['unique_metrics'][platform_name] = sorted(list(unique_metrics))
        
        # Matrice de comparaison
        for platform1, metrics1 in platform_metrics.items():
            comparison['comparison_matrix'][platform1] = {}
            for platform2, metrics2 in platform_metrics.items():
                if platform1 != platform2:
                    intersection = len(metrics1.intersection(metrics2))
                    union = len(metrics1.union(metrics2))
                    similarity = intersection / union if union > 0 else 0
                    
                    comparison['comparison_matrix'][platform1][platform2] = {
                        'common_metrics': intersection,
                        'similarity_score': round(similarity, 3),
                        'total_combined': union
                    }
        
        return comparison


# ========================================
# EXEMPLE D'UTILISATION OPTIMISÉ
# ========================================

if __name__ == "__main__":
    # Initialiser le gestionnaire optimisé
    manager = MetricsManager()
    
    print("=== GESTIONNAIRE DE MÉTRIQUES OPTIMISÉ ===\n")
    
    # Statistiques générales
    for platform in ['linkedin', 'facebook']:
        metrics = manager.get_all_metrics(platform)
        print(f"{platform.title()}: {len(metrics)} métriques disponibles")
    
    print("\n=== SCHÉMA LOOKER COMBINÉ ===")
    # Générer schéma pour les deux plateformes
    schema = manager.get_looker_schema(['facebook', 'linkedin'])
    print(f"Schéma généré: {schema['metadata']['total_metrics']} métriques, {schema['metadata']['total_dimensions']} dimensions")
    print(f"Conflits résolus: {schema['metadata']['conflicts_resolved']}")
    
    print("\n=== VALIDATION DES MÉTRIQUES ===")
    # Tester la validation
    test_metrics = ['page_fans', 'total_followers', 'invalid_metric', 'post_impressions']
    validation_fb = manager.validate_metrics('facebook', test_metrics)
    validation_li = manager.validate_metrics('linkedin', test_metrics)
    
    print(f"Facebook - Valides: {validation_fb['summary']['total_valid']}/{validation_fb['summary']['total_requested']}")
    print(f"LinkedIn - Valides: {validation_li['summary']['total_valid']}/{validation_li['summary']['total_requested']}")
    
    print("\n=== COMPARAISON DES PLATEFORMES ===")
    comparison = manager.get_platform_comparison()
    print(f"Métriques communes: {len(comparison['common_metrics'])}")
    for platform, unique in comparison['unique_metrics'].items():
        print(f"Métriques uniques {platform}: {len(unique)}")
    
    print("\n=== EXPORT DE CONFIGURATION ===")
    # Exporter la configuration
    config = manager.export_metrics_config('metrics_config_optimized.json')
    print(f"Configuration exportée: {config['summary']['total_metrics']} métriques totales")
    
    print("\n=== GESTION DES VERSIONS ===")
    # Test du gestionnaire de versions
    version_manager = manager.version_manager
    
    # Créer un exemple de changelog
    changes = {
        'added': ['new_video_metric', 'enhanced_engagement_rate'],
        'deprecated': ['old_click_metric'],
        'modified': ['page_impressions']
    }
    
    changelog_entry = version_manager.create_changelog_entry(
        platform='linkedin',
        old_version='202312',
        new_version='202505',
        changes=changes,
        migration_notes="Migration majeure avec nouvelles métriques vidéo"
    )
    
    print(f"Changelog créé - Impact: {changelog_entry['impact']}")
    print(f"Migration requise: {changelog_entry['compatibility']['requires_migration']}")
    
    print("\n=== PERFORMANCE ===")
    # Test de performance du cache
    import time
    
    start_time = time.time()
    schema1 = manager.get_looker_schema(['facebook', 'linkedin'])
    first_call_time = time.time() - start_time
    
    start_time = time.time()
    schema2 = manager.get_looker_schema(['facebook', 'linkedin'])  # Depuis le cache
    cached_call_time = time.time() - start_time
    
    print(f"Premier appel: {first_call_time:.4f}s")
    print(f"Appel depuis cache: {cached_call_time:.4f}s")
    print(f"Amélioration: {first_call_time/cached_call_time:.1f}x plus rapide")