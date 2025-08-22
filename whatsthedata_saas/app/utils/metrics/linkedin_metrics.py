"""
LinkedIn Metrics Configuration pour WhatsTheData
==============================================

Extrait de l'analyse des scripts LinkedIn pour créer une structure cohérente
des métriques LinkedIn disponibles dans l'API REST v202505.

Architecture modulaire synchronisée avec base_metrics.py et metrics_manager.py
"""

from typing import Dict, List, Any
from datetime import datetime
from .base_metrics import BaseMetrics

class LinkedInMetrics(BaseMetrics):
    """Gestionnaire des métriques LinkedIn héritant de BaseMetrics"""
    
    def __init__(self):
        super().__init__(platform_name="linkedin", api_version="202505")
        
        # Métriques extraites des scripts fournis
        self._page_metrics = [
            # Statistiques de page (vues et clics)
            "total_page_views",
            "unique_page_views", 
            "desktop_page_views",
            "mobile_page_views",
            "overview_page_views",
            "about_page_views",
            "people_page_views",
            "jobs_page_views",
            "careers_page_views", 
            "life_at_page_views",
            "desktop_button_clicks",
            "mobile_button_clicks",
            "total_button_clicks",
            
            # Statistiques followers lifetime 
            "total_followers",
            "followers_by_country",
            "followers_by_industry",
            "followers_by_function", 
            "followers_by_seniority",
            "followers_by_company_size",
            
            # Statistiques de partage agrégées
            "total_impressions",
            "total_unique_impressions",
            "total_clicks",
            "total_shares",
            "total_comments",
            "total_engagement_rate",
            "total_share_mentions",
            "total_comment_mentions"
        ]
        
        self._post_metrics = [
            # Métriques posts individuels 
            "post_impressions",
            "post_unique_impressions", 
            "post_clicks",
            "post_shares",
            "post_comments",
            "post_engagement_rate",
            "post_click_through_rate",
            "post_share_mentions",
            "post_comment_mentions",
            
            # Réactions LinkedIn détaillées
            "reactions_like",
            "reactions_celebrate", 
            "reactions_love",
            "reactions_insightful",
            "reactions_support",
            "reactions_funny",
            "total_reactions",
            
            # Pourcentages réactions
            "like_percentage",
            "celebrate_percentage",
            "love_percentage", 
            "insightful_percentage",
            "support_percentage",
            "funny_percentage",
            
            # Métriques calculées posts
            "total_interactions",
            "interaction_rate",
            "avg_reactions_per_post",
            
            # Followers quotidiens
            "organic_follower_gain",
            "paid_follower_gain", 
            "total_follower_gain"
        ]
        
        # Mapping complet API -> Affichage français
        self._column_mapping = {
            # Dimensions de base
            "platform": "Plateforme",
            "date": "Date",
            "account_id": "ID Organisation LinkedIn", 
            "account_name": "Nom Organisation LinkedIn",
            "content_type": "Type de Contenu LinkedIn",
            
            # Dimensions posts
            "post_id": "ID Post LinkedIn",
            "post_type": "Type de Publication LinkedIn",
            "post_subtype": "Sous-type Publication LinkedIn",
            "post_creation_date": "Date Publication LinkedIn",
            "post_text": "Texte Publication LinkedIn",
            "media_type": "Type Média LinkedIn",
            "media_url": "URL Média LinkedIn", 
            "is_reshare": "Est un Repost LinkedIn",
            "original_post": "Post Original LinkedIn",
            
            # Dimensions breakdown
            "breakdown_type": "Type Breakdown LinkedIn",
            "breakdown_value": "Valeur Breakdown LinkedIn",
            "breakdown_label": "Label Breakdown LinkedIn",
            
            # MÉTRIQUES PAGE
            "total_page_views": "LinkedIn - Vues Page Totales",
            "unique_page_views": "LinkedIn - Vues Page Uniques",
            "desktop_page_views": "LinkedIn - Vues Page Desktop", 
            "mobile_page_views": "LinkedIn - Vues Page Mobile",
            "overview_page_views": "LinkedIn - Vues Page Accueil",
            "about_page_views": "LinkedIn - Vues Page À Propos",
            "people_page_views": "LinkedIn - Vues Page Employés",
            "jobs_page_views": "LinkedIn - Vues Page Emplois",
            "careers_page_views": "LinkedIn - Vues Page Carrières",
            "life_at_page_views": "LinkedIn - Vues Page Vie Entreprise",
            "desktop_button_clicks": "LinkedIn - Clics Boutons Desktop",
            "mobile_button_clicks": "LinkedIn - Clics Boutons Mobile", 
            "total_button_clicks": "LinkedIn - Clics Boutons Total",
            
            # MÉTRIQUES FOLLOWERS
            "total_followers": "LinkedIn - Total Abonnés",
            "organic_follower_gain": "LinkedIn - Nouveaux Abonnés Organiques",
            "paid_follower_gain": "LinkedIn - Nouveaux Abonnés Payants",
            "total_follower_gain": "LinkedIn - Nouveaux Abonnés Total",
            "followers_by_country": "LinkedIn - Abonnés par Pays",
            "followers_by_industry": "LinkedIn - Abonnés par Industrie", 
            "followers_by_function": "LinkedIn - Abonnés par Fonction",
            "followers_by_seniority": "LinkedIn - Abonnés par Ancienneté",
            "followers_by_company_size": "LinkedIn - Abonnés par Taille Entreprise",
            
            # MÉTRIQUES PARTAGE/ENGAGEMENT
            "total_impressions": "LinkedIn - Affichages Totaux",
            "total_unique_impressions": "LinkedIn - Affichages Uniques Totaux",
            "post_impressions": "LinkedIn - Affichages Post",
            "post_unique_impressions": "LinkedIn - Affichages Uniques Post",
            "total_clicks": "LinkedIn - Clics Totaux",
            "post_clicks": "LinkedIn - Clics Post",
            "total_shares": "LinkedIn - Partages Totaux", 
            "post_shares": "LinkedIn - Partages Post",
            "total_comments": "LinkedIn - Commentaires Totaux",
            "post_comments": "LinkedIn - Commentaires Post",
            "total_engagement_rate": "LinkedIn - Taux Engagement Total",
            "post_engagement_rate": "LinkedIn - Taux Engagement Post",
            "post_click_through_rate": "LinkedIn - Taux de Clic Post",
            "total_share_mentions": "LinkedIn - Mentions Partage Totales",
            "total_comment_mentions": "LinkedIn - Mentions Commentaires Totales",
            "post_share_mentions": "LinkedIn - Mentions Partage Post",
            "post_comment_mentions": "LinkedIn - Mentions Commentaires Post",
            
            # MÉTRIQUES RÉACTIONS
            "reactions_like": "LinkedIn - Réactions J'aime",
            "reactions_celebrate": "LinkedIn - Réactions Bravo",
            "reactions_love": "LinkedIn - Réactions J'adore",
            "reactions_insightful": "LinkedIn - Réactions Instructif",
            "reactions_support": "LinkedIn - Réactions Soutien", 
            "reactions_funny": "LinkedIn - Réactions Amusant",
            "total_reactions": "LinkedIn - Total Réactions",
            "like_percentage": "LinkedIn - % J'aime",
            "celebrate_percentage": "LinkedIn - % Bravo",
            "love_percentage": "LinkedIn - % J'adore",
            "insightful_percentage": "LinkedIn - % Instructif",
            "support_percentage": "LinkedIn - % Soutien",
            "funny_percentage": "LinkedIn - % Amusant",
            
            # MÉTRIQUES CALCULÉES
            "total_interactions": "LinkedIn - Total Interactions",
            "post_interaction_rate": "LinkedIn - Taux Interaction Post",
            "avg_reactions_per_post": "LinkedIn - Réactions Moyennes par Post",
            "reach_rate": "LinkedIn - Taux de Portée"
        }
        
        # Formules des métriques calculées
        self._calculated_metrics = {
            "total_button_clicks": "desktop_button_clicks + mobile_button_clicks",
            "total_follower_gain": "organic_follower_gain + paid_follower_gain", 
            "total_reactions": "reactions_like + reactions_celebrate + reactions_love + reactions_insightful + reactions_support + reactions_funny",
            "total_interactions": "post_clicks + post_shares + post_comments + total_reactions",
            "post_click_through_rate": "post_clicks / post_impressions",
            "post_interaction_rate": "total_interactions / post_impressions",
            "like_percentage": "reactions_like / total_reactions",
            "celebrate_percentage": "reactions_celebrate / total_reactions",
            "love_percentage": "reactions_love / total_reactions", 
            "insightful_percentage": "reactions_insightful / total_reactions",
            "support_percentage": "reactions_support / total_reactions",
            "funny_percentage": "reactions_funny / total_reactions",
            "avg_reactions_per_post": "total_reactions / post_count",
            "reach_rate": "total_unique_impressions / total_followers"
        }
        
        # Métriques dépréciées (pour migration)
        self._deprecated_metrics = [
            # Anciennes métriques API v2 
            "ugc_post_impressions",
            "ugc_post_clicks",
            "legacy_share_statistics"
        ]
        
        # Mapping API REST vers champs BD
        self._api_field_mapping = {
            "total_followers": "firstDegreeSize",
            "organic_follower_gain": "organicFollowerGain", 
            "paid_follower_gain": "paidFollowerGain",
            "total_page_views": "allPageViews.pageViews",
            "unique_page_views": "allPageViews.uniquePageViews",
            "desktop_page_views": "allDesktopPageViews.pageViews",
            "mobile_page_views": "allMobilePageViews.pageViews",
            "overview_page_views": "overviewPageViews.pageViews",
            "about_page_views": "aboutPageViews.pageViews",
            "people_page_views": "peoplePageViews.pageViews",
            "jobs_page_views": "jobsPageViews.pageViews",
            "careers_page_views": "careersPageViews.pageViews",
            "life_at_page_views": "lifeAtPageViews.pageViews",
            "desktop_button_clicks": "desktopCustomButtonClickCounts",
            "mobile_button_clicks": "mobileCustomButtonClickCounts",
            "post_impressions": "impressionCount",
            "post_unique_impressions": "uniqueImpressionsCount",
            "post_clicks": "clickCount",
            "post_shares": "shareCount", 
            "post_comments": "commentCount",
            "post_engagement_rate": "engagement",
            "post_share_mentions": "shareMentionsCount",
            "post_comment_mentions": "commentMentionsCount",
            "reactions_like": "likeCount",
            "reactions_celebrate": "praiseCount",
            "reactions_love": "empathyCount",
            "reactions_insightful": "interestCount",
            "reactions_support": "appreciationCount",
            "reactions_funny": "entertainmentCount",
            "followers_by_country": "followerCountsByGeoCountry",
            "followers_by_industry": "followerCountsByIndustry",
            "followers_by_function": "followerCountsByFunction",
            "followers_by_seniority": "followerCountsBySeniority", 
            "followers_by_company_size": "followerCountsByStaffCountRange"
        }
    
    def get_page_metrics(self) -> List[str]:
        """Retourne les métriques de page LinkedIn"""
        return self._page_metrics
    
    def get_post_metrics(self) -> List[str]:
        """Retourne les métriques de posts LinkedIn"""
        return self._post_metrics
    
    def get_column_mapping(self) -> Dict[str, str]:
        """Retourne le mapping API -> Nom d'affichage"""
        return self._column_mapping
    
    def get_calculated_metrics(self) -> Dict[str, str]:
        """Retourne les formules des métriques calculées"""
        return self._calculated_metrics
    
    def get_deprecated_metrics(self) -> List[str]:
        """Retourne les métriques dépréciées"""
        return self._deprecated_metrics
    
    def get_api_field_mapping(self) -> Dict[str, str]:
        """Retourne le mapping vers les champs API REST"""
        return self._api_field_mapping
    
    def get_metrics_by_category(self, category: str) -> List[str]:
        """Retourne les métriques par catégorie"""
        categories = {
            'page_views': [
                'total_page_views', 'unique_page_views', 'desktop_page_views',
                'mobile_page_views', 'overview_page_views', 'about_page_views',
                'people_page_views', 'jobs_page_views', 'careers_page_views',
                'life_at_page_views'
            ],
            'followers': [
                'total_followers', 'organic_follower_gain', 'paid_follower_gain',
                'total_follower_gain', 'followers_by_country', 'followers_by_industry',
                'followers_by_function', 'followers_by_seniority', 'followers_by_company_size'
            ],
            'engagement': [
                'post_impressions', 'post_unique_impressions', 'post_clicks',
                'post_shares', 'post_comments', 'post_engagement_rate',
                'post_click_through_rate', 'total_interactions', 'post_interaction_rate'
            ],
            'reactions': [
                'reactions_like', 'reactions_celebrate', 'reactions_love',
                'reactions_insightful', 'reactions_support', 'reactions_funny',
                'total_reactions', 'like_percentage', 'celebrate_percentage',
                'love_percentage', 'insightful_percentage', 'support_percentage',
                'funny_percentage'
            ],
            'buttons': [
                'desktop_button_clicks', 'mobile_button_clicks', 'total_button_clicks'
            ]
        }
        return categories.get(category, [])
    
    def get_looker_schema(self) -> Dict[str, Any]:
        """Génère le schéma spécialisé LinkedIn pour Looker Studio"""
        base_schema = super().get_looker_schema()
        
        # Ajouter dimensions spécifiques LinkedIn
        linkedin_dimensions = [
            {'id': 'post_id', 'name': 'ID Post LinkedIn', 'type': 'TEXT'},
            {'id': 'post_type', 'name': 'Type de Publication LinkedIn', 'type': 'TEXT'},
            {'id': 'post_subtype', 'name': 'Sous-type Publication LinkedIn', 'type': 'TEXT'},
            {'id': 'post_creation_date', 'name': 'Date Publication LinkedIn', 'type': 'DATETIME'},
            {'id': 'post_text', 'name': 'Texte Publication LinkedIn', 'type': 'TEXT'},
            {'id': 'media_type', 'name': 'Type Média LinkedIn', 'type': 'TEXT'},
            {'id': 'media_url', 'name': 'URL Média LinkedIn', 'type': 'URL'},
            {'id': 'is_reshare', 'name': 'Est un Repost LinkedIn', 'type': 'BOOLEAN'},
            {'id': 'breakdown_type', 'name': 'Type Breakdown LinkedIn', 'type': 'TEXT'},
            {'id': 'breakdown_value', 'name': 'Valeur Breakdown LinkedIn', 'type': 'TEXT'},
            {'id': 'breakdown_label', 'name': 'Label Breakdown LinkedIn', 'type': 'TEXT'}
        ]
        
        base_schema['dimensions'].extend(linkedin_dimensions)
        
        # Marquer métriques calculées
        for metric in base_schema['metrics']:
            if metric['id'] in self._calculated_metrics:
                metric['is_calculated'] = True
                metric['formula'] = self._calculated_metrics[metric['id']]
        
        return base_schema
    
    def validate_api_compatibility(self, api_version: str = None) -> Dict[str, Any]:
        """Valide la compatibilité avec une version d'API"""
        current_api = api_version or self.api_version
        
        compatible_versions = ["202505", "202312", "202309"]
        is_compatible = current_api in compatible_versions
        
        warnings = []
        if current_api not in compatible_versions:
            warnings.append(f"Version API {current_api} non testée")
        
        if current_api < "202505":
            warnings.append("Certaines métriques peuvent ne pas être disponibles")
        
        return {
            'compatible': is_compatible,
            'current_version': self.api_version,
            'tested_versions': compatible_versions,
            'warnings': warnings
        }
    
    def export_for_connector(self, connector_type: str = "looker") -> Dict[str, Any]:
        """Exporte la configuration pour un connecteur spécifique"""
        if connector_type == "looker":
            return self.get_looker_schema()
        
        # Configuration générique
        return {
            'platform': self.platform_name,
            'api_version': self.api_version,
            'total_metrics': len(self.get_all_metrics()),
            'page_metrics_count': len(self._page_metrics),
            'post_metrics_count': len(self._post_metrics),
            'calculated_metrics_count': len(self._calculated_metrics),
            'deprecated_metrics_count': len(self._deprecated_metrics),
            'last_updated': self.last_updated.isoformat(),
            'all_metrics': self.get_all_metrics(),
            'column_mapping': self._column_mapping,
            'api_field_mapping': self._api_field_mapping
        }


# ========================================
# FONCTIONS UTILITAIRES SPÉCIFIQUES LINKEDIN
# ========================================

def get_linkedin_reaction_types() -> Dict[str, str]:
    """Retourne les types de réactions LinkedIn avec traduction"""
    return {
        'LIKE': 'J\'aime',
        'PRAISE': 'Bravo/Célébrer', 
        'EMPATHY': 'J\'adore',
        'INTEREST': 'Instructif',
        'APPRECIATION': 'Soutien',
        'ENTERTAINMENT': 'Amusant'
    }

def get_linkedin_post_types() -> Dict[str, str]:
    """Retourne les types de posts LinkedIn avec traduction"""
    return {
        'ugcPost': 'Publication native',
        'share': 'Partage de contenu',
        'post': 'Publication',
        'instantRepost': 'Repost instantané'
    }

def get_linkedin_media_types() -> Dict[str, str]:
    """Retourne les types de médias LinkedIn avec traduction"""
    return {
        'NONE': 'Texte uniquement',
        'IMAGE': 'Image',
        'VIDEO': 'Vidéo', 
        'DOCUMENT': 'Document',
        'ARTICLE': 'Article',
        'RICH_MEDIA': 'Média enrichi',
        'LINK': 'Lien partagé',
        'CAROUSEL': 'Carrousel d\'images',
        'POLL': 'Sondage'
    }

def get_linkedin_company_sizes() -> Dict[str, str]:
    """Retourne les tailles d'entreprises LinkedIn avec traduction"""
    return {
        'SIZE_1': '1 employé',
        'SIZE_2_TO_10': '2-10 employés',
        'SIZE_11_TO_50': '11-50 employés',
        'SIZE_51_TO_200': '51-200 employés',
        'SIZE_201_TO_500': '201-500 employés',
        'SIZE_501_TO_1000': '501-1000 employés',
        'SIZE_1001_TO_5000': '1001-5000 employés',
        'SIZE_5001_TO_10000': '5001-10000 employés',
        'SIZE_10001_OR_MORE': '10001+ employés'
    }


# ========================================
# EXAMPLE USAGE
# ========================================

if __name__ == "__main__":
    # Test de la classe LinkedIn
    linkedin = LinkedInMetrics()
    
    print(f"LinkedIn Metrics - API v{linkedin.api_version}")
    print(f"Total métriques: {len(linkedin.get_all_metrics())}")
    print(f"Métriques page: {len(linkedin.get_page_metrics())}")
    print(f"Métriques posts: {len(linkedin.get_post_metrics())}")
    print(f"Métriques calculées: {len(linkedin.get_calculated_metrics())}")
    
    # Test validation API
    validation = linkedin.validate_api_compatibility()
    print(f"Compatibilité API: {validation}")
    
    # Test export pour Looker
    looker_schema = linkedin.export_for_connector("looker")
    print(f"Schéma Looker: {len(looker_schema['metrics'])} métriques, {len(looker_schema['dimensions'])} dimensions")
    
    # Test métriques par catégorie
    reactions = linkedin.get_metrics_by_category('reactions')
    print(f"Métriques réactions: {reactions}")