"""
Configuration complète des métriques Facebook pour WhatsTheData
Extrait des scripts fb_page_metrics.py, fb_posts_lifetime.py et fb_posts_metadata.py
"""

# ========================================
# MÉTRIQUES PAGE FACEBOOK
# ========================================

PAGE_METRICS = [
    # Impressions Page
    "page_impressions", 
    "page_impressions_unique", 
    "page_impressions_nonviral", 
    "page_impressions_viral",
    
    # Impressions Posts de la Page
    "page_posts_impressions", 
    "page_posts_impressions_unique", 
    "page_posts_impressions_paid", 
    "page_posts_impressions_organic", 
    "page_posts_impressions_organic_unique",
    
    # Vues Page
    "page_views_total",

    # Fans et Abonnements
    "page_fans",
    "page_fan_adds", 
    "page_fan_removes",
    "page_fan_adds_by_paid_non_paid_unique",
    "page_follows",
    "page_daily_follows", 
    "page_daily_unfollows",
    "page_daily_follows_unique",

    # Métriques Vidéo Page
    "page_video_views", 
    "page_video_views_unique", 
    "page_video_views_paid", 
    "page_video_views_organic",  
    "page_video_repeat_views", 
    "page_video_view_time",
    "page_video_complete_views_30s", 
    "page_video_complete_views_30s_unique", 
    "page_video_complete_views_30s_paid", 
    "page_video_complete_views_30s_organic", 
    "page_video_complete_views_30s_autoplayed", 
    "page_video_complete_views_30s_repeat_views",

    # Engagement Page
    "page_post_engagements", 
    "page_total_actions", 
    "page_actions_post_reactions_like_total", 
    "page_actions_post_reactions_love_total", 
    "page_actions_post_reactions_wow_total", 
    "page_actions_post_reactions_haha_total", 
    "page_actions_post_reactions_sorry_total", 
    "page_actions_post_reactions_anger_total"
]

# ========================================
# MÉTRIQUES POSTS FACEBOOK (LIFETIME)
# ========================================

POST_METRICS = [
    # Impressions Posts
    "post_impressions", 
    "post_impressions_organic", 
    "post_impressions_paid",
    "post_impressions_viral", 
    "post_impressions_fan", 
    "post_impressions_nonviral",
    "post_impressions_unique", 
    "post_impressions_viral_unique",
    "post_impressions_organic_unique", 
    "post_impressions_paid_unique", 
    "post_impressions_nonviral_unique",
    
    # Réactions Posts
    "post_reactions_like_total", 
    "post_reactions_love_total", 
    "post_reactions_wow_total",
    "post_reactions_haha_total", 
    "post_reactions_sorry_total", 
    "post_reactions_anger_total",
    "post_reactions_by_type_total",
    
    # Clics et Engagement Posts
    "post_clicks",
    "post_clicks_by_type",
    "post_consumptions", 
    "post_fan_reach", 
    "post_activity_by_action_type",
    "post_activity_by_action_type_unique", 
    
    # Métriques Vidéo Posts
    "post_video_views", 
    "post_video_views_organic", 
    "post_video_views_paid",
    "post_video_views_unique", 
    "post_video_views_organic_unique",
    "post_video_views_paid_unique", 
    "post_video_views_sound_on",
    "post_video_complete_views_30s", 
    "post_video_avg_time_watched", 
    "post_video_view_time",
    "post_video_views_by_distribution_type", 
    "post_video_retention_graph",
    "post_video_followers", 
    "post_video_social_actions",
    "post_video_view_time_by_region_id"
]

# ========================================
# CHAMPS MÉTADONNÉES POSTS
# ========================================

POST_METADATA_FIELDS = [
    "id",
    "created_time",
    "status_type",
    "message",
    "permalink_url",
    "full_picture",
    "from",
    "story",
    "comments.summary(true)",
    "likes.summary(true)",
    "shares",
    "attachments{type,media_type,url,subattachments}"
]

# ========================================
# MAPPING DES NOMS DE COLONNES FRANÇAIS
# ========================================

FACEBOOK_COLUMN_MAPPING = {
    # Informations de base
    "date": "Date",
    "platform": "Plateforme",
    "account_name": "Nom du compte",
    "account_id": "ID du compte",
    "post_id": "ID publication",
    "created_time": "Date de publication",
    "status_type": "Type de publication",
    "message": "Message",
    "permalink_url": "Lien permanent",
    "full_picture": "Image",
    "media_url": "Lien média",
    "media_embedded": "URL média",
    "author_name": "Auteur",
    "author_id": "ID auteur",
    
    # Métriques Page - Impressions
    "page_impressions": "Affichages de la page",
    "page_impressions_unique": "Visiteurs de la page",
    "page_impressions_nonviral": "Affichages non viraux",
    "page_impressions_viral": "Affichages viraux",
    "page_posts_impressions": "Affichages des publications",
    "page_posts_impressions_unique": "Visiteurs de la publication",
    "page_posts_impressions_paid": "Affichages publicitaires",
    "page_posts_impressions_organic": "Affichages organiques",
    "page_posts_impressions_organic_unique": "Visiteurs uniques organiques",
    "page_views_total": "Vues totales de la page",
    
    # Métriques Page - Fans & Abonnés
    "page_fans": "Nbre de fans",
    "page_fan_adds": "Nouveaux fans",
    "page_fan_removes": "Fans perdus",
    "page_fan_adds_by_paid_non_paid_unique_total": "Total nouveaux fans (payants + organiques)",
    "page_fan_adds_by_paid_non_paid_unique_paid": "Nouveaux fans via pub",
    "page_fan_adds_by_paid_non_paid_unique_unpaid": "Nouveaux fans organiques",
    "page_follows": "Nbre d'abonnés",
    "page_daily_follows": "Nouveaux abonnés",
    "page_daily_unfollows": "Désabonnements",
    "page_daily_follows_unique": "Abonnés uniques du jour",
    
    # Métriques Page - Vidéo
    "page_video_views": "Vues de vidéos",
    "page_video_views_unique": "Vues uniques de vidéos",
    "page_video_views_paid": "Vues vidéos via pub",
    "page_video_views_organic": "Vues vidéos organiques",
    "page_video_repeat_views": "Relectures vidéos",
    "page_video_view_time": "Temps de visionnage (sec)",
    "page_video_complete_views_30s": "Vues complètes (30s)",
    "page_video_complete_views_30s_unique": "Vues complètes uniques (30s)",
    "page_video_complete_views_30s_paid": "Vues complètes via pub (30s)",
    "page_video_complete_views_30s_organic": "Vues complètes organiques (30s)",
    "page_video_complete_views_30s_autoplayed": "Vues complètes auto (30s)",
    "page_video_complete_views_30s_repeat_views": "Relectures complètes (30s)",
    
    # Métriques Page - Engagement
    "page_post_engagements": "Interactions sur publications",
    "page_total_actions": "Actions totales",
    "page_actions_post_reactions_like_total": "Nbre de \"J'aime\"",
    "page_actions_post_reactions_love_total": "Nbre de \"J'adore\"",
    "page_actions_post_reactions_wow_total": "Nbre de \"Wow\"",
    "page_actions_post_reactions_haha_total": "Nbre de \"Haha\"",
    "page_actions_post_reactions_sorry_total": "Nbre de \"Triste\"",
    "page_actions_post_reactions_anger_total": "Nbre de \"En colère\"",
    
    # Métriques Post - Impressions
    "post_impressions": "Affichages publication",
    "post_impressions_organic": "Affichages organiques",
    "post_impressions_paid": "Affichages sponsorisés",
    "post_impressions_viral": "Affichages viraux",
    "post_impressions_fan": "Affichages par fans",
    "post_impressions_nonviral": "Affichages non viraux",
    "post_impressions_unique": "Visiteurs de la publication",
    "post_impressions_organic_unique": "Visiteurs organiques",
    "post_impressions_paid_unique": "Visiteurs via pub",
    "post_impressions_viral_unique": "Visiteurs viraux",
    "post_impressions_nonviral_unique": "Visiteurs non viraux",
    
    # Métriques Post - Réactions
    "post_reactions_like_total": "Nbre de \"J'aime\"",
    "post_reactions_love_total": "Nbre de \"J'adore\"",
    "post_reactions_wow_total": "Nbre de \"Wow\"",
    "post_reactions_haha_total": "Nbre de \"Haha\"",
    "post_reactions_sorry_total": "Nbre de \"Triste\"",
    "post_reactions_anger_total": "Nbre de \"En colère\"",
    "post_reactions_by_type_total_like": "Réactions J'aime",
    "post_reactions_by_type_total_love": "Réactions J'adore",
    
    # Métriques Post - Clics & Engagement
    "post_clicks": "Nbre de clics",
    "post_clicks_by_type_other clicks": "Autres clics",
    "post_clicks_by_type_link clicks": "Clics sur liens",
    "post_clicks_by_type_photo view": "Clics sur photos",
    "post_consumptions": "Interactions totales",
    "post_fan_reach": "Portée fans",
    "post_activity_by_action_type_share": "Partages",
    "post_activity_by_action_type_like": "J'aime sur activité",
    "post_activity_by_action_type_comment": "Nbre de commentaires",
    "post_activity_by_action_type_unique_share": "Partages uniques",
    "post_activity_by_action_type_unique_like": "J'aime uniques",
    "post_activity_by_action_type_unique_comment": "Commentaires uniques",
    
    # Métriques Post - Vidéo
    "post_video_views": "Vues vidéo",
    "post_video_views_organic": "Vues vidéo organiques",
    "post_video_views_paid": "Vues vidéo sponsorisées",
    "post_video_views_unique": "Visiteurs vidéo uniques",
    "post_video_views_organic_unique": "Visiteurs vidéo organiques",
    "post_video_views_paid_unique": "Visiteurs vidéo sponsorisés",
    "post_video_views_sound_on": "Vues avec son",
    "post_video_complete_views_30s": "Vues complètes (30s)",
    "post_video_avg_time_watched": "Temps moyen visionné",
    "post_video_view_time": "Durée totale visionnage",
    "post_video_views_by_distribution_type_page_owned": "Vues sur la page",
    "post_video_views_by_distribution_type_shared": "Vues via partages",
    "post_video_followers": "Nouveaux abonnés vidéo",
    "post_video_social_actions": "Interactions vidéo",
    
    # Métadonnées Post
    "comments_count": "Nbre de commentaires",
    "likes_count": "Nbre de J'aime",
    "shares_count": "Nbre de partages",
    
    # Métriques Calculées
    "taux_engagement_page": "Tx d'engagement (%)",
    "frequence_impressions": "Fréquence des affichages",
    "actions_totales_calculees": "Actions calculées",
    "vtr_percentage_page": "VTR %",
    "taux_de_clic": "Tx de clic (%)",
    "taux_engagement_complet": "Tx d'engagement (%)",
    "reactions_positives": "Réactions positives",
    "reactions_negatives": "Réactions négatives",
    "total_reactions": "Total réactions",
    
    # Métriques génériques
    "total_followers": "Total Followers",
    "new_followers": "Nouveaux Followers",
    "followers_lost": "Followers Perdus",
    "page_impressions": "Impressions Page",
    "total_engagement": "Engagement Total",
    "engagement_rate": "Taux d'Engagement (%)",
    "likes": "Likes",
    "comments": "Commentaires",
    "shares": "Partages",
    "clicks": "Clics",
    "click_through_rate": "Taux de Clic (%)"
}

# ========================================
# MÉTRIQUES CALCULÉES FACEBOOK
# ========================================

FACEBOOK_CALCULATED_METRICS = {
    # Page
    "taux_engagement_page": "page_post_engagements / page_impressions",
    "frequence_impressions": "page_impressions / page_impressions_unique",
    "actions_totales_calculees": "page_total_actions + sum(reactions)",
    "vtr_percentage_page": "page_video_complete_views_30s / page_impressions",
    
    # Posts
    "taux_de_clic": "post_clicks / post_impressions",
    "taux_engagement_complet": "(reactions + post_clicks) / post_impressions",
    "reactions_positives": "like + love + wow + haha",
    "reactions_negatives": "sorry + anger",
    "total_reactions": "sum(all_reactions)"
}

# ========================================
# FONCTIONS UTILITAIRES
# ========================================

def get_all_facebook_metrics():
    """Retourne toutes les métriques Facebook disponibles"""
    return PAGE_METRICS + POST_METRICS

def get_facebook_column_mapping():
    """Retourne le mapping complet des colonnes Facebook"""
    return FACEBOOK_COLUMN_MAPPING

def get_facebook_page_metrics():
    """Retourne uniquement les métriques de page Facebook"""
    return PAGE_METRICS

def get_facebook_post_metrics():
    """Retourne uniquement les métriques de posts Facebook"""
    return POST_METRICS

def get_facebook_metadata_fields():
    """Retourne les champs de métadonnées des posts"""
    return POST_METADATA_FIELDS

def get_facebook_calculated_metrics():
    """Retourne les métriques calculées Facebook"""
    return FACEBOOK_CALCULATED_METRICS

# ========================================
# TYPES DE PUBLICATION FACEBOOK
# ========================================

FACEBOOK_POST_TYPES = {
    "shared_story": "Contenu partagé",
    "added_video": "Vidéo publiée",
    "mobile_status_update": "Statut publié depuis mobile",
    "added_photos": "Photo publiée",
    "status_update": "Statut texte",
    "note": "Note",
    "event": "Événement",
    "offer": "Offre commerciale",
    "link": "Lien externe",
    "photo": "Photo publiée",
    "video": "Vidéo publiée",
    "album": "Album photo",
    "cover_photo": "Photo de couverture mise à jour",
    "profile_picture": "Photo de profil mise à jour",
    "created_event": "Événement créé",
    "created_note": "Note publiée",
    "tagged_in_photo": "Identification sur une photo",
    "app_created_story": "Publication créée par une application",
    "approved_friend": "Nouvel ami accepté",
    "wall_post": "Publication sur le mur",
    "timeline_cover_photo": "Photo de couverture mise à jour",
    "timeline_profile_picture": "Photo de profil mise à jour",
    "created_group": "Groupe créé",
    "tagged": "Identification",
    "application": "Publication d'application"
}

# ========================================
# CONFIGURATION POUR LOOKER STUDIO
# ========================================

def get_facebook_looker_schema():
    """
    Retourne le schéma complet pour Looker Studio avec toutes les métriques Facebook
    """
    schema = {
        'dimensions': [
            {'id': 'platform', 'name': 'Plateforme', 'type': 'TEXT'},
            {'id': 'date', 'name': 'Date', 'type': 'DATE'},
            {'id': 'account_name', 'name': 'Nom du compte', 'type': 'TEXT'},
            {'id': 'account_id', 'name': 'ID du compte', 'type': 'TEXT'},
            {'id': 'post_id', 'name': 'ID publication', 'type': 'TEXT'},
            {'id': 'status_type', 'name': 'Type de publication', 'type': 'TEXT'},
            {'id': 'message', 'name': 'Message', 'type': 'TEXT'},
            {'id': 'permalink_url', 'name': 'Lien permanent', 'type': 'URL'},
            {'id': 'author_name', 'name': 'Auteur', 'type': 'TEXT'}
        ],
        'metrics': []
    }
    
    # Ajouter toutes les métriques comme metrics
    for api_name, display_name in FACEBOOK_COLUMN_MAPPING.items():
        if api_name not in ['date', 'platform', 'account_name', 'account_id', 'post_id', 'status_type', 'message', 'permalink_url', 'author_name']:
            metric_type = 'PERCENT' if any(keyword in display_name.lower() for keyword in ['%', 'tx', 'taux']) else 'NUMBER'
            schema['metrics'].append({
                'id': api_name,
                'name': display_name,
                'type': metric_type
            })
    
    return schema

# ========================================
# EXPORT PRINCIPAL
# ========================================

# Toutes les métriques Facebook (102 métriques au total)
ALL_FACEBOOK_METRICS = PAGE_METRICS + POST_METRICS
TOTAL_FACEBOOK_METRICS_COUNT = len(ALL_FACEBOOK_METRICS)

print(f"Configuration Facebook chargée: {TOTAL_FACEBOOK_METRICS_COUNT} métriques disponibles")