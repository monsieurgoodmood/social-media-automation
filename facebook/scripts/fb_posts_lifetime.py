"""
Script adapté pour collecter les métriques lifetime des posts Facebook dans Cloud Functions
Fait partie du système d'automatisation Facebook sur Google Cloud
"""
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import json

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scripts.fb_base_collector import FacebookBaseCollector
except ImportError:
    # Pour les tests locaux
    from fb_base_collector import FacebookBaseCollector

logger = logging.getLogger(__name__)

# Liste complète des métriques lifetime des posts
METRICS = [
    "post_impressions", "post_reactions_like_total", "post_video_views", "post_clicks",
    "post_impressions_organic", "post_reactions_love_total", "post_reactions_wow_total",
    "post_video_complete_views_30s", "post_video_avg_time_watched", "post_video_views_sound_on",
    "post_reactions_haha_total", "post_reactions_sorry_total", "post_reactions_anger_total",
    "post_consumptions", "post_video_views_unique", "post_video_views_organic_unique",
    "post_video_view_time", "post_impressions_paid", "post_impressions_viral", "post_video_views_paid",
    "post_video_views_paid_unique", "post_video_views_by_distribution_type", "post_video_retention_graph",
    "post_reactions_by_type_total", "post_fan_reach", "post_video_views_organic", "post_clicks_by_type",
    "post_impressions_fan", "post_impressions_nonviral", "post_impressions_unique", "post_impressions_viral_unique",
    "post_impressions_organic_unique", "post_impressions_paid_unique", "post_activity_by_action_type",
    "post_activity_by_action_type_unique", "post_video_followers", "post_video_social_actions",
    "post_video_view_time_by_region_id", "post_impressions_nonviral", "post_impressions_nonviral_unique"
]

def adjust_column_types(df):
    """
    Ajuste les types des colonnes selon leur nature :
    - Dates en datetime
    - Métriques numériques en int ou float selon le cas
    - URLs et textes en string
    """
    # Colonnes de type datetime
    datetime_columns = ["created_time"]
    
    # Colonnes qui doivent être des entiers
    integer_columns = [
        "post_impressions", "post_impressions_organic", "post_impressions_paid",
        "post_impressions_viral", "post_impressions_fan", "post_impressions_nonviral",
        "post_impressions_unique", "post_impressions_organic_unique",
        "post_impressions_paid_unique", "post_impressions_viral_unique",
        "post_reactions_like_total", "post_reactions_love_total",
        "post_reactions_wow_total", "post_reactions_haha_total",
        "post_reactions_sorry_total", "post_reactions_anger_total",
        "post_reactions_by_type_total_like", "post_reactions_by_type_total_love",
        "post_clicks", "post_clicks_by_type_other clicks",
        "post_clicks_by_type_link clicks", "post_clicks_by_type_photo view",
        "post_video_views", "post_video_views_organic", "post_video_views_paid",
        "post_video_views_unique", "post_video_views_organic_unique",
        "post_video_views_paid_unique", "post_video_views_sound_on",
        "post_video_complete_views_30s", "post_video_followers",
        "post_video_social_actions", "post_consumptions",
        "post_impressions_nonviral_unique"
    ]
    
    # Colonnes qui doivent être des nombres décimaux
    float_columns = [
        "post_video_avg_time_watched",
        "post_video_view_time"
    ]
    
    # Colonnes qui doivent être des chaînes de caractères
    string_columns = [
        "post_id", "message", "media_url", "media_embedded",
        "post_video_views_by_distribution_type_page_owned",
        "post_video_views_by_distribution_type_shared"
    ]

    try:
        # Traitement des dates
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")


        # Traitement des entiers
        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        # Traitement des nombres décimaux
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0.0).astype(float)

        # Traitement des chaînes de caractères
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)

        # Pour toutes les autres colonnes non explicitement définies
        remaining_columns = [col for col in df.columns if col not in 
                           datetime_columns + integer_columns + 
                           float_columns + string_columns]
        
        for col in remaining_columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                if all(df[col].dropna() % 1 == 0):
                    df[col] = df[col].fillna(0).astype(int)
                else:
                    df[col] = df[col].fillna(0).astype(float)
            else:
                df[col] = df[col].fillna('').astype(str)

        return df

    except Exception as e:
        logger.error(f"Erreur lors de l'ajustement des types de colonnes: {str(e)}")
        raise

def add_calculated_metrics(df):
    """
    Ajoute les métriques calculées au DataFrame de manière optimisée
    """
    # Calculer toutes les métriques en une fois pour éviter la fragmentation
    calculated_metrics = pd.DataFrame(index=df.index)
    
    # Taux de clic - en décimal pour le format pourcentage
    calculated_metrics['taux_de_clic'] = df.apply(
        lambda row: (row.get('post_clicks', 0) / row.get('post_impressions', 1)) 
        if row.get('post_impressions', 0) > 0 else 0, 
        axis=1
    )
    
    # (J'aime + J'adore + Wow + Haha + Triste + En colère + Clics) / Impressions
    calculated_metrics['taux_engagement_complet'] = df.apply(
        lambda row: (
            row.get('post_reactions_like_total', 0) +
            row.get('post_reactions_love_total', 0) +
            row.get('post_reactions_wow_total', 0) +
            row.get('post_reactions_haha_total', 0) +
            row.get('post_reactions_sorry_total', 0) +
            row.get('post_reactions_anger_total', 0) +
            row.get('post_clicks', 0)
        ) / row.get('post_impressions', 1)
        if row.get('post_impressions', 0) > 0 else 0,
        axis=1
    )
    
    # Réactions positives
    calculated_metrics['reactions_positives'] = (
        df.get('post_reactions_like_total', 0) + 
        df.get('post_reactions_love_total', 0) + 
        df.get('post_reactions_wow_total', 0) + 
        df.get('post_reactions_haha_total', 0)
    )
    
    # Réactions négatives
    calculated_metrics['reactions_negatives'] = (
        df.get('post_reactions_sorry_total', 0) + 
        df.get('post_reactions_anger_total', 0)
    )
    
    # Total des réactions
    calculated_metrics['total_reactions'] = (
        df.get('post_reactions_like_total', 0) + 
        df.get('post_reactions_love_total', 0) + 
        df.get('post_reactions_wow_total', 0) + 
        df.get('post_reactions_haha_total', 0) + 
        df.get('post_reactions_sorry_total', 0) + 
        df.get('post_reactions_anger_total', 0)
    )
    
    # Concaténer toutes les métriques calculées en une fois
    return pd.concat([df, calculated_metrics], axis=1)

def rename_columns(df):
    """
    Renomme les colonnes du DataFrame - VERSION CORRIGÉE pour cohérence
    """
    # D'abord ajouter les métriques calculées
    df = add_calculated_metrics(df)
    
    column_mapping = {
        # Informations de base
        "created_time": "Date de publication",
        "post_id": "ID publication",
        "media_embedded": "URL média",
        "media_url": "Lien média",
        "message": "Message",
        
        # Métriques d'impressions
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
        
        # Réactions
        "post_reactions_like_total": "Nbre de \"J'aime\"",
        "post_reactions_love_total": "Nbre de \"J'adore\"",
        "post_reactions_wow_total": "Nbre de \"Wow\"",
        "post_reactions_haha_total": "Nbre de \"Haha\"",
        "post_reactions_sorry_total": "Nbre de \"Triste\"",
        "post_reactions_anger_total": "Nbre de \"En colère\"",
        "post_reactions_by_type_total_like": "Réactions J'aime",
        "post_reactions_by_type_total_love": "Réactions J'adore",
        
        # Clics - 🔥 CORRECTION CRITIQUE
        "post_clicks": "Nbre de clics",  # ✅ ÉTAIT "Clics totaux"
        "post_clicks_by_type_other clicks": "Autres clics",
        "post_clicks_by_type_link clicks": "Clics sur liens",
        "post_clicks_by_type_photo view": "Clics sur photos",
        
        # Métriques vidéo
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
        
        # Autres métriques
        "post_fan_reach": "Portée fans",
        "post_activity_by_action_type_share": "Partages",
        "post_activity_by_action_type_like": "J'aime sur activité",
        "post_activity_by_action_type_unique_share": "Partages uniques",
        "post_activity_by_action_type_unique_like": "J'aime uniques",
        "post_consumptions": "Interactions totales",
        
        # 🔥 CORRECTIONS CRITIQUES pour les commentaires:
        "post_activity_by_action_type_comment": "Nbre de commentaires",  # ✅ COHÉRENT
        "post_activity_by_action_type_unique_comment": "Commentaires uniques",  # ➕ AJOUTÉ
        
        # Métriques calculées
        "taux_de_clic": "Tx de clic (%)",
        "taux_engagement_complet": "Tx d'engagement (%)",
        "reactions_positives": "Réactions positives",
        "reactions_negatives": "Réactions négatives",
        "total_reactions": "Total réactions"
    }
    
    return df.rename(columns=column_mapping)


def reorder_columns(df):
    """
    Réorganise les colonnes selon l'ordre spécifié
    """
    main_columns = [
        "created_time", "post_id", "media_embedded", "media_url", "message",
        "post_impressions", "post_impressions_organic", "post_impressions_paid", "post_impressions_viral",
        "post_impressions_fan", "post_impressions_nonviral", "post_impressions_unique",
        "post_impressions_organic_unique", "post_impressions_paid_unique", "post_impressions_viral_unique",
        "post_reactions_like_total", "post_reactions_love_total", "post_reactions_wow_total",
        "post_reactions_haha_total", "post_reactions_sorry_total", "post_reactions_anger_total",
        "post_reactions_by_type_total_like", "post_reactions_by_type_total_love",
        "post_clicks", "post_clicks_by_type_other clicks", "post_clicks_by_type_link clicks",
        "post_clicks_by_type_photo view",
        "post_video_views", "post_video_views_organic", "post_video_views_paid", "post_video_views_unique",
        "post_video_views_organic_unique", "post_video_views_paid_unique", "post_video_views_sound_on",
        "post_video_complete_views_30s", "post_video_avg_time_watched",
        "post_video_views_by_distribution_type_page_owned", "post_video_views_by_distribution_type_shared",
        "post_fan_reach", "post_video_view_time",
        "post_activity_by_action_type_share", "post_activity_by_action_type_like",
        "post_activity_by_action_type_unique_share", "post_activity_by_action_type_unique_like",
        "post_video_followers", "post_video_social_actions", "post_consumptions",
        "post_impressions_nonviral_unique",
        # Ajouter les métriques calculées à la fin
        "taux_de_clic", "taux_engagement_complet", "reactions_positives", "reactions_negatives", "total_reactions"
    ]
    
    # Garder les colonnes dans l'ordre spécifié si elles existent
    ordered_columns = [col for col in main_columns if col in df.columns]
    
    # Ajouter les colonnes restantes à la fin
    other_columns = [col for col in df.columns if col not in main_columns]
    
    return df[ordered_columns + other_columns]



def retry_with_backoff(func, max_retries=3, initial_delay=2):
    """Wrapper pour réessayer une fonction avec un délai exponentiel"""
    def wrapper(*args, **kwargs):
        delay = initial_delay
        last_exception = None
        
        for retry in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if "An unknown error has occurred" in str(e):
                    logger.warning(f"Tentative {retry + 1}/{max_retries} échouée. Nouvelle tentative dans {delay} secondes...")
                    time.sleep(delay)
                    delay *= 2  # Backoff exponentiel
                else:
                    raise  # Si ce n'est pas une erreur temporaire, on la propage
        
        raise last_exception
    
    return wrapper

def clean_dataframe(df):
    """
    Nettoie le DataFrame en supprimant les colonnes inutiles
    """
    # Liste des colonnes à supprimer (commence par ces préfixes)
    columns_to_remove_prefixes = [
        'post_video_view_time_by_region_id_',
        'post_video_retention_graph_',
        'post_video_view_time_by_age_bucket_and_gender_',
        'post_video_view_time_by_country_id_'
    ]
    
    # Identifier toutes les colonnes à supprimer
    columns_to_drop = []
    for col in df.columns:
        for prefix in columns_to_remove_prefixes:
            if col.startswith(prefix):
                columns_to_drop.append(col)
                break
    
    # Supprimer les colonnes
    if columns_to_drop:
        logger.info(f"Suppression de {len(columns_to_drop)} colonnes de données régionales/démographiques détaillées")
        df = df.drop(columns=columns_to_drop, errors='ignore')
    
    return df

class FacebookPostsCollector:
    """
    Collecteur spécialisé pour les métriques lifetime des posts
    """
    def __init__(self, page_token, page_id):
        self.page_token = page_token
        self.page_id = page_id
        self.api_version = "v21.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

    def get_posts(self, since_date):
        """Récupère tous les posts depuis une date donnée"""
        url = f"{self.base_url}/{self.page_id}/posts"
        params = {
            "access_token": self.page_token,
            "fields": "id,created_time,message",
            "since": since_date,
            "limit": 100
        }
        
        all_posts = []
        try:
            while url:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                
                if "error" in data:
                    logger.error(f"Erreur API Facebook: {data['error']}")
                    raise ValueError(f"Erreur API Facebook: {data['error'].get('message')}")
                
                if "data" in data:
                    all_posts.extend(data["data"])
                    logger.info(f"Récupéré {len(data['data'])} posts")
                
                url = data.get("paging", {}).get("next")
                if url:
                    params = {}
                    time.sleep(1)
            
            return all_posts
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des posts: {e}")
            raise

    @retry_with_backoff
    def get_metrics(self, post_id):
        """Récupère toutes les métriques pour un post avec retry en cas d'erreur"""
        metrics_data = {
            "post_id": post_id,
            "media_url": None,
            "media_embedded": None
        }

        try:
            # Récupérer les métriques
            response = requests.get(
                f"{self.base_url}/{post_id}/insights",
                params={
                    "access_token": self.page_token,
                    "metric": ",".join(METRICS),
                    "period": "lifetime"
                }
            ).json()

            if "error" in response:
                logger.error(f"Erreur API metrics: {response['error']}")
                raise ValueError(f"Erreur API metrics: {response['error'].get('message')}")

            if "data" in response:
                for metric in response["data"]:
                    if metric["values"]:
                        value = metric["values"][0]["value"]
                        metric_name = metric["name"]
                        
                        # Filtrer les métriques régionales détaillées
                        if metric_name == "post_video_view_time_by_region_id":
                            # On ne traite pas cette métrique
                            continue
                        elif metric_name == "post_video_retention_graph":
                            # On ne traite pas cette métrique non plus
                            continue
                        elif isinstance(value, dict):
                            # Pour les autres métriques dict, on garde seulement les clés principales
                            for k, v in value.items():
                                # Filtrer les clés qui sont des IDs de région ou des données trop détaillées
                                if not k.isdigit() and not k.startswith('U.'):
                                    metrics_data[f"{metric_name}_{k}"] = v
                        else:
                            metrics_data[metric_name] = value

            # Récupérer les détails du média
            time.sleep(0.5)  # Petit délai entre les requêtes
            details = requests.get(
                f"{self.base_url}/{post_id}",
                params={
                    "access_token": self.page_token,
                    "fields": "attachments"
                }
            ).json()

            if "attachments" in details:
                attachments = details["attachments"].get("data", [])
                if attachments:
                    media = attachments[0].get("media", {})
                    if "image" in media:
                        url = media["image"]["src"]
                        metrics_data["media_url"] = url
                        metrics_data["media_embedded"] = url
                    elif "source" in media:
                        url = media["source"]
                        metrics_data["media_url"] = url
                        metrics_data["media_embedded"] = f"[VIDEO] {url}"

            return metrics_data

        except Exception as e:
            logger.error(f"Erreur lors de la récupération des métriques pour {post_id}: {e}")
            raise


def get_page_token(user_token, page_id):
    """
    Récupère le token spécifique à une page Facebook
    Version améliorée qui utilise d'abord les tokens sauvegardés
    """
    import json  # Import local pour s'assurer qu'il est disponible
    import os
    from datetime import datetime
    
    # D'abord essayer de récupérer le token depuis la configuration sauvegardée
    try:
        # Essayer avec ConfigManager si disponible (Cloud Functions)
        try:
            from utils.config_manager import ConfigManager
            config_manager = ConfigManager()
            tokens_config = config_manager.load_config("page_tokens.json")
            
            if tokens_config and page_id in tokens_config.get("tokens", {}):
                saved_token = tokens_config["tokens"][page_id].get("access_token")
                if saved_token:
                    logger.info(f"Token trouvé dans la configuration pour la page {page_id}")
                    return saved_token
        except ImportError:
            # En local, charger depuis le fichier
            if os.path.exists("configs/page_tokens.json"):
                with open("configs/page_tokens.json", 'r') as f:
                    tokens_config = json.load(f)
                    
                if page_id in tokens_config.get("tokens", {}):
                    saved_token = tokens_config["tokens"][page_id].get("access_token")
                    if saved_token:
                        logger.info(f"Token trouvé dans le fichier local pour la page {page_id}")
                        return saved_token
    except Exception as e:
        logger.warning(f"Impossible de charger les tokens sauvegardés: {e}")
    
    # Si pas de token sauvegardé, utiliser l'API Facebook
    logger.info(f"Récupération du token via l'API pour la page {page_id}")
    url = f"https://graph.facebook.com/v21.0/me/accounts"
    params = {"access_token": user_token}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if "error" in data:
            raise Exception(f"Erreur API: {data['error'].get('message')}")
        
        for page in data.get("data", []):
            if page["id"] == page_id:
                page_token = page["access_token"]
                
                # Sauvegarder ce token pour les prochaines utilisations
                try:
                    # Essayer de sauvegarder avec ConfigManager
                    try:
                        from utils.config_manager import ConfigManager
                        config_manager = ConfigManager()
                        tokens_config = config_manager.load_config("page_tokens.json") or {"tokens": {}}
                        
                        tokens_config["tokens"][page_id] = {
                            "page_name": page.get("name", "Unknown"),
                            "access_token": page_token
                        }
                        tokens_config["last_updated"] = datetime.now().isoformat()
                        
                        config_manager.save_config("page_tokens.json", tokens_config)
                        logger.info(f"Token sauvegardé pour la page {page_id}")
                    except ImportError:
                        # En local
                        os.makedirs("configs", exist_ok=True)
                        tokens_file = "configs/page_tokens.json"
                        
                        if os.path.exists(tokens_file):
                            with open(tokens_file, 'r') as f:
                                tokens_config = json.load(f)
                        else:
                            tokens_config = {"tokens": {}}
                        
                        tokens_config["tokens"][page_id] = {
                            "page_name": page.get("name", "Unknown"),
                            "access_token": page_token
                        }
                        tokens_config["last_updated"] = datetime.now().isoformat()
                        
                        with open(tokens_file, 'w') as f:
                            json.dump(tokens_config, f, indent=2)
                        logger.info(f"Token sauvegardé localement pour la page {page_id}")
                except Exception as e:
                    logger.warning(f"Impossible de sauvegarder le token: {e}")
                
                return page_token
        
        raise Exception(f"Page {page_id} non trouvée ou non autorisée")
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du token de page: {e}")
        raise

# Modifier process_posts_lifetime pour utiliser clean_dataframe
def process_posts_lifetime(token, page_id, page_name):
    """
    Fonction principale pour traiter les métriques lifetime des posts Facebook
    Compatible avec Cloud Functions
    
    Args:
        token: Token utilisateur Facebook
        page_id: ID de la page Facebook
        page_name: Nom de la page Facebook
        
    Returns:
        spreadsheet_id: ID du Google Sheet créé/mis à jour
    """
    try:
        logger.info(f"Début du traitement des posts lifetime pour {page_name} ({page_id})")
        
        # Récupérer le token de la page
        page_token = get_page_token(token, page_id)
        
        # Initialiser les collecteurs
        collector = FacebookPostsCollector(page_token, page_id)
        base_collector = FacebookBaseCollector(page_token)
        
        # Obtenir ou créer le spreadsheet
        spreadsheet_id = base_collector.get_or_update_spreadsheet(
            page_name, page_id, "posts_lifetime"
        )
        
        # Récupérer les posts des 24 derniers mois
        since_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        posts = collector.get_posts(since_date)
        
        logger.info(f"Nombre de posts à traiter: {len(posts)}")
        
        # Collecter les métriques pour chaque post
        all_metrics = []
        errors = 0
        
        for i, post in enumerate(posts, 1):
            try:
                if i % 10 == 0:
                    logger.info(f"Progression: {i}/{len(posts)} posts traités")
                
                metrics = collector.get_metrics(post["id"])
                metrics.update({
                    "created_time": post.get("created_time"),
                    "message": post.get("message", "")
                })
                
                # VÉRIFICATION FINALE : S'assurer qu'il y a toujours un message
                if not metrics.get("message") or metrics["message"] == "" or metrics["message"].strip() == "":
                    # Construire un message par défaut avec les informations disponibles
                    post_date = post.get("created_time", "")
                    if post_date:
                        try:
                            # Formater la date pour qu'elle soit lisible
                            date_obj = pd.to_datetime(post_date)
                            formatted_date = date_obj.strftime("%d/%m/%Y à %H:%M")
                            metrics["message"] = f"[Publication du {formatted_date}]"
                        except:
                            metrics["message"] = f"[Publication - ID: {post.get('id', 'inconnu')}]"
                    else:
                        # Utiliser l'ID du post comme dernier recours
                        metrics["message"] = f"[Publication - ID: {post.get('id', 'inconnu')}]"
                
                all_metrics.append(metrics)
                
                # Respecter les limites de l'API
                time.sleep(2)
                
                # Pause plus longue tous les 50 posts
                if i % 50 == 0:
                    logger.info("Pause de 30 secondes pour respecter les limites d'API...")
                    time.sleep(30)
                    
            except Exception as e:
                logger.error(f"Erreur post {post['id']}: {e}")
                errors += 1
                continue
        
        if errors > 0:
            logger.warning(f"{errors} posts n'ont pas pu être traités")
        
        if all_metrics:
            # Créer et formater le DataFrame
            df = pd.DataFrame(all_metrics)
            
            # IMPORTANT: Nettoyer les colonnes inutiles AVANT tout autre traitement
            df = clean_dataframe(df)
            
            # Ajuster les types de colonnes
            df = adjust_column_types(df)
            
            # Réorganiser les colonnes
            df = reorder_columns(df)
            
            # Renommer les colonnes (inclut l'ajout des métriques calculées)
            df = rename_columns(df)
            
            # Mettre à jour le Google Sheet
            base_collector.update_sheet_data(spreadsheet_id, df)
            
            logger.info(f"✓ Posts lifetime mis à jour pour {page_name}: {len(df)} posts")
        else:
            logger.warning(f"Aucune métrique de posts collectée pour {page_name}")
        
        return spreadsheet_id
        
    except Exception as e:
        logger.error(f"Erreur dans process_posts_lifetime pour {page_name}: {e}")
        raise

# Pour les tests locaux uniquement
if __name__ == "__main__":
    # Ce bloc ne s'exécute que lors des tests locaux
    import sys
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Configuration de logging pour les tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) != 4:
        print("Usage: python fb_posts_lifetime.py <token> <page_id> <page_name>")
        sys.exit(1)
    
    test_token = sys.argv[1]
    test_page_id = sys.argv[2]
    test_page_name = sys.argv[3]
    
    try:
        spreadsheet_id = process_posts_lifetime(test_token, test_page_id, test_page_name)
        print(f"✓ Test réussi! Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    except Exception as e:
        print(f"✗ Erreur lors du test: {e}")
        sys.exit(1)