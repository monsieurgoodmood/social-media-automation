"""
Script adapté pour collecter les métadonnées des posts Facebook dans Cloud Functions
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

def adjust_column_types(df):
    """
    Ajuste les types des colonnes selon leur nature pour le metadata post.
    - Dates en datetime
    - Métriques (compteurs) en int
    - URLs et textes en string
    """
    try:
        # Colonnes de type datetime
        if "created_time" in df.columns:
            df["created_time"] = pd.to_datetime(df["created_time"], errors='coerce')

        # Colonnes qui doivent être des entiers (compteurs)
        integer_columns = [
            "comments_count",
            "likes_count", 
            "shares_count"
        ]
        for col in integer_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Colonnes de type texte
        string_columns = [
            "post_id",
            "status_type",
            "message",
            "permalink_url",
            "full_picture",
            "author_name",
            "author_id"
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)

        # Pour toute autre colonne non définie explicitement
        remaining_columns = [col for col in df.columns 
                           if col not in integer_columns + string_columns + ["created_time"]]
        
        for col in remaining_columns:
            # Si la colonne contient des nombres
            if pd.api.types.is_numeric_dtype(df[col]):
                # Si tous les nombres sont des entiers
                if df[col].dropna().apply(lambda x: float(x).is_integer()).all():
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
            else:
                df[col] = df[col].fillna('').astype(str)

        return df

    except Exception as e:
        logger.error(f"Erreur lors de l'ajustement des types de colonnes: {str(e)}")
        raise

def rename_columns(df):
    """
    Renomme les colonnes du DataFrame pour une meilleure compréhension.
    Utilise des noms simplifiés et plus courts.
    """
    column_mapping = {
        "post_id": "ID publication",
        "created_time": "Date de publication",
        "status_type": "Type de publication",
        "message": "Message",
        "permalink_url": "Lien permanent",
        "full_picture": "Image",
        "author_name": "Auteur",
        "author_id": "ID auteur",
        "comments_count": "Nbre de commentaires",  # Simplifié avec Nbre
        "likes_count": "Nbre de J'aime",  # Simplifié avec Nbre
        "shares_count": "Nbre de partages"  # Simplifié avec Nbre
    }
    return df.rename(columns=column_mapping)


def traduire_type_publication(type_publication, post_data=None):
    """
    Traduit les types de publication Facebook en français et fournit des détails clairs.
    Analyse les données du post pour déterminer le type réel quand le type est vide ou ambigu.
    
    Args:
        type_publication (str): Type de publication original de Facebook
        post_data (dict): Données complètes du post pour extraire plus d'informations
        
    Returns:
        str: Type de publication traduit et détaillé en français
    """
    # Traitement des valeurs vides, "no data" ou manquantes en analysant le contenu
    if not type_publication or type_publication == "" or type_publication.lower() == "no data" or type_publication.lower() == "none":
        # Vérifier les attachements s'ils existent
        if post_data and 'attachments' in post_data and 'data' in post_data['attachments']:
            for attachment in post_data['attachments']['data']:
                attachment_type = attachment.get('type', '').lower()
                media_type = attachment.get('media_type', '').lower()
                
                if attachment_type == 'video_inline' or media_type == 'video':
                    return "Vidéo publiée"
                elif attachment_type == 'photo' or media_type == 'photo':
                    if len(post_data['attachments']['data']) > 1:
                        return f"Album de {len(post_data['attachments']['data'])} photos"
                    return "Photo publiée"
                elif attachment_type == 'share':
                    return "Contenu partagé"
                elif attachment_type == 'link' or attachment.get('url'):
                    return "Lien externe"
                elif attachment_type == 'album':
                    return "Album photo"
                elif attachment_type == 'video':
                    return "Vidéo publiée"
        
        # Vérifier si le message contient une image
        if post_data and post_data.get('full_picture'):
            # Vérifier si c'est une vidéo via l'URL
            picture_url = post_data.get('full_picture', '')
            if 'video' in picture_url.lower() or '.mp4' in picture_url.lower():
                return "Vidéo publiée"
            return "Photo publiée"
        
        # Vérifier si c'est un partage de post
        if post_data and (post_data.get('shares') or 'sharedposts' in post_data):
            return "Contenu partagé"
        
        # Vérifier la story
        if post_data and post_data.get('story'):
            story = post_data['story'].lower()
            if 'shared' in story or 'partagé' in story:
                return "Contenu partagé"
            elif 'photo' in story:
                return "Photo publiée"
            elif 'video' in story or 'vidéo' in story:
                return "Vidéo publiée"
            elif 'event' in story or 'événement' in story:
                return "Événement"
            elif 'cover' in story:
                return "Photo de couverture mise à jour"
            elif 'profile' in story:
                return "Photo de profil mise à jour"
            else:
                return "Publication"
            
        # Par défaut, si le post a un message texte
        if post_data and post_data.get('message'):
            return "Statut texte"
        
        # Si on n'a vraiment aucune information
        return "Publication"  # Retour par défaut
    
    # Mapping détaillé des types courants
    type_mapping = {
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
    
    # Cas spécial pour "mobile_status_update" - déterminer le contenu réel
    if type_publication.lower() == "mobile_status_update":
        if post_data:
            # Vérifier d'abord les attachements
            if 'attachments' in post_data and 'data' in post_data['attachments']:
                for attachment in post_data['attachments'].get('data', []):
                    attachment_type = attachment.get('type', '').lower()
                    media_type = attachment.get('media_type', '').lower()
                    
                    if attachment_type == 'video_inline' or media_type == 'video':
                        return "Vidéo publiée depuis mobile"
                    elif attachment_type == 'photo' or media_type == 'photo':
                        if len(post_data['attachments']['data']) > 1:
                            return f"Album de {len(post_data['attachments']['data'])} photos depuis mobile"
                        return "Photo publiée depuis mobile"
                    elif attachment_type == 'share':
                        return "Contenu partagé depuis mobile"
                    elif attachment_type == 'link' or attachment.get('url'):
                        return "Lien partagé depuis mobile"
            
            # Ensuite vérifier full_picture
            if post_data.get('full_picture'):
                return "Photo publiée depuis mobile"
            
            # Par défaut pour mobile_status_update avec message
            if post_data.get('message'):
                return "Statut texte publié depuis mobile"
            
            return "Publication depuis mobile"
    
    # Traduire le type ou retourner une description générique
    type_traduit = type_mapping.get(type_publication.lower())
    
    if type_traduit:
        return type_traduit
    else:
        # Pour les types non mappés, essayer d'être descriptif
        type_clean = type_publication.replace('_', ' ').title()
        if len(type_clean) > 50:  # Si le type est trop long
            return "Publication"
        return f"Publication ({type_clean})"

class FacebookMetadataCollector:
    """
    Collecteur spécialisé pour les métadonnées des posts Facebook
    """
    def __init__(self, page_token, page_id):
        self.page_token = page_token
        self.page_id = page_id
        self.api_version = "v21.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"

    def get_posts(self, since_date):
        """Récupère tous les posts avec leurs métadonnées depuis une date donnée"""
        url = f"{self.base_url}/{self.page_id}/posts"
        params = {
            "access_token": self.page_token,
            # Utiliser attachments avec la syntaxe spécifique qui fonctionne encore
            "fields": "id,created_time,status_type,message,permalink_url,full_picture,from,comments.summary(true),likes.summary(true),shares,story,attachments{type,media_type,url,subattachments}",
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

    def normalize_post_data(self, post):
        """Normalise les données d'un post"""
        # Récupération du type traduit avec le contexte du post
        status_type = traduire_type_publication(post.get("status_type"), post)
        
        # Récupérer le message
        message = post.get("message", "")
        
        # VÉRIFICATION : S'assurer qu'il y a toujours un message
        if not message or message == "" or message.strip() == "":
            # Construire un message par défaut avec les informations disponibles
            post_date = post.get("created_time", "")
            if post_date:
                try:
                    # Formater la date pour qu'elle soit lisible
                    date_obj = pd.to_datetime(post_date)
                    formatted_date = date_obj.strftime("%d/%m/%Y à %H:%M")
                    message = f"[Publication du {formatted_date}]"
                except:
                    message = f"[Publication - ID: {post.get('id', 'inconnu')}]"
            else:
                # Utiliser l'ID du post comme dernier recours
                message = f"[Publication - ID: {post.get('id', 'inconnu')}]"
        
        return {
            "post_id": post.get("id"),
            "created_time": post.get("created_time"),
            "status_type": status_type,  # Type traduit
            "message": message,  # Message avec gestion des vides
            "permalink_url": post.get("permalink_url"),
            "full_picture": post.get("full_picture"),
            "author_name": post.get("from", {}).get("name"),
            "author_id": post.get("from", {}).get("id"),
            "comments_count": post.get("comments", {}).get("summary", {}).get("total_count", 0),
            "likes_count": post.get("likes", {}).get("summary", {}).get("total_count", 0),
            "shares_count": post.get("shares", {}).get("count", 0)
        }

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
    
    

def process_posts_metadata(token, page_id, page_name):
    """
    Fonction principale pour traiter les métadonnées des posts Facebook
    Compatible avec Cloud Functions
    
    Args:
        token: Token utilisateur Facebook
        page_id: ID de la page Facebook
        page_name: Nom de la page Facebook
        
    Returns:
        spreadsheet_id: ID du Google Sheet créé/mis à jour
    """
    try:
        logger.info(f"Début du traitement des métadonnées pour {page_name} ({page_id})")
        
        # Récupérer le token de la page
        page_token = get_page_token(token, page_id)
        
        # Initialiser les collecteurs
        collector = FacebookMetadataCollector(page_token, page_id)
        base_collector = FacebookBaseCollector(page_token)
        
        # Obtenir ou créer le spreadsheet
        spreadsheet_id = base_collector.get_or_update_spreadsheet(
            page_name, page_id, "posts_metadata"
        )
        
        # Récupérer les posts des 24 derniers mois
        since_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        logger.info(f"Récupération des posts depuis le {since_date}")
        
        posts = collector.get_posts(since_date)
        logger.info(f"Nombre total de posts récupérés: {len(posts)}")
        
        if posts:
            # Normaliser les données de tous les posts
            all_metrics = []
            for post in posts:
                try:
                    normalized_data = collector.normalize_post_data(post)
                    all_metrics.append(normalized_data)
                except Exception as e:
                    logger.error(f"Erreur lors de la normalisation du post {post.get('id')}: {e}")
                    continue
            
            if all_metrics:
                # Créer et formater le DataFrame
                df = pd.DataFrame(all_metrics)
                df = adjust_column_types(df)
                df = rename_columns(df)
                
                # Trier par date de création (plus récent en premier)
                df = df.sort_values(by="Date de publication", ascending=False)
                
                # Mettre à jour le Google Sheet
                base_collector.update_sheet_data(spreadsheet_id, df)
                
                logger.info(f"✓ Métadonnées mises à jour pour {page_name}: {len(df)} posts")
                
                # Afficher un résumé des types de posts
                if "Type de publication" in df.columns:
                    type_summary = df["Type de publication"].value_counts()
                    logger.info(f"Résumé des types de posts:\n{type_summary}")
            else:
                logger.warning(f"Aucune métadonnée normalisée pour {page_name}")
        else:
            logger.warning(f"Aucun post trouvé pour {page_name}")
        
        return spreadsheet_id
        
    except Exception as e:
        logger.error(f"Erreur dans process_posts_metadata pour {page_name}: {e}")
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
        print("Usage: python fb_posts_metadata.py <token> <page_id> <page_name>")
        sys.exit(1)
    
    test_token = sys.argv[1]
    test_page_id = sys.argv[2]
    test_page_name = sys.argv[3]
    
    try:
        spreadsheet_id = process_posts_metadata(test_token, test_page_id, test_page_name)
        print(f"✓ Test réussi! Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    except Exception as e:
        print(f"✗ Erreur lors du test: {e}")
        sys.exit(1)