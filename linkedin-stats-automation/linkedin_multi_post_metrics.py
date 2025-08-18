#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Post Metrics Tracker - VERSION OPTIMISÉE
Ce script collecte les métriques détaillées des posts LinkedIn pour plusieurs organisations
et les enregistre dans Google Sheets.
"""

import os
import sys
import traceback
import requests
import urllib.parse
import json
import random
from datetime import datetime, timedelta
import time
from collections import defaultdict
from pathlib import Path
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import concurrent.futures
import threading

# Chargement des variables d'environnement
load_dotenv()

# CACHE GLOBAL POUR ÉVITER LES APPELS RÉPÉTITIFS
METRICS_CACHE = {}
CACHE_LOCK = threading.Lock()

# AJOUTEZ LA FONCTION ICI
def ensure_percentage_as_decimal(value):
    """
    Convertit une valeur en décimal pour Google Sheets PERCENT
    
    Args:
        value: La valeur à convertir (peut être 5 pour 5% ou 0.05 pour 5%)
    
    Returns:
        float: Valeur en décimal (0.05 pour 5%)
    """
    if value is None:
        return 0.0
    
    if isinstance(value, str):
        # Enlever le symbole % si présent
        value = value.replace('%', '').strip()
        try:
            value = float(value)
        except:
            return 0.0
    
    if isinstance(value, (int, float)):
        # Si la valeur est > 1, on assume que c'est un pourcentage
        if value > 1:
            return float(value / 100)
        else:
            return float(value)
    
    return 0.0


def get_column_letter(col_idx):
    """Convertit un indice de colonne (0-based) en lettre de colonne pour Google Sheets (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    col_idx = col_idx + 1  # Convertir de 0-based à 1-based
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result

def verify_token(access_token):
    """Vérifie si le token d'accès est valide"""
    print("Vérification du token d'accès...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version": "202505"  # Utiliser la version la plus récente
    }
    
    url = "https://api.linkedin.com/rest/me"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            user_data = response.json()
            print(f"Token valide pour l'utilisateur: {user_data.get('localizedFirstName', '')} {user_data.get('localizedLastName', '')}")
            return True, user_data
        else:
            print(f"Erreur de validation du token: {response.status_code} - {response.text}")
            return False, f"Erreur {response.status_code}: {response.text}"
    except Exception as e:
        print(f"Exception lors de la vérification du token: {e}")
        traceback.print_exc()
        return False, str(e)

class LinkedInPostMetricsTracker:
    """Classe pour suivre les métriques des posts LinkedIn"""
    
    def __init__(self, access_token, portability_token, organization_id):
        """Initialise le tracker avec les tokens d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.portability_token = portability_token
        self.organization_id = organization_id
        self.sheet_name = f"LinkedIn_Post_Metrics_{organization_id}"
        self.base_url_rest = "https://api.linkedin.com/rest"
        self.reaction_types = [
            "LIKE",        # Like
            "PRAISE",      # Celebrate 
            "EMPATHY",     # Love
            "INTEREST",    # Insightful
            "APPRECIATION", # Support
            "ENTERTAINMENT" # Funny
        ]
        
        # Dictionnaires de traduction pour les types de média et de post
        self.media_type_translation = {
            'NONE': 'Texte uniquement',
            'None': 'Texte uniquement',
            'IMAGE': 'Image',
            'VIDEO': 'Vidéo',
            'DOCUMENT': 'Document',
            'ARTICLE': 'Article',
            'RICH_MEDIA': 'Média enrichi',
            'LINK': 'Lien partagé',
            'CAROUSEL': 'Carrousel d\'images',
            'POLL': 'Sondage',
            'EVENT': 'Événement',
            'REPOST': 'Repost',
            'UNKNOWN': 'Type inconnu',
            'Unknown': 'Type inconnu'
        }
        
        self.post_type_translation = {
            'ugcPost': 'Publication native',
            'share': 'Partage de contenu',
            'post': 'Publication',
            'instantRepost': 'Repost instantané',
            'unknown': 'Type inconnu'
        }
        
        # Configuration pour l'optimisation
        self.max_posts_to_analyze = int(os.getenv('MAX_POSTS_PER_ORG', '50'))  # Limiter le nombre de posts
        self.skip_old_posts_days = int(os.getenv('SKIP_OLD_POSTS_DAYS', '365'))  # Ignorer les posts trop anciens
        self.use_cache = True
        self.parallel_requests = int(os.getenv('PARALLEL_REQUESTS', '3'))  # Nombre de requêtes en parallèle
        self.include_instant_reposts = True  # Nouveau paramètre pour inclure les reposts sans commentaire
    
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202505",
            "Content-Type": "application/json"
        }
    
    def format_media_type(self, media_type):
        """Formate le type de média en français"""
        if not media_type or media_type in ['None', 'NONE', '']:
            return 'Texte uniquement'
        return self.media_type_translation.get(media_type, f'Autre ({media_type})')
    
    def format_post_type(self, post_type):
        """Formate le type de post en français"""
        return self.post_type_translation.get(post_type, f'Autre ({post_type})')
    
    def get_organization_posts(self, count=None):
        """Récupère tous les posts de l'organisation avec l'API REST versionnée"""
        if count is None:
            count = self.max_posts_to_analyze
            
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Utiliser l'API REST /posts
        base_url = f"{self.base_url_rest}/posts?q=author&author={encoded_urn}&count={count}&sortBy=CREATED"
        
        all_posts = {'elements': []}
        next_url = base_url
        
        print(f"   Récupération des posts de l'organisation {self.organization_id} (max: {count})...")
        
        # Limite de pagination
        pages_fetched = 0
        max_pages = 3
        
        while next_url and pages_fetched < max_pages:
            max_retries = 2
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(next_url, headers=self.get_headers())
                    
                    if response.status_code == 200:
                        data = response.json()
                        all_posts['elements'].extend(data.get('elements', []))
                        
                        print(f"   Posts récupérés: {len(all_posts['elements'])} au total")
                        
                        if len(all_posts['elements']) >= count:
                            print(f"   Limite de {count} posts atteinte")
                            next_url = None
                            break
                        
                        # Pagination
                        next_url = None
                        if 'paging' in data and 'links' in data['paging']:
                            for link in data['paging']['links']:
                                if link.get('rel') == 'next':
                                    next_url = link.get('href')
                                    if not next_url.startswith('http'):
                                        next_url = f"https://api.linkedin.com/rest{next_url}"
                                    break
                        
                        pages_fetched += 1
                        break
                        
                    elif response.status_code == 429:
                        print(f"   Rate limit atteint, attente de {retry_delay} secondes...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    else:
                        print(f"   Erreur API: {response.status_code} - {response.text}")
                        if all_posts['elements']:
                            next_url = None
                            break
                        else:
                            return {'elements': []}
                            
                except Exception as e:
                    print(f"   Exception: {e}")
                    if attempt == max_retries - 1:
                        return {'elements': []}
                    time.sleep(retry_delay)
            
            if next_url:
                time.sleep(0.5)
        
        if len(all_posts['elements']) > count:
            all_posts['elements'] = all_posts['elements'][:count]
            
        print(f"   Total des posts récupérés: {len(all_posts['elements'])}")
        return all_posts

    def get_organization_instant_reposts(self):
        """Récupère les reposts SANS commentaire (instant reposts) de l'organisation"""
        print("   Recherche des reposts instantanés...")
        instant_reposts = []
        
        # Récupérer le feed de l'organisation pour trouver ses instant reposts
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Utiliser l'endpoint de feed pour récupérer tous les contenus de l'organisation
        url = f"{self.base_url_rest}/dmaFeedContentsExternal?q=author&author={encoded_urn}&count=100"
        
        try:
            response = requests.get(url, headers=self.get_headers(), timeout=10)
            if response.status_code == 200:
                data = response.json()
                # Parcourir les éléments pour trouver les instant reposts
                for content_urn in data.get('elements', []):
                    if 'instantRepost' in content_urn:
                        # Extraire l'URN du post original depuis l'URN de l'instant repost
                        # Format: urn:li:instantRepost:(urn:li:share:XXXXX,YYYYY)
                        try:
                            parts = content_urn.split('(')[1].split(')')[0].split(',')
                            original_post = parts[0] if parts else ''
                            
                            instant_reposts.append({
                                'repost_urn': content_urn,
                                'original_post': original_post,
                                'type': 'instantRepost'
                            })
                        except:
                            instant_reposts.append({
                                'repost_urn': content_urn,
                                'original_post': '',
                                'type': 'instantRepost'
                            })
            elif response.status_code == 404:
                # L'endpoint n'existe peut-être pas, essayer une approche alternative
                print("   Endpoint dmaFeedContentsExternal non disponible, utilisation de l'approche alternative...")
                
                # Approche alternative : parcourir les activités de l'organisation
                activity_url = f"{self.base_url_rest}/organizationPageStatistics/{self.organization_id}/updates?count=100"
                
                try:
                    activity_response = requests.get(activity_url, headers=self.get_headers(), timeout=10)
                    if activity_response.status_code == 200:
                        activity_data = activity_response.json()
                        # Analyser les activités pour trouver les instant reposts
                        for activity in activity_data.get('elements', []):
                            if activity.get('updateType') == 'INSTANT_REPOST':
                                instant_reposts.append({
                                    'repost_urn': activity.get('urn', ''),
                                    'original_post': activity.get('originalShare', ''),
                                    'type': 'instantRepost'
                                })
                except:
                    pass
                    
        except Exception as e:
            print(f"   Erreur lors de la récupération des reposts instantanés: {e}")
            # Ne pas faire échouer le processus si cet endpoint n'est pas disponible
            pass
        
        print(f"   {len(instant_reposts)} reposts instantanés trouvés")
        return instant_reposts

    def get_instant_repost_details(self, instant_repost_urns):
        """Récupère les détails des instant reposts via batch get"""
        if not instant_repost_urns:
            return {}
        
        # Construire la liste pour le batch get (limité à 50 à la fois)
        batch_size = 50
        all_details = {}
        
        for i in range(0, len(instant_repost_urns), batch_size):
            batch = instant_repost_urns[i:i+batch_size]
            ids_param = ','.join([urllib.parse.quote(urn) for urn in batch])
            url = f"{self.base_url_rest}/dmaInstantReposts?ids=List({ids_param})"
            
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    all_details.update(data.get('results', {}))
                elif response.status_code == 404:
                    # Si l'endpoint n'existe pas, créer des détails basiques
                    print("   Endpoint dmaInstantReposts non disponible, utilisation de données basiques")
                    for urn in batch:
                        all_details[urn] = {
                            'createdAt': int(datetime.now().timestamp() * 1000),
                            'author': f"urn:li:organization:{self.organization_id}"
                        }
            except Exception as e:
                # Si erreur, créer des détails basiques pour ne pas bloquer
                for urn in batch:
                    all_details[urn] = {
                        'createdAt': int(datetime.now().timestamp() * 1000),
                        'author': f"urn:li:organization:{self.organization_id}"
                    }
        
        return all_details


    def extract_post_content(self, post):
        """Extrait le contenu (texte, image, vidéo) d'un post"""
        content = {
            'id': post.get('id', ''),
            'creation_date': None,
            'text': '',
            'media_type': 'None',
            'media_url': '',
            'author': post.get('author', ''),
            'is_reshare': False,
            'original_post': '',
            'is_instant_repost': False  # Nouveau champ
        }
        
        # Vérifier si c'est un instant repost
        if post.get('_is_instant_repost', False):
            content['is_instant_repost'] = True
            content['is_reshare'] = True
            content['original_post'] = post.get('_original_post', '')
            content['text'] = ''  # Les instant reposts n'ont pas de texte
            
            # Date de création
            if 'publishedAt' in post:
                timestamp = post['publishedAt'] / 1000
                content['creation_date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            return content
        
        # Vérifier si c'est un repost
        if 'resharedShare' in post or 'resharedPost' in post:
            content['is_reshare'] = True
            content['original_post'] = post.get('resharedShare', post.get('resharedPost', ''))
        
        # Vérifier si c'est un post de l'API REST ou v2
        if 'publishedAt' in post:  # API REST
            # Date de création pour l'API REST
            timestamp = post['publishedAt'] / 1000
            content['creation_date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            # Texte du post pour l'API REST
            if 'commentary' in post:
                content['text'] = post['commentary']
            
            # Extraire l'auteur
            content['author'] = post.get('author', '')
            
            # Médias pour l'API REST - analyse plus détaillée
            if 'content' in post:
                content_data = post['content']
                
                # Analyser le type de contenu
                if isinstance(content_data, dict):
                    # Si c'est un article ou lien
                    if 'article' in content_data:
                        content['media_type'] = 'ARTICLE'
                        content['media_url'] = content_data.get('article', {}).get('source', '')
                    elif 'media' in content_data:
                        # Média (image, vidéo, etc.)
                        media_data = content_data['media']
                        if isinstance(media_data, list) and len(media_data) > 0:
                            first_media = media_data[0]
                            if 'media' in first_media:
                                media_info = first_media['media']
                                if 'com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest' in str(media_info):
                                    content['media_type'] = 'VIDEO'
                                else:
                                    content['media_type'] = 'IMAGE'
                            content['media_url'] = first_media.get('originalUrl', '')
                        else:
                            content['media_type'] = 'RICH_MEDIA'
                    else:
                        content['media_type'] = 'LINK'
                else:
                    content['media_type'] = 'Content'
                
        else:  # API v2 (UGC Posts)
            # Date de création
            if 'created' in post and 'time' in post['created']:
                timestamp = post['created']['time'] / 1000
                content['creation_date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            
            # Texte du post
            if 'specificContent' in post and 'com.linkedin.ugc.ShareContent' in post['specificContent']:
                share_content = post['specificContent']['com.linkedin.ugc.ShareContent']
                
                # Extraire le texte
                if 'shareCommentary' in share_content and 'text' in share_content['shareCommentary']:
                    content['text'] = share_content['shareCommentary']['text']
                
                # Extraire les informations sur les médias avec analyse détaillée
                media_category = share_content.get('shareMediaCategory', 'NONE')
                
                if 'media' in share_content and len(share_content['media']) > 0:
                    media = share_content['media'][0]
                    
                    # Déterminer le type de média plus précisément
                    if media_category == 'IMAGE':
                        content['media_type'] = 'IMAGE'
                    elif media_category == 'VIDEO':
                        content['media_type'] = 'VIDEO'
                    elif media_category == 'ARTICLE':
                        content['media_type'] = 'ARTICLE'
                    elif media_category == 'RICH':
                        content['media_type'] = 'RICH_MEDIA'
                    elif media_category == 'CAROUSEL_CONTENT':
                        content['media_type'] = 'CAROUSEL'
                    elif media_category == 'POLL':
                        content['media_type'] = 'POLL'
                    else:
                        content['media_type'] = media_category or 'UNKNOWN'
                    
                    # URL du média (image, vidéo, etc.)
                    if 'thumbnails' in media and len(media['thumbnails']) > 0:
                        content['media_url'] = media['thumbnails'][0].get('url', '')
                    elif 'originalUrl' in media:
                        content['media_url'] = media.get('originalUrl', '')
                else:
                    # Pas de média, donc texte seulement
                    content['media_type'] = 'NONE'
        
        return content
    
    def get_post_metrics_batch(self, post_urn):
        """Récupère toutes les métriques d'un post en une seule fois (OPTIMISÉ)"""
        global METRICS_CACHE
        
        # Vérifier le cache
        if self.use_cache:
            with CACHE_LOCK:
                if post_urn in METRICS_CACHE:
                    return METRICS_CACHE[post_urn]
        
        metrics = {
            'social_actions': {'likesSummary': {}, 'commentsSummary': {}},
            'share_stats': {},
            'reactions': {}
        }
        
        # Déterminer le type de post
        if "share" in post_urn:
            post_type = "share"
        elif "ugcPost" in post_urn:
            post_type = "ugcPost"
        elif "post" in post_urn:
            post_type = "post"
        elif "instantRepost" in post_urn:
            post_type = "instantRepost"
        else:
            post_type = "unknown"
        
        # Pour les instant reposts, les métriques sont généralement limitées
        if post_type == "instantRepost":
            # Les instant reposts n'ont généralement pas de métriques propres
            metrics = {
                'social_actions': {'likesSummary': {}, 'commentsSummary': {}},
                'share_stats': {},
                'reactions': {}
            }
        else:
            # Essayer de récupérer toutes les métriques d'un coup
            try:
                # 1. Actions sociales
                metrics['social_actions'] = self.get_post_social_actions(post_urn)
                
                # 2. Statistiques de partage (selon le type)
                if post_type == "share":
                    metrics['share_stats'] = self.get_share_statistics(post_urn)
                elif post_type == "post":
                    metrics['share_stats'] = self.get_post_statistics_rest(post_urn)
                else:
                    metrics['share_stats'] = self.get_ugcpost_statistics(post_urn)
                
                # 3. Réactions détaillées
                metrics['reactions'] = self.get_post_reactions(post_urn)
                
            except Exception as e:
                print(f"   Erreur lors de la récupération des métriques pour {post_urn}: {e}")
        
        # Mettre en cache
        if self.use_cache:
            with CACHE_LOCK:
                METRICS_CACHE[post_urn] = metrics
        
        return metrics
    
    def get_post_social_actions(self, post_urn):
        """Obtient les actions sociales (commentaires, likes) pour un post"""
        encoded_urn = urllib.parse.quote(post_urn)
        url = f"{self.base_url_rest}/socialActions/{encoded_urn}"
        
        max_retries = 2  # Réduit
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    return data
                elif response.status_code in [404, 403]:
                    return {}
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return {}
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    return {}
                time.sleep(retry_delay)
        
        return {}
    
    def get_share_statistics(self, share_urn):
        """Obtient les statistiques pour un post de type share"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_share_urn = urllib.parse.quote(share_urn)
        
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&shares=List({encoded_share_urn})"
        
        max_retries = 2
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    return {}
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return {}
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    return {}
                time.sleep(retry_delay)
        
        return {}
    
    def get_ugcpost_statistics(self, ugcpost_urn):
        """Obtient les statistiques pour un post de type ugcPost"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_ugcpost_urn = urllib.parse.quote(ugcpost_urn)
        
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&ugcPosts=List({encoded_ugcpost_urn})"
        
        max_retries = 2
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    return {}
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return {}
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    return {}
                time.sleep(retry_delay)
        
        return {}
    
    def get_post_reactions(self, post_urn):
        """Obtient les réactions détaillées pour un post"""
        encoded_urn = urllib.parse.quote(post_urn)
        url = f"{self.base_url_rest}/socialMetadata/{encoded_urn}"
        
        max_retries = 2
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    return data.get('reactionSummaries', {})
                elif response.status_code in [404, 403]:
                    return {}
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return {}
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    return {}
                time.sleep(retry_delay)
        
        return {}
    
    def get_post_statistics_rest(self, post_urn):
        """Obtient les statistiques pour un post de l'API REST"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_post_urn = urllib.parse.quote(post_urn)
        
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&posts=List({encoded_post_urn})"
        
        max_retries = 2
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(), timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    return {}
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    return {}
                    
            except Exception as e:
                if attempt == max_retries - 1:
                    return {}
                time.sleep(retry_delay)
        
        return {}
    
    def process_post_batch(self, posts_batch):
        """Traite un lot de posts en parallèle"""
        post_metrics = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel_requests) as executor:
            # Créer les futures pour chaque post
            future_to_post = {}
            
            for post in posts_batch:
                post_urn = post['id']
                
                # Extraire le contenu du post
                content = self.extract_post_content(post)
                
                # Soumettre la tâche de récupération des métriques
                future = executor.submit(self.get_post_metrics_batch, post_urn)
                future_to_post[future] = (post, content)
            
            # Récupérer les résultats
            for future in concurrent.futures.as_completed(future_to_post):
                post, content = future_to_post[future]
                post_urn = post['id']
                
                try:
                    metrics = future.result(timeout=30)
                    
                    # Extraire les données
                    social_actions = metrics.get('social_actions', {})
                    share_stats = metrics.get('share_stats', {})
                    reactions = metrics.get('reactions', {})
                    
                    # Déterminer le type de post
                    if "share" in post_urn:
                        post_type = "share"
                    elif "ugcPost" in post_urn:
                        post_type = "ugcPost"
                    elif "post" in post_urn:
                        post_type = "post"
                    elif "instantRepost" in post_urn:
                        post_type = "instantRepost"
                    else:
                        post_type = "unknown"
                    
                    # Déterminer le sous-type
                    if content.get('is_instant_repost'):
                        sous_type = "Repost instantané"
                    elif content.get('is_reshare'):
                        if content.get('text'):
                            sous_type = "Repost avec commentaire"
                        else:
                            sous_type = "Repost avec commentaire vide"
                    else:
                        sous_type = "Contenu original"
                    
                    # Extraire les métriques
                    likes_summary = social_actions.get('likesSummary', {})
                    comments_summary = social_actions.get('commentsSummary', {})
                    
                    # Extraire les compteurs pour chaque type de réaction
                    like_count = reactions.get('LIKE', {}).get('count', 0)
                    praise_count = reactions.get('PRAISE', {}).get('count', 0)
                    empathy_count = reactions.get('EMPATHY', {}).get('count', 0)
                    interest_count = reactions.get('INTEREST', {}).get('count', 0)
                    appreciation_count = reactions.get('APPRECIATION', {}).get('count', 0)
                    entertainment_count = reactions.get('ENTERTAINMENT', {}).get('count', 0)
                    
                    # Calculer le total des réactions
                    total_reactions = (like_count + praise_count + empathy_count + 
                                      interest_count + appreciation_count + entertainment_count)
                    
                    # Agrégation des métriques avec formatage en français
                    post_metric = {
                        'post_id': post_urn,
                        'post_type': self.format_post_type(post_type),
                        'sous_type': sous_type,
                        'is_reshare': content.get('is_reshare', False),
                        'original_post': content.get('original_post', ''),
                        'creation_date': content['creation_date'],
                        'text': content['text'],
                        'media_type': self.format_media_type(content['media_type']),
                        'media_url': content['media_url'],
                        'author': content['author'],
                        'total_comments': comments_summary.get('aggregatedTotalComments', 0),
                        'impressions': share_stats.get('impressionCount', 0),
                        'unique_impressions': share_stats.get('uniqueImpressionsCount', 0),
                        'clicks': share_stats.get('clickCount', 0),
                        'shares': share_stats.get('shareCount', 0),
                        'engagement_rate': share_stats.get('engagement', 0),
                        'total_reactions': total_reactions,
                        'like_count': like_count,
                        'praise_count': praise_count,
                        'empathy_count': empathy_count,
                        'interest_count': interest_count,
                        'appreciation_count': appreciation_count,
                        'entertainment_count': entertainment_count
                    }
                    
                    post_metrics.append(post_metric)
                    
                except Exception as e:
                    print(f"   Erreur lors du traitement du post {post_urn}: {e}")
        
        return post_metrics
    
    def get_all_post_metrics(self):
        """Récupère toutes les métriques pour tous les posts de l'organisation (VERSION OPTIMISÉE)"""
        # Récupérer les posts normaux (originaux + reposts avec commentaire)
        posts_data = self.get_organization_posts()
        
        if not posts_data or 'elements' not in posts_data or not posts_data['elements']:
            print("   Aucun post récupéré.")
            return []
        
        all_posts = posts_data['elements']
        print(f"   Analyse de {len(all_posts)} posts (originaux + reposts avec commentaire)...")
        
        # Récupérer les instant reposts (reposts sans commentaire)
        instant_reposts = self.get_organization_instant_reposts()
        
        # Récupérer les détails des instant reposts
        if instant_reposts:
            repost_urns = [r['repost_urn'] for r in instant_reposts]
            repost_details = self.get_instant_repost_details(repost_urns)
            
            # Convertir les instant reposts en format de post pour traitement uniforme
            for repost in instant_reposts:
                details = repost_details.get(repost['repost_urn'], {})
                
                # Créer un objet post-like pour les instant reposts
                instant_post = {
                    'id': repost['repost_urn'],
                    'publishedAt': details.get('createdAt', int(datetime.now().timestamp() * 1000)),
                    'commentary': '',  # Pas de commentaire pour les instant reposts
                    'author': f"urn:li:organization:{self.organization_id}",
                    'content': {},
                    '_is_instant_repost': True,
                    '_original_post': repost['original_post']
                }
                all_posts.append(instant_post)
        
        print(f"   Total à analyser: {len(all_posts)} posts (incluant {len(instant_reposts)} reposts instantanés)")            
            
        # Date limite pour les posts
        cutoff_date = datetime.now() - timedelta(days=self.skip_old_posts_days)
        cutoff_timestamp = int(cutoff_date.timestamp() * 1000)
        
        # Filtrer les posts trop anciens
        filtered_posts = []
        skipped_posts = 0
        
        for post in all_posts:
            # Vérifier si le post est trop ancien
            if 'publishedAt' in post:  # API REST
                if post['publishedAt'] < cutoff_timestamp:
                    skipped_posts += 1
                    continue
            elif 'created' in post and 'time' in post['created']:  # API v2
                if post['created']['time'] < cutoff_timestamp:
                    skipped_posts += 1
                    continue
            
            filtered_posts.append(post)
        
        print(f"   Posts filtrés: {len(filtered_posts)} posts à analyser, {skipped_posts} posts ignorés (trop anciens)")
        
        # Limiter encore si nécessaire
        if len(filtered_posts) > self.max_posts_to_analyze:
            filtered_posts = filtered_posts[:self.max_posts_to_analyze]
            print(f"   Limitation à {self.max_posts_to_analyze} posts les plus récents")
        
        # Traiter les posts par lots
        batch_size = 10
        all_metrics = []
        
        for i in range(0, len(filtered_posts), batch_size):
            batch = filtered_posts[i:i+batch_size]
            print(f"   Traitement du lot {i//batch_size + 1}/{(len(filtered_posts)-1)//batch_size + 1} ({len(batch)} posts)")
            
            # Traiter le lot en parallèle
            batch_metrics = self.process_post_batch(batch)
            all_metrics.extend(batch_metrics)
            
            # Pause courte entre les lots
            if i + batch_size < len(filtered_posts):
                time.sleep(0.5)
        
        # Trier par date de création (plus récent au plus ancien)
        all_metrics.sort(key=lambda x: x['creation_date'] if x['creation_date'] else '', reverse=True)
        
        print(f"   Analyse terminée. {len(all_metrics)} posts analysés avec succès.")
        return all_metrics

# Le reste du code (GoogleSheetsExporter et MultiOrganizationPostMetricsTracker) reste identique
# mais avec quelques ajustements mineurs...

class GoogleSheetsExporter:
    """Classe pour exporter les données vers Google Sheets"""
    
    def __init__(self, spreadsheet_name, credentials_path, admin_email="byteberry.analytics@gmail.com"):
        """Initialise l'exportateur avec le nom du spreadsheet et le chemin des credentials"""
        self.spreadsheet_name = spreadsheet_name
        self.credentials_path = credentials_path
        self.admin_email = admin_email
        self.client = None
        self.spreadsheet = None
        
    def connect(self):
        """Établit la connexion avec Google Sheets API"""
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(str(self.credentials_path), scope)
            self.client = gspread.authorize(creds)
            
            # Vérifier si le spreadsheet existe déjà, sinon le créer
            try:
                self.spreadsheet = self.client.open(self.spreadsheet_name)
                print(f"   Spreadsheet existant trouvé: {self.spreadsheet_name}")
            except gspread.exceptions.SpreadsheetNotFound:
                self.spreadsheet = self.client.create(self.spreadsheet_name)
                print(f"   Nouveau spreadsheet créé: {self.spreadsheet_name}")
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée
                self.spreadsheet.share(self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
            
            return True
        except Exception as e:
            print(f"   Erreur de connexion à Google Sheets: {e}")
            traceback.print_exc()
            return False
    
    def ensure_admin_access(self):
        """Vérifie et garantit que l'admin a toujours accès au document"""
        try:
            # Récupérer les permissions actuelles
            permissions = self.spreadsheet.list_permissions()
            
            # Vérifier si l'email admin est déjà dans les permissions
            admin_has_access = False
            for permission in permissions:
                if 'emailAddress' in permission and permission['emailAddress'] == self.admin_email:
                    admin_has_access = True
                    # Vérifier si le rôle est au moins "writer"
                    if permission.get('role') not in ['writer', 'owner']:
                        # Mettre à jour le rôle si nécessaire
                        self.spreadsheet.share(self.admin_email, perm_type="user", role="writer")
                        print(f"   Rôle mis à jour pour {self.admin_email} (writer)")
                    break
            
            # Si l'admin n'a pas encore accès, lui donner
            if not admin_has_access:
                self.spreadsheet.share(self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
                
        except Exception as e:
            print(f"   Erreur lors de la vérification des permissions: {e}")
    
    def format_columns_for_looker(self, worksheet, headers):
        """Applique le formatage approprié aux colonnes pour que Looker détecte correctement les types"""
        try:
            # Vérifier le nombre de colonnes disponibles dans la feuille
            actual_col_count = worksheet.col_count
            max_col_index = len(headers) - 1  # Index maximum basé sur les en-têtes réels
            
            print(f"   Feuille a {actual_col_count} colonnes, headers: {len(headers)}")
            
            # Définir les types de colonnes (ajusté pour les nouvelles colonnes)
            column_formats = {
                # Colonnes de texte
                0: {"numberFormat": {"type": "TEXT"}},  # Post ID
                1: {"numberFormat": {"type": "TEXT"}},  # Type de post
                2: {"numberFormat": {"type": "TEXT"}},  # Sous-type
                3: {"numberFormat": {"type": "TEXT"}},  # Est un repost
                4: {"numberFormat": {"type": "TEXT"}},  # Post original
                6: {"numberFormat": {"type": "TEXT"}},  # Texte du post
                7: {"numberFormat": {"type": "TEXT"}},  # Type de média
                8: {"numberFormat": {"type": "TEXT"}},  # URL du média
                9: {"numberFormat": {"type": "TEXT"}},  # Auteur
                
                # Colonne datetime
                5: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de création
                31: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de collecte (index 31 = 32e colonne)
                
                # Colonnes numériques (entiers)
                10: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Affichages
                11: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Personnes atteintes
                12: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Clics
                13: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Partages
                15: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Nombre de commentaires
                16: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Total des réactions
                17: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # J'aime
                18: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Bravo
                19: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # J'adore
                20: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Instructif
                21: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Soutien
                22: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Amusant
                30: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Nbre d'interactions (index 30 = 31e colonne)
                
                # Colonnes pourcentage
                14: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # Taux d'engagement
                23: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'aime
                24: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Bravo
                25: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'adore
                26: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Instructif
                27: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Soutien
                28: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Amusant
            }
            
            # CORRECTION: Filtrer les colonnes qui existent réellement
            valid_column_formats = {}
            for col_idx, format_spec in column_formats.items():
                if col_idx <= max_col_index and col_idx < actual_col_count:
                    valid_column_formats[col_idx] = format_spec
                else:
                    print(f"   Colonne {col_idx} ignorée (dépasse les limites: max_headers={max_col_index}, sheet_cols={actual_col_count})")
            
            # Grouper les colonnes pour le formatage par lot
            # Traiter par groupes de 5 colonnes pour éviter le rate limiting
            columns_to_format = list(valid_column_formats.items())
            batch_size = 5
            
            for i in range(0, len(columns_to_format), batch_size):
                batch = columns_to_format[i:i+batch_size]
                
                # Appliquer le formatage pour chaque colonne du lot
                for col_idx, format_spec in batch:
                    col_letter = get_column_letter(col_idx)
                    range_name = f"{col_letter}2:{col_letter}"
                    
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            worksheet.format(range_name, format_spec)
                            break  # Succès, sortir de la boucle de retry
                        except gspread.exceptions.APIError as e:
                            if "exceeds grid limits" in str(e):
                                print(f"   Colonne {col_letter} (index {col_idx}) dépasse les limites de la feuille - ignorée")
                                break
                            elif self.handle_rate_limit(e) and attempt < max_retries - 1:
                                continue
                            else:
                                print(f"   Avertissement: Impossible de formater la colonne {col_letter}: {e}")
                                break
                        except Exception as e:
                            print(f"   Avertissement: Impossible de formater la colonne {col_letter}: {e}")
                            break
                
                # Pause entre les lots pour éviter le rate limiting
                if i + batch_size < len(columns_to_format):
                    time.sleep(2)
            
            print("   ✓ Formatage des colonnes appliqué pour Looker")
            
        except Exception as e:
            print(f"   Erreur lors du formatage des colonnes: {e}")
            
            
    def format_columns_robust(self, sheet, column_formats, max_rows=1000):
        """Applique le formatage de manière robuste"""
        for col_idx, format_spec in column_formats.items():
            col_letter = get_column_letter(col_idx)
            range_name = f"{col_letter}2:{col_letter}{max_rows}"
            
            try:
                sheet.format(range_name, format_spec)
            except Exception as e:
                print(f"   Avertissement: Impossible de formater la colonne {col_letter}: {e}")
    
    def apply_post_metrics_formatting(self, sheet, headers, data_rows_count):
        """Applique le formatage spécifique pour les métriques de posts"""
        
        column_formats = {
            # Colonnes de texte
            0: {"numberFormat": {"type": "TEXT"}},  # Post ID
            1: {"numberFormat": {"type": "TEXT"}},  # Type de post
            2: {"numberFormat": {"type": "TEXT"}},  # Sous-type
            3: {"numberFormat": {"type": "TEXT"}},  # Est un repost
            4: {"numberFormat": {"type": "TEXT"}},  # Post original
            6: {"numberFormat": {"type": "TEXT"}},  # Texte du post
            7: {"numberFormat": {"type": "TEXT"}},  # Type de média
            8: {"numberFormat": {"type": "TEXT"}},  # URL du média
            9: {"numberFormat": {"type": "TEXT"}},  # Auteur
            
            # Colonnes datetime
            5: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de création
            31: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de collecte
            
            # Colonnes pourcentage
            14: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # Taux d'engagement
            23: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'aime
            24: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Bravo
            25: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'adore
            26: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Instructif
            27: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Soutien
            28: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Amusant
        }
        
        # Ajouter toutes les colonnes numériques
        number_columns = [10, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 30]
        for col in number_columns:
            column_formats[col] = {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}
        
        # Appliquer le formatage robuste
        self.format_columns_robust(sheet, column_formats, max_rows=data_rows_count + 10)
    
    def handle_rate_limit(self, error):
        """Gère les erreurs de rate limit de Google Sheets"""
        error_str = str(error)
        if "429" in error_str or "Quota exceeded" in error_str:
            if "per minute" in error_str:
                wait_time = 65  # Attendre 65 secondes pour les limites par minute
                print(f"   ⏳ Rate limit Google Sheets atteint. Attente de {wait_time} secondes...")
            elif "per user per 100 seconds" in error_str:
                wait_time = 105  # Attendre 105 secondes
                print(f"   ⏳ Rate limit Google Sheets atteint (100s). Attente de {wait_time} secondes...")
            else:
                wait_time = 30  # Par défaut
                print(f"   ⏳ Rate limit Google Sheets atteint. Attente de {wait_time} secondes...")
            
            time.sleep(wait_time)
            return True
        return False
    
    def update_post_metrics_sheet(self, post_metrics):
        """Met à jour la feuille des métriques de posts avec gestion améliorée des colonnes"""
        try:
            # Vérifier si la feuille existe ou la créer
            worksheet_name = "Métriques des Posts"
            
            try:
                worksheet = self.spreadsheet.worksheet(worksheet_name)
                print(f"   Feuille '{worksheet_name}' trouvée")
                
                # CORRECTION: S'assurer d'avoir exactement 32 colonnes (0-31)
                required_cols = 32
                if worksheet.col_count < required_cols:
                    cols_to_add = required_cols - worksheet.col_count
                    worksheet.add_cols(cols_to_add)
                    print(f"   Colonnes ajoutées: {worksheet.col_count - cols_to_add} → {worksheet.col_count}")
                elif worksheet.col_count > required_cols:
                    # Si on a trop de colonnes, on les garde mais on n'utilisera que les 32 premières
                    print(f"   Feuille a {worksheet.col_count} colonnes (utilisation des {required_cols} premières)")
                    
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=32)
                print(f"   Nouvelle feuille '{worksheet_name}' créée avec 32 colonnes")
            
            # Définir les en-têtes avec les nouvelles colonnes
            headers = [
                # Identification du post
                "Identifiant unique du post",           # 0
                "Type de publication",                  # 1
                "Sous-type de publication",             # 2
                "Est un repost",                        # 3
                "Post original",                        # 4
                "Date et heure de publication",         # 5
                "Texte de la publication",              # 6
                "Type de média",                        # 7
                "Lien du média",                        # 8
                "Auteur de la publication",             # 9
                
                # Métriques de portée
                "Nbre d'affichages",                    # 10
                "Nbre de personnes atteintes",          # 11
                
                # Métriques d'engagement
                "Nbre de clics",                        # 12
                "Nbre de partages",                     # 13
                "Taux d'engagement de la publication",  # 14
                "Nbre de commentaires",                 # 15
                
                # Détail des réactions
                "Nbre de réactions",                    # 16
                "J'aime",                               # 17
                "Bravo",                                # 18
                "J'adore",                              # 19
                "Instructif",                           # 20
                "Soutien",                              # 21
                "Amusant",                              # 22
                
                # Pourcentages des réactions
                "% J'aime",                             # 23
                "% Bravo",                              # 24
                "% J'adore",                            # 25
                "% Instructif",                         # 26
                "% Soutien",                            # 27
                "% Amusant",                            # 28
                
                # Métrique globale
                "Nbre d'interactions",                  # 29
                
                # Métadonnées
                "Date de collecte des données"          # 30
            ]
            
            # VÉRIFICATION: S'assurer qu'on a exactement 31 headers (index 0-30)
            print(f"   Nombre d'en-têtes définis: {len(headers)} (index 0-{len(headers)-1})")
            
            # Mettre à jour les en-têtes avec retry
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    worksheet.update(values=[headers], range_name='A1')
                    break
                except gspread.exceptions.APIError as e:
                    if self.handle_rate_limit(e) and attempt < max_retries - 1:
                        continue
                    else:
                        raise
            
            # Formater les en-têtes avec retry
            for attempt in range(max_retries):
                try:
                    header_range = f'A1:{get_column_letter(len(headers)-1)}1'
                    worksheet.format(header_range, {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                    })
                    break
                except gspread.exceptions.APIError as e:
                    if self.handle_rate_limit(e) and attempt < max_retries - 1:
                        continue
                    else:
                        print(f"   Avertissement: Impossible de formater les en-têtes: {e}")
                        break
            
            # Préparer les données
            rows = []
            collection_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for post in post_metrics:
                # Limiter la longueur du texte
                text = post['text']
                if len(text) > 1000:
                    text = text[:997] + "..."
                
                # CORRECTION - S'assurer que le taux d'engagement est en décimal
                engagement_rate = ensure_percentage_as_decimal(post['engagement_rate'])
                
                # Calculer les pourcentages de chaque type de réaction (en décimal)
                total_reactions = post['total_reactions']
                pct_like = float(post['like_count'] / total_reactions) if total_reactions > 0 else 0.0
                pct_praise = float(post['praise_count'] / total_reactions) if total_reactions > 0 else 0.0
                pct_empathy = float(post['empathy_count'] / total_reactions) if total_reactions > 0 else 0.0
                pct_interest = float(post['interest_count'] / total_reactions) if total_reactions > 0 else 0.0
                pct_appreciation = float(post['appreciation_count'] / total_reactions) if total_reactions > 0 else 0.0
                pct_entertainment = float(post['entertainment_count'] / total_reactions) if total_reactions > 0 else 0.0
                
                # Calculer les engagements totaux
                total_engagements = int(post['clicks'] + post['shares'] + post['total_comments'] + post['total_reactions'])
                
                row = [
                    # Informations sur le post (index 0-9)
                    str(post['post_id']),
                    str(post['post_type']),
                    str(post.get('sous_type', '')),
                    "Oui" if post.get('is_reshare', False) else "Non",
                    str(post.get('original_post', '')),
                    str(post['creation_date']),
                    str(text),
                    str(post['media_type']),
                    str(post['media_url']) if post['media_url'] else '',
                    str(post['author']),
                    
                    # Métriques - s'assurer que ce sont des entiers (index 10-15)
                    int(post['impressions']),
                    int(post['unique_impressions']),
                    int(post['clicks']),
                    int(post['shares']),
                    float(engagement_rate),  # Utiliser la valeur convertie
                    int(post['total_comments']),
                    
                    # Métriques de réactions - entiers (index 16-22)
                    int(post['total_reactions']),
                    int(post['like_count']),
                    int(post['praise_count']),
                    int(post['empathy_count']),
                    int(post['interest_count']),
                    int(post['appreciation_count']),
                    int(post['entertainment_count']),
                    
                    # Pourcentages de réactions - décimaux pour PERCENT (index 23-28)
                    float(pct_like),
                    float(pct_praise),
                    float(pct_empathy),
                    float(pct_interest),
                    float(pct_appreciation),
                    float(pct_entertainment),
                    
                    # Engagements totaux (index 29)
                    int(total_engagements),
                    
                    # Date de collecte (index 30)
                    str(collection_date)
                ]
                rows.append(row)
            
            # Vérifier que chaque ligne a exactement le bon nombre de colonnes
            expected_cols = len(headers)
            for i, row in enumerate(rows):
                if len(row) != expected_cols:
                    print(f"   Avertissement: Ligne {i+1} a {len(row)} colonnes au lieu de {expected_cols}")
            
            # Effacer les données existantes (sauf les en-têtes) avec retry
            if worksheet.row_count > 1:
                for attempt in range(max_retries):
                    try:
                        clear_range = f"A2:{get_column_letter(len(headers)-1)}1000"
                        worksheet.batch_clear([clear_range])
                        break
                    except gspread.exceptions.APIError as e:
                        if self.handle_rate_limit(e) and attempt < max_retries - 1:
                            continue
                        else:
                            print(f"   Avertissement: Impossible d'effacer les données: {e}")
                            break
            
            # Ajouter les nouvelles données
            if rows:
                # Diviser en lots pour éviter les limites de l'API
                batch_size = 50  # Réduit de 100 à 50 pour éviter les rate limits
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    start_row = i + 2  # +2 car on commence après l'en-tête
                    
                    # Retry pour chaque lot
                    for attempt in range(max_retries):
                        try:
                            worksheet.update(values=batch, range_name=f'A{start_row}')
                            print(f"   Lot {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} exporté ({len(batch)} lignes)")
                            break
                        except gspread.exceptions.APIError as e:
                            if self.handle_rate_limit(e) and attempt < max_retries - 1:
                                continue
                            else:
                                raise
                    
                    # Pause entre les lots pour éviter le rate limiting
                    if i + batch_size < len(rows):
                        time.sleep(3)  # Augmenté de 2 à 3 secondes
                
                print(f"   Données mises à jour dans la feuille '{worksheet_name}'")
                
                # Appliquer le formatage pour Looker avec la fonction corrigée
                self.format_columns_for_looker(worksheet, headers)
                
                # Trier les données par date (du plus récent au plus ancien)
                max_sort_retries = 3
                for sort_attempt in range(max_sort_retries):
                    try:
                        sort_range = f'A2:{get_column_letter(len(headers)-1)}{len(rows)+1}'
                        worksheet.sort((6, 'des'), range=sort_range)
                        print("   Données triées par date (du plus récent au plus ancien)")
                        break
                    except ValueError as e:
                        if "should be specified as sort order" in str(e):
                            try:
                                worksheet.sort((6, 'desc'), range=sort_range)
                                print("   Données triées par date (du plus récent au plus ancien)")
                                break
                            except Exception as e2:
                                if sort_attempt < max_sort_retries - 1:
                                    if self.handle_rate_limit(e2):
                                        continue
                                print(f"   Avertissement: Impossible de trier les données: {e2}")
                                break
                        else:
                            raise e
                    except gspread.exceptions.APIError as e:
                        if self.handle_rate_limit(e) and sort_attempt < max_sort_retries - 1:
                            continue
                        else:
                            print(f"   Avertissement: Impossible de trier les données: {e}")
                            break
                    except Exception as sort_error:
                        print(f"   Avertissement: Impossible de trier les données: {sort_error}")
                        break
            else:
                print("   Aucune donnée à exporter")
            
            return True
            
        except Exception as e:
            print(f"   Erreur lors de l'exportation des métriques: {e}")
            traceback.print_exc()
            return False
    
    def export_post_metrics(self, post_metrics):
        """Exporte les métriques des posts vers Google Sheets"""
        if not self.connect():
            print("   Impossible de se connecter à Google Sheets")
            return False
        
        # Vérifier les permissions
        self.ensure_admin_access()
        
        # Mettre à jour la feuille
        return self.update_post_metrics_sheet(post_metrics)

class MultiOrganizationPostMetricsTracker:
    """Gestionnaire pour les métriques de posts de plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.portability_token = os.getenv("PORTABILITY_LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        self.post_metrics_mapping_file = 'post_metrics_mapping.json'
        
    def load_organizations(self):
        """Charge la configuration des organisations depuis un fichier JSON"""
        try:
            # D'abord essayer de charger depuis un fichier local
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Sinon, charger depuis une variable d'environnement
            orgs_json = os.getenv("LINKEDIN_ORGANIZATIONS_CONFIG")
            if orgs_json:
                return json.loads(orgs_json)
            
            # Configuration par défaut
            print("Aucune configuration trouvée, utilisation de la configuration par défaut")
            return []
            
        except Exception as e:
            print(f"Erreur lors du chargement de la configuration: {e}")
            return []
    
    def get_sheet_info_for_org(self, org_id, org_name):
        """Récupère ou crée l'ID et le nom du Google Sheet pour une organisation"""
        try:
            if os.path.exists(self.post_metrics_mapping_file):
                with open(self.post_metrics_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                print(f"   📂 Réutilisation du Google Sheet existant")
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Post_Metrics_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(self.post_metrics_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Post_Metrics_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        try:
            if os.path.exists(self.post_metrics_mapping_file):
                with open(self.post_metrics_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(self.post_metrics_mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(mapping, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            print(f"Erreur lors de la mise à jour du mapping: {e}")
    
    def process_all_organizations(self):
        """Traite toutes les organisations configurées"""
        if not self.access_token or not self.portability_token:
            print("Erreur: Tokens LinkedIn manquants")
            print("Variables nécessaires:")
            print("- COMMUNITY_LINKEDIN_TOKEN ou LINKEDIN_ACCESS_TOKEN")
            print("- PORTABILITY_LINKEDIN_TOKEN")
            return False
        
        # Vérifier le token une seule fois
        print("\n--- Vérification du token ---")
        is_valid, result = verify_token(self.access_token)
        
        if not is_valid:
            print(f"❌ Token invalide: {result}")
            return False
        
        print("✅ Token valide!")
        
        # Traiter chaque organisation
        results = []
        total_orgs = len(self.organizations)
        successful_urls = []
        
        for idx, org in enumerate(self.organizations, 1):
            org_id = org['id']
            org_name = org['name']
            
            print(f"\n{'='*60}")
            print(f"[{idx}/{total_orgs}] Traitement de: {org_name}")
            print(f"ID: {org_id}")
            print(f"{'='*60}")
            
            try:
                sheet_url = self.process_single_organization(org_id, org_name)
                if sheet_url:
                    results.append({
                        'org_id': org_id,
                        'org_name': org_name,
                        'success': True,
                        'sheet_url': sheet_url,
                        'timestamp': datetime.now().isoformat()
                    })
                    successful_urls.append({
                        'name': org_name,
                        'url': sheet_url
                    })
                else:
                    results.append({
                        'org_id': org_id,
                        'org_name': org_name,
                        'success': False,
                        'timestamp': datetime.now().isoformat()
                    })
            except Exception as e:
                print(f"❌ Erreur lors du traitement de {org_name}: {e}")
                results.append({
                    'org_id': org_id,
                    'org_name': org_name,
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
        
        # Résumé
        print(f"\n{'='*60}")
        print("RÉSUMÉ DU TRAITEMENT - MÉTRIQUES DES POSTS")
        print(f"{'='*60}")
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        print(f"✅ Organisations traitées avec succès: {successful}/{total_orgs}")
        if failed > 0:
            print(f"❌ Organisations en échec: {failed}/{total_orgs}")
        
        if failed > 0:
            print("\nDétail des échecs:")
            for r in results:
                if not r['success']:
                    error_msg = r.get('error', 'Erreur inconnue')
                    print(f"  - {r['org_name']}: {error_msg}")
        
        # Afficher les URLs des sheets créés
        if successful > 0:
            print("\n📊 Google Sheets de métriques de posts créés/mis à jour:")
            if os.path.exists(self.post_metrics_mapping_file):
                with open(self.post_metrics_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
                
                for r in results:
                    if r['success'] and r['org_id'] in mapping:
                        sheet_info = mapping[r['org_id']]
                        if sheet_info.get('sheet_id'):
                            print(f"  - {r['org_name']}:")
                            url = sheet_info.get('sheet_url') or f"https://docs.google.com/spreadsheets/d/{sheet_info['sheet_id']}"
                            print(f"    {url}")
        
        return successful > 0
    
    def process_single_organization(self, org_id, org_name):
        """Traite une organisation unique"""
        # Obtenir le nom du sheet pour cette organisation
        sheet_id, sheet_name = self.get_sheet_info_for_org(org_id, org_name)
        
        print(f"\n📊 Google Sheet: {sheet_name}")
        
        # Initialisation du tracker
        tracker = LinkedInPostMetricsTracker(self.access_token, self.portability_token, org_id)
        
        # Récupération des métriques
        print("\n1. Récupération des métriques des posts...")
        post_metrics = tracker.get_all_post_metrics()
        
        if not post_metrics:
            print("   ❌ Aucune métrique à exporter")
            return None
        
        print(f"   ✅ Métriques récupérées pour {len(post_metrics)} posts")
        
        # Afficher le nombre de reposts
        reposts_count = sum(1 for p in post_metrics if p.get('is_reshare', False))
        instant_reposts_count = sum(1 for p in post_metrics if p.get('sous_type') == 'Repost instantané')
        print(f"   📊 Dont {reposts_count} reposts ({instant_reposts_count} instantanés)")
        
        # Déterminer le chemin des credentials selon l'environnement
        if os.getenv('K_SERVICE'):  # Cloud Run/Functions
            credentials_path = Path('/tmp/credentials/service_account_credentials.json')
        else:  # Local
            credentials_path = Path(__file__).resolve().parent / 'credentials' / 'service_account_credentials.json'
        
        if not credentials_path.exists():
            # Essayer de créer les credentials depuis une variable d'environnement
            creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            if creds_json:
                # Créer le dossier seulement si on n'est pas dans /app
                if not str(credentials_path).startswith('/app'):
                    credentials_path.parent.mkdir(parents=True, exist_ok=True)
                with open(credentials_path, 'w') as f:
                    f.write(creds_json)
                print("   ✅ Credentials créés depuis la variable d'environnement")
            else:
                print(f"   ❌ Erreur: Fichier de credentials non trouvé: {credentials_path}")
                return None
        else:
            print("   ✅ Credentials trouvés")
        
        # Export vers Google Sheets
        print("\n2. Export vers Google Sheets...")
        exporter = GoogleSheetsExporter(sheet_name, credentials_path, self.admin_email)
        success = exporter.export_post_metrics(post_metrics)
        
        if success and exporter.spreadsheet:
            # Mettre à jour le mapping avec l'ID du sheet
            self.update_sheet_mapping(org_id, exporter.spreadsheet.id)
            sheet_url = f"https://docs.google.com/spreadsheets/d/{exporter.spreadsheet.id}"
            print(f"\n✅ Export réussi pour {org_name}!")
            print(f"📊 URL du Sheet: {sheet_url}")
            return sheet_url
        else:
            print(f"\n❌ Échec de l'export pour {org_name}")
            return None

def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION POST METRICS TRACKER")
    print("Version avec support des reposts instantanés")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationPostMetricsTracker()
    
    if not tracker.organizations:
        print("\n❌ Aucune organisation configurée!")
        print("\nPour configurer des organisations:")
        print("1. Lancez d'abord: python3 discover_organizations.py")
        print("2. Ou créez manuellement 'organizations_config.json' avec le format:")
        print(json.dumps([
            {"id": "123456", "name": "Entreprise A"},
            {"id": "789012", "name": "Entreprise B"}
        ], indent=2))
        sys.exit(1)
    
    print(f"\n📋 Organisations configurées: {len(tracker.organizations)}")
    for org in tracker.organizations:
        print(f"   - {org['name']} (ID: {org['id']})")
    
    print(f"\n⚙️  Configuration:")
    print(f"   - Email admin: {tracker.admin_email}")
    print(f"   - Type de données: Métriques détaillées des posts")
    print(f"   - Inclut: Posts originaux, reposts avec commentaire, reposts instantanés")
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
        if os.getenv('AUTOMATED_MODE', 'false').lower() == 'true':
            response = 'o'
            print('🤖 Mode automatisé: réponse automatique \'o\'')
        else:
            response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement des métriques de posts...")
    
    # Lancer le traitement
    start_time = datetime.now()
    success = tracker.process_all_organizations()
    end_time = datetime.now()
    
    # Afficher le temps d'exécution
    duration = end_time - start_time
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)
    
    print(f"\n⏱️  Temps d'exécution: {minutes}m {seconds}s")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()