#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Post Metrics Tracker
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

# Chargement des variables d'environnement
load_dotenv()

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
        "LinkedIn-Version": "202404"  # Utiliser la version la plus récente
    }
    
    url = "https://api.linkedin.com/v2/me"
    
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
        self.base_url_v2 = "https://api.linkedin.com/v2"
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
            'UNKNOWN': 'Type inconnu',
            'Unknown': 'Type inconnu'
        }
        
        self.post_type_translation = {
            'ugcPost': 'Publication native',
            'share': 'Partage de contenu',
            'post': 'Publication',
            'unknown': 'Type inconnu'
        }
        
    def get_headers(self, is_rest_api=False, use_portability_token=False):
        """Retourne les en-têtes pour les requêtes API"""
        token = self.portability_token if use_portability_token else self.access_token
        return {
            "Authorization": f"Bearer {token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202404",
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
    
    def get_organization_posts(self, count=100):
        """Récupère tous les posts de l'organisation avec pagination"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Essayer d'abord avec l'API REST posts
        try:
            # Utiliser l'API REST posts
            base_url = f"{self.base_url_rest}/posts?q=author&author={encoded_urn}&count={count}"
            
            all_posts = {'elements': []}
            next_url = base_url
            
            print(f"   Récupération de tous les posts de l'organisation {self.organization_id} (API REST)...")
            
            # Boucle pour gérer la pagination
            while next_url:
                # Effectuer la requête avec gestion des erreurs et retry
                max_retries = 3
                retry_delay = 2  # secondes
                
                for attempt in range(max_retries):
                    try:
                        response = requests.get(next_url, headers=self.get_headers(is_rest_api=True))
                        
                        if response.status_code == 200:
                            data = response.json()
                            
                            # Ajouter les posts à notre liste
                            all_posts['elements'].extend(data.get('elements', []))
                            
                            print(f"   Posts récupérés: {len(all_posts['elements'])} au total")
                            
                            # Vérifier s'il y a une page suivante
                            next_url = None
                            if 'paging' in data and 'links' in data['paging']:
                                for link in data['paging']['links']:
                                    if link.get('rel') == 'next':
                                        next_url = link.get('href')
                                        if not next_url.startswith('http'):
                                            next_url = f"{self.base_url_rest}{next_url}"
                                        break
                            
                            break  # Sortir de la boucle des tentatives
                            
                        elif response.status_code == 429:
                            # Rate limit, attendre avant de réessayer
                            print(f"   Rate limit atteint, attente de {retry_delay} secondes...")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Backoff exponentiel
                        elif response.status_code == 404:
                            # Si on a déjà des posts et qu'on obtient un 404, c'est probablement la fin
                            if all_posts['elements']:
                                print(f"   Fin de pagination détectée (404) après {len(all_posts['elements'])} posts")
                                next_url = None
                                break
                            else:
                                # Si on n'a pas de posts et 404, l'API REST ne fonctionne pas
                                raise Exception(f"API REST error: {response.status_code}")
                        else:
                            print(f"   Erreur API REST: {response.status_code} - {response.text}")
                            # Si on a déjà des posts, on continue avec ce qu'on a
                            if all_posts['elements']:
                                print(f"   Poursuite avec {len(all_posts['elements'])} posts déjà récupérés")
                                next_url = None
                                break
                            else:
                                raise Exception(f"API REST error: {response.status_code}")
                            
                    except Exception as e:
                        if attempt == max_retries - 1:
                            # Si on a déjà des posts, on les retourne
                            if all_posts['elements']:
                                print(f"   Erreur lors de la pagination, mais {len(all_posts['elements'])} posts déjà récupérés")
                                next_url = None
                                break
                            else:
                                raise e
                        time.sleep(retry_delay)
                        retry_delay *= 2
                
                # Pause entre les pages pour respecter les limites de l'API
                if next_url:
                    time.sleep(2)
            
            # Si on a des posts, les retourner
            if all_posts['elements']:
                print(f"   Total des posts récupérés avec l'API REST: {len(all_posts['elements'])}")
                return all_posts
                
        except Exception as e:
            print(f"   Échec avec l'API REST ({str(e)}), essai avec l'API v2...")
        
        # Si l'API REST échoue, essayer avec l'API v2 UGC Posts
        base_url = f"{self.base_url_v2}/ugcPosts?q=authors&authors=List({encoded_urn})&count={count}"
        
        all_posts = {'elements': []}
        next_url = base_url
        
        print(f"   Récupération de tous les posts de l'organisation {self.organization_id} (API v2)...")
        
        # Boucle pour gérer la pagination
        while next_url:
            # Effectuer la requête avec gestion des erreurs et retry
            max_retries = 3
            retry_delay = 2  # secondes
            
            for attempt in range(max_retries):
                try:
                    response = requests.get(next_url, headers=self.get_headers())
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        # Ajouter les posts à notre liste
                        all_posts['elements'].extend(data.get('elements', []))
                        
                        print(f"   Posts récupérés: {len(all_posts['elements'])} au total")
                        
                        # Vérifier s'il y a une page suivante
                        next_url = None
                        if 'paging' in data and 'links' in data['paging']:
                            for link in data['paging']['links']:
                                if link.get('rel') == 'next':
                                    next_url = link.get('href')
                                    if not next_url.startswith('http'):
                                        next_url = f"{self.base_url_v2}{next_url}"
                                    break
                        
                        break  # Sortir de la boucle des tentatives
                        
                    elif response.status_code == 429:
                        # Rate limit, attendre avant de réessayer
                        print(f"   Rate limit atteint, attente de {retry_delay} secondes...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponentiel
                    else:
                        print(f"   Erreur API: {response.status_code} - {response.text}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        
                except Exception as e:
                    print(f"   Exception lors de la requête: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
            
            # Si on n'a pas pu récupérer la page, sortir de la boucle
            if next_url and attempt == max_retries - 1:
                print("   Échec après plusieurs tentatives pour obtenir la page suivante.")
                break
            
            # Pause entre les pages pour respecter les limites de l'API
            if next_url:
                time.sleep(2)
        
        print(f"   Total des posts récupérés: {len(all_posts['elements'])}")
        return all_posts
    
    def extract_post_content(self, post):
        """Extrait le contenu (texte, image, vidéo) d'un post"""
        content = {
            'id': post.get('id', ''),
            'creation_date': None,
            'text': '',
            'media_type': 'None',
            'media_url': '',
            'author': post.get('author', '')
        }
        
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
                        # Analyser plus finement le contenu riche
                        if 'status' in media and 'status' in str(media['status']):
                            if 'VIDEO' in str(media['status']).upper():
                                content['media_type'] = 'VIDEO'
                            else:
                                content['media_type'] = 'RICH_MEDIA'
                        else:
                            content['media_type'] = 'RICH_MEDIA'
                    elif media_category == 'CAROUSEL_CONTENT':
                        content['media_type'] = 'CAROUSEL'
                    elif media_category == 'LIVE_VIDEO':
                        content['media_type'] = 'VIDEO'
                    elif media_category == 'DOCUMENT':
                        content['media_type'] = 'DOCUMENT'
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
    
    def get_post_social_actions(self, post_urn):
        """Obtient les actions sociales (commentaires, likes) pour un post"""
        encoded_urn = urllib.parse.quote(post_urn)
        url = f"{self.base_url_v2}/socialActions/{encoded_urn}"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    return data
                    
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code == 404:
                    return {}
                elif response.status_code == 403:
                    return {}
                else:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as e:
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return {}
    
    def get_share_statistics(self, share_urn):
        """Obtient les statistiques pour un post de type share"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_share_urn = urllib.parse.quote(share_urn)
        
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&shares=List({encoded_share_urn})"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    else:
                        return {}
                    
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                else:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                    
            except Exception as e:
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return {}
    
    def get_ugcpost_statistics(self, ugcpost_urn):
        """Obtient les statistiques pour un post de type ugcPost"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_ugcpost_urn = urllib.parse.quote(ugcpost_urn)
        
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&ugcPosts=List({encoded_ugcpost_urn})"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    else:
                        return {}
                    
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                else:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                    
            except Exception as e:
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return {}
    
    def get_post_reactions(self, post_urn):
        """Obtient les réactions détaillées pour un post"""
        encoded_urn = urllib.parse.quote(post_urn)
        url = f"{self.base_url_rest}/socialMetadata/{encoded_urn}"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    return data.get('reactionSummaries', {})
                    
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code in [404, 403]:
                    return {}
                else:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as e:
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return {}
    
    def get_post_statistics_rest(self, post_urn):
        """Obtient les statistiques pour un post de l'API REST"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_post_urn = urllib.parse.quote(post_urn)
        
        # Pour l'API REST, utiliser l'endpoint postStatistics
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&posts=List({encoded_post_urn})"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and 'elements' in data and len(data['elements']) > 0:
                        return data['elements'][0].get('totalShareStatistics', {})
                    else:
                        return {}
                    
                elif response.status_code == 429:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code in [404, 403, 400, 500]:
                    return {}
                else:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                    
            except Exception as e:
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return {}
    
    def get_all_post_metrics(self):
        """Récupère toutes les métriques pour tous les posts de l'organisation"""
        # Récupérer tous les posts
        posts_data = self.get_organization_posts(count=100)
        
        if not posts_data or 'elements' not in posts_data or not posts_data['elements']:
            print("   Aucun post récupéré.")
            return []
        
        all_posts = posts_data['elements']
        print(f"   Analyse de {len(all_posts)} posts...")
        
        # Liste pour stocker les métriques de tous les posts
        post_metrics = []
        skipped_posts = 0
        
        # Date limite pour les posts (filtrer les posts plus anciens que 24 mois)
        cutoff_date = datetime.now() - timedelta(days=730)
        cutoff_timestamp = int(cutoff_date.timestamp() * 1000)
        
        # Traiter chaque post
        for i, post in enumerate(all_posts, 1):
            post_urn = post['id']
            
            # Déterminer le type de post
            if "share" in post_urn:
                post_type = "share"
            elif "ugcPost" in post_urn:
                post_type = "ugcPost"
            elif "post" in post_urn:
                post_type = "post"  # Pour l'API REST
            else:
                post_type = "unknown"
            
            # Vérifier si le post est trop ancien
            if 'created' in post and 'time' in post['created']:
                if post['created']['time'] < cutoff_timestamp:
                    skipped_posts += 1
                    continue
            
            if i % 10 == 1:  # Afficher la progression tous les 10 posts
                print(f"   Progression: {i}/{len(all_posts)} posts analysés")
            
            # Vérifier l'auteur du post
            org_urn = f"urn:li:organization:{self.organization_id}"
            if post.get('author', '') != org_urn:
                # On continue quand même
                pass
            
            # Extraire le contenu du post
            content = self.extract_post_content(post)
            
            # Récupérer les actions sociales
            try:
                social_actions = self.get_post_social_actions(post_urn)
                if not social_actions:
                    social_actions = {'likesSummary': {}, 'commentsSummary': {}}
            except Exception as e:
                social_actions = {'likesSummary': {}, 'commentsSummary': {}}
            
            # Récupérer les statistiques détaillées
            share_stats = {}
            try:
                if post_type == "share":
                    share_stats = self.get_share_statistics(post_urn)
                elif post_type == "post":
                    # Pour les posts de l'API REST, utiliser une méthode différente
                    share_stats = self.get_post_statistics_rest(post_urn)
                else:
                    share_stats = self.get_ugcpost_statistics(post_urn)
            except Exception as e:
                pass
            
            # Récupérer les réactions détaillées
            reactions = {}
            try:
                reactions = self.get_post_reactions(post_urn)
            except Exception as e:
                pass
            
            # Pause pour respecter les limites de l'API
            time.sleep(2)
            
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
        
        # Trier par date de création (plus récent au plus ancien)
        post_metrics.sort(key=lambda x: x['creation_date'] if x['creation_date'] else '', reverse=True)
        
        print(f"   Analyse terminée. {len(post_metrics)} posts analysés, {skipped_posts} posts ignorés.")
        return post_metrics

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
            # Définir les types de colonnes
            # Les indices sont basés sur l'ordre des headers définis dans update_post_metrics_sheet
            column_formats = {
                # Colonnes de texte
                0: {"numberFormat": {"type": "TEXT"}},  # Post ID
                1: {"numberFormat": {"type": "TEXT"}},  # Type de post
                3: {"numberFormat": {"type": "TEXT"}},  # Texte du post
                4: {"numberFormat": {"type": "TEXT"}},  # Type de média
                5: {"numberFormat": {"type": "TEXT"}},  # URL du média
                6: {"numberFormat": {"type": "TEXT"}},  # Auteur
                
                # Colonne datetime
                2: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de création
                30: {"numberFormat": {"type": "DATE_TIME", "pattern": "yyyy-mm-dd hh:mm:ss"}},  # Date de collecte
                
                # Colonnes numériques (entiers)
                7: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},   # Impressions
                8: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},   # Impressions uniques
                9: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},   # Clics
                10: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Partages
                12: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Nombre de commentaires
                13: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Total des réactions
                14: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # J'aime
                15: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Célébration
                16: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # J'adore
                17: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Intéressant
                18: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Soutien
                19: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Amusant
                28: {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}},  # Engagements totaux
                
                # Colonnes pourcentage
                11: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # Taux d'engagement
                20: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'aime
                21: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Célébration
                22: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % J'adore
                23: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Intéressant
                24: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Soutien
                25: {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}, # % Amusant
            }
            
            # Appliquer le formatage pour chaque colonne
            for col_idx, format_spec in column_formats.items():
                col_letter = get_column_letter(col_idx)
                range_name = f"{col_letter}2:{col_letter}"
                
                try:
                    worksheet.format(range_name, format_spec)
                except Exception as e:
                    print(f"   Avertissement: Impossible de formater la colonne {col_letter}: {e}")
            
            print("   ✓ Formatage des colonnes appliqué pour Looker")
            
        except Exception as e:
            print(f"   Erreur lors du formatage des colonnes: {e}")
    
    def update_post_metrics_sheet(self, post_metrics):
        """Met à jour la feuille des métriques de posts"""
        try:
            # Vérifier si la feuille existe ou la créer
            worksheet_name = "Métriques des Posts"
            
            try:
                worksheet = self.spreadsheet.worksheet(worksheet_name)
                print(f"   Feuille '{worksheet_name}' trouvée")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=32)
                print(f"   Nouvelle feuille '{worksheet_name}' créée")
            
            # Définir les en-têtes
            headers = [
                # Informations sur le post
                "Post ID",
                "Type de post",
                "Date de création",
                "Texte du post",
                "Type de média",
                "URL du média",
                "Auteur",
                
                # Métriques d'impressions et d'interactions
                "Impressions",
                "Impressions uniques",
                "Clics",
                "Partages",
                "Taux d'engagement (%)",
                "Nombre de commentaires",
                
                # Métriques de réactions
                "Total des réactions",
                "J'aime (Like)",
                "Célébration (Praise)",
                "J'adore (Empathy)",
                "Intéressant (Interest)",
                "Soutien (Appreciation)", 
                "Amusant (Entertainment)",
                "% J'aime",
                "% Célébration",
                "% J'adore",
                "% Intéressant", 
                "% Soutien",
                "% Amusant",
                
                # Nouvelle colonne pour les engagements totaux
                "Engagements totaux",
                
                # Date de récupération des données
                "Date de collecte"
            ]
            
            # Mettre à jour les en-têtes
            worksheet.update(values=[headers], range_name='A1')
            
            # Formater les en-têtes
            worksheet.format('A1:' + get_column_letter(len(headers)-1) + '1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            # Préparer les données
            rows = []
            collection_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for post in post_metrics:
                # Limiter la longueur du texte
                text = post['text']
                if len(text) > 1000:
                    text = text[:997] + "..."
                
                # Calculer le taux d'engagement (en décimal pour le format PERCENT)
                # Si engagement_rate est déjà en décimal (ex: 0.05 pour 5%), on le garde
                # Si c'est déjà en pourcentage (ex: 5 pour 5%), on divise par 100
                engagement_rate = post['engagement_rate'] if isinstance(post['engagement_rate'], (int, float)) else 0
                
                # Calculer les pourcentages de chaque type de réaction (en décimal pour le format PERCENT)
                total_reactions = post['total_reactions']
                pct_like = (post['like_count'] / total_reactions) if total_reactions > 0 else 0
                pct_praise = (post['praise_count'] / total_reactions) if total_reactions > 0 else 0
                pct_empathy = (post['empathy_count'] / total_reactions) if total_reactions > 0 else 0
                pct_interest = (post['interest_count'] / total_reactions) if total_reactions > 0 else 0
                pct_appreciation = (post['appreciation_count'] / total_reactions) if total_reactions > 0 else 0
                pct_entertainment = (post['entertainment_count'] / total_reactions) if total_reactions > 0 else 0
                
                # Calculer les engagements totaux
                total_engagements = post['clicks'] + post['shares'] + post['total_comments'] + post['total_reactions']
                
                row = [
                    # Informations sur le post
                    post['post_id'],
                    post['post_type'],
                    post['creation_date'],
                    text,
                    post['media_type'],
                    post['media_url'],
                    post['author'],
                    
                    # Métriques d'impressions et d'interactions
                    post['impressions'],
                    post['unique_impressions'],
                    post['clicks'],
                    post['shares'],
                    engagement_rate,
                    post['total_comments'],
                    
                    # Métriques de réactions
                    post['total_reactions'],
                    post['like_count'],
                    post['praise_count'],
                    post['empathy_count'],
                    post['interest_count'],
                    post['appreciation_count'],
                    post['entertainment_count'],
                    
                    # Pourcentages de réactions (en décimal pour le format PERCENT)
                    pct_like,
                    pct_praise,
                    pct_empathy,
                    pct_interest,
                    pct_appreciation,
                    pct_entertainment,
                    
                    # Engagements totaux
                    total_engagements,
                    
                    # Date de collecte
                    collection_date
                ]
                rows.append(row)
            
            # Effacer les données existantes (sauf les en-têtes)
            if worksheet.row_count > 1:
                worksheet.batch_clear(["A2:Z1000"])
            
            # Ajouter les nouvelles données
            if rows:
                # Diviser en lots pour éviter les limites de l'API
                batch_size = 100
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    start_row = i + 2  # +2 car on commence après l'en-tête
                    worksheet.update(values=batch, range_name=f'A{start_row}')
                    print(f"   Lot {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} exporté ({len(batch)} lignes)")
                    # Pause entre les lots
                    if i + batch_size < len(rows):
                        time.sleep(2)
                
                print(f"   Données mises à jour dans la feuille '{worksheet_name}'")
                
                # Appliquer le formatage pour Looker
                self.format_columns_for_looker(worksheet, headers)
                
                # Trier les données par date (du plus récent au plus ancien)
                try:
                    worksheet.sort((3, 'des'), range=f'A2:{get_column_letter(len(headers)-1)}{len(rows)+1}')
                    print("   Données triées par date (du plus récent au plus ancien)")
                except ValueError as e:
                    if "should be specified as sort order" in str(e):
                        worksheet.sort((3, 'desc'), range=f'A2:{get_column_letter(len(headers)-1)}{len(rows)+1}')
                        print("   Données triées par date (du plus récent au plus ancien)")
                    else:
                        raise e
                except Exception as sort_error:
                    print(f"   Avertissement: Impossible de trier les données: {sort_error}")
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
        
        # Chemin vers les credentials
        credentials_path = Path(__file__).resolve().parent / 'credentials' / 'service_account_credentials.json'
        
        # Pour Google Cloud Run, utiliser le chemin monté
        if os.getenv('K_SERVICE'):
            credentials_path = Path('/app/credentials/service_account_credentials.json')
        
        if not credentials_path.exists():
            # Essayer de créer les credentials depuis une variable d'environnement
            creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
            if creds_json:
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
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
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