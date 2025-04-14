#!/usr/bin/env python3
"""
LinkedIn Post Metrics Tracker
Ce script collecte les métriques détaillées des posts LinkedIn
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
            print(f"Token valide pour l'utilisateur: {response.json().get('localizedFirstName', '')} {response.json().get('localizedLastName', '')}")
            return True, response.json()
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
        
    def get_headers(self, is_rest_api=False, use_portability_token=False):
        """Retourne les en-têtes pour les requêtes API"""
        token = self.portability_token if use_portability_token else self.access_token
        return {
            "Authorization": f"Bearer {token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202404",
            "Content-Type": "application/json"
        }
    
    def get_organization_posts(self, count=100):
        """Récupère tous les posts de l'organisation avec pagination"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Utiliser l'API UGC Posts qui est plus stable
        base_url = f"{self.base_url_v2}/ugcPosts?q=authors&authors=List({encoded_urn})&count={count}"
        
        all_posts = {'elements': []}
        next_url = base_url
        
        print(f"Récupération de tous les posts de l'organisation {self.organization_id}...")
        
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
                        
                        print(f"Posts récupérés: {len(all_posts['elements'])} au total")
                        
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
                        print(f"Rate limit atteint, attente de {retry_delay} secondes...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponentiel
                    else:
                        print(f"Erreur API: {response.status_code} - {response.text}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        
                except Exception as e:
                    print(f"Exception lors de la requête: {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
            
            # Si on n'a pas pu récupérer la page, sortir de la boucle
            if next_url and attempt == max_retries - 1:
                print("Échec après plusieurs tentatives pour obtenir la page suivante.")
                break
            
            # Pause entre les pages pour respecter les limites de l'API
            if next_url:
                time.sleep(2)
        
        print(f"Total des posts récupérés: {len(all_posts['elements'])}")
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
            
            # Extraire les informations sur les médias
            if 'media' in share_content and len(share_content['media']) > 0:
                media = share_content['media'][0]
                content['media_type'] = share_content.get('shareMediaCategory', 'Unknown')
                
                # URL du média (image, vidéo, etc.)
                if 'thumbnails' in media and len(media['thumbnails']) > 0:
                    content['media_url'] = media['thumbnails'][0].get('url', '')
                elif 'originalUrl' in media:
                    content['media_url'] = media.get('originalUrl', '')
        
        return content
    
    def get_post_social_actions(self, post_urn):
        """Obtient les actions sociales (commentaires, likes) pour un post"""
        encoded_urn = urllib.parse.quote(post_urn)
        url = f"{self.base_url_v2}/socialActions/{encoded_urn}"
        
        max_retries = 3
        retry_delay = 2
        
        print(f"Récupération des actions sociales pour le post: {post_urn}")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Actions sociales récupérées avec succès pour {post_urn}")
                    return data
                    
                elif response.status_code == 429:
                    print(f"Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code == 404:
                    print(f"Post non trouvé ou pas d'actions sociales: {post_urn}")
                    return {}
                elif response.status_code == 403:
                    print(f"Non autorisé à accéder aux actions sociales pour le post: {post_urn}")
                    # Pour les erreurs 403, on arrête les tentatives car c'est un problème d'autorisation
                    return {}
                else:
                    print(f"Erreur API socialActions: {response.status_code} - {response.text}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as e:
                print(f"Exception lors de la requête socialActions: {e}")
                traceback.print_exc()
                time.sleep(retry_delay)
                retry_delay *= 2
        
        print(f"Échec après plusieurs tentatives pour obtenir les actions sociales du post {post_urn}")
        return {}
    
    def get_share_statistics(self, share_urn):
        """Obtient les statistiques pour un post de type share"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_share_urn = urllib.parse.quote(share_urn)
        
        # CORRECTION: utiliser le paramètre 'shares' pour les posts de type share
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&shares=List({encoded_share_urn})"
        
        max_retries = 3
        retry_delay = 2
        
        print(f"Récupération des statistiques pour le post (share): {share_urn}")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and 'elements' in data and len(data['elements']) > 0:
                        print(f"Statistiques récupérées avec succès pour {share_urn}")
                        return data['elements'][0].get('totalShareStatistics', {})
                    else:
                        print(f"Aucune statistique disponible pour {share_urn}")
                        return {}
                    
                elif response.status_code == 429:
                    print(f"Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code == 404 or response.status_code == 403:
                    print(f"Post non trouvé ou pas d'accès aux statistiques: {share_urn}")
                    return {}
                elif response.status_code == 400:
                    error_msg = response.text
                    if "organizationalEntity did not post them" in error_msg or "Unable to get activityIds" in error_msg:
                        print(f"Ce post n'appartient pas à votre organisation ou n'a pas d'activité associée: {share_urn}")
                        # Pour les erreurs 400 liées à l'appartenance, on arrête les tentatives
                        return {}
                    else:
                        print(f"Erreur API Share Statistics: {response.status_code} - {error_msg}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                else:
                    print(f"Erreur API Share Statistics: {response.status_code} - {response.text}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    # Ne pas continuer avec un succès après une erreur
                    continue
                    
            except Exception as e:
                print(f"Exception lors de la requête Share Statistics: {e}")
                traceback.print_exc()
                time.sleep(retry_delay)
                retry_delay *= 2
        
        print(f"Échec après plusieurs tentatives pour obtenir les statistiques du post {share_urn}")
        return {}
    
    def get_ugcpost_statistics(self, ugcpost_urn):
        """Obtient les statistiques pour un post de type ugcPost"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_org_urn = urllib.parse.quote(organization_urn)
        encoded_ugcpost_urn = urllib.parse.quote(ugcpost_urn)
        
        # Utiliser le paramètre ugcPosts pour les posts de type ugcPost
        url = f"{self.base_url_rest}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_org_urn}&ugcPosts=List({encoded_ugcpost_urn})"
        
        max_retries = 3
        retry_delay = 2
        
        print(f"Récupération des statistiques pour le post (ugcPost): {ugcpost_urn}")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers(is_rest_api=True))
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data and 'elements' in data and len(data['elements']) > 0:
                        print(f"Statistiques récupérées avec succès pour {ugcpost_urn}")
                        return data['elements'][0].get('totalShareStatistics', {})
                    else:
                        print(f"Aucune statistique disponible pour {ugcpost_urn}")
                        return {}
                    
                elif response.status_code == 429:
                    print(f"Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                elif response.status_code == 404 or response.status_code == 403:
                    print(f"Post non trouvé ou pas d'accès aux statistiques: {ugcpost_urn}")
                    return {}
                elif response.status_code == 400:
                    error_msg = response.text
                    if "organizationalEntity did not post them" in error_msg or "Unable to get activityIds" in error_msg:
                        print(f"Ce post n'appartient pas à votre organisation ou n'a pas d'activité associée: {ugcpost_urn}")
                        # Pour les erreurs 400 liées à l'appartenance, on arrête les tentatives
                        return {}
                    elif "SERVICE_INTERNAL_ERROR" in error_msg:
                        print(f"Erreur interne du service LinkedIn pour {ugcpost_urn}. Certains posts anciens peuvent provoquer cette erreur.")
                        return {}
                    else:
                        print(f"Erreur API Share Statistics: {response.status_code} - {error_msg}")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                elif response.status_code == 500:
                    print(f"Erreur interne du serveur LinkedIn pour {ugcpost_urn}. Poursuite avec des valeurs par défaut.")
                    return {}
                else:
                    print(f"Erreur API Share Statistics: {response.status_code} - {response.text}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    # Ne pas continuer avec un succès après une erreur
                    continue
                    
            except Exception as e:
                print(f"Exception lors de la requête Share Statistics: {e}")
                traceback.print_exc()
                time.sleep(retry_delay)
                retry_delay *= 2
        
        print(f"Échec après plusieurs tentatives pour obtenir les statistiques du post {ugcpost_urn}")
        return {}
    
    def get_all_post_metrics(self):
        """Récupère toutes les métriques pour tous les posts de l'organisation"""
        # Récupérer tous les posts
        posts_data = self.get_organization_posts(count=100)
        
        if not posts_data or 'elements' not in posts_data or not posts_data['elements']:
            print("Aucun post récupéré. Impossible de compiler les métriques.")
            return []
        
        all_posts = posts_data['elements']
        print(f"Analyse de {len(all_posts)} posts...")
        
        # Liste pour stocker les métriques de tous les posts
        post_metrics = []
        skipped_posts = 0
        
        # Date limite pour les posts (filtrer les posts plus anciens que 24 mois)
        cutoff_date = datetime.now() - timedelta(days=730)
        cutoff_timestamp = int(cutoff_date.timestamp() * 1000)  # En millisecondes pour LinkedIn
        
        # Traiter chaque post
        for i, post in enumerate(all_posts, 1):
            post_urn = post['id']
            
            # CORRECTION: Déterminer le type de post en vérifiant l'URN
            post_type = "share" if "share" in post_urn else "ugcPost"
            
            # Vérifier si le post est trop ancien à partir du timestamp dans l'URN ou des métadonnées
            try:
                # Extraire l'epoch time à partir de l'URN pour les ugcPosts
                if post_type == "ugcPost" and ":" in post_urn:
                    parts = post_urn.split(':')
                    if len(parts) > 2:
                        post_id = parts[-1]
                        # Les IDs de LinkedIn contiennent souvent un timestamp
                        if post_id.isdigit() and len(post_id) > 10:
                            # Si l'ID semble être un nombre très grand, c'est peut-être un timestamp
                            # Pour les anciens posts (circa 2020), le format peut être différent
                            if int(post_id) < cutoff_timestamp and int(post_id) > 1000000000000:
                                print(f"Post {post_urn} ignoré car trop ancien")
                                skipped_posts += 1
                                continue
                
                # Vérifier également la date dans les métadonnées
                if 'created' in post and 'time' in post['created']:
                    if post['created']['time'] < cutoff_timestamp:
                        print(f"Post {post_urn} ignoré car trop ancien (selon les métadonnées)")
                        skipped_posts += 1
                        continue
            except Exception as e:
                print(f"Erreur lors de la vérification de la date du post {post_urn}: {e}")
                # Continuer avec ce post en cas d'erreur dans la vérification de la date
            
            print(f"\nAnalyse du post {i}/{len(all_posts)}: {post_urn}")
            
            # Vérifier l'auteur du post
            org_urn = f"urn:li:organization:{self.organization_id}"
            if post.get('author', '') != org_urn:
                print(f"⚠️ Attention: Le post {post_urn} n'a pas été créé par votre organisation")
                print(f"   Auteur: {post.get('author', 'Non spécifié')} vs Attendu: {org_urn}")
                # On continue quand même, car le post pourrait être associé à l'organisation d'une autre manière
            
            # Extraire le contenu du post
            content = self.extract_post_content(post)
            
            # Récupérer les actions sociales avec gestion des erreurs d'autorisation
            try:
                social_actions = self.get_post_social_actions(post_urn)
                if not social_actions:
                    print(f"⚠️ Impossible d'obtenir les actions sociales pour {post_urn}, utilisation de valeurs par défaut")
                    social_actions = {'likesSummary': {}, 'commentsSummary': {}}
            except Exception as e:
                print(f"Erreur lors de la récupération des actions sociales pour {post_urn}: {e}")
                social_actions = {'likesSummary': {}, 'commentsSummary': {}}
            
            # Récupérer les statistiques détaillées en fonction du type avec gestion des erreurs
            share_stats = {}
            try:
                # CORRECTION: Appeler la fonction appropriée selon le type de post
                if post_type == "share":
                    share_stats = self.get_share_statistics(post_urn)
                else:
                    share_stats = self.get_ugcpost_statistics(post_urn)
                
                if not share_stats:
                    print(f"⚠️ Impossible d'obtenir les statistiques pour {post_urn}, utilisation de valeurs par défaut")
            except Exception as e:
                print(f"Erreur lors de la récupération des statistiques pour {post_urn}: {e}")
            
            # Pause pour respecter les limites de l'API avant de continuer
            time.sleep(3)
            
            # Extraire les métriques de commentaires et likes
            likes_summary = social_actions.get('likesSummary', {})
            comments_summary = social_actions.get('commentsSummary', {})
            
            # Agrégation des métriques dans un dictionnaire
            post_metric = {
                'post_id': post_urn,
                'post_type': post_type,
                'creation_date': content['creation_date'],
                'text': content['text'],
                'media_type': content['media_type'],
                'media_url': content['media_url'],
                'author': content['author'],
                'total_comments': comments_summary.get('aggregatedTotalComments', 0),
                'total_likes': likes_summary.get('aggregatedTotalLikes', 0),
                'impressions': share_stats.get('impressionCount', 0),
                'unique_impressions': share_stats.get('uniqueImpressionsCount', 0),
                'clicks': share_stats.get('clickCount', 0),
                'shares': share_stats.get('shareCount', 0),
                'engagement_rate': share_stats.get('engagement', 0)
            }
            
            # Ajouter les métriques à notre liste
            post_metrics.append(post_metric)
            
            # Petite pause pour éviter les rate limits
            time.sleep(1)
            
            # Log de progression
            if i % 5 == 0 or i == len(all_posts):
                print(f"Progression: {i}/{len(all_posts)} posts analysés")
        
        # Trier par date de création (plus récent au plus ancien)
        post_metrics.sort(key=lambda x: x['creation_date'] if x['creation_date'] else '', reverse=True)
        
        print(f"\nAnalyse terminée. {len(post_metrics)} posts analysés, {skipped_posts} posts ignorés.")
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
                print(f"Spreadsheet existant trouvé: {self.spreadsheet_name}")
            except gspread.exceptions.SpreadsheetNotFound:
                self.spreadsheet = self.client.create(self.spreadsheet_name)
                print(f"Nouveau spreadsheet créé: {self.spreadsheet_name}")
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée
                self.spreadsheet.share(self.admin_email, perm_type="user", role="writer")
                print(f"Accès en édition accordé à {self.admin_email}")
            
            return True
        except Exception as e:
            print(f"Erreur de connexion à Google Sheets: {e}")
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
                        print(f"Rôle mis à jour pour {self.admin_email} (writer)")
                    break
            
            # Si l'admin n'a pas encore accès, lui donner
            if not admin_has_access:
                self.spreadsheet.share(self.admin_email, perm_type="user", role="writer")
                print(f"Accès en édition accordé à {self.admin_email}")
                
        except Exception as e:
            print(f"Erreur lors de la vérification des permissions: {e}")
    
    def update_post_metrics_sheet(self, post_metrics):
        """Met à jour la feuille des métriques de posts"""
        try:
            # Vérifier si la feuille existe ou la créer
            worksheet_name = "Métriques des Posts"
            
            try:
                worksheet = self.spreadsheet.worksheet(worksheet_name)
                print(f"Feuille '{worksheet_name}' trouvée")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = self.spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                print(f"Nouvelle feuille '{worksheet_name}' créée")
            
            # Définir les en-têtes
            headers = [
                "Post ID",
                "Type de post",
                "Date de création",
                "Texte du post",
                "Type de média",
                "URL du média",
                "Auteur",
                "Nombre de commentaires",
                "Nombre de likes",
                "Impressions",
                "Impressions uniques",
                "Clics",
                "Partages",
                "Taux d'engagement (%)"
            ]
            
            # CORRECTION: Ordre des arguments pour worksheet.update
            # Mettre à jour les en-têtes
            worksheet.update(values=[headers], range_name='A1')
            
            # Formater les en-têtes
            worksheet.format('A1:' + get_column_letter(len(headers)-1) + '1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
            })
            
            # Préparer les données
            rows = []
            for post in post_metrics:
                # Limiter la longueur du texte pour éviter les problèmes avec Google Sheets
                text = post['text']
                if len(text) > 1000:
                    text = text[:997] + "..."
                
                # Calculer le taux d'engagement en pourcentage
                engagement_rate = post['engagement_rate'] * 100 if isinstance(post['engagement_rate'], (int, float)) else 0
                
                row = [
                    post['post_id'],
                    post['post_type'],  # Type de post
                    post['creation_date'],
                    text,
                    post['media_type'],
                    post['media_url'],
                    post['author'],
                    post['total_comments'],
                    post['total_likes'],
                    post['impressions'],
                    post['unique_impressions'],
                    post['clicks'],
                    post['shares'],
                    engagement_rate
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
                    # CORRECTION: Ordre des arguments pour worksheet.update
                    worksheet.update(values=batch, range_name=f'A{start_row}')
                    print(f"Lot {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} exporté ({len(batch)} lignes)")
                    # Pause entre les lots
                    if i + batch_size < len(rows):
                        time.sleep(2)
                
                print(f"Données mises à jour dans la feuille '{worksheet_name}'")
                
                # CORRECTION: Utiliser 'des' au lieu de 'desc' pour le tri
                try:
                    # Trier les données par date (du plus récent au plus ancien)
                    # Notez que certaines versions de gspread utilisent 'des' et d'autres 'desc'
                    try:
                        worksheet.sort((3, 'des'), range=f'A2:{get_column_letter(len(headers)-1)}{len(rows)+1}')
                        print("Données triées par date (du plus récent au plus ancien)")
                    except ValueError as e:
                        if "should be specified as sort order" in str(e):
                            # Essayer l'autre format
                            worksheet.sort((3, 'desc'), range=f'A2:{get_column_letter(len(headers)-1)}{len(rows)+1}')
                            print("Données triées par date (du plus récent au plus ancien)")
                        else:
                            # Une autre erreur
                            raise e
                except Exception as sort_error:
                    print(f"Avertissement: Impossible de trier les données: {sort_error}")
                    print("Les données ont été importées mais ne sont pas triées.")
            else:
                print("Aucune donnée à exporter")
            
            return True
            
        except Exception as e:
            print(f"Erreur lors de l'exportation des métriques: {e}")
            traceback.print_exc()
            return False
    
    def export_post_metrics(self, post_metrics):
        """Exporte les métriques des posts vers Google Sheets"""
        if not self.connect():
            print("Impossible de se connecter à Google Sheets")
            return False
        
        # Vérifier les permissions
        self.ensure_admin_access()
        
        # Mettre à jour la feuille
        return self.update_post_metrics_sheet(post_metrics)

def main():
    try:
        print("\n=== LINKEDIN POST METRICS TRACKER ===\n")
        
        # Récupération des tokens et paramètres
        access_token = os.getenv("COMMUNITY_LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN") or "AQVjbxSk2seZx-2ZJpqncNAnIZlRrPnBQAg43SIxgN4XMh77rtc8tYla9MjXPm9vcQCsgH5y527EoNA6EslRpdKapOPMoHSQqqteyPxfqetqFyISr6BfG80R9b4uLvp_1eQtx3xGoKCkfQw0Q5Y1LI_k9Rc8UKqzMFEn16UZezijH2zCkMtw_OkYXV6Mz6o57GOhe18YlmLbiBxN5tNfO1T9tDwGzrNBg8FW7T3-kKeJGnRRgg-MrxV8Rv6_g27neegijzOVQn-EwtC9vKCoNlup0eKD65wnHYI-d-PzHouE8ri2bypfvSdaVqH2FQsNowM6KA0r-tfQlCMjIG0rpZXLqOZB6w"
        portability_token = os.getenv("PORTABILITY_LINKEDIN_TOKEN") or "AQVJfStgImQgSc2rPYePM6Fw5eLtOWZpysFz9K5TjB3eIEWhbCUpf4mjEkBY3-__Wnh8YJIXFk2VvArtf_cc8atQIHkM5upPGoC6vYC1ObZ8gik2Kks96sh-JSe8_7ZjMN7Zwlk_t7SFjv2ZVCHA0MdRK2UTk76QxYVBCIk9RigwF8W92OIEluKQiRqE28cfvtpY6L_4zJ58z3mktOrPtjGu8vmYYy8o7BoZ6ulEBCRHv2wVzvaR2I5fvp-hrF_gNcLM4SRujMUdQtcvLwpXqBGlSdKrPbxRCPE4_j_mK6zYc0TzQVgg2KDjQZFy8yQtHj43__jtEGdgK0QVFj_VxmT7CBPgpQ"
        organization_id = os.getenv("LINKEDIN_ORGANIZATION_ID") or "51699835"
        sheet_name = os.getenv("GOOGLE_SHEET_NAME") or f"LinkedIn_Post_Metrics_{organization_id}"
        admin_email = os.getenv("ADMIN_EMAIL") or "byteberry.analytics@gmail.com"
        
        if not access_token or not portability_token:
            print("Erreur: Tokens LinkedIn manquants")
            sys.exit(1)
        
        # Vérification du token
        print("\n--- Vérification du token ---")
        is_valid, user_info = verify_token(access_token)
        
        if not is_valid:
            print("❌ Token invalide. Vérifiez vos credentials.")
            sys.exit(1)
        
        print("✅ Token valide!")
        
        # Initialisation du tracker
        print("\n--- Initialisation du tracker de métriques ---")
        metrics_tracker = LinkedInPostMetricsTracker(access_token, portability_token, organization_id)
        
        # Récupération des métriques
        print("\n--- Récupération des métriques des posts LinkedIn ---")
        post_metrics = metrics_tracker.get_all_post_metrics()
        
        if not post_metrics:
            print("❌ Aucune métrique à exporter")
            sys.exit(1)
        
        print(f"✅ Métriques récupérées pour {len(post_metrics)} posts")
        
        # Export vers Google Sheets
        print("\n--- Export vers Google Sheets ---")
        
        # Chemin vers les credentials
        base_dir = Path(__file__).resolve().parent
        credentials_path = base_dir / 'credentials' / 'service_account_credentials.json'
        
        if not credentials_path.exists():
            alt_path = base_dir.parent.parent / 'credentials' / 'service_account_credentials.json'
            if alt_path.exists():
                credentials_path = alt_path
            else:
                print(f"❌ Fichier de credentials Google non trouvé à {credentials_path}")
                print("Assurez-vous de créer le dossier 'credentials' et d'y placer votre fichier 'service_account_credentials.json'")
                sys.exit(1)
        
        exporter = GoogleSheetsExporter(sheet_name, credentials_path, admin_email)
        success = exporter.export_post_metrics(post_metrics)
        
        if success:
            print("✅ Export réussi!")
            sheet_url = f"https://docs.google.com/spreadsheets/d/{exporter.spreadsheet.id}"
            print(f"URL du tableau: {sheet_url}")
        else:
            print("❌ Échec de l'export")
            
    except KeyboardInterrupt:
        print("\nInterruption utilisateur, arrêt du script.")
    except Exception as e:
        print(f"Une erreur s'est produite: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()