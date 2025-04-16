#!/usr/bin/env python3
"""
LinkedIn Statistics Tracker
Ce script collecte les statistiques quotidiennes des vues de page, followers et partages LinkedIn
et les enregistre dans Google Sheets avec gestion des quotas API.
"""

import os
import requests
import urllib.parse
import json
import random
from datetime import datetime, timedelta
import time
from dateutil import parser
import pytz

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import sys
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

class LinkedInDailyPageStatisticsTracker:
    """Classe pour suivre les statistiques quotidiennes des vues de page LinkedIn"""
    
    def __init__(self, access_token, organization_id, days_history=365, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.days_history = days_history
        self.sheet_name = sheet_name or f"LinkedIn_Daily_Stats_{organization_id}"
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_daily_page_statistics(self):
        """Obtient les statistiques quotidiennes de vues de page pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Calculer les timestamps (millisecondes depuis l'époque)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_history)
        
        # Convertir en millisecondes depuis l'époque
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        # Construire l'URL avec le format RESTli 2.0
        url = (f"{self.base_url}/organizationPageStatistics?q=organization&"
               f"organization={encoded_urn}&"
               f"timeIntervals=(timeRange:(start:{start_timestamp},end:{end_timestamp}),"
               f"timeGranularityType:DAY)")
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Données de statistiques quotidiennes des pages récupérées avec succès")
                    return data
                    
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
        
        print("Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des pages.")
        return None
    
    def parse_daily_page_statistics(self, data):
        """Analyse les données quotidiennes de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("Aucune donnée de statistiques quotidiennes de pages valide trouvée.")
            return daily_stats
        
        # Parcourir chaque élément (un par jour)
        for element in data['elements']:
            # Ignorer les éléments sans plage de temps ou statistiques
            if 'timeRange' not in element or 'totalPageStatistics' not in element:
                continue
            
            time_range = element['timeRange']
            stats = element['totalPageStatistics']
            
            # Convertir le timestamp en date lisible
            # Le timestamp est en millisecondes depuis l'époque
            start_date = datetime.fromtimestamp(time_range['start'] / 1000)
            date_str = start_date.strftime('%Y-%m-%d')
            
            # Extraire les vues pour chaque type
            views = stats.get('views', {})
            
            # Extraire les données de vues pertinentes
            all_page_views = views.get('allPageViews', {}).get('pageViews', 0)
            unique_page_views = views.get('allPageViews', {}).get('uniquePageViews', 0)
            
            all_desktop_views = views.get('allDesktopPageViews', {}).get('pageViews', 0)
            unique_desktop_views = views.get('allDesktopPageViews', {}).get('uniquePageViews', 0)
            
            all_mobile_views = views.get('allMobilePageViews', {}).get('pageViews', 0)
            unique_mobile_views = views.get('allMobilePageViews', {}).get('uniquePageViews', 0)
            
            # Détails par type de page
            overview_views = views.get('overviewPageViews', {}).get('pageViews', 0)
            unique_overview_views = views.get('overviewPageViews', {}).get('uniquePageViews', 0)
            
            about_views = views.get('aboutPageViews', {}).get('pageViews', 0)
            unique_about_views = views.get('aboutPageViews', {}).get('uniquePageViews', 0)
            
            people_views = views.get('peoplePageViews', {}).get('pageViews', 0)
            unique_people_views = views.get('peoplePageViews', {}).get('uniquePageViews', 0)
            
            jobs_views = views.get('jobsPageViews', {}).get('pageViews', 0)
            unique_jobs_views = views.get('jobsPageViews', {}).get('uniquePageViews', 0)
            
            careers_views = views.get('careersPageViews', {}).get('pageViews', 0)
            unique_careers_views = views.get('careersPageViews', {}).get('uniquePageViews', 0)
            
            # Nouveaux champs extraits de la documentation
            desktop_careers_views = views.get('desktopCareersPageViews', {}).get('pageViews', 0)
            desktop_jobs_views = views.get('desktopJobsPageViews', {}).get('pageViews', 0)
            desktop_overview_views = views.get('desktopOverviewPageViews', {}).get('pageViews', 0)
            desktop_life_at_views = views.get('desktopLifeAtPageViews', {}).get('pageViews', 0)
            
            mobile_careers_views = views.get('mobileCareersPageViews', {}).get('pageViews', 0)
            mobile_jobs_views = views.get('mobileJobsPageViews', {}).get('pageViews', 0)
            mobile_overview_views = views.get('mobileOverviewPageViews', {}).get('pageViews', 0)
            mobile_life_at_views = views.get('mobileLifeAtPageViews', {}).get('pageViews', 0)
            
            life_at_views = views.get('lifeAtPageViews', {}).get('pageViews', 0)
            unique_life_at_views = views.get('lifeAtPageViews', {}).get('uniquePageViews', 0)
            
            # Extraire les données des clics sur les boutons personnalisés
            clicks = stats.get('clicks', {})
            desktop_custom_button_clicks = clicks.get('desktopCustomButtonClickCounts', [])
            mobile_custom_button_clicks = clicks.get('mobileCustomButtonClickCounts', [])
            
            # Calculer le total des clics sur les boutons personnalisés
            desktop_button_clicks_total = sum([click.get('count', 0) for click in desktop_custom_button_clicks]) if desktop_custom_button_clicks else 0
            mobile_button_clicks_total = sum([click.get('count', 0) for click in mobile_custom_button_clicks]) if mobile_custom_button_clicks else 0
            
            # Stocker les données du jour
            day_stats = {
                'date': date_str,
                # Vue générales
                'total_views': all_page_views,
                'unique_views': unique_page_views,
                # Vues desktop et mobile
                'desktop_views': all_desktop_views,
                'unique_desktop_views': unique_desktop_views,
                'mobile_views': all_mobile_views,
                'unique_mobile_views': unique_mobile_views,
                # Vues par type de page
                'overview_views': overview_views,
                'unique_overview_views': unique_overview_views,
                'about_views': about_views,
                'unique_about_views': unique_about_views,
                'people_views': people_views,
                'unique_people_views': unique_people_views,
                'jobs_views': jobs_views,
                'unique_jobs_views': unique_jobs_views,
                'careers_views': careers_views,
                'unique_careers_views': unique_careers_views,
                # Nouveaux champs détaillés
                'desktop_careers_views': desktop_careers_views,
                'desktop_jobs_views': desktop_jobs_views,
                'desktop_overview_views': desktop_overview_views,
                'desktop_life_at_views': desktop_life_at_views,
                'mobile_careers_views': mobile_careers_views,
                'mobile_jobs_views': mobile_jobs_views,
                'mobile_overview_views': mobile_overview_views,
                'mobile_life_at_views': mobile_life_at_views,
                'life_at_views': life_at_views,
                'unique_life_at_views': unique_life_at_views,
                # Clics sur les boutons personnalisés
                'desktop_button_clicks': desktop_button_clicks_total,
                'mobile_button_clicks': mobile_button_clicks_total,
                'total_button_clicks': desktop_button_clicks_total + mobile_button_clicks_total
            }
            
            daily_stats.append(day_stats)
        
        # Trier les statistiques par date (plus ancien au plus récent)
        daily_stats.sort(key=lambda x: x['date'], reverse=False)
        
        return daily_stats


class LinkedInFollowerStatisticsTracker:
    """Classe pour suivre les statistiques quotidiennes des followers LinkedIn"""
    
    def __init__(self, access_token, organization_id, days_history=365):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.days_history = min(days_history, 365)  # Maximum 12 mois selon la documentation
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_daily_follower_statistics(self):
        """Obtient les statistiques quotidiennes des followers pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Calculer les timestamps (millisecondes depuis l'époque)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_history)
        
        # Convertir en millisecondes depuis l'époque
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        # Construire l'URL avec le format RESTli 2.0
        url = (f"{self.base_url}/organizationalEntityFollowerStatistics?q=organizationalEntity&"
               f"organizationalEntity={encoded_urn}&"
               f"timeIntervals=(timeRange:(start:{start_timestamp},end:{end_timestamp}),"
               f"timeGranularityType:DAY)")
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Données de statistiques quotidiennes des followers récupérées avec succès")
                    return data
                    
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
        
        print("Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des followers.")
        return None
    
    def parse_daily_follower_statistics(self, data):
        """Analyse les données quotidiennes de followers de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("Aucune donnée de statistiques quotidiennes de followers valide trouvée.")
            return daily_stats
        
        # Parcourir chaque élément (un par jour)
        for element in data['elements']:
            # Ignorer les éléments sans plage de temps ou statistiques de followers
            if 'timeRange' not in element or 'followerGains' not in element:
                continue
            
            time_range = element['timeRange']
            follower_gains = element['followerGains']
            
            # Convertir le timestamp en date lisible
            # Le timestamp est en millisecondes depuis l'époque
            start_date = datetime.fromtimestamp(time_range['start'] / 1000)
            date_str = start_date.strftime('%Y-%m-%d')
            
            # Extraire les gains de followers
            organic_follower_gain = follower_gains.get('organicFollowerGain', 0)
            paid_follower_gain = follower_gains.get('paidFollowerGain', 0)
            
            # Stocker les données du jour
            day_stats = {
                'date': date_str,
                'organic_follower_gain': organic_follower_gain,
                'paid_follower_gain': paid_follower_gain,
                'total_follower_gain': organic_follower_gain + paid_follower_gain
            }
            
            daily_stats.append(day_stats)
        
        # Trier les statistiques par date (plus ancien au plus récent)
        daily_stats.sort(key=lambda x: x['date'], reverse=False)
        
        return daily_stats


class LinkedInShareStatisticsTracker:
    """Classe pour suivre les statistiques quotidiennes des partages LinkedIn"""
    
    def __init__(self, access_token, organization_id, days_history=365):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.days_history = min(days_history, 365)  # Maximum 12 mois selon la documentation
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_daily_share_statistics(self):
        """Obtient les statistiques quotidiennes des partages pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Calculer les timestamps (millisecondes depuis l'époque)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_history)
        
        # Convertir en millisecondes depuis l'époque
        start_timestamp = int(start_date.timestamp() * 1000)
        end_timestamp = int(end_date.timestamp() * 1000)
        
        # Construire l'URL avec le format RESTli 2.0
        url = (f"{self.base_url}/organizationalEntityShareStatistics?q=organizationalEntity&"
               f"organizationalEntity={encoded_urn}&"
               f"timeIntervals=(timeRange:(start:{start_timestamp},end:{end_timestamp}),"
               f"timeGranularityType:DAY)")
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Données de statistiques quotidiennes des partages récupérées avec succès")
                    return data
                    
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
        
        print("Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des partages.")
        return None
    
    def parse_daily_share_statistics(self, data):
        """Analyse les données quotidiennes de partages de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("Aucune donnée de statistiques quotidiennes de partages valide trouvée.")
            return daily_stats
        
        # Parcourir chaque élément (un par jour)
        for element in data['elements']:
            # Ignorer les éléments sans plage de temps ou statistiques
            if 'timeRange' not in element or 'totalShareStatistics' not in element:
                continue
            
            time_range = element['timeRange']
            share_stats = element['totalShareStatistics']
            
            # Convertir le timestamp en date lisible
            # Le timestamp est en millisecondes depuis l'époque
            start_date = datetime.fromtimestamp(time_range['start'] / 1000)
            date_str = start_date.strftime('%Y-%m-%d')
            
            # Extraire les métriques des partages
            click_count = share_stats.get('clickCount', 0)
            engagement = share_stats.get('engagement', 0)
            like_count = share_stats.get('likeCount', 0)
            comment_count = share_stats.get('commentCount', 0)
            share_count = share_stats.get('shareCount', 0)
            impression_count = share_stats.get('impressionCount', 0)
            unique_impressions_count = share_stats.get('uniqueImpressionsCount', 0)
            
            # Extraire les métriques additionnelles si disponibles
            share_mentions_count = share_stats.get('shareMentionsCount', 0)
            comment_mentions_count = share_stats.get('commentMentionsCount', 0)
            
            # Stocker les données du jour
            day_stats = {
                'date': date_str,
                'click_count': click_count,
                'engagement': engagement,
                'like_count': like_count,
                'comment_count': comment_count,
                'share_count': share_count,
                'impression_count': impression_count,
                'unique_impressions_count': unique_impressions_count,
                'share_mentions_count': share_mentions_count,
                'comment_mentions_count': comment_mentions_count
            }
            
            daily_stats.append(day_stats)
        
        # Trier les statistiques par date (plus ancien au plus récent)
        daily_stats.sort(key=lambda x: x['date'], reverse=False)
        
        return daily_stats


class GoogleSheetsExporter:
    """Classe pour exporter les données vers Google Sheets avec gestion améliorée des quotas"""
    
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
            
            # Vérifier si Sheet1 existe et le renommer en "Statistiques quotidiennes"
            try:
                sheet1 = self.spreadsheet.worksheet("Sheet1")
                sheet1.update_title("Statistiques quotidiennes")
                print("Feuille 'Sheet1' renommée en 'Statistiques quotidiennes'")
            except gspread.exceptions.WorksheetNotFound:
                pass  # Sheet1 n'existe pas, pas besoin de la renommer
            
            return True
        except Exception as e:
            print(f"Erreur de connexion à Google Sheets: {e}")
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
    
    def wait_with_backoff(self, base_delay=1, max_delay=60, factor=2, jitter=0.1):
        """Implémente une attente avec backoff exponentiel et jitter aléatoire"""
        delay = base_delay + random.uniform(0, jitter * base_delay)
        time.sleep(delay)
        # Retourne le prochain délai à utiliser en cas de nouvelle erreur
        next_delay = min(delay * factor, max_delay)
        return next_delay
    
    def api_request_with_retry(self, api_func, *args, max_retries=5, initial_delay=1, **kwargs):
        """Exécute une requête API avec retry et backoff exponentiel en cas d'erreur"""
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return api_func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if "429" in str(e):  # Quota exceeded
                    print(f"Quota API dépassé (tentative {attempt+1}/{max_retries}). Attente de {delay:.1f}s...")
                    delay = self.wait_with_backoff(base_delay=delay)
                    if attempt == max_retries - 1:
                        print("Nombre maximum de tentatives atteint. Échec de l'opération.")
                        raise
                else:
                    # Autres erreurs d'API, retry mais avec un log différent
                    print(f"Erreur API (tentative {attempt+1}/{max_retries}): {e}. Attente de {delay:.1f}s...")
                    delay = self.wait_with_backoff(base_delay=delay)
                    if attempt == max_retries - 1:
                        raise
            except Exception as e:
                # Autres exceptions génériques, ne pas retry
                print(f"Erreur non récupérable: {e}")
                raise
        
        # Si on arrive ici, c'est qu'on a épuisé toutes les tentatives
        raise Exception("Nombre maximum de tentatives atteint. Échec de l'opération.")
    
    def save_progress(self, processed_dates):
        """Sauvegarde l'état de progression dans un fichier pour pouvoir reprendre plus tard"""
        try:
            with open('linkedin_stats_progress.json', 'w') as f:
                json.dump({"processed_dates": list(processed_dates)}, f)
            print(f"Progression sauvegardée: {len(processed_dates)} dates traitées")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde de la progression: {e}")
    
    def load_progress(self):
        """Charge l'état de progression depuis un fichier"""
        try:
            if os.path.exists('linkedin_stats_progress.json'):
                with open('linkedin_stats_progress.json', 'r') as f:
                    data = json.load(f)
                processed_dates = set(data.get("processed_dates", []))
                print(f"Progression chargée: {len(processed_dates)} dates déjà traitées")
                return processed_dates
        except Exception as e:
            print(f"Erreur lors du chargement de la progression (reprise à zéro): {e}")
        return set()
    
    def merge_all_stats(self, page_stats, follower_stats, share_stats):
        """Combine les statistiques de pages, followers et partages par date"""
        merged_stats = {}
        
        # D'abord, ajouter toutes les statistiques de pages par date
        for stat in page_stats:
            date = stat['date']
            merged_stats[date] = stat.copy()
            
            # Initialiser les champs de followers à 0
            merged_stats[date]['organic_follower_gain'] = 0
            merged_stats[date]['paid_follower_gain'] = 0
            merged_stats[date]['total_follower_gain'] = 0
            
            # Initialiser les champs de partages à 0
            merged_stats[date]['click_count'] = 0
            merged_stats[date]['engagement'] = 0
            merged_stats[date]['like_count'] = 0
            merged_stats[date]['comment_count'] = 0
            merged_stats[date]['share_count'] = 0
            merged_stats[date]['impression_count'] = 0
            merged_stats[date]['unique_impressions_count'] = 0
            merged_stats[date]['share_mentions_count'] = 0
            merged_stats[date]['comment_mentions_count'] = 0
        
        # Ensuite, ajouter ou mettre à jour avec les statistiques de followers
        for stat in follower_stats:
            date = stat['date']
            if date in merged_stats:
                # Mettre à jour une entrée existante
                merged_stats[date]['organic_follower_gain'] = stat['organic_follower_gain']
                merged_stats[date]['paid_follower_gain'] = stat['paid_follower_gain']
                merged_stats[date]['total_follower_gain'] = stat['total_follower_gain']
            else:
                # Créer une nouvelle entrée (avec des vues à 0)
                new_entry = {
                    'date': date,
                    'total_views': 0,
                    'unique_views': 0,
                    'desktop_views': 0,
                    'unique_desktop_views': 0,
                    'mobile_views': 0,
                    'unique_mobile_views': 0,
                    'overview_views': 0,
                    'unique_overview_views': 0,
                    'about_views': 0,
                    'unique_about_views': 0,
                    'people_views': 0,
                    'unique_people_views': 0,
                    'jobs_views': 0,
                    'unique_jobs_views': 0,
                    'careers_views': 0,
                    'unique_careers_views': 0,
                    'desktop_careers_views': 0,
                    'desktop_jobs_views': 0,
                    'desktop_overview_views': 0,
                    'desktop_life_at_views': 0,
                    'mobile_careers_views': 0,
                    'mobile_jobs_views': 0,
                    'mobile_overview_views': 0,
                    'mobile_life_at_views': 0,
                    'life_at_views': 0,
                    'unique_life_at_views': 0,
                    'desktop_button_clicks': 0,
                    'mobile_button_clicks': 0,
                    'total_button_clicks': 0,
                    'organic_follower_gain': stat['organic_follower_gain'],
                    'paid_follower_gain': stat['paid_follower_gain'],
                    'total_follower_gain': stat['total_follower_gain'],
                    'click_count': 0,
                    'engagement': 0,
                    'like_count': 0,
                    'comment_count': 0,
                    'share_count': 0,
                    'impression_count': 0,
                    'unique_impressions_count': 0,
                    'share_mentions_count': 0,
                    'comment_mentions_count': 0
                }
                merged_stats[date] = new_entry
        
        # Finalement, ajouter ou mettre à jour avec les statistiques de partages
        for stat in share_stats:
            date = stat['date']
            if date in merged_stats:
                # Mettre à jour une entrée existante
                merged_stats[date]['click_count'] = stat['click_count']
                merged_stats[date]['engagement'] = stat['engagement']
                merged_stats[date]['like_count'] = stat['like_count']
                merged_stats[date]['comment_count'] = stat['comment_count']
                merged_stats[date]['share_count'] = stat['share_count']
                merged_stats[date]['impression_count'] = stat['impression_count']
                merged_stats[date]['unique_impressions_count'] = stat['unique_impressions_count']
                # Ajout des nouveaux champs
                merged_stats[date]['share_mentions_count'] = stat.get('share_mentions_count', 0)
                merged_stats[date]['comment_mentions_count'] = stat.get('comment_mentions_count', 0)
            else:
                # Créer une nouvelle entrée (avec des vues et followers à 0)
                new_entry = {
                    'date': date,
                    'total_views': 0,
                    'unique_views': 0,
                    'desktop_views': 0,
                    'unique_desktop_views': 0,
                    'mobile_views': 0,
                    'unique_mobile_views': 0,
                    'overview_views': 0,
                    'unique_overview_views': 0,
                    'about_views': 0,
                    'unique_about_views': 0,
                    'people_views': 0,
                    'unique_people_views': 0,
                    'jobs_views': 0,
                    'unique_jobs_views': 0,
                    'careers_views': 0,
                    'unique_careers_views': 0,
                    'desktop_careers_views': 0,
                    'desktop_jobs_views': 0,
                    'desktop_overview_views': 0,
                    'desktop_life_at_views': 0,
                    'mobile_careers_views': 0,
                    'mobile_jobs_views': 0,
                    'mobile_overview_views': 0,
                    'mobile_life_at_views': 0,
                    'life_at_views': 0,
                    'unique_life_at_views': 0,
                    'desktop_button_clicks': 0,
                    'mobile_button_clicks': 0,
                    'total_button_clicks': 0,
                    'organic_follower_gain': 0,
                    'paid_follower_gain': 0,
                    'total_follower_gain': 0,
                    'click_count': stat['click_count'],
                    'engagement': stat['engagement'],
                    'like_count': stat['like_count'],
                    'comment_count': stat['comment_count'],
                    'share_count': stat['share_count'],
                    'impression_count': stat['impression_count'],
                    'unique_impressions_count': stat['unique_impressions_count'],
                    'share_mentions_count': stat.get('share_mentions_count', 0),
                    'comment_mentions_count': stat.get('comment_mentions_count', 0)
                }
                merged_stats[date] = new_entry
        
        # Convertir le dictionnaire en liste et trier par date
        merged_list = list(merged_stats.values())
        merged_list.sort(key=lambda x: x['date'], reverse=False)
        
        return merged_list
    
    def update_daily_stats_sheet(self, combined_stats):
        """Met à jour la feuille des statistiques quotidiennes avec gestion améliorée des quotas API"""
        try:
            # Charger l'état de progression antérieur si disponible
            processed_dates = self.load_progress()
            
            # Vérifier si la feuille existe ou la créer
            try:
                sheet = self.spreadsheet.worksheet("Statistiques quotidiennes")
                print("Feuille 'Statistiques quotidiennes' trouvée")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet1")
                    self.api_request_with_retry(sheet.update_title, "Statistiques quotidiennes")
                    print("Feuille par défaut 'Sheet1' renommée en 'Statistiques quotidiennes'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = self.api_request_with_retry(
                        self.spreadsheet.add_worksheet, 
                        title="Statistiques quotidiennes", 
                        rows=500, 
                        cols=50  # Augmentation du nombre de colonnes pour les nouvelles métriques
                    )
                    print("Nouvelle feuille 'Statistiques quotidiennes' créée")
            
            # Récupérer les données existantes avec des retries si nécessaire
            existing_data = self.api_request_with_retry(sheet.get_all_values)
            has_headers = len(existing_data) > 0
            
            # Créer les en-têtes si nécessaire
            headers = [
                "Date",
                
                # --- Vues de page ---
                # Vues générales
                "Vues totales (allPageViews)", 
                "Vues uniques (uniquePageViews)", 
                
                # Vues par appareil
                "Vues Desktop (allDesktopPageViews)", 
                "Vues Desktop uniques (uniqueDesktopPageViews)", 
                "Vues Mobile (allMobilePageViews)",
                "Vues Mobile uniques (uniqueMobilePageViews)",
                
                # Vues par section - Aperçu
                "Vues Accueil (overviewPageViews)", 
                "Vues Accueil uniques (uniqueOverviewPageViews)",
                "Vues Accueil Desktop (desktopOverviewPageViews)",
                "Vues Accueil Mobile (mobileOverviewPageViews)",
                
                # Vues par section - À propos
                "Vues À propos (aboutPageViews)", 
                "Vues À propos uniques (uniqueAboutPageViews)",
                
                # Vues par section - Personnes
                "Vues Personnes (peoplePageViews)", 
                "Vues Personnes uniques (uniquePeoplePageViews)",
                
                # Vues par section - Emplois
                "Vues Emplois (jobsPageViews)", 
                "Vues Emplois uniques (uniqueJobsPageViews)",
                "Vues Emplois Desktop (desktopJobsPageViews)",
                "Vues Emplois Mobile (mobileJobsPageViews)",
                
                # Vues par section - Carrières
                "Vues Carrières (careersPageViews)", 
                "Vues Carrières uniques (uniqueCareersPageViews)",
                "Vues Carrières Desktop (desktopCareersPageViews)",
                "Vues Carrières Mobile (mobileCareersPageViews)",
                
                # Vues par section - Vie en entreprise
                "Vues Vie en entreprise (lifeAtPageViews)",
                "Vues Vie en entreprise uniques (uniqueLifeAtPageViews)",
                "Vues Vie en entreprise Desktop (desktopLifeAtPageViews)",
                "Vues Vie en entreprise Mobile (mobileLifeAtPageViews)",
                
                # Clics sur boutons
                "Clics sur boutons Desktop (desktopCustomButtonClickCounts)",
                "Clics sur boutons Mobile (mobileCustomButtonClickCounts)",
                "Total clics sur boutons",
                
                # --- Followers ---
                "Nouveaux followers organiques (organicFollowerGain)",
                "Nouveaux followers payants (paidFollowerGain)",
                "Total nouveaux followers (totalFollowerGain)",
                
                # --- Partages ---
                "Nombre de clics (clickCount)",
                "Taux d'engagement (engagement)",
                "Nombre de J'aime (likeCount)",
                "Nombre de commentaires (commentCount)",
                "Nombre de partages (shareCount)",
                "Mentions dans partages (shareMentionsCount)",
                "Mentions dans commentaires (commentMentionsCount)",
                "Nombre d'impressions (impressionCount)",
                "Nombre d'impressions uniques (uniqueImpressionsCount)"
            ]
            
            # S'assurer que la première ligne contient les en-têtes
            if not has_headers or len(existing_data) == 0 or len(existing_data[0]) == 0 or existing_data[0][0] == '':
                print("Ajout des en-têtes en première ligne")
                # Ajouter les en-têtes en une seule opération avec retry
                self.api_request_with_retry(sheet.update, values=[headers], range_name='A1')
                
                # Formater les en-têtes
                last_col = get_column_letter(len(headers) - 1)
                header_range = f'A1:{last_col}1'
                self.api_request_with_retry(sheet.format, header_range, {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                })
                
                # Pour le cas où les en-têtes viennent d'être ajoutés
                existing_data = self.api_request_with_retry(sheet.get_all_values)
                if len(existing_data) <= 1:
                    existing_dates = []
                else:
                    existing_dates = [row[0] for row in existing_data[1:]]
            else:
                # Récupérer les dates existantes (colonne A)
                if len(existing_data) > 1:  # Si des données existent au-delà des en-têtes
                    existing_dates = [row[0] for row in existing_data[1:]]
                else:
                    existing_dates = []
                    
                # Vérifier si les en-têtes correspondent à ce qu'on attend
                if len(existing_data[0]) < len(headers):
                    print("Mise à jour des en-têtes pour correspondre au format attendu")
                    self.api_request_with_retry(sheet.update, values=[headers], range_name='A1')
                    
                    # Formater les en-têtes
                    last_col = get_column_letter(len(headers) - 1)
                    header_range = f'A1:{last_col}1'
                    self.api_request_with_retry(sheet.format, header_range, {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                    })
            
            # Préparer les mises à jour en lots
            updates_by_date = {}
            new_rows = []
            
            # Filtrer les statistiques déjà traitées (pour reprise)
            combined_stats_filtered = [stat for stat in combined_stats if stat['date'] not in processed_dates]
            print(f"Statistiques à traiter: {len(combined_stats_filtered)}/{len(combined_stats)} (les autres ont déjà été traitées)")
            
            for day_stats in combined_stats_filtered:
                date = day_stats['date']
                
                # Préparation des données de ligne
                row_data = [
                    date,
                    
                    # Vues générales
                    day_stats['total_views'],
                    day_stats['unique_views'],
                    
                    # Vues par appareil
                    day_stats['desktop_views'],
                    day_stats['unique_desktop_views'],
                    day_stats['mobile_views'],
                    day_stats['unique_mobile_views'],
                    
                    # Vues par section - Aperçu
                    day_stats['overview_views'],
                    day_stats['unique_overview_views'],
                    day_stats['desktop_overview_views'],
                    day_stats['mobile_overview_views'],
                    
                    # Vues par section - À propos
                    day_stats['about_views'],
                    day_stats['unique_about_views'],
                    
                    # Vues par section - Personnes
                    day_stats['people_views'],
                    day_stats['unique_people_views'],
                    
                    # Vues par section - Emplois
                    day_stats['jobs_views'],
                    day_stats['unique_jobs_views'],
                    day_stats['desktop_jobs_views'],
                    day_stats['mobile_jobs_views'],
                    
                    # Vues par section - Carrières
                    day_stats['careers_views'],
                    day_stats['unique_careers_views'],
                    day_stats['desktop_careers_views'],
                    day_stats['mobile_careers_views'],
                    
                    # Vues par section - Vie en entreprise
                    day_stats['life_at_views'],
                    day_stats['unique_life_at_views'],
                    day_stats['desktop_life_at_views'],
                    day_stats['mobile_life_at_views'],
                    
                    # Clics sur boutons
                    day_stats['desktop_button_clicks'],
                    day_stats['mobile_button_clicks'],
                    day_stats['total_button_clicks'],
                    
                    # Followers
                    day_stats['organic_follower_gain'],
                    day_stats['paid_follower_gain'],
                    day_stats['total_follower_gain'],
                    
                    # Partages
                    day_stats['click_count'],
                    day_stats['engagement'],
                    day_stats['like_count'],
                    day_stats['comment_count'],
                    day_stats['share_count'],
                    day_stats.get('share_mentions_count', 0),
                    day_stats.get('comment_mentions_count', 0),
                    day_stats['impression_count'],
                    day_stats['unique_impressions_count']
                ]
                
                # Vérifier si cette date existe déjà
                if date in existing_dates:
                    # Stocker pour mise à jour ultérieure
                    row_index = existing_dates.index(date) + 2  # +2 car on a l'index 0 et l'en-tête
                    updates_by_date[row_index] = row_data
                else:
                    # Stocker pour ajout ultérieur
                    new_rows.append(row_data)
            
            # 1. Ajouter de nouvelles lignes en lots
            if new_rows:
                print(f"Ajout de {len(new_rows)} nouvelles dates en lot")
                # Réduire à des lots plus petits (12 au lieu de 15)
                batch_size = 12
                for i in range(0, len(new_rows), batch_size):
                    batch = new_rows[i:i+batch_size]
                    # S'assurer d'avoir au moins une ligne d'en-tête avant d'ajouter
                    first_empty_row = len(existing_data) + 1
                    if first_empty_row == 1:  # Si aucune donnée n'existe encore
                        first_empty_row = 2  # Commencer à la ligne 2 (après les en-têtes)
                    
                    try:
                        # Calculer le range pour l'ajout
                        last_col = get_column_letter(len(headers) - 1)
                        range_name = f'A{first_empty_row}:{last_col}{first_empty_row + len(batch) - 1}'
                        self.api_request_with_retry(sheet.update, values=batch, range_name=range_name)
                        
                        # Mettre à jour les variables pour la prochaine itération
                        existing_data.extend(batch)
                        
                        print(f"Lot {i//batch_size + 1}/{(len(new_rows)-1)//batch_size + 1} ajouté ({len(batch)} lignes)")
                        
                        # Ajouter les dates traitées à la liste de progression
                        for row in batch:
                            processed_dates.add(row[0])  # Ajouter la date (première colonne)
                        
                        # Sauvegarder la progression régulièrement
                        if i % (batch_size * 2) == 0:
                            self.save_progress(processed_dates)
                        
                        # Attendre entre les lots (7s)
                        time.sleep(7)
                    except Exception as e:
                        print(f"Erreur lors de l'ajout du lot {i//batch_size + 1}: {e}")
                        # Sauvegarder l'état actuel avant de continuer
                        self.save_progress(processed_dates)
            
            # 2. Mettre à jour les lignes existantes en lots
            if updates_by_date:
                print(f"Mise à jour de {len(updates_by_date)} dates existantes en lots")
                # Convertir en liste pour faciliter le traitement par lots
                updates_list = list(updates_by_date.items())
                
                # Approche alternative: mise à jour par colonne plutôt que par ligne
                try:
                    # Organiser les données par colonne
                    print("Utilisation d'une approche de mise à jour par colonnes pour éviter les problèmes de plage")
                    
                    # Obtenir tous les indices de ligne
                    row_indices = [row_idx for row_idx, _ in updates_list]
                    
                    # Pour chaque colonne, mettre à jour toutes les lignes
                    for col_idx in range(len(headers)):
                        col_letter = get_column_letter(col_idx)  # A, B, C, etc.
                        col_values = [[updates_by_date[row_idx][col_idx]] for row_idx in row_indices]
                        
                        # Créer des lots de lignes pour la colonne actuelle
                        batch_size = 20  # Plus de lignes par lot car une seule colonne
                        for i in range(0, len(row_indices), batch_size):
                            batch_rows = row_indices[i:i+batch_size]
                            batch_values = col_values[i:i+batch_size]
                            
                            # Création de la plage pour le lot actuel (une seule colonne)
                            first_row = batch_rows[0]
                            last_row = batch_rows[-1]
                            range_name = f'{col_letter}{first_row}:{col_letter}{last_row}'
                            
                            print(f"Mise à jour de la colonne {col_letter} (lignes {first_row}-{last_row}), {i//batch_size + 1}/{(len(row_indices)-1)//batch_size + 1}")
                            
                            try:
                                self.api_request_with_retry(sheet.update, values=batch_values, range_name=range_name)
                                time.sleep(2)  # Attendre entre les lots de colonnes
                                
                                # Marquer toutes les dates du lot comme traitées si c'est la dernière colonne
                                if col_idx == len(headers) - 1:
                                    for row_idx in batch_rows:
                                        processed_dates.add(updates_by_date[row_idx][0])  # La date est dans la première colonne
                                    
                                    # Sauvegarder la progression
                                    self.save_progress(processed_dates)
                            except Exception as e:
                                print(f"Erreur lors de la mise à jour de la colonne {col_letter}, lot {i//batch_size + 1}: {e}")
                                # Continue avec la colonne suivante
                        
                        # Attendre plus longtemps entre les colonnes
                        time.sleep(5)
                except Exception as e:
                    print(f"Erreur générale lors de la mise à jour par colonnes: {e}")
            
            # Sauvegarder la progression finale
            self.save_progress(processed_dates)
            
            # 3. Tri des données - optionnel et potentiellement coûteux en quotas
            sort_data = os.getenv("LINKEDIN_SORT_SHEET_DATA", "False").lower() == "true"
            
            if sort_data and len(existing_data) > 1 and (new_rows or updates_by_date):
                print("Tri des données par date (du plus ancien au plus récent)...")
                try:
                    # Récupérer toutes les données actualisées
                    updated_data = self.api_request_with_retry(sheet.get_all_values)
                    if len(updated_data) > 1:
                        # Exclure la ligne d'en-tête pour le tri
                        last_col = get_column_letter(len(headers) - 1)
                        data_range = f'A2:{last_col}{len(updated_data)}'
                        try:
                            self.api_request_with_retry(sheet.sort, (1, 'asc'), range=data_range)
                            print("Tri terminé")
                        except Exception as e:
                            print(f"Impossible de trier les données: {e}")
                            print("Le tri sera ignoré pour cette exécution")
                except Exception as e:
                    print(f"Erreur lors du tri des données: {e}")
            elif not sort_data:
                print("Tri des données désactivé (définissez LINKEDIN_SORT_SHEET_DATA=true pour l'activer)")
            
            print(f"Statistiques quotidiennes: {len(updates_by_date)} lignes mises à jour, {len(new_rows)} nouvelles lignes ajoutées")
            # Supprimer le fichier de progression si tout s'est bien passé
            if os.path.exists('linkedin_stats_progress.json'):
                os.remove('linkedin_stats_progress.json')
                print("Fichier de progression supprimé (traitement terminé avec succès)")
                
            return sheet
        except Exception as e:
            print(f"Erreur lors de la mise à jour de la feuille des statistiques quotidiennes: {e}")
            return None
    
    def add_combined_statistics(self, page_stats, follower_stats, share_stats):
        """Ajoute les statistiques quotidiennes combinées (pages, followers et partages)"""
        if not self.connect():
            print("Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Combiner les statistiques
        combined_stats = self.merge_all_stats(page_stats, follower_stats, share_stats)
        
        # Mettre à jour la feuille principale
        if not self.update_daily_stats_sheet(combined_stats):
            return False
        
        # URL du spreadsheet
        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
        print(f"URL du tableau: {sheet_url}")
        
        return True


def verify_token(access_token):
    """Vérifie si le token d'accès est valide"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version": "202312"
    }
    
    url = "https://api.linkedin.com/v2/me"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"Erreur {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)


if __name__ == "__main__":
    # Récupération des variables d'environnement
    access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
    organization_id = os.getenv("LINKEDIN_ORGANIZATION_ID", "")
    sheet_name = os.getenv("GOOGLE_SHEET_NAME_DAILY_STATS", "LinkedIn_Daily_Statistics")
    days_history = int(os.getenv("LINKEDIN_DAYS_HISTORY", "365"))

    if not access_token or not organization_id:
        print("Erreur: Variables d'environnement LINKEDIN_ACCESS_TOKEN ou LINKEDIN_ORGANIZATION_ID manquantes")
        print("Créez un fichier .env avec les variables:")
        print("LINKEDIN_ACCESS_TOKEN='votre_token'")
        print("LINKEDIN_ORGANIZATION_ID='votre_id_organisation'")
        print("GOOGLE_SHEET_NAME_DAILY_STATS='nom_de_votre_sheet'  # Optionnel")
        print("LINKEDIN_DAYS_HISTORY=365  # Optionnel, nombre de jours d'historique à récupérer")
        print("LINKEDIN_SORT_SHEET_DATA=False  # Optionnel, désactive le tri final coûteux en quotas")
        sys.exit(1)
    
    # Vérification du token
    print("\n--- Vérification du token ---")
    is_valid, result = verify_token(access_token)
    
    if is_valid:
        print("✅ Token valide!")
    else:
        print(f"❌ Token invalide: {result}")
        sys.exit(1)

    # 1. Obtention des statistiques quotidiennes de vues de page
    print(f"\n--- Récupération des statistiques quotidiennes de pages (sur {days_history} jours) ---")
    page_tracker = LinkedInDailyPageStatisticsTracker(access_token, organization_id, days_history, sheet_name)
    raw_page_stats = page_tracker.get_daily_page_statistics()
    
    if raw_page_stats:
        # Traitement des données de vues de page
        print("Analyse des données statistiques quotidiennes de pages...")
        page_stats = page_tracker.parse_daily_page_statistics(raw_page_stats)
        
        if not page_stats:
            print("❌ Aucune donnée quotidienne de pages valide à exporter")
            page_stats = []
        else:
            print(f"✅ {len(page_stats)} jours de statistiques de pages récupérés")
    else:
        print("❌ Impossible de récupérer les statistiques quotidiennes de vues de page")
        page_stats = []
        
    # 2. Obtention des statistiques quotidiennes de followers
    print(f"\n--- Récupération des statistiques quotidiennes de followers (sur {min(days_history, 365)} jours) ---")
    follower_tracker = LinkedInFollowerStatisticsTracker(access_token, organization_id, days_history)
    raw_follower_stats = follower_tracker.get_daily_follower_statistics()
    
    if raw_follower_stats:
        # Traitement des données de followers
        print("Analyse des données statistiques quotidiennes de followers...")
        follower_stats = follower_tracker.parse_daily_follower_statistics(raw_follower_stats)
        
        if not follower_stats:
            print("❌ Aucune donnée quotidienne de followers valide à exporter")
            follower_stats = []
        else:
            print(f"✅ {len(follower_stats)} jours de statistiques de followers récupérés")
    else:
        print("❌ Impossible de récupérer les statistiques quotidiennes de followers")
        follower_stats = []
    
    # 3. Obtention des statistiques quotidiennes de partages
    print(f"\n--- Récupération des statistiques quotidiennes de partages (sur {min(days_history, 365)} jours) ---")
    share_tracker = LinkedInShareStatisticsTracker(access_token, organization_id, days_history)
    raw_share_stats = share_tracker.get_daily_share_statistics()
    
    if raw_share_stats:
        # Traitement des données de partages
        print("Analyse des données statistiques quotidiennes de partages...")
        share_stats = share_tracker.parse_daily_share_statistics(raw_share_stats)
        
        if not share_stats:
            print("❌ Aucune donnée quotidienne de partages valide à exporter")
            share_stats = []
        else:
            print(f"✅ {len(share_stats)} jours de statistiques de partages récupérés")
    else:
        print("❌ Impossible de récupérer les statistiques quotidiennes de partages")
        share_stats = []
    
    # Vérifier si au moins un type de statistiques est disponible
    if not page_stats and not follower_stats and not share_stats:
        print("❌ Aucune donnée valide à exporter (ni pages, ni followers, ni partages)")
        sys.exit(1)
    
    # Chemin vers les credentials
    # Remonter aux répertoires parents pour trouver le dossier credentials
    base_dir = Path(__file__).resolve().parent.parent.parent
    credentials_path = base_dir / 'credentials' / 'service_account_credentials.json'
    
    if not credentials_path.exists():
        print(f"Erreur: Fichier de credentials Google non trouvé à {credentials_path}")
        print("Assurez-vous de créer le dossier 'credentials' et d'y placer votre fichier 'service_account_credentials.json'")
        sys.exit(1)
    
    # Export vers Google Sheets
    print("\n--- Export vers Google Sheets ---")
    exporter = GoogleSheetsExporter(sheet_name, credentials_path)
    success = exporter.add_combined_statistics(page_stats, follower_stats, share_stats)
    
    if success:
        print("✅ Export réussi!")
    else:
        print("❌ Échec de l'export")