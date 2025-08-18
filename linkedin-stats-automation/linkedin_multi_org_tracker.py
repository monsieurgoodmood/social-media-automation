#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Statistics Tracker
Version améliorée avec mise à jour forcée pour automatisation
"""

import os
import json
import requests
import urllib.parse
import random
from pathlib import Path
from datetime import datetime, timedelta
import time
from dateutil import parser
import pytz
import sys
from dotenv import load_dotenv

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Chargement des variables d'environnement
load_dotenv()

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
    """Convertit un indice de colonne (0-based) en lettre de colonne pour Google Sheets"""
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
                    print(f"   Données de statistiques quotidiennes des pages récupérées avec succès")
                    return data
                    
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des pages.")
        return None
    
    def parse_daily_page_statistics(self, data):
        """Analyse les données quotidiennes de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("   Aucune donnée de statistiques quotidiennes de pages valide trouvée.")
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
                    print(f"   Données de statistiques quotidiennes des followers récupérées avec succès")
                    return data
                    
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des followers.")
        return None
    
    def parse_daily_follower_statistics(self, data):
        """Analyse les données quotidiennes de followers de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("   Aucune donnée de statistiques quotidiennes de followers valide trouvée.")
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
                    print(f"   Données de statistiques quotidiennes des partages récupérées avec succès")
                    return data
                    
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques quotidiennes des partages.")
        return None
    
    def parse_daily_share_statistics(self, data):
        """Analyse les données quotidiennes de partages de l'API et extrait les statistiques pertinentes"""
        daily_stats = []
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("   Aucune donnée de statistiques quotidiennes de partages valide trouvée.")
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
    """Classe pour exporter les données vers Google Sheets avec gestion optimisée des quotas"""
    
    def __init__(self, spreadsheet_name, credentials_path, admin_email="byteberry.analytics@gmail.com"):
        """Initialise l'exportateur avec le nom du spreadsheet et le chemin des credentials"""
        self.spreadsheet_name = spreadsheet_name
        self.credentials_path = credentials_path
        self.admin_email = admin_email
        self.client = None
        self.spreadsheet = None
        # Détecter si on est en mode automatisé
        self.is_automated = os.getenv('AUTOMATED_MODE', 'false').lower() == 'true'
        
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
            
            # Vérifier si Sheet1 existe et le renommer en "Statistiques quotidiennes"
            try:
                sheet1 = self.spreadsheet.worksheet("Sheet1")
                sheet1.update_title("Statistiques quotidiennes")
                print("   Feuille 'Sheet1' renommée en 'Statistiques quotidiennes'")
            except gspread.exceptions.WorksheetNotFound:
                pass  # Sheet1 n'existe pas, pas besoin de la renommer
            
            return True
        except Exception as e:
            print(f"   Erreur de connexion à Google Sheets: {e}")
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
    
    def wait_with_backoff(self, base_delay=2, max_delay=120, factor=2, jitter=0.2):
        """Implémente une attente avec backoff exponentiel et jitter aléatoire"""
        delay = base_delay + random.uniform(0, jitter * base_delay)
        time.sleep(delay)
        # Retourne le prochain délai à utiliser en cas de nouvelle erreur
        next_delay = min(delay * factor, max_delay)
        return next_delay
    
    def api_request_with_retry(self, api_func, *args, max_retries=6, initial_delay=2, **kwargs):
        """Exécute une requête API avec retry et backoff exponentiel en cas d'erreur"""
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                return api_func(*args, **kwargs)
            except gspread.exceptions.APIError as e:
                if "429" in str(e) or "Quota exceeded" in str(e):  # Quota exceeded
                    if attempt == max_retries - 1:
                        print(f"   ❌ Quota API définitivement dépassé après {max_retries} tentatives")
                        raise Exception(f"Quota API dépassé - abandon après {max_retries} tentatives")
                    
                    print(f"   ⚠️  Quota API dépassé (tentative {attempt+1}/{max_retries}). Attente de {delay:.1f}s...")
                    delay = self.wait_with_backoff(base_delay=delay)
                else:
                    # Autres erreurs d'API, retry mais avec un log différent
                    if attempt == max_retries - 1:
                        print(f"   ❌ Erreur API persistante après {max_retries} tentatives: {e}")
                        raise
                    print(f"   ⚠️  Erreur API (tentative {attempt+1}/{max_retries}): {e}. Attente de {delay:.1f}s...")
                    delay = self.wait_with_backoff(base_delay=delay)
            except Exception as e:
                # Autres exceptions génériques, ne pas retry automatiquement
                print(f"   ❌ Erreur non récupérable: {e}")
                raise
        
        # Si on arrive ici, c'est qu'on a épuisé toutes les tentatives
        raise Exception(f"Nombre maximum de tentatives ({max_retries}) atteint. Échec de l'opération.")
    
    def save_progress(self, processed_dates, org_id):
        """Sauvegarde l'état de progression dans un fichier pour pouvoir reprendre plus tard"""
        try:
            progress_file = f'linkedin_stats_progress_{org_id}.json'
            with open(progress_file, 'w') as f:
                json.dump({"processed_dates": list(processed_dates)}, f)
            print(f"   📄 Progression sauvegardée: {len(processed_dates)} dates traitées")
        except Exception as e:
            print(f"   ⚠️  Erreur lors de la sauvegarde de la progression: {e}")
    
    def load_progress(self, org_id):
        """Charge l'état de progression depuis un fichier"""
        try:
            progress_file = f'linkedin_stats_progress_{org_id}.json'
            if os.path.exists(progress_file):
                with open(progress_file, 'r') as f:
                    data = json.load(f)
                processed_dates = set(data.get("processed_dates", []))
                print(f"   📋 Progression chargée: {len(processed_dates)} dates déjà traitées")
                return processed_dates
        except Exception as e:
            print(f"   ⚠️  Erreur lors du chargement de la progression (reprise à zéro): {e}")
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
                
                # CORRECTION ICI - Utiliser ensure_percentage_as_decimal
                merged_stats[date]['engagement'] = ensure_percentage_as_decimal(stat['engagement'])
                
                merged_stats[date]['like_count'] = stat['like_count']
                merged_stats[date]['comment_count'] = stat['comment_count']
                merged_stats[date]['share_count'] = stat['share_count']
                merged_stats[date]['impression_count'] = stat['impression_count']
                merged_stats[date]['unique_impressions_count'] = stat['unique_impressions_count']
                merged_stats[date]['share_mentions_count'] = stat.get('share_mentions_count', 0)
                merged_stats[date]['comment_mentions_count'] = stat.get('comment_mentions_count', 0)
            else:
                # Créer une nouvelle entrée (avec des vues et followers à 0)
                engagement_value = ensure_percentage_as_decimal(stat['engagement'])
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
                    'engagement': engagement_value,
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
    
    # Modifier la fonction format_columns_optimized
    def format_columns_optimized(self, sheet, headers):
        """Applique le formatage optimisé avec moins de requêtes API"""
        try:
            print("   🎨 Application du formatage optimisé des colonnes...")
            
            # 1. Formatage de la colonne date (A)
            try:
                self.api_request_with_retry(
                    sheet.format, 
                    "A:A", 
                    {
                        "numberFormat": {
                            "type": "DATE",
                            "pattern": "yyyy-mm-dd"
                        }
                    }
                )
                print("   ✅ Formatage date appliqué à la colonne A")
            except Exception as e:
                print(f"   ⚠️  Impossible de formater la colonne date: {e}")
            
            # 2. Identifier la colonne du taux d'engagement
            engagement_col_index = None
            try:
                # Trouver l'index de la colonne "Taux d'engagement"
                for i, header in enumerate(headers):
                    if "Taux d'engagement" in header or "engagement" in header:
                        engagement_col_index = i
                        break
                
                if engagement_col_index is not None:
                    engagement_col_letter = get_column_letter(engagement_col_index)
                
                self.api_request_with_retry(
                    sheet.format, 
                    f"{engagement_col_letter}:{engagement_col_letter}", 
                    {
                        "numberFormat": {
                            "type": "PERCENT",
                            "pattern": "0.00%"
                        }
                    }
                )
                print(f"   ✅ Formatage pourcentage appliqué à la colonne {engagement_col_letter}")
            except Exception as e:
                print(f"   ⚠️  Impossible de formater la colonne engagement: {e}")
            
            # 3. Formater toutes les autres colonnes numériques
            try:
                # Toutes les colonnes sauf A (date) et le taux d'engagement
                last_col = get_column_letter(len(headers) - 1)
                
                # Formater toutes les colonnes numériques
                for i in range(1, len(headers)):
                    if i != engagement_col_index:  # Skip engagement column
                        col_letter = get_column_letter(i)
                        try:
                            self.api_request_with_retry(
                                sheet.format, 
                                f"{col_letter}:{col_letter}", 
                                {
                                    "numberFormat": {
                                        "type": "NUMBER",
                                        "pattern": "#,##0"
                                    }
                                }
                            )
                        except:
                            pass
                
                print("   ✅ Formatage numérique appliqué")
                    
            except Exception as e:
                print(f"   ⚠️  Impossible d'appliquer le formatage numérique global: {e}")
            
            # 4. Formatage des en-têtes
            try:
                header_range = f'A1:{last_col}1'
                self.api_request_with_retry(
                    sheet.format, 
                    header_range, 
                    {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                    }
                )
                print("   ✅ Formatage des en-têtes appliqué")
            except Exception as e:
                print(f"   ⚠️  Impossible de formater les en-têtes: {e}")
                
            print("   ✅ Formatage optimisé terminé")
                
        except Exception as e:
            print(f"   ❌ Erreur lors du formatage optimisé des colonnes: {e}")
            
    def update_daily_stats_sheet(self, combined_stats, org_id):
        """Met à jour la feuille des statistiques quotidiennes avec vérification et mise à jour intelligente"""
        try:
            print("   🔄 Mode mise à jour intelligente activé")
            
            # Headers attendus
            expected_headers = [
                "Date",
                "Vues totales page",
                "Vues uniques page",
                "Vues Desktop",
                "Vues Desktop uniques",
                "Vues Mobile",
                "Vues Mobile uniques",
                "Nbre de vues Accueil",
                "Vues Accueil uniques",
                "Vues Accueil Desktop",
                "Vues Accueil Mobile",
                "Nbre de vues À propos",
                "Vues À propos uniques",
                "Nbre de vues Personnes",
                "Vues Personnes uniques",
                "Nbre de vues Emplois",
                "Vues Emplois uniques",
                "Vues Emplois Desktop",
                "Vues Emplois Mobile",
                "Vues Carrières",
                "Vues Carrières uniques",
                "Vues Carrières Desktop",
                "Vues Carrières Mobile",
                "Vues Vie en entreprise",
                "Vues Vie en entreprise uniques",
                "Vues Vie en entreprise Desktop",
                "Vues Vie en entreprise Mobile",
                "Clics sur boutons Desktop",
                "Clics sur boutons Mobile",
                "Nbre clics sur boutons",
                "Nouveaux abonnés organiques",
                "Nouveaux abonnés payants",
                "Nouveaux abonnés",
                "Nbre de clics",
                "Taux d'engagement de la page",
                "Nbre de réactions",
                "Nbre de commentaires",
                "Nbre de partages",
                "Nbre de mentions partage",
                "Nbre de mentions commentaires",
                "Nbre d'affichages",
                "Nbre d'affichages uniques"
            ]
            
            # Vérifier si la feuille existe ou la créer
            try:
                sheet = self.spreadsheet.worksheet("Statistiques quotidiennes")
                print("   📊 Feuille 'Statistiques quotidiennes' trouvée")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet1")
                    self.api_request_with_retry(sheet.update_title, "Statistiques quotidiennes")
                    print("   📊 Feuille par défaut 'Sheet1' renommée en 'Statistiques quotidiennes'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = self.api_request_with_retry(
                        self.spreadsheet.add_worksheet, 
                        title="Statistiques quotidiennes", 
                        rows=500, 
                        cols=50
                    )
                    print("   📊 Nouvelle feuille 'Statistiques quotidiennes' créée")
            
            # Récupérer les données existantes
            existing_data = self.api_request_with_retry(sheet.get_all_values)
            headers_need_update = False
            last_existing_date = None
            existing_dates = set()
            
            # Vérifier si des données existent
            if len(existing_data) > 0:
                # Vérifier les headers
                current_headers = existing_data[0] if existing_data[0] else []
                
                # Comparer les headers
                if current_headers != expected_headers:
                    print("   ⚠️  Headers incorrects détectés, mise à jour nécessaire")
                    headers_need_update = True
                else:
                    print("   ✅ Headers corrects")
                
                # Récupérer les dates existantes si on a des données
                if len(existing_data) > 1:
                    for row in existing_data[1:]:
                        if row and row[0]:  # Si la ligne existe et a une date
                            existing_dates.add(row[0])
                    
                    # Trouver la dernière date
                    if existing_dates:
                        sorted_dates = sorted(list(existing_dates))
                        last_existing_date = sorted_dates[-1]
                        print(f"   📅 Dernière date dans le sheet: {last_existing_date}")
            else:
                print("   📝 Feuille vide, ajout des headers nécessaire")
                headers_need_update = True
            
            # Mettre à jour les headers si nécessaire
            if headers_need_update:
                print("   🔄 Mise à jour des headers...")
                self.api_request_with_retry(sheet.update, values=[expected_headers], range_name='A1')
                print("   ✅ Headers mis à jour")
            
            # Vérifier et appliquer le formatage des colonnes
            print("   🎨 Vérification du formatage des colonnes...")
            self.verify_and_apply_formatting(sheet, expected_headers)
            
            # Déterminer les dates à traiter
            dates_to_update = []
            new_dates = []
            
            # Créer un dictionnaire des nouvelles données par date
            new_data_dict = {stat['date']: stat for stat in combined_stats}
            
            # Si on a une dernière date existante
            if last_existing_date:
                # La dernière date doit être mise à jour (données partielles possibles)
                if last_existing_date in new_data_dict:
                    dates_to_update.append(last_existing_date)
                    print(f"   🔄 Mise à jour de la dernière date: {last_existing_date}")
                
                # Ajouter toutes les dates après la dernière date existante
                for date in sorted(new_data_dict.keys()):
                    if date > last_existing_date:
                        new_dates.append(date)
                
                if new_dates:
                    print(f"   ➕ {len(new_dates)} nouvelles dates à ajouter")
            else:
                # Si aucune donnée existante, toutes les dates sont nouvelles
                new_dates = sorted(new_data_dict.keys())
                print(f"   ➕ Ajout de {len(new_dates)} dates (historique complet)")
            
            # Mettre à jour la dernière date existante
            if dates_to_update:
                # Trouver la ligne de la dernière date
                for idx, row in enumerate(existing_data[1:], start=2):
                    if row and row[0] == last_existing_date:
                        # Préparer les données pour cette date
                        day_stats = new_data_dict[last_existing_date]
                        row_data = self.prepare_row_data(day_stats)
                        
                        # Mettre à jour cette ligne
                        last_col = get_column_letter(len(expected_headers) - 1)
                        range_name = f'A{idx}:{last_col}{idx}'
                        self.api_request_with_retry(sheet.update, values=[row_data], range_name=range_name)
                        print(f"   ✅ Mise à jour effectuée pour {last_existing_date} (ligne {idx})")
                        break
            
            # Ajouter les nouvelles dates
            if new_dates:
                print(f"   📊 Ajout de {len(new_dates)} nouvelles lignes...")
                
                # Préparer toutes les nouvelles lignes
                new_rows = []
                for date in sorted(new_dates):
                    if date in new_data_dict:
                        day_stats = new_data_dict[date]
                        row_data = self.prepare_row_data(day_stats)
                        new_rows.append(row_data)
                
                # Déterminer où commencer l'ajout
                if len(existing_data) > 0:
                    start_row = len(existing_data) + 1
                else:
                    start_row = 2  # Après les headers
                
                # Ajouter par lots
                batch_size = 50
                for i in range(0, len(new_rows), batch_size):
                    batch = new_rows[i:i+batch_size]
                    current_start_row = start_row + i
                    last_col = get_column_letter(len(expected_headers) - 1)
                    range_name = f'A{current_start_row}:{last_col}{current_start_row + len(batch) - 1}'
                    
                    self.api_request_with_retry(sheet.update, values=batch, range_name=range_name)
                    print(f"   ✅ Lot {i//batch_size + 1}/{(len(new_rows)-1)//batch_size + 1} ajouté")
                    
                    if i + batch_size < len(new_rows):
                        time.sleep(3)
            
            # Trier les données par date
            print("   🔄 Tri des données par date...")
            try:
                # Récupérer le nombre total de lignes
                updated_data = self.api_request_with_retry(sheet.get_all_values)
                if len(updated_data) > 1:
                    last_col = get_column_letter(len(expected_headers) - 1)
                    data_range = f'A2:{last_col}{len(updated_data)}'
                    self.api_request_with_retry(sheet.sort, (1, 'asc'), range=data_range)
                    print("   ✅ Tri terminé")
            except Exception as e:
                print(f"   ⚠️  Impossible de trier les données: {e}")
            
            # Résumé
            print(f"\n   📊 Résumé de la mise à jour:")
            if dates_to_update:
                print(f"   - Date mise à jour: {dates_to_update[0]}")
            print(f"   - Nouvelles dates ajoutées: {len(new_dates)}")
            if new_dates:
                print(f"   - Période ajoutée: du {new_dates[0]} au {new_dates[-1]}")
            
            return sheet
            
        except Exception as e:
            print(f"   ❌ Erreur lors de la mise à jour de la feuille: {e}")
            return None
    
    def prepare_row_data(self, day_stats):
        """Prépare une ligne de données à partir des statistiques du jour"""
        return [
            day_stats['date'],
            day_stats['total_views'],
            day_stats['unique_views'],
            day_stats['desktop_views'],
            day_stats['unique_desktop_views'],
            day_stats['mobile_views'],
            day_stats['unique_mobile_views'],
            day_stats['overview_views'],
            day_stats['unique_overview_views'],
            day_stats['desktop_overview_views'],
            day_stats['mobile_overview_views'],
            day_stats['about_views'],
            day_stats['unique_about_views'],
            day_stats['people_views'],
            day_stats['unique_people_views'],
            day_stats['jobs_views'],
            day_stats['unique_jobs_views'],
            day_stats['desktop_jobs_views'],
            day_stats['mobile_jobs_views'],
            day_stats['careers_views'],
            day_stats['unique_careers_views'],
            day_stats['desktop_careers_views'],
            day_stats['mobile_careers_views'],
            day_stats['life_at_views'],
            day_stats['unique_life_at_views'],
            day_stats['desktop_life_at_views'],
            day_stats['mobile_life_at_views'],
            day_stats['desktop_button_clicks'],
            day_stats['mobile_button_clicks'],
            day_stats['total_button_clicks'],
            day_stats['organic_follower_gain'],
            day_stats['paid_follower_gain'],
            day_stats['total_follower_gain'],
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
    
    def verify_and_apply_formatting(self, sheet, headers):
        """Vérifie et applique le formatage correct aux colonnes"""
        try:
            print("   🔍 Vérification et application du formatage...")
            
            # 1. Formatage de la colonne date (A)
            try:
                self.api_request_with_retry(
                    sheet.format, 
                    "A:A", 
                    {
                        "numberFormat": {
                            "type": "DATE",
                            "pattern": "yyyy-mm-dd"
                        }
                    }
                )
                print("   ✅ Formatage date appliqué/vérifié")
            except Exception as e:
                print(f"   ⚠️  Impossible de formater la colonne date: {e}")
            
            # 2. Trouver et formater la colonne taux d'engagement
            engagement_col_index = None
            for i, header in enumerate(headers):
                if "Taux d'engagement de la page" in header:
                    engagement_col_index = i
                    break
            
            if engagement_col_index is not None:
                engagement_col_letter = get_column_letter(engagement_col_index)
                try:
                    self.api_request_with_retry(
                        sheet.format, 
                        f"{engagement_col_letter}:{engagement_col_letter}", 
                        {
                            "numberFormat": {
                                "type": "PERCENT",
                                "pattern": "0.00%"
                            }
                        }
                    )
                    print(f"   ✅ Formatage pourcentage appliqué/vérifié (colonne {engagement_col_letter})")
                except Exception as e:
                    print(f"   ⚠️  Impossible de formater la colonne engagement: {e}")
            
            # 3. Formater toutes les autres colonnes numériques
            for i in range(1, len(headers)):
                if i != engagement_col_index:  # Skip engagement column
                    col_letter = get_column_letter(i)
                    try:
                        self.api_request_with_retry(
                            sheet.format, 
                            f"{col_letter}:{col_letter}", 
                            {
                                "numberFormat": {
                                    "type": "NUMBER",
                                    "pattern": "#,##0"
                                }
                            },
                            max_retries=2,  # Moins de retries pour le formatage
                            initial_delay=1
                        )
                    except:
                        # Silencieusement ignorer les erreurs de formatage pour les colonnes individuelles
                        pass
            
            print("   ✅ Formatage numérique appliqué/vérifié")
            
            # 4. Formatage des en-têtes
            try:
                last_col = get_column_letter(len(headers) - 1)
                header_range = f'A1:{last_col}1'
                self.api_request_with_retry(
                    sheet.format, 
                    header_range, 
                    {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                    }
                )
                print("   ✅ Formatage des en-têtes appliqué/vérifié")
            except Exception as e:
                print(f"   ⚠️  Impossible de formater les en-têtes: {e}")
                
        except Exception as e:
            print(f"   ⚠️  Erreur lors de la vérification du formatage: {e}")
    
    def add_combined_statistics(self, page_stats, follower_stats, share_stats, org_id):
        """Ajoute les statistiques quotidiennes combinées (pages, followers et partages)"""
        if not self.connect():
            print("   ❌ Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Combiner les statistiques
        combined_stats = self.merge_all_stats(page_stats, follower_stats, share_stats)
        
        # Mettre à jour la feuille principale
        if not self.update_daily_stats_sheet(combined_stats, org_id):
            return False
        
        # URL du spreadsheet
        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
        print(f"   📊 URL du tableau: {sheet_url}")
        
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


class MultiOrganizationTracker:
    """Gestionnaire pour plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.days_history = int(os.getenv("LINKEDIN_DAYS_HISTORY", "365"))
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        # Détecter si on est en mode automatisé
        self.is_automated = os.getenv('AUTOMATED_MODE', 'false').lower() == 'true'
        
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
    
    def get_sheet_id_for_org(self, org_id, org_name):
        """Récupère ou crée l'ID du Google Sheet pour une organisation"""
        # Utiliser un mapping stocké dans les variables d'environnement ou un fichier
        sheet_mapping_file = 'sheet_mapping.json'
        
        try:
            if os.path.exists(sheet_mapping_file):
                with open(sheet_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            # Nettoyer le nom pour éviter les caractères problématiques
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Stats_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(sheet_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Stats_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        sheet_mapping_file = 'sheet_mapping.json'
        
        try:
            if os.path.exists(sheet_mapping_file):
                with open(sheet_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(sheet_mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(mapping, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            print(f"Erreur lors de la mise à jour du mapping: {e}")
    
    def process_all_organizations(self):
        """Traite toutes les organisations configurées"""
        if not self.access_token:
            print("❌ Erreur: LINKEDIN_ACCESS_TOKEN manquant")
            return False
        
        # Afficher le mode d'exécution
        print("\n🔄 === MODE MISE À JOUR INTELLIGENTE ===")
        print("Vérification des headers, formats et ajout des dates manquantes")
        
        # Vérifier le token une seule fois
        print("\n--- Vérification du token ---")
        is_valid, result = verify_token(self.access_token)
        
        if not is_valid:
            print(f"❌ Token invalide: {result}")
            return False
        
        print("✅ Token valide!")
        user_info = result
        print(f"   Utilisateur: {user_info.get('localizedFirstName', '')} {user_info.get('localizedLastName', '')}")
        
        # Traiter chaque organisation avec une pause plus longue entre chaque org
        results = []
        total_orgs = len(self.organizations)
        
        for idx, org in enumerate(self.organizations, 1):
            org_id = org['id']
            org_name = org['name']
            
            print(f"\n{'='*60}")
            print(f"[{idx}/{total_orgs}] Traitement de: {org_name}")
            print(f"ID: {org_id}")
            print(f"{'='*60}")
            
            try:
                success = self.process_single_organization(org_id, org_name)
                results.append({
                    'org_id': org_id,
                    'org_name': org_name,
                    'success': success,
                    'timestamp': datetime.now().isoformat()
                })
                
                # Pause plus longue entre les organisations pour éviter les quotas
                if idx < total_orgs:  # Pas de pause après la dernière organisation
                    delay = 15 if self.is_automated else 30  # Pause plus courte en mode automatisé
                    print(f"\n⏱️  Pause de {delay} secondes avant la prochaine organisation...")
                    time.sleep(delay)
                    
            except Exception as e:
                print(f"❌ Erreur lors du traitement de {org_name}: {e}")
                results.append({
                    'org_id': org_id,
                    'org_name': org_name,
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
                
                # Pause même en cas d'erreur
                if idx < total_orgs:
                    delay = 15 if self.is_automated else 30
                    print(f"\n⏱️  Pause de {delay} secondes avant la prochaine organisation...")
                    time.sleep(delay)
        
        # Résumé
        print(f"\n{'='*60}")
        print("RÉSUMÉ DU TRAITEMENT")
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
            print("\n📊 Google Sheets créés/mis à jour:")
            sheet_mapping_file = 'sheet_mapping.json'
            if os.path.exists(sheet_mapping_file):
                with open(sheet_mapping_file, 'r', encoding='utf-8') as f:
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
        sheet_id, sheet_name = self.get_sheet_id_for_org(org_id, org_name)
        
        print(f"\n📊 Google Sheet: {sheet_name}")
        
        # 1. Récupération des statistiques de pages
        print(f"\n1. Récupération des statistiques de pages ({self.days_history} jours)...")
        page_tracker = LinkedInDailyPageStatisticsTracker(
            self.access_token, org_id, self.days_history, sheet_name
        )
        raw_page_stats = page_tracker.get_daily_page_statistics()
        
        if raw_page_stats:
            page_stats = page_tracker.parse_daily_page_statistics(raw_page_stats)
            if page_stats:
                print(f"   ✅ {len(page_stats)} jours de statistiques de pages récupérés")
            else:
                print("   ⚠️  Données récupérées mais aucune statistique valide")
                page_stats = []
        else:
            print("   ❌ Impossible de récupérer les statistiques de pages")
            page_stats = []
        
        # 2. Récupération des statistiques de followers
        print(f"\n2. Récupération des statistiques de followers...")
        follower_tracker = LinkedInFollowerStatisticsTracker(
            self.access_token, org_id, self.days_history
        )
        raw_follower_stats = follower_tracker.get_daily_follower_statistics()
        
        if raw_follower_stats:
            follower_stats = follower_tracker.parse_daily_follower_statistics(raw_follower_stats)
            if follower_stats:
                print(f"   ✅ {len(follower_stats)} jours de statistiques de followers récupérés")
            else:
                print("   ⚠️  Données récupérées mais aucune statistique valide")
                follower_stats = []
        else:
            print("   ❌ Impossible de récupérer les statistiques de followers")
            follower_stats = []
        
        # 3. Récupération des statistiques de partages
        print(f"\n3. Récupération des statistiques de partages...")
        share_tracker = LinkedInShareStatisticsTracker(
            self.access_token, org_id, self.days_history
        )
        raw_share_stats = share_tracker.get_daily_share_statistics()
        
        if raw_share_stats:
            share_stats = share_tracker.parse_daily_share_statistics(raw_share_stats)
            if share_stats:
                print(f"   ✅ {len(share_stats)} jours de statistiques de partages récupérés")
            else:
                print("   ⚠️  Données récupérées mais aucune statistique valide")
                share_stats = []
        else:
            print("   ❌ Impossible de récupérer les statistiques de partages")
            share_stats = []
        
        # Vérifier si des données sont disponibles
        if not page_stats and not follower_stats and not share_stats:
            print("\n❌ Aucune donnée disponible pour cette organisation")
            return False
        
        # Résumé des données collectées
        print(f"\n📈 Résumé des données collectées:")
        print(f"   - Pages: {len(page_stats)} jours")
        print(f"   - Followers: {len(follower_stats)} jours")
        print(f"   - Partages: {len(share_stats)} jours")
        
        # 4. Export vers Google Sheets
        print(f"\n4. Export vers Google Sheets...")
        
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
                return False
        else:
            print("   ✅ Credentials trouvés")
        
        exporter = GoogleSheetsExporter(sheet_name, credentials_path, self.admin_email)
        success = exporter.add_combined_statistics(page_stats, follower_stats, share_stats, org_id)
        
        if success and exporter.spreadsheet:
            # Mettre à jour le mapping avec l'ID du sheet
            self.update_sheet_mapping(org_id, exporter.spreadsheet.id)
            print(f"\n✅ Export réussi pour {org_name}!")
            print(f"📊 URL du Sheet: https://docs.google.com/spreadsheets/d/{exporter.spreadsheet.id}")
        else:
            print(f"\n❌ Échec de l'export pour {org_name}")
        
        return success


def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION STATISTICS TRACKER")
    print("Version avec mise à jour intelligente")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationTracker()
    
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
    print(f"   - Jours d'historique: {tracker.days_history}")
    print(f"   - Email admin: {tracker.admin_email}")
    print(f"   - Mode: 🔄 MISE À JOUR INTELLIGENTE")
    
    # Demander confirmation pour plusieurs organisations
    if len(tracker.organizations) > 3:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Le système va:")
        print("   - Vérifier et corriger les headers si nécessaire")
        print("   - Vérifier et appliquer le formatage correct")
        print("   - Mettre à jour la dernière date (données partielles)")
        print("   - Ajouter uniquement les dates manquantes")
        print("   Des pauses de 30 secondes seront appliquées entre chaque organisation.")
        if os.getenv('AUTOMATED_MODE', 'false').lower() == 'true':
            response = 'o'
            print('🤖 Mode automatisé: réponse automatique "o"')
        else:
            response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement avec mise à jour intelligente...")
    
    # Lancer le traitement
    start_time = datetime.now()
    success = tracker.process_all_organizations()
    end_time = datetime.now()
    
    # Afficher le temps d'exécution
    duration = end_time - start_time
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)
    
    print(f"\n⏱️  Temps d'exécution: {minutes}m {seconds}s")
    
    # Nettoyer les fichiers de progression temporaires
    for file in os.listdir('.'):
        if file.startswith('linkedin_stats_progress_') and file.endswith('.json'):
            os.remove(file)
            print(f"🧹 Fichier temporaire supprimé: {file}")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()