#!/usr/bin/env python3
"""
LinkedIn Share Statistics Tracker
Ce script collecte les statistiques des partages LinkedIn d'une organisation
et les enregistre dans Google Sheets.
"""

import os
import requests
import urllib.parse
import json
from datetime import datetime
import time

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import sys
from dotenv import load_dotenv

# Chargement des variables d'environnement
load_dotenv()

class LinkedInShareStatisticsTracker:
    """Classe pour suivre les statistiques des partages LinkedIn d'une organisation"""
    
    def __init__(self, access_token, organization_id, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.sheet_name = sheet_name or f"LinkedIn_Share_Stats_{organization_id}"
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_share_statistics(self):
        """Obtient les statistiques de partage pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Construire l'URL
        url = f"{self.base_url}/organizationalEntityShareStatistics?q=organizationalEntity&organizationalEntity={encoded_urn}"
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Données de statistiques de partage récupérées avec succès")
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
        
        print("Échec après plusieurs tentatives pour obtenir les statistiques de partage.")
        return None
    
    def get_page_statistics(self):
        """Obtient les statistiques de la page pour l'organisation (métriques lifetime)"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Construire l'URL
        url = f"{self.base_url}/organizationPageStatistics?q=organization&organization={encoded_urn}"
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"Données de statistiques lifetime de la page récupérées avec succès")
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
        
        print("Échec après plusieurs tentatives pour obtenir les statistiques lifetime de la page.")
        return None
    
    def parse_share_statistics(self, share_data, page_data=None):
        """Analyse les données de l'API et extrait les statistiques pertinentes"""
        stats = {}
        
        # Date de récupération
        stats['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # S'assurer que les données de partage sont valides
        if not share_data or 'elements' not in share_data or len(share_data['elements']) == 0:
            print("Aucune donnée de statistiques de partage valide trouvée.")
            # Initialiser avec des valeurs par défaut
            stats['impressions'] = {'total': 0, 'unique': 0}
            stats['engagement'] = {
                'rate': 0,
                'clicks': 0,
                'likes': 0,
                'comments': 0,
                'shares': 0,
                'share_mentions': 0,
                'comment_mentions': 0,
                'total_interactions': 0,
                'click_through_rate': 0,
                'interaction_rate': 0
            }
        else:
            # Obtenir le premier élément (qui contient les stats d'organisation)
            element = share_data['elements'][0]
            
            if 'totalShareStatistics' not in element:
                print("Aucune statistique de partage trouvée dans les données.")
                # Initialiser avec des valeurs par défaut
                stats['impressions'] = {'total': 0, 'unique': 0}
                stats['engagement'] = {
                    'rate': 0,
                    'clicks': 0,
                    'likes': 0,
                    'comments': 0,
                    'shares': 0,
                    'share_mentions': 0,
                    'comment_mentions': 0,
                    'total_interactions': 0,
                    'click_through_rate': 0,
                    'interaction_rate': 0
                }
            else:
                # Extraire les statistiques principales
                share_stats = element['totalShareStatistics']
                
                stats['impressions'] = {
                    'total': share_stats.get('impressionCount', 0),
                    'unique': share_stats.get('uniqueImpressionsCount', 0)
                }
                
                stats['engagement'] = {
                    'rate': share_stats.get('engagement', 0) * 100,  # Convertir en pourcentage
                    'clicks': share_stats.get('clickCount', 0),
                    'likes': share_stats.get('likeCount', 0),
                    'comments': share_stats.get('commentCount', 0),
                    'shares': share_stats.get('shareCount', 0),
                    'share_mentions': share_stats.get('shareMentionsCount', 0),
                    'comment_mentions': share_stats.get('commentMentionsCount', 0)
                }
                
                # Calcul de métriques dérivées
                if stats['impressions']['total'] > 0:
                    stats['engagement']['click_through_rate'] = (stats['engagement']['clicks'] / stats['impressions']['total']) * 100
                else:
                    stats['engagement']['click_through_rate'] = 0
                    
                # Calculer le nombre total d'interactions
                total_interactions = (
                    stats['engagement']['clicks'] +
                    stats['engagement']['likes'] +
                    stats['engagement']['comments'] +
                    stats['engagement']['shares']
                )
                
                stats['engagement']['total_interactions'] = total_interactions
                
                if stats['impressions']['total'] > 0:
                    stats['engagement']['interaction_rate'] = (total_interactions / stats['impressions']['total']) * 100
                else:
                    stats['engagement']['interaction_rate'] = 0
        
        # Ajout des statistiques lifetime de la page si disponibles
        stats['page_lifetime'] = {}
        
        if page_data and 'elements' in page_data and len(page_data['elements']) > 0:
            element = page_data['elements'][0]
            
            if 'totalPageStatistics' in element:
                total_stats = element['totalPageStatistics']
                
                # Statistiques de vues
                if 'views' in total_stats:
                    views = total_stats['views']
                    stats['page_lifetime']['views'] = {
                        'all_page_views': views.get('allPageViews', {}).get('pageViews', 0),
                        'desktop_page_views': views.get('allDesktopPageViews', {}).get('pageViews', 0),
                        'mobile_page_views': views.get('allMobilePageViews', {}).get('pageViews', 0),
                        
                        # Vues par section
                        'overview_page_views': views.get('overviewPageViews', {}).get('pageViews', 0),
                        'about_page_views': views.get('aboutPageViews', {}).get('pageViews', 0) if 'aboutPageViews' in views else 0,
                        'people_page_views': views.get('peoplePageViews', {}).get('pageViews', 0) if 'peoplePageViews' in views else 0,
                        'jobs_page_views': views.get('jobsPageViews', {}).get('pageViews', 0) if 'jobsPageViews' in views else 0,
                        'careers_page_views': views.get('careersPageViews', {}).get('pageViews', 0) if 'careersPageViews' in views else 0,
                        'life_at_page_views': views.get('lifeAtPageViews', {}).get('pageViews', 0) if 'lifeAtPageViews' in views else 0,
                        
                        # Desktop par section
                        'desktop_overview_views': views.get('desktopOverviewPageViews', {}).get('pageViews', 0) if 'desktopOverviewPageViews' in views else 0,
                        'desktop_careers_views': views.get('desktopCareersPageViews', {}).get('pageViews', 0) if 'desktopCareersPageViews' in views else 0,
                        'desktop_jobs_views': views.get('desktopJobsPageViews', {}).get('pageViews', 0) if 'desktopJobsPageViews' in views else 0,
                        'desktop_life_at_views': views.get('desktopLifeAtPageViews', {}).get('pageViews', 0) if 'desktopLifeAtPageViews' in views else 0,
                        
                        # Mobile par section
                        'mobile_overview_views': views.get('mobileOverviewPageViews', {}).get('pageViews', 0) if 'mobileOverviewPageViews' in views else 0,
                        'mobile_careers_views': views.get('mobileCareersPageViews', {}).get('pageViews', 0) if 'mobileCareersPageViews' in views else 0,
                        'mobile_jobs_views': views.get('mobileJobsPageViews', {}).get('pageViews', 0) if 'mobileJobsPageViews' in views else 0,
                        'mobile_life_at_views': views.get('mobileLifeAtPageViews', {}).get('pageViews', 0) if 'mobileLifeAtPageViews' in views else 0
                    }
                
                # Statistiques de clics sur boutons
                if 'clicks' in total_stats:
                    clicks = total_stats['clicks']
                    
                    # Extraire les clics sur les boutons personnalisés
                    desktop_custom_button_clicks = clicks.get('desktopCustomButtonClickCounts', [])
                    mobile_custom_button_clicks = clicks.get('mobileCustomButtonClickCounts', [])
                    
                    # Calculer le total des clics sur les boutons personnalisés
                    desktop_button_clicks_total = sum([click.get('count', 0) for click in desktop_custom_button_clicks]) if desktop_custom_button_clicks else 0
                    mobile_button_clicks_total = sum([click.get('count', 0) for click in mobile_custom_button_clicks]) if mobile_custom_button_clicks else 0
                    
                    stats['page_lifetime']['clicks'] = {
                        'desktop_button_clicks': desktop_button_clicks_total,
                        'mobile_button_clicks': mobile_button_clicks_total,
                        'total_button_clicks': desktop_button_clicks_total + mobile_button_clicks_total
                    }
        
        return stats


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
    
    def update_stats_sheet(self, stats):
        """Met à jour la feuille des statistiques avec toutes les données dans un seul onglet"""
        try:
            # Utiliser la feuille Sheet1 existante ou en créer une nouvelle
            try:
                sheet = self.spreadsheet.worksheet("Sheet1")
                sheet.update_title("Statistiques LinkedIn")
                print("Feuille 'Sheet1' renommée en 'Statistiques LinkedIn'")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Statistiques LinkedIn")
                    print("Feuille 'Statistiques LinkedIn' utilisée pour les statistiques")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = self.spreadsheet.add_worksheet(title="Statistiques LinkedIn", rows=1000, cols=35)
                    print("Nouvelle feuille 'Statistiques LinkedIn' créée pour les statistiques")
            
            # Vérifier si nous avons déjà des données dans la feuille
            existing_data = sheet.get_all_values()
            headers_exist = len(existing_data) > 0 and len(existing_data[0]) > 1  # Vérifier que la première ligne a du contenu
            
            # Définir les en-têtes (tous dans un seul onglet)
            headers = [
                # Informations générales
                "Date de mesure", 
                
                # Statistiques de partage
                "Impressions totales (depuis création)", 
                "Impressions uniques (depuis création)", 
                "Taux d'engagement global (%)", 
                "Nombre total de clics (depuis création)", 
                "Nombre total de J'aime (depuis création)", 
                "Nombre total de commentaires (depuis création)", 
                "Nombre total de partages (depuis création)",
                "Nombre total de mentions dans partages (depuis création)",
                "Nombre total de mentions dans commentaires (depuis création)",
                "Nombre total d'interactions (depuis création)", 
                "Taux de clic global (%)", 
                "Taux d'interaction global (%)",
                
                # Statistiques lifetime générales
                "Vues totales (lifetime)",
                "Vues desktop (lifetime)",
                "Vues mobile (lifetime)",
                "Clics boutons desktop (lifetime)",
                "Clics boutons mobile (lifetime)",
                "Total clics boutons (lifetime)",
                
                # Vues par section
                "Vues page d'accueil (lifetime)",
                "Vues page À propos (lifetime)",
                "Vues page Personnes (lifetime)",
                "Vues page Emplois (lifetime)",
                "Vues page Carrières (lifetime)",
                "Vues page Vie en entreprise (lifetime)",
                
                # Vues desktop par section
                "Vues desktop accueil (lifetime)",
                "Vues desktop carrières (lifetime)",
                "Vues desktop emplois (lifetime)",
                "Vues desktop vie entreprise (lifetime)",
                
                # Vues mobile par section
                "Vues mobile accueil (lifetime)",
                "Vues mobile carrières (lifetime)",
                "Vues mobile emplois (lifetime)",
                "Vues mobile vie entreprise (lifetime)"
            ]
            
            # Vérifier si les en-têtes doivent être mis à jour
            headers_need_update = True
            
            if headers_exist:
                # Comparer les en-têtes existants avec ceux définis
                existing_headers = existing_data[0]
                
                # Vérifier si le nombre d'en-têtes correspond
                if len(existing_headers) == len(headers):
                    # Vérifier si tous les en-têtes correspondent
                    headers_match = all(eh.strip() == h.strip() for eh, h in zip(existing_headers, headers))
                    if headers_match:
                        headers_need_update = False
            
            # Si les en-têtes ont besoin d'être mis à jour ou n'existent pas
            if headers_need_update:
                sheet.update([headers], "A1")
                
                # Formater les en-têtes
                last_col = chr(ord('A') + len(headers) - 1)  # Convertir le nombre de colonnes en lettre
                sheet.format(f'A1:{last_col}1', {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                })
                
                print("En-têtes ajoutés ou mis à jour dans la feuille")
                
                # Recharger les données après la mise à jour des en-têtes
                if headers_exist:
                    existing_data = sheet.get_all_values()
            
            # Préparer les nouvelles données
            views = stats['page_lifetime'].get('views', {})
            new_row = [
                # Informations générales
                stats['date'],
                
                # Statistiques de partage
                stats['impressions']['total'],
                stats['impressions']['unique'],
                f"{stats['engagement']['rate']:.2f}",
                stats['engagement']['clicks'],
                stats['engagement']['likes'],
                stats['engagement']['comments'],
                stats['engagement']['shares'],
                stats['engagement']['share_mentions'],
                stats['engagement']['comment_mentions'],
                stats['engagement']['total_interactions'],
                f"{stats['engagement']['click_through_rate']:.2f}",
                f"{stats['engagement']['interaction_rate']:.2f}",
                
                # Statistiques lifetime générales
                views.get('all_page_views', 0),
                views.get('desktop_page_views', 0),
                views.get('mobile_page_views', 0),
                stats['page_lifetime'].get('clicks', {}).get('desktop_button_clicks', 0),
                stats['page_lifetime'].get('clicks', {}).get('mobile_button_clicks', 0),
                stats['page_lifetime'].get('clicks', {}).get('total_button_clicks', 0),
                
                # Vues par section
                views.get('overview_page_views', 0),
                views.get('about_page_views', 0),
                views.get('people_page_views', 0),
                views.get('jobs_page_views', 0),
                views.get('careers_page_views', 0),
                views.get('life_at_page_views', 0),
                
                # Vues desktop par section
                views.get('desktop_overview_views', 0),
                views.get('desktop_careers_views', 0),
                views.get('desktop_jobs_views', 0),
                views.get('desktop_life_at_views', 0),
                
                # Vues mobile par section
                views.get('mobile_overview_views', 0),
                views.get('mobile_careers_views', 0),
                views.get('mobile_jobs_views', 0),
                views.get('mobile_life_at_views', 0)
            ]
            
            # Vérifier si la date existe déjà
            current_date = stats['date']
            date_exists = False
            update_row = 0
            
            if headers_exist and len(existing_data) > 1:  # Si nous avons des données (pas seulement les en-têtes)
                for i, row in enumerate(existing_data[1:], 2):  # Commencer à l'index 2 (ligne 2, après les en-têtes)
                    if row and row[0] == current_date:
                        date_exists = True
                        update_row = i
                        break
            
            # Si la date existe, mettre à jour cette ligne, sinon ajouter une nouvelle ligne
            if date_exists:
                sheet.update([new_row], f"A{update_row}")
                print(f"Données mises à jour pour la date {current_date} à la ligne {update_row}")
            else:
                next_row = len(existing_data) + 1 if headers_exist else 2
                sheet.update([new_row], f"A{next_row}")
                print(f"Nouvelle entrée ajoutée pour la date {current_date} à la ligne {next_row}")
                
            # Ajouter une note explicative sous le tableau
            note_row = len(existing_data) + 2
            note = ["Note: Ces statistiques représentent les données cumulatives depuis la création de la page LinkedIn."]
            try:
                # Vérifier si la note existe déjà
                if note_row <= sheet.row_count and sheet.cell(note_row, 1).value == "":
                    sheet.update([note], f"A{note_row}")
                    sheet.format(f"A{note_row}", {
                        "textFormat": {"italic": True},
                        "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
                    })
                    # Fusionner les cellules pour la note
                    last_col = chr(ord('A') + len(headers) - 1)
                    sheet.merge_cells(f"A{note_row}:{last_col}{note_row}")
            except:
                # Si une erreur survient lors de la tentative d'ajout de la note, l'ignorer
                pass
            
            return sheet
        except Exception as e:
            print(f"Erreur lors de la mise à jour de la feuille de statistiques: {e}")
            return None
    
    def add_share_statistics(self, stats):
        """Ajoute les statistiques de partage"""
        if not self.connect():
            print("Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Mettre à jour la feuille avec toutes les données
        self.update_stats_sheet(stats)
        
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
    sheet_name = os.getenv("GOOGLE_SHEET_NAME_SHARE_STATS", "LinkedIn_Share_Statistics")  # Nom spécifique pour ce tracker

    if not access_token or not organization_id:
        print("Erreur: Variables d'environnement LINKEDIN_ACCESS_TOKEN ou LINKEDIN_ORGANIZATION_ID manquantes")
        print("Créez un fichier .env avec les variables:")
        print("LINKEDIN_ACCESS_TOKEN='votre_token'")
        print("LINKEDIN_ORGANIZATION_ID='votre_id_organisation'")
        print("GOOGLE_SHEET_NAME_SHARE_STATS='nom_de_votre_sheet_stats'  # Optionnel")
        sys.exit(1)
    
    # Vérification du token
    print("\n--- Vérification du token ---")
    is_valid, result = verify_token(access_token)
    
    if is_valid:
        print("✅ Token valide!")
    else:
        print(f"❌ Token invalide: {result}")
        sys.exit(1)

    # Initialisation du tracker
    tracker = LinkedInShareStatisticsTracker(access_token, organization_id, sheet_name)
    
    # Obtention des statistiques de partage
    print("\n--- Récupération des statistiques cumulatives de partage ---")
    raw_share_stats = tracker.get_share_statistics()
    
    # Obtention des statistiques lifetime de la page
    print("\n--- Récupération des statistiques lifetime de la page ---")
    raw_page_stats = tracker.get_page_statistics()
    
    if raw_share_stats or raw_page_stats:
        # Traitement des données
        print("Analyse des données statistiques...")
        stats = tracker.parse_share_statistics(raw_share_stats, raw_page_stats)
        
        # Afficher un aperçu des données
        print("\n--- Aperçu des statistiques cumulatives (depuis la création de la page) ---")
        print(f"Date de mesure: {stats['date']}")
        print(f"Impressions totales (depuis création): {stats['impressions']['total']}")
        print(f"Impressions uniques (depuis création): {stats['impressions']['unique']}")
        print(f"Taux d'engagement global: {stats['engagement']['rate']:.2f}%")
        print(f"Nombre total de clics (depuis création): {stats['engagement']['clicks']}")
        print(f"Nombre total de J'aime (depuis création): {stats['engagement']['likes']}")
        print(f"Nombre total de commentaires (depuis création): {stats['engagement']['comments']}")
        print(f"Nombre total de partages (depuis création): {stats['engagement']['shares']}")
        print(f"Nombre total d'interactions (depuis création): {stats['engagement']['total_interactions']}")
        print(f"Taux de clic global: {stats['engagement']['click_through_rate']:.2f}%")
        print(f"Taux d'interaction global: {stats['engagement']['interaction_rate']:.2f}%")
        
        # Afficher les métriques lifetime
        if 'page_lifetime' in stats and stats['page_lifetime']:
            print("\n--- Aperçu des statistiques lifetime de la page ---")
            if 'views' in stats['page_lifetime']:
                views = stats['page_lifetime']['views']
                print(f"Vues totales (lifetime): {views.get('all_page_views', 0)}")
                print(f"Vues desktop (lifetime): {views.get('desktop_page_views', 0)}")
                print(f"Vues mobile (lifetime): {views.get('mobile_page_views', 0)}")
            
            if 'clicks' in stats['page_lifetime']:
                clicks = stats['page_lifetime']['clicks']
                print(f"Clics boutons desktop (lifetime): {clicks.get('desktop_button_clicks', 0)}")
                print(f"Clics boutons mobile (lifetime): {clicks.get('mobile_button_clicks', 0)}")
                print(f"Total clics boutons (lifetime): {clicks.get('total_button_clicks', 0)}")
        
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
        exporter = GoogleSheetsExporter(tracker.sheet_name, credentials_path)
        success = exporter.add_share_statistics(stats)
        
        if success:
            print("✅ Export réussi!")
        else:
            print("❌ Échec de l'export")
    else:
        print("❌ Impossible de récupérer les statistiques")