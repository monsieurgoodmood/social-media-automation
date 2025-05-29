#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Share Statistics Tracker
Ce script collecte les statistiques de partage pour plusieurs organisations LinkedIn
et les enregistre dans Google Sheets avec un formatage optimisé pour Looker Studio.
"""

import os
import json
import requests
import urllib.parse
from pathlib import Path
from datetime import datetime
import time
import sys
from dotenv import load_dotenv
import random

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# Chargement des variables d'environnement
load_dotenv()

def safe_sheets_operation(operation, *args, max_retries=5, **kwargs):
    """
    Exécute une opération Google Sheets avec gestion des erreurs de quota et de service
    """
    for attempt in range(max_retries):
        try:
            return operation(*args, **kwargs)
        except APIError as e:
            error_code = str(e)
            
            # Gestion des différents types d'erreurs
            if '429' in error_code or 'Quota exceeded' in error_code:
                # Quota dépassé
                base_delay = min(60, (2 ** attempt) * 5)
                jitter = random.uniform(0.5, 1.5)
                delay = base_delay * jitter
                print(f"   ⏳ Quota dépassé (tentative {attempt + 1}/{max_retries}), attente de {delay:.1f}s...")
                time.sleep(delay)
            elif '503' in error_code or 'unavailable' in error_code.lower():
                # Service indisponible
                base_delay = min(120, (2 ** attempt) * 10)
                jitter = random.uniform(0.8, 1.2)
                delay = base_delay * jitter
                print(f"   🔄 Service Google Sheets indisponible (tentative {attempt + 1}/{max_retries}), attente de {delay:.1f}s...")
                time.sleep(delay)
            elif '500' in error_code or '502' in error_code or '504' in error_code:
                # Erreurs serveur
                base_delay = min(60, (2 ** attempt) * 8)
                jitter = random.uniform(0.7, 1.3)
                delay = base_delay * jitter
                print(f"   🔧 Erreur serveur Google (tentative {attempt + 1}/{max_retries}), attente de {delay:.1f}s...")
                time.sleep(delay)
            elif '400' in error_code and 'exceeds grid limits' in error_code:
                # Erreur de limites de grille
                print(f"   ⚠️ Erreur de limites de grille: {e}")
                print(f"   🔧 Tentative d'ignorer cette opération de formatage...")
                return None  # Ignorer cette opération
            else:
                print(f"   ❌ Erreur API non gérée: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(5)
                
            if attempt == max_retries - 1:
                print(f"   ❌ Échec après {max_retries} tentatives: {e}")
                raise
                
        except Exception as e:
            print(f"   ❌ Erreur inattendue: {e}")
            if attempt == max_retries - 1:
                raise
            # Attendre un peu avant de réessayer
            time.sleep(min(30, (2 ** attempt) * 2))

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
                    print(f"   Données de statistiques de partage récupérées avec succès")
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques de partage.")
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
                    print(f"   Données de statistiques lifetime de la page récupérées avec succès")
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques lifetime de la page.")
        return None
    
    def parse_share_statistics(self, share_data, page_data=None):
        """Analyse les données de l'API et extrait les statistiques pertinentes"""
        stats = {}
        
        # Date de récupération (en string pour éviter les erreurs de sérialisation)
        stats['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # S'assurer que les données de partage sont valides
        if not share_data or 'elements' not in share_data or len(share_data['elements']) == 0:
            print("   Aucune donnée de statistiques de partage valide trouvée.")
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
                print("   Aucune statistique de partage trouvée dans les données.")
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
                    'rate': share_stats.get('engagement', 0),  # Garder en décimal pour Looker
                    'clicks': share_stats.get('clickCount', 0),
                    'likes': share_stats.get('likeCount', 0),
                    'comments': share_stats.get('commentCount', 0),
                    'shares': share_stats.get('shareCount', 0),
                    'share_mentions': share_stats.get('shareMentionsCount', 0),
                    'comment_mentions': share_stats.get('commentMentionsCount', 0)
                }
                
                # Calcul de métriques dérivées
                if stats['impressions']['total'] > 0:
                    stats['engagement']['click_through_rate'] = (stats['engagement']['clicks'] / stats['impressions']['total'])  # Décimal
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
                    stats['engagement']['interaction_rate'] = (total_interactions / stats['impressions']['total'])  # Décimal
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
    """Classe pour exporter les données vers Google Sheets avec formatage optimisé pour Looker Studio"""
    
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
                self.spreadsheet = safe_sheets_operation(self.client.create, self.spreadsheet_name)
                print(f"   Nouveau spreadsheet créé: {self.spreadsheet_name}")
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée
                safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
            
            return True
        except Exception as e:
            print(f"   Erreur de connexion à Google Sheets: {e}")
            return False
    
    def ensure_admin_access(self):
        """Vérifie et garantit que l'admin a toujours accès au document"""
        try:
            # Récupérer les permissions actuelles
            permissions = safe_sheets_operation(self.spreadsheet.list_permissions)
            
            # Vérifier si l'email admin est déjà dans les permissions
            admin_has_access = False
            for permission in permissions:
                if 'emailAddress' in permission and permission['emailAddress'] == self.admin_email:
                    admin_has_access = True
                    # Vérifier si le rôle est au moins "writer"
                    if permission.get('role') not in ['writer', 'owner']:
                        # Mettre à jour le rôle si nécessaire
                        safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                        print(f"   Rôle mis à jour pour {self.admin_email} (writer)")
                    break
            
            # Si l'admin n'a pas encore accès, lui donner
            if not admin_has_access:
                safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
                
        except Exception as e:
            print(f"   Erreur lors de la vérification des permissions: {e}")
    
    def update_stats_sheet(self, stats):
        """Met à jour la feuille des statistiques avec UNE SEULE LIGNE (données lifetime)"""
        try:
            # Définir les en-têtes optimisés pour Looker (sans espaces, caractères spéciaux)
            headers = [
                # Informations générales
                "Date_Mesure", 
                
                # Statistiques de partage (données cumulatives)
                "Impressions_Totales", 
                "Impressions_Uniques", 
                "Taux_Engagement", 
                "Clics_Totaux", 
                "Likes_Totaux", 
                "Commentaires_Totaux", 
                "Partages_Totaux",
                "Mentions_Partages_Totaux",
                "Mentions_Commentaires_Totaux",
                "Interactions_Totales", 
                "Taux_Clic", 
                "Taux_Interaction",
                
                # Statistiques lifetime de la page
                "Vues_Page_Totales",
                "Vues_Desktop",
                "Vues_Mobile",
                "Clics_Boutons_Desktop",
                "Clics_Boutons_Mobile",
                "Clics_Boutons_Totaux",
                
                # Vues par section
                "Vues_Accueil",
                "Vues_A_Propos",
                "Vues_Personnes",
                "Vues_Emplois",
                "Vues_Carrieres",
                "Vues_Vie_Entreprise",
                
                # Vues desktop par section
                "Vues_Desktop_Accueil",
                "Vues_Desktop_Carrieres",
                "Vues_Desktop_Emplois",
                "Vues_Desktop_Vie_Entreprise",
                
                # Vues mobile par section
                "Vues_Mobile_Accueil",
                "Vues_Mobile_Carrieres",
                "Vues_Mobile_Emplois",
                "Vues_Mobile_Vie_Entreprise"
            ]
            
            # Calculer le nombre de colonnes nécessaires
            num_cols = len(headers)
            print(f"   Nombre de colonnes nécessaires: {num_cols}")
            
            # Utiliser la feuille Sheet1 existante ou en créer une nouvelle
            try:
                sheet = self.spreadsheet.worksheet("Sheet1")
                safe_sheets_operation(sheet.update_title, "Statistiques LinkedIn")
                print("   Feuille 'Sheet1' renommée en 'Statistiques LinkedIn'")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Statistiques LinkedIn")
                    print("   Feuille 'Statistiques LinkedIn' utilisée pour les statistiques")
                except gspread.exceptions.WorksheetNotFound:
                    # Créer une nouvelle feuille avec le bon nombre de colonnes
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, 
                                                title="Statistiques LinkedIn", 
                                                rows=100, 
                                                cols=num_cols)
                    print(f"   Nouvelle feuille 'Statistiques LinkedIn' créée avec {num_cols} colonnes")
            
            # Vérifier si la feuille a assez de colonnes, sinon l'ajuster
            current_cols = sheet.col_count
            if current_cols < num_cols:
                print(f"   Ajustement du nombre de colonnes de {current_cols} à {num_cols}")
                safe_sheets_operation(sheet.add_cols, num_cols - current_cols)
                time.sleep(1)
                
            # Nettoyer complètement la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(1)
            
            # Ajouter les en-têtes
            safe_sheets_operation(sheet.update, [headers], "A1")
            time.sleep(1)
            print("   En-têtes ajoutés dans la feuille")
            
            # Préparer les nouvelles données (UNE SEULE LIGNE)
            views = stats['page_lifetime'].get('views', {})
            
            # Utiliser la date comme STRING (pas d'objet datetime)
            new_row = [
                # Informations générales - Date comme string
                stats['date'],  # STRING au lieu d'objet datetime
                
                # Statistiques de partage - Nombres entiers
                stats['impressions']['total'],
                stats['impressions']['unique'],
                stats['engagement']['rate'],  # Décimal pour PERCENT
                stats['engagement']['clicks'],
                stats['engagement']['likes'],
                stats['engagement']['comments'],
                stats['engagement']['shares'],
                stats['engagement']['share_mentions'],
                stats['engagement']['comment_mentions'],
                stats['engagement']['total_interactions'],
                stats['engagement']['click_through_rate'],  # Décimal pour PERCENT
                stats['engagement']['interaction_rate'],  # Décimal pour PERCENT
                
                # Statistiques lifetime générales - Nombres entiers
                views.get('all_page_views', 0),
                views.get('desktop_page_views', 0),
                views.get('mobile_page_views', 0),
                stats['page_lifetime'].get('clicks', {}).get('desktop_button_clicks', 0),
                stats['page_lifetime'].get('clicks', {}).get('mobile_button_clicks', 0),
                stats['page_lifetime'].get('clicks', {}).get('total_button_clicks', 0),
                
                # Vues par section - Nombres entiers
                views.get('overview_page_views', 0),
                views.get('about_page_views', 0),
                views.get('people_page_views', 0),
                views.get('jobs_page_views', 0),
                views.get('careers_page_views', 0),
                views.get('life_at_page_views', 0),
                
                # Vues desktop par section - Nombres entiers
                views.get('desktop_overview_views', 0),
                views.get('desktop_careers_views', 0),
                views.get('desktop_jobs_views', 0),
                views.get('desktop_life_at_views', 0),
                
                # Vues mobile par section - Nombres entiers
                views.get('mobile_overview_views', 0),
                views.get('mobile_careers_views', 0),
                views.get('mobile_jobs_views', 0),
                views.get('mobile_life_at_views', 0)
            ]
            
            # Ajouter la ligne de données (toujours à la ligne 2)
            safe_sheets_operation(sheet.update, [new_row], "A2")
            time.sleep(1)
            print(f"   Données lifetime mises à jour pour la date {stats['date']}")
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO avec gestion des limites
            
            # Formater les en-têtes
            last_col_index = len(headers) - 1
            last_col = self._get_column_letter(last_col_index)
            safe_sheets_operation(sheet.format, f'A1:{last_col}1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne Date (A) comme DATE
            safe_sheets_operation(sheet.format, 'A2:A2', {
                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
            })
            time.sleep(1)
            
            # Formater les colonnes de nombres entiers comme NUMBER (avec vérification des limites)
            number_column_indices = [1, 2, 4, 5, 6, 7, 8, 9, 10, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]
            
            for col_index in number_column_indices:
                if col_index < len(headers):  # Vérifier que l'index est dans les limites
                    col_letter = self._get_column_letter(col_index)
                    result = safe_sheets_operation(sheet.format, f'{col_letter}2:{col_letter}2', {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
                    })
                    if result is None:  # L'opération a été ignorée à cause des limites
                        print(f"   ⚠️ Formatage ignoré pour la colonne {col_letter} (limites dépassées)")
                    time.sleep(0.3)
            
            # Formater les colonnes de pourcentages (D=3, L=11, M=12) comme PERCENT
            percent_column_indices = [3, 11, 12]
            for col_index in percent_column_indices:
                if col_index < len(headers):  # Vérifier que l'index est dans les limites
                    col_letter = self._get_column_letter(col_index)
                    result = safe_sheets_operation(sheet.format, f'{col_letter}2:{col_letter}2', {
                        "numberFormat": {"type": "PERCENT", "pattern": "0.00%"}
                    })
                    if result is None:  # L'opération a été ignorée à cause des limites
                        print(f"   ⚠️ Formatage ignoré pour la colonne {col_letter} (limites dépassées)")
                    time.sleep(0.3)
            
            # Ajouter une note explicative
            try:
                note_row = 4
                note = [f"Note: Statistiques lifetime mises à jour le {stats['date']}. Une seule ligne de données car il s'agit de données cumulatives."]
                
                safe_sheets_operation(sheet.update, [note], f"A{note_row}")
                safe_sheets_operation(sheet.format, f"A{note_row}", {
                    "textFormat": {"italic": True},
                    "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95}
                })
                
                # Fusionner les cellules pour la note (avec vérification des limites)
                if len(headers) <= sheet.col_count:
                    safe_sheets_operation(sheet.merge_cells, f"A{note_row}:{last_col}{note_row}")
            except Exception as e:
                print(f"   ⚠️ Impossible d'ajouter la note: {e}")
            
            print(f"\n   📊 Résumé:")
            print(f"   - Données lifetime mises à jour pour le {stats['date']}")
            print(f"   - Une seule ligne de données (écrasement du précédent relevé)")
            print(f"   - Formatage optimisé pour Looker Studio appliqué ({len(headers)} colonnes)")
            
            return sheet
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille de statistiques: {e}")
            return None
    
    def _get_column_letter(self, col_idx):
        """Convertit un indice de colonne (0-based) en lettre de colonne Excel"""
        result = ""
        col_idx = col_idx + 1  # Convertir de 0-based à 1-based
        while col_idx > 0:
            col_idx, remainder = divmod(col_idx - 1, 26)
            result = chr(65 + remainder) + result
        return result
    
    def add_share_statistics(self, stats):
        """Ajoute les statistiques de partage"""
        if not self.connect():
            print("   Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Attendre un peu avant de commencer les mises à jour
        time.sleep(2)
        
        # Mettre à jour la feuille avec toutes les données
        sheet = self.update_stats_sheet(stats)
        if not sheet:
            print("   ❌ Échec de la mise à jour de la feuille")
            return False
        
        # URL du spreadsheet
        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
        print(f"   URL du tableau: {sheet_url}")
        
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


class MultiOrganizationShareTracker:
    """Gestionnaire pour les statistiques de partage de plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        self.share_mapping_file = 'share_stats_mapping.json'  # Fichier de mapping spécifique pour les stats de partage
        
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
            if os.path.exists(self.share_mapping_file):
                with open(self.share_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                print(f"   📂 Réutilisation du Google Sheet existant")
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Share_Stats_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(self.share_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Share_Stats_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        try:
            if os.path.exists(self.share_mapping_file):
                with open(self.share_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(self.share_mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(mapping, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            print(f"Erreur lors de la mise à jour du mapping: {e}")
    
    def process_all_organizations(self):
        """Traite toutes les organisations configurées"""
        if not self.access_token:
            print("Erreur: LINKEDIN_ACCESS_TOKEN manquant")
            return False
        
        # Vérifier le token une seule fois
        print("\n--- Vérification du token ---")
        is_valid, result = verify_token(self.access_token)
        
        if not is_valid:
            print(f"❌ Token invalide: {result}")
            return False
        
        print("✅ Token valide!")
        user_info = result
        print(f"   Utilisateur: {user_info.get('localizedFirstName', '')} {user_info.get('localizedLastName', '')}")
        
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
                    
                # Attendre entre chaque organisation pour éviter les problèmes de quota
                if idx < total_orgs:  # Ne pas attendre après la dernière organisation
                    print(f"   ⏳ Attente de 5 secondes avant la prochaine organisation...")
                    time.sleep(5)
                    
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
        print("RÉSUMÉ DU TRAITEMENT - STATISTIQUES DE PARTAGE")
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
            print("\n📊 Google Sheets de statistiques de partage créés/mis à jour:")
            if os.path.exists(self.share_mapping_file):
                with open(self.share_mapping_file, 'r', encoding='utf-8') as f:
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
        tracker = LinkedInShareStatisticsTracker(self.access_token, org_id, sheet_name)
        
        # 1. Obtention des statistiques de partage
        print("\n1. Récupération des statistiques cumulatives de partage...")
        raw_share_stats = tracker.get_share_statistics()
        
        # 2. Obtention des statistiques lifetime de la page
        print("\n2. Récupération des statistiques lifetime de la page...")
        raw_page_stats = tracker.get_page_statistics()
        
        if raw_share_stats or raw_page_stats:
            # Traitement des données
            print("\n3. Analyse des données statistiques...")
            stats = tracker.parse_share_statistics(raw_share_stats, raw_page_stats)
            
            # Afficher un aperçu des données
            print("\n📈 Aperçu des statistiques:")
            print(f"   Date de mesure: {stats['date']}")
            print(f"   Impressions totales: {stats['impressions']['total']}")
            print(f"   Taux d'engagement: {stats['engagement']['rate']:.2%}")
            print(f"   Total interactions: {stats['engagement']['total_interactions']}")
            
            # Chemin vers les credentials
            credentials_path = Path(__file__).resolve().parent / 'credentials' / 'service_account_credentials.json'
            
            # Pour Google Cloud Run, utiliser le chemin monté
            if os.getenv('K_SERVICE'):  # Variable d'environnement de Cloud Run
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
            
            # 4. Export vers Google Sheets
            print("\n4. Export vers Google Sheets...")
            exporter = GoogleSheetsExporter(tracker.sheet_name, credentials_path, self.admin_email)
            success = exporter.add_share_statistics(stats)
            
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
        else:
            print("❌ Impossible de récupérer les statistiques")
            return None


def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION SHARE STATISTICS TRACKER")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationShareTracker()
    
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
    print(f"   - Type de données: Statistiques cumulatives de partage (lifetime)")
    print(f"   - Formatage: Optimisé pour Looker Studio")
    print(f"   - Mode: Une seule ligne par organisation (écrasement)")
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
        response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement des statistiques de partage...")
    print("⏳ Note: Le traitement inclut des délais pour respecter les quotas Google Sheets")
    
    # Lancer le traitement
    start_time = datetime.now()
    success = tracker.process_all_organizations()
    end_time = datetime.now()
    
    # Afficher le temps d'exécution
    duration = end_time - start_time
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)
    
    print(f"\n⏱️  Temps d'exécution: {minutes}m {seconds}s")
    
    if success:
        print("\n📊 Les Google Sheets sont maintenant optimisés pour Looker Studio avec:")
        print("   ✅ Formatage des dates (DATE)")
        print("   ✅ Formatage des nombres (NUMBER)")
        print("   ✅ Formatage des pourcentages (PERCENT)")
        print("   ✅ Noms de colonnes sans espaces ni caractères spéciaux")
        print("   ✅ Types de données cohérents pour chaque colonne")
        print("   ✅ Une seule ligne de données par organisation (lifetime)")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()