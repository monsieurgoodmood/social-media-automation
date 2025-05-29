#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Page Statistics Tracker
Ce script collecte les statistiques des vues de page LinkedIn par pays, séniorité et industrie
pour plusieurs organisations et les enregistre dans Google Sheets.
"""

import os
import requests
import urllib.parse
import json
from datetime import datetime
import time
from pathlib import Path
import sys
from dotenv import load_dotenv
import random

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

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

def safe_sheets_operation(operation, *args, max_retries=5, **kwargs):
    """
    Exécute une opération Google Sheets avec gestion des erreurs de quota
    """
    for attempt in range(max_retries):
        try:
            return operation(*args, **kwargs)
        except APIError as e:
            if '429' in str(e) or 'Quota exceeded' in str(e):
                # Calcul du délai d'attente avec backoff exponentiel + jitter
                base_delay = min(60, (2 ** attempt) * 5)  # Maximum 60 secondes
                jitter = random.uniform(0.5, 1.5)  # Ajouter du jitter pour éviter la synchronisation
                delay = base_delay * jitter
                
                print(f"   ⏳ Quota dépassé (tentative {attempt + 1}/{max_retries}), attente de {delay:.1f}s...")
                time.sleep(delay)
                
                if attempt == max_retries - 1:
                    print(f"   ❌ Échec après {max_retries} tentatives: {e}")
                    raise
            else:
                print(f"   ❌ Erreur API non liée au quota: {e}")
                raise
        except Exception as e:
            print(f"   ❌ Erreur inattendue: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

class LinkedInPageStatisticsTracker:
    """Classe pour suivre les statistiques des vues de page LinkedIn par catégorie"""
    
    def __init__(self, access_token, organization_id, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.sheet_name = sheet_name or f"LinkedIn_Page_Stats_{organization_id}"
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_page_statistics(self):
        """Obtient les statistiques de vues de page pour l'organisation"""
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
                    print(f"   Données de statistiques de page récupérées avec succès")
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques de page.")
        return None
    
    def parse_page_statistics(self, data):
        """Analyse les données de l'API et extrait les statistiques pertinentes"""
        stats = {}
        
        # Date de récupération
        stats['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("   Aucune donnée de statistiques valide trouvée.")
            return stats
        
        # Obtenir le premier élément (qui contient toutes les stats)
        element = data['elements'][0]
        
        # 1. Statistiques par pays
        stats['by_country'] = {}
        if 'pageStatisticsByCountry' in element:
            for item in element['pageStatisticsByCountry']:
                country = item.get('country', 'unknown')
                country_code = country.split(':')[-1] if ':' in country else country
                country_name = self._get_country_name(country_code)
                
                views = item.get('pageStatistics', {}).get('views', {})
                
                # Extraire les données de vues pertinentes
                total_views = views.get('allPageViews', {}).get('pageViews', 0)
                desktop_views = views.get('allDesktopPageViews', {}).get('pageViews', 0)
                mobile_views = views.get('allMobilePageViews', {}).get('pageViews', 0)
                
                # Détails par type de page
                overview_views = views.get('overviewPageViews', {}).get('pageViews', 0)
                about_views = views.get('aboutPageViews', {}).get('pageViews', 0)
                people_views = views.get('peoplePageViews', {}).get('pageViews', 0)
                jobs_views = views.get('jobsPageViews', {}).get('pageViews', 0)
                careers_views = views.get('careersPageViews', {}).get('pageViews', 0)
                
                stats['by_country'][country_code] = {
                    'name': country_name,
                    'total_views': total_views,
                    'desktop_views': desktop_views,
                    'mobile_views': mobile_views,
                    'overview_views': overview_views,
                    'about_views': about_views,
                    'people_views': people_views,
                    'jobs_views': jobs_views,
                    'careers_views': careers_views
                }
        
        # 2. Statistiques par niveau de séniorité
        stats['by_seniority'] = {}
        if 'pageStatisticsBySeniority' in element:
            for item in element['pageStatisticsBySeniority']:
                seniority = item.get('seniority', 'unknown')
                seniority_id = seniority.split(':')[-1] if ':' in seniority else seniority
                seniority_name = self._get_seniority_description(seniority_id)
                
                views = item.get('pageStatistics', {}).get('views', {})
                
                # Extraire les données de vues pertinentes
                total_views = views.get('allPageViews', {}).get('pageViews', 0)
                desktop_views = views.get('allDesktopPageViews', {}).get('pageViews', 0)
                mobile_views = views.get('allMobilePageViews', {}).get('pageViews', 0)
                
                # Détails par type de page
                overview_views = views.get('overviewPageViews', {}).get('pageViews', 0)
                about_views = views.get('aboutPageViews', {}).get('pageViews', 0)
                people_views = views.get('peoplePageViews', {}).get('pageViews', 0)
                jobs_views = views.get('jobsPageViews', {}).get('pageViews', 0)
                careers_views = views.get('careersPageViews', {}).get('pageViews', 0)
                
                stats['by_seniority'][seniority_id] = {
                    'name': seniority_name,
                    'total_views': total_views,
                    'desktop_views': desktop_views,
                    'mobile_views': mobile_views,
                    'overview_views': overview_views,
                    'about_views': about_views,
                    'people_views': people_views,
                    'jobs_views': jobs_views,
                    'careers_views': careers_views
                }
        
        # 3. Statistiques par industrie
        stats['by_industry'] = {}
        if 'pageStatisticsByIndustry' in element:
            for item in element['pageStatisticsByIndustry']:
                industry = item.get('industry', 'unknown')
                industry_id = industry.split(':')[-1] if ':' in industry else industry
                industry_name = self._get_industry_description(industry_id)
                
                views = item.get('pageStatistics', {}).get('views', {})
                
                # Extraire les données de vues pertinentes
                total_views = views.get('allPageViews', {}).get('pageViews', 0)
                desktop_views = views.get('allDesktopPageViews', {}).get('pageViews', 0)
                mobile_views = views.get('allMobilePageViews', {}).get('pageViews', 0)
                
                # Détails par type de page
                overview_views = views.get('overviewPageViews', {}).get('pageViews', 0)
                about_views = views.get('aboutPageViews', {}).get('pageViews', 0)
                people_views = views.get('peoplePageViews', {}).get('pageViews', 0)
                jobs_views = views.get('jobsPageViews', {}).get('pageViews', 0)
                careers_views = views.get('careersPageViews', {}).get('pageViews', 0)
                
                stats['by_industry'][industry_id] = {
                    'name': industry_name,
                    'total_views': total_views,
                    'desktop_views': desktop_views,
                    'mobile_views': mobile_views,
                    'overview_views': overview_views,
                    'about_views': about_views,
                    'people_views': people_views,
                    'jobs_views': jobs_views,
                    'careers_views': careers_views
                }
                
        # 4. Calcul des totaux globaux
        total_page_views = 0
        total_desktop_views = 0
        total_mobile_views = 0
        total_overview_views = 0
        total_about_views = 0
        total_people_views = 0
        total_jobs_views = 0
        total_careers_views = 0
        
        # Utiliser les données par pays pour le total (le plus fiable)
        for country_code, country_data in stats['by_country'].items():
            total_page_views += country_data['total_views']
            total_desktop_views += country_data['desktop_views']
            total_mobile_views += country_data['mobile_views']
            total_overview_views += country_data['overview_views']
            total_about_views += country_data['about_views']
            total_people_views += country_data['people_views']
            total_jobs_views += country_data['jobs_views']
            total_careers_views += country_data['careers_views']
        
        stats['totals'] = {
            'total_page_views': total_page_views,
            'total_desktop_views': total_desktop_views,
            'total_mobile_views': total_mobile_views,
            'total_overview_views': total_overview_views,
            'total_about_views': total_about_views,
            'total_people_views': total_people_views,
            'total_jobs_views': total_jobs_views,
            'total_careers_views': total_careers_views
        }
        
        return stats
    
    def _get_country_name(self, country_code):
        """Obtient le nom du pays à partir du code pays"""
        countries = {
            'ae': 'Émirats arabes unis',
            'ar': 'Argentine',
            'at': 'Autriche',
            'au': 'Australie',
            'be': 'Belgique',
            'bf': 'Burkina Faso',
            'br': 'Brésil',
            'ca': 'Canada',
            'ch': 'Suisse',
            'ci': 'Côte d\'Ivoire',
            'cl': 'Chili',
            'cm': 'Cameroun',
            'cn': 'Chine',
            'co': 'Colombie',
            'cz': 'République tchèque',
            'de': 'Allemagne',
            'dk': 'Danemark',
            'dz': 'Algérie',
            'eg': 'Égypte',
            'es': 'Espagne',
            'fi': 'Finlande',
            'fr': 'France',
            'gb': 'Royaume-Uni',
            'gr': 'Grèce',
            'hk': 'Hong Kong',
            'hu': 'Hongrie',
            'id': 'Indonésie',
            'ie': 'Irlande',
            'il': 'Israël',
            'in': 'Inde',
            'it': 'Italie',
            'jp': 'Japon',
            'kr': 'Corée du Sud',
            'lu': 'Luxembourg',
            'ma': 'Maroc',
            'mg': 'Madagascar',
            'mx': 'Mexique',
            'my': 'Malaisie',
            'ng': 'Nigeria',
            'nl': 'Pays-Bas',
            'no': 'Norvège',
            'nz': 'Nouvelle-Zélande',
            'ph': 'Philippines',
            'pl': 'Pologne',
            'pt': 'Portugal',
            'ro': 'Roumanie',
            'ru': 'Russie',
            'sa': 'Arabie saoudite',
            'se': 'Suède',
            'sg': 'Singapour',
            'th': 'Thaïlande',
            'tn': 'Tunisie',
            'tr': 'Turquie',
            'tw': 'Taïwan',
            'ua': 'Ukraine',
            'us': 'États-Unis',
            'vn': 'Vietnam',
            'za': 'Afrique du Sud'
        }
        return countries.get(country_code.lower(), f"Pays {country_code}")
    
    def _get_seniority_description(self, seniority_id):
        """Fournit une description pour les niveaux de séniorité"""
        seniority_map = {
            "1": "Stagiaire",
            "2": "Débutant",
            "3": "Junior",
            "4": "Intermédiaire",
            "5": "Senior",
            "6": "Chef d'équipe",
            "7": "Directeur",
            "8": "Vice-président",
            "9": "C-suite",
            "10": "Cadre dirigeant"
        }
        return seniority_map.get(seniority_id, f"Niveau {seniority_id}")
    
    def _get_industry_description(self, industry_id):
        """Fournit une description pour les identifiants d'industrie"""
        industry_map = {
            "1": "Agriculture",
            "2": "Élevage",
            "3": "Pêche et aquaculture",
            "4": "Banque",
            "5": "Services de télécommunications",
            "6": "Construction",
            "7": "Coopératives",
            "8": "Éducation préscolaire et primaire",
            "9": "Éducation collégiale",
            "10": "Éducation secondaire",
            "11": "Éducation",
            "12": "Divertissement",
            "13": "Environnement",
            "14": "Finance",
            "15": "Gouvernement",
            "16": "Santé",
            "17": "Industrie manufacturière",
            "18": "Médias",
            "19": "Mines et métaux",
            "20": "Commerce de détail",
            "21": "Transport",
            "22": "Sports",
            "23": "Électronique",
            "24": "Énergie",
            "25": "Hôpitaux et santé",
            "26": "Assurance",
            "27": "Technologies et services de l'information",
            "28": "Luxe et bijoux",
            "29": "Machinerie",
            "30": "Maritime",
            "31": "Santé et bien-être",
            "32": "Services juridiques",
            "33": "Bibliothèques",
            "34": "Logistique et chaîne d'approvisionnement",
            "35": "Matériaux",
            "36": "Industrie militaire",
            "37": "Industrie musicale",
            "38": "Nanotechnologie",
            "39": "Journaux",
            "40": "Organisation à but non lucratif",
            "41": "Pétrole et énergie",
            "42": "Services en ligne",
            "43": "Outsourcing/Offshoring",
            "44": "Emballage et conteneurs",
            "45": "Papier et produits forestiers",
            "46": "Services philanthropiques",
            "47": "Photographie",
            "48": "Marketing et publicité",
            "49": "Imprimerie",
            "50": "Biens de consommation",
            "51": "Édition",
            "52": "Chemins de fer",
            "53": "Organisation à but non lucratif",
            "54": "Recherche",
            "55": "Restaurants",
            "56": "Sécurité et investigations",
            "57": "Semi-conducteurs",
            "58": "Transport maritime",
            "59": "Textiles",
            "60": "Mode et vêtements",
            "61": "Arts",
            "62": "Fabrication de moteurs de véhicules",
            "63": "Vins et spiritueux",
            "64": "Industrie du fil et de la fibre",
            "65": "Industrie aéronautique et aérospatiale",
            "66": "Automobile",
            "67": "Génie civil",
            "68": "Comptabilité",
            "69": "Services financiers",
            "70": "Aviation",
            "71": "Architecture et urbanisme",
            "72": "Biotechnologie",
            "73": "Construction navale",
            "74": "Produits chimiques",
            "75": "Internet",
            "76": "Équipements électriques/électroniques",
            "77": "Gestion de l'environnement",
            "78": "Produits alimentaires",
            "79": "Collecte de fonds",
            "80": "Jeux vidéo et électroniques",
            "81": "Hôtellerie",
            "82": "Mobilier d'entreprise",
            "83": "Intelligence artificielle",
            "84": "Industrie pharmaceutique",
            "85": "Plastiques",
            "86": "Commerce international et développement",
            "87": "Industrie du vin et des spiritueux",
            "88": "Commerce de gros",
            "89": "Élevage d'animaux",
            "90": "Gestion des marchés",
            "91": "Affaires politiques",
            "92": "Ressources humaines",
            "93": "Industrie laitière",
            "94": "Design",
            "95": "Services aux consommateurs",
            "96": "Logiciels informatiques",
            "97": "Événements",
            "98": "Arts et artisanat",
            "99": "Formation professionnelle et coaching",
            "100": "Affaires gouvernementales",
            "101": "Meubles",
            "102": "Équipement de loisir",
            "103": "Matériel de bureau",
            "104": "Réseaux informatiques",
            "105": "Relations publiques et communications",
            "106": "Activités de loisirs",
            "107": "Immobilier",
            "108": "Équipement sportif",
            "109": "Vente en gros",
            "110": "Services publics",
            "111": "Animation",
            "112": "Produits cosmétiques",
            "113": "Œuvres de bienfaisance",
            "114": "Industrie du bétail",
            "115": "Arts de la scène",
            "116": "Gestion des installations",
            "117": "Design graphique",
            "118": "Équipement médical",
            "119": "Conseil",
            "120": "Industrie du bois",
            "121": "Industrie de la construction",
            "122": "Enseignement supérieur",
            "123": "Promotion immobilière",
            "124": "Gestion de placements",
            "125": "Services médicaux",
            "126": "Services vétérinaires",
            "127": "Commerce de détail",
            "128": "Génie électrique",
            "129": "Informatique et réseau de sécurité",
            "130": "Matériel informatique",
            "131": "Bâtiment et travaux publics",
            "132": "Traduction et localisation",
            "133": "Programmation informatique",
            "134": "Architecture",
            "135": "Relations publiques et communications",
            "136": "Exploitation minière",
            "137": "Design",
            "138": "Santé, bien-être et fitness",
            "139": "E-learning",
            "140": "Services bancaires d'investissement",
            "141": "Musées et institutions",
            "142": "Vétérinaire",
            "143": "Industrie du voyage",
            "144": "Technologies de l'information et services",
            "145": "Gestion de l'éducation",
            "147": "Développement Web",
            "148": "Technologies et services de l'information"
        }
        return industry_map.get(industry_id, f"Industrie {industry_id}")


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
                self.spreadsheet = safe_sheets_operation(self.client.create, self.spreadsheet_name)
                print(f"   Nouveau spreadsheet créé: {self.spreadsheet_name}")
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée
                safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
            
            # Vérifier si Sheet1 existe et le renommer en "Résumé"
            try:
                sheet1 = self.spreadsheet.worksheet("Sheet1")
                safe_sheets_operation(sheet1.update_title, "Résumé")
                print("   Feuille 'Sheet1' renommée en 'Résumé'")
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
    
    def format_sheet_for_looker(self, sheet, headers, data_start_row=2):
        """Applique le formatage approprié aux colonnes pour que Looker détecte correctement les types"""
        try:
            # Définir les indices des colonnes selon leur type
            date_columns = [0]  # Colonne A (Date)
            text_columns = [1, 2]  # Colonnes B et C (Pays/Niveau/Industrie et Code/Description)
            number_columns = list(range(3, len(headers)))  # Toutes les autres colonnes sont numériques
            
            # Formater la colonne date
            for col_idx in date_columns:
                col_letter = get_column_letter(col_idx)
                range_name = f"{col_letter}{data_start_row}:{col_letter}"
                safe_sheets_operation(sheet.format, range_name, {
                    "numberFormat": {
                        "type": "DATE",
                        "pattern": "yyyy-mm-dd"
                    }
                })
            
            # Formater les colonnes de texte
            for col_idx in text_columns:
                if col_idx < len(headers):  # Vérifier que l'index existe
                    col_letter = get_column_letter(col_idx)
                    range_name = f"{col_letter}{data_start_row}:{col_letter}"
                    safe_sheets_operation(sheet.format, range_name, {
                        "numberFormat": {
                            "type": "TEXT"
                        }
                    })
            
            # Formater les colonnes numériques
            for col_idx in number_columns:
                col_letter = get_column_letter(col_idx)
                range_name = f"{col_letter}{data_start_row}:{col_letter}"
                safe_sheets_operation(sheet.format, range_name, {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "#,##0"
                    }
                })
            
            print("   ✓ Formatage appliqué pour Looker")
            
        except Exception as e:
            print(f"   Erreur lors du formatage des colonnes: {e}")
    
    def prepare_and_update_summary_sheet(self, stats):
        """Prépare et met à jour la feuille de résumé des statistiques"""
        try:
            # Vérifier si la feuille Résumé existe et l'utiliser
            try:
                sheet = self.spreadsheet.worksheet("Résumé")
                print("   Feuille 'Résumé' utilisée pour le résumé")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet1")
                    safe_sheets_operation(sheet.update_title, "Résumé")
                    print("   Feuille par défaut 'Sheet1' renommée en 'Résumé'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Résumé", rows=100, cols=10)
                    print("   Nouvelle feuille 'Résumé' créée")
            
            # Nettoyer la feuille existante
            safe_sheets_operation(sheet.clear)
            
            # Attendre un peu pour éviter les problèmes de quota
            time.sleep(2)
            
            # Préparer les données pour le résumé
            data = []
            
            # En-têtes
            headers = ["Date", "Total vues", "Desktop", "Mobile", "Accueil", "À propos", "Personnes", "Emplois", "Carrières"]
            data.append(headers)
            
            # Données totales
            totals = stats.get('totals', {})
            data.append([
                stats.get('date'),
                totals.get('total_page_views', 0),
                totals.get('total_desktop_views', 0),
                totals.get('total_mobile_views', 0),
                totals.get('total_overview_views', 0),
                totals.get('total_about_views', 0),
                totals.get('total_people_views', 0),
                totals.get('total_jobs_views', 0),
                totals.get('total_careers_views', 0)
            ])
            
            # Mettre à jour la feuille avec les données
            safe_sheets_operation(sheet.update, data, 'A1')
            
            # Attendre un peu avant le formatage
            time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:I1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            
            # Attendre un peu avant le prochain formatage
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
            return sheet
        except Exception as e:
            print(f"   Erreur lors de la préparation de la feuille de résumé: {e}")
            return None
    
    def prepare_and_update_detail_sheets(self, stats):
        """Prépare et met à jour les feuilles détaillées pour chaque catégorie"""
        try:
            # Ajouter un délai plus long entre les mises à jour pour éviter les problèmes de quota
            print("   📊 Mise à jour des feuilles détaillées...")
            
            self._update_country_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)  # Attendre plus longtemps entre chaque mise à jour
            
            self._update_seniority_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_industry_sheet(stats)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour des feuilles détaillées: {e}")
    
    def _update_country_sheet(self, stats):
        """Met à jour la feuille des statistiques par pays"""
        try:
            print("   📍 Mise à jour de la feuille 'Par Pays'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Pays")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet2")
                    safe_sheets_operation(sheet.update_title, "Par Pays")
                    print("   Feuille par défaut 'Sheet2' renommée en 'Par Pays'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Pays", rows=100, cols=10)
                    print("   Nouvelle feuille 'Par Pays' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Pays", "Code pays", "Total vues", "Desktop", "Mobile", "Accueil", "À propos", "Personnes", "Emplois"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par nombre de vues décroissant
            country_entries = []
            for country_code, values in stats['by_country'].items():
                country_entries.append((
                    values['name'],
                    country_code,
                    values['total_views'],
                    values['desktop_views'],
                    values['mobile_views'],
                    values['overview_views'],
                    values['about_views'],
                    values['people_views'],
                    values['jobs_views']
                ))
            
            # Trier par nombre de vues (décroissant)
            country_entries.sort(key=lambda x: x[2], reverse=True)
            
            # Ajouter à la liste de données
            for entry in country_entries:
                data.append([date] + list(entry))
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:J1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des pays: {e}")
    
    def _update_seniority_sheet(self, stats):
        """Met à jour la feuille des statistiques par séniorité"""
        try:
            print("   👔 Mise à jour de la feuille 'Par Séniorité'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Séniorité")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet3")
                    safe_sheets_operation(sheet.update_title, "Par Séniorité")
                    print("   Feuille par défaut 'Sheet3' renommée en 'Par Séniorité'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Séniorité", rows=100, cols=10)
                    print("   Nouvelle feuille 'Par Séniorité' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Niveau", "Description", "Total vues", "Desktop", "Mobile", "Accueil", "À propos", "Personnes", "Emplois"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par niveau de séniorité (ordre croissant)
            seniority_entries = []
            for seniority_id, values in stats['by_seniority'].items():
                try:
                    level = int(seniority_id)
                except ValueError:
                    level = 999  # Pour les valeurs non numériques
                
                seniority_entries.append((
                    level,
                    values['name'],
                    values['total_views'],
                    values['desktop_views'],
                    values['mobile_views'],
                    values['overview_views'],
                    values['about_views'],
                    values['people_views'],
                    values['jobs_views']
                ))
            
            # Trier par niveau
            seniority_entries.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for entry in seniority_entries:
                data.append([date, entry[0], entry[1]] + list(entry[2:]))
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:J1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker (la colonne Niveau est numérique ici)
            headers_modified = headers.copy()
            self.format_sheet_for_looker(sheet, headers_modified)
            
            # Formater spécifiquement la colonne Niveau comme nombre
            safe_sheets_operation(sheet.format, 'B2:B', {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "0"
                }
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des séniorités: {e}")
    
    def _update_industry_sheet(self, stats):
        """Met à jour la feuille des statistiques par industrie"""
        try:
            print("   🏭 Mise à jour de la feuille 'Par Industrie'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Industrie")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet4")
                    safe_sheets_operation(sheet.update_title, "Par Industrie")
                    print("   Feuille par défaut 'Sheet4' renommée en 'Par Industrie'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Industrie", rows=1000, cols=10)
                    print("   Nouvelle feuille 'Par Industrie' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Industrie ID", "Nom de l'industrie", "Total vues", "Desktop", "Mobile", "Accueil", "À propos", "Personnes", "Emplois"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par nombre de vues (décroissant)
            industry_entries = []
            for industry_id, values in stats['by_industry'].items():
                industry_entries.append((
                    industry_id,
                    values['name'],
                    values['total_views'],
                    values['desktop_views'],
                    values['mobile_views'],
                    values['overview_views'],
                    values['about_views'],
                    values['people_views'],
                    values['jobs_views']
                ))
            
            # Trier par nombre de vues
            industry_entries.sort(key=lambda x: x[2], reverse=True)
            
            # Ajouter à la liste de données
            for entry in industry_entries:
                data.append([date] + list(entry))
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:J1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des industries: {e}")
    
    def add_page_statistics(self, stats):
        """Ajoute les statistiques de vues de page"""
        if not self.connect():
            print("   Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Attendre un peu avant de commencer les mises à jour
        time.sleep(2)
        
        # Mettre à jour les feuilles
        summary_sheet = self.prepare_and_update_summary_sheet(stats)
        if summary_sheet:
            print("   ✅ Feuille de résumé mise à jour avec succès")
            # Attendre avant de passer aux feuilles détaillées
            print("   ⏳ Attente de 5 secondes avant les feuilles détaillées...")
            time.sleep(5)
            
            self.prepare_and_update_detail_sheets(stats)
        else:
            print("   ❌ Échec de la mise à jour de la feuille de résumé")
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


class MultiOrganizationPageStatsTracker:
    """Gestionnaire pour les statistiques de pages de plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        self.page_stats_mapping_file = 'page_stats_mapping.json'
        
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
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                print(f"   📂 Réutilisation du Google Sheet existant")
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Page_Stats_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(self.page_stats_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Page_Stats_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        try:
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(self.page_stats_mapping_file, 'w', encoding='utf-8') as f:
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
                    print(f"   ⏳ Attente de 15 secondes avant la prochaine organisation...")
                    time.sleep(15)
                    
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
        print("RÉSUMÉ DU TRAITEMENT - STATISTIQUES DE PAGES")
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
            print("\n📊 Google Sheets de statistiques de pages créés/mis à jour:")
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
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
        tracker = LinkedInPageStatisticsTracker(self.access_token, org_id, sheet_name)
        
        # Obtention des statistiques
        print("\n1. Récupération des statistiques de vues de page...")
        raw_stats = tracker.get_page_statistics()
        
        if raw_stats:
            # Traitement des données
            print("\n2. Analyse des données statistiques...")
            stats = tracker.parse_page_statistics(raw_stats)
            
            # Afficher un aperçu
            print("\n📈 Aperçu des statistiques:")
            totals = stats.get('totals', {})
            print(f"   Total vues: {totals.get('total_page_views', 0)}")
            print(f"   Vues desktop: {totals.get('total_desktop_views', 0)}")
            print(f"   Vues mobile: {totals.get('total_mobile_views', 0)}")
            print(f"   Pays uniques: {len(stats.get('by_country', {}))}")
            print(f"   Industries représentées: {len(stats.get('by_industry', {}))}")
            
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
            print("\n3. Export vers Google Sheets...")
            exporter = GoogleSheetsExporter(tracker.sheet_name, credentials_path, self.admin_email)
            success = exporter.add_page_statistics(stats)
            
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
            print("   ❌ Impossible de récupérer les statistiques de vues de page")
            return None


def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION PAGE STATISTICS TRACKER")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationPageStatsTracker()
    
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
    print(f"   - Type de données: Statistiques par pays, séniorité et industrie")
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
        response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement des statistiques de pages...")
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
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()