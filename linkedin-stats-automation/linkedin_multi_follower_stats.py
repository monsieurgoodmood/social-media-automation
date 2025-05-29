#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Follower Statistics Tracker
Ce script collecte les statistiques des followers LinkedIn par catégorie (industrie, fonction, séniorité, etc.)
pour plusieurs organisations et les enregistre dans Google Sheets avec un formatage optimisé pour Looker Studio.
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

class LinkedInFollowerStatisticsTracker:
    """Classe pour suivre les statistiques des followers LinkedIn par catégorie"""
    
    def __init__(self, access_token, organization_id, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.sheet_name = sheet_name or f"LinkedIn_Follower_Stats_{organization_id}"
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_follower_statistics(self):
        """Obtient les statistiques de followers pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Construire l'URL
        url = f"{self.base_url}/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity={encoded_urn}"
        
        # Récupérer le nombre total de followers (pour calculer la catégorie "Autre")
        total_followers = self._get_total_followers()
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"   Données statistiques récupérées avec succès")
                    # Ajouter le nombre total de followers aux données
                    if 'elements' in data and len(data['elements']) > 0:
                        data['elements'][0]['totalFollowers'] = total_followers
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
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques des followers.")
        return None
        
    def _get_total_followers(self):
        """Récupère le nombre total de followers via une autre API"""
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        url = f"{self.base_url}/networkSizes/{encoded_urn}?edgeType=CompanyFollowedByMember"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    followers = data.get('firstDegreeSize', 0)
                    print(f"   Nombre total de followers: {followers}")
                    return followers
                elif response.status_code == 429:
                    print(f"   Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    print(f"   Erreur lors de la récupération du nombre total de followers: {response.status_code}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
            except Exception as e:
                print(f"   Exception lors de la récupération du nombre total de followers: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2
        
        print("   Impossible de récupérer le nombre total de followers.")
        return 0
    
    def parse_follower_statistics(self, data):
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
        
        # Récupérer le nombre total de followers
        total_followers = element.get('totalFollowers', 0)
        stats['total_followers'] = total_followers
        
        # Extraire les statistiques par taille d'entreprise
        stats['by_company_size'] = {}
        if 'followerCountsByStaffCountRange' in element:
            size_total = 0
            for item in element['followerCountsByStaffCountRange']:
                size_range = self._format_company_size(item.get('staffCountRange', ''))
                numeric_size = self._get_numeric_size(size_range)
                organic_count = item.get('followerCounts', {}).get('organicFollowerCount', 0)
                paid_count = item.get('followerCounts', {}).get('paidFollowerCount', 0)
                size_total += organic_count + paid_count
                stats['by_company_size'][size_range] = {
                    'numeric_size': numeric_size,
                    'organic': organic_count,
                    'paid': paid_count,
                    'total': organic_count + paid_count
                }
            
            # Ajouter une catégorie "Autre" pour les followers non catégorisés
            other_followers = total_followers - size_total
            if other_followers > 0:
                stats['by_company_size']['Autre'] = {
                    'numeric_size': 999999,  # Valeur très élevée pour qu'elle apparaisse en dernier
                    'organic': other_followers,
                    'paid': 0,
                    'total': other_followers
                }
        
        # Extraire les statistiques par fonction
        stats['by_function'] = {}
        function_descriptions = self._get_function_descriptions()
        if 'followerCountsByFunction' in element:
            function_total = 0
            for item in element['followerCountsByFunction']:
                function = item.get('function', '')
                function_id = function.split(':')[-1] if ':' in function else function
                function_name = function_descriptions.get(function_id, f"Fonction {function_id}")
                organic_count = item.get('followerCounts', {}).get('organicFollowerCount', 0)
                paid_count = item.get('followerCounts', {}).get('paidFollowerCount', 0)
                function_total += organic_count + paid_count
                stats['by_function'][function_id] = {
                    'name': function_name,
                    'organic': organic_count,
                    'paid': paid_count,
                    'total': organic_count + paid_count
                }
            
            # Ajouter une catégorie "Autre" pour les followers non catégorisés
            other_followers = total_followers - function_total
            if other_followers > 0:
                stats['by_function']['0'] = {
                    'name': 'Autre',
                    'organic': other_followers,
                    'paid': 0,
                    'total': other_followers
                }
                
        # Extraire les statistiques par ancienneté
        stats['by_seniority'] = {}
        seniority_descriptions = self._get_seniority_descriptions()
        if 'followerCountsBySeniority' in element:
            seniority_total = 0
            for item in element['followerCountsBySeniority']:
                seniority = item.get('seniority', '')
                seniority_id = seniority.split(':')[-1] if ':' in seniority else seniority
                seniority_name = seniority_descriptions.get(seniority_id, f"Niveau {seniority_id}")
                organic_count = item.get('followerCounts', {}).get('organicFollowerCount', 0)
                paid_count = item.get('followerCounts', {}).get('paidFollowerCount', 0)
                seniority_total += organic_count + paid_count
                stats['by_seniority'][seniority_id] = {
                    'name': seniority_name,
                    'organic': organic_count,
                    'paid': paid_count,
                    'total': organic_count + paid_count
                }
            
            # Ajouter une catégorie "Autre" pour les followers non catégorisés
            other_followers = total_followers - seniority_total
            if other_followers > 0:
                stats['by_seniority']['0'] = {
                    'name': 'Autre',
                    'organic': other_followers,
                    'paid': 0,
                    'total': other_followers
                }
                
        # Extraire les statistiques par industrie
        stats['by_industry'] = {}
        industry_descriptions = self._get_industry_descriptions()
        if 'followerCountsByIndustry' in element:
            industry_total = 0
            for item in element['followerCountsByIndustry']:
                industry = item.get('industry', '')
                industry_id = industry.split(':')[-1] if ':' in industry else industry
                industry_name = industry_descriptions.get(industry_id, f"Industrie {industry_id}")
                organic_count = item.get('followerCounts', {}).get('organicFollowerCount', 0)
                paid_count = item.get('followerCounts', {}).get('paidFollowerCount', 0)
                industry_total += organic_count + paid_count
                stats['by_industry'][industry_id] = {
                    'name': industry_name,
                    'organic': organic_count,
                    'paid': paid_count,
                    'total': organic_count + paid_count
                }
            
            # Ajouter une catégorie "Autre" pour les followers non catégorisés
            other_followers = total_followers - industry_total
            if other_followers > 0:
                stats['by_industry']['0'] = {
                    'name': 'Autre',
                    'organic': other_followers,
                    'paid': 0,
                    'total': other_followers
                }
        
        return stats
    
    def _get_numeric_size(self, size_range):
        """Extrait la valeur numérique maximale de la taille d'entreprise"""
        size_mapping = {
            '1 employé': 1,
            '2-10 employés': 10,
            '11-50 employés': 50,
            '51-200 employés': 200,
            '201-500 employés': 500,
            '501-1000 employés': 1000,
            '1001-5000 employés': 5000,
            '5001-10000 employés': 10000,
            '10001+ employés': 10001
        }
        return size_mapping.get(size_range, 0)
    
    def _get_function_descriptions(self):
        """Fournit une description pour les identifiants de fonction"""
        return {
            "1": "Comptabilité",
            "2": "Administration",
            "3": "Arts et design",
            "4": "Commercial",
            "5": "Support clientèle",
            "6": "Éducation",
            "7": "Ingénierie",
            "8": "Finance",
            "9": "Santé",
            "10": "Ressources humaines",
            "11": "Technologies de l'information",
            "12": "Juridique",
            "13": "Marketing",
            "14": "Médias et communication",
            "15": "Militaire et forces de l'ordre",
            "16": "Opérations",
            "17": "Autre",
            "18": "Gestion de produit",
            "19": "Achat",
            "20": "Immobilier",
            "21": "Recherche",
            "22": "Vente",
            "23": "Services sociaux",
            "24": "Support",
            "25": "Gestion de programme",
            "26": "Qualité"
        }
    
    def _get_seniority_descriptions(self):
        """Fournit une description pour les niveaux de séniorité"""
        return {
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
    
    def _get_industry_descriptions(self):
        """Fournit une description pour les identifiants d'industrie LinkedIn"""
        return {
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
            "453": "Médias en ligne",
            "840": "Distribution alimentaire",
            "1212": "Recherche et développement",
            "1324": "Télétravail",
            "1359": "Capital-risque",
            "1623": "Logiciels open source",
            "1673": "Équipement informatique",
            "1862": "Santé numérique",
            "1965": "Assurtech",
            "2029": "Fintech",
            "2353": "Développement durable",
            "3128": "Énergie renouvelable",
            "3240": "Intelligence artificielle générative",
            "3241": "Blockchain"
        }
        
    def _format_company_size(self, size_code):
        """Convertit les codes de taille d'entreprise en étiquettes lisibles"""
        size_map = {
            'SIZE_1': '1 employé',
            'SIZE_2_TO_10': '2-10 employés',
            'SIZE_11_TO_50': '11-50 employés',
            'SIZE_51_TO_200': '51-200 employés',
            'SIZE_201_TO_500': '201-500 employés',
            'SIZE_501_TO_1000': '501-1000 employés',
            'SIZE_1001_TO_5000': '1001-5000 employés',
            'SIZE_5001_TO_10000': '5001-10000 employés',
            'SIZE_10001_OR_MORE': '10001+ employés'
        }
        return size_map.get(size_code, size_code)


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
    
    def prepare_and_update_summary_sheet(self, stats):
        """Prépare et met à jour la feuille de résumé des statistiques avec formatage optimisé pour Looker"""
        try:
            # Vérifier si la Résumé existe et l'utiliser en priorité
            try:
                sheet = self.spreadsheet.worksheet("Résumé")
                print("   Feuille 'Résumé' utilisée pour le résumé")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet1")
                    safe_sheets_operation(sheet.update_title, "Résumé")
                    print("   Feuille par défaut 'Sheet1' renommée en 'Résumé'")
                except gspread.exceptions.WorksheetNotFound:
                    try:
                        sheet = self.spreadsheet.worksheet("Feuille1")
                        safe_sheets_operation(sheet.update_title, "Résumé")
                        print("   Feuille par défaut 'Feuille1' renommée en 'Résumé'")
                    except gspread.exceptions.WorksheetNotFound:
                        sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Résumé", rows=100, cols=10)
                        print("   Nouvelle feuille 'Résumé' créée")
            
            # Nettoyer la feuille existante
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Calculer le total pour les pourcentages
            total_followers = stats.get('total_followers', 0)
            if total_followers == 0:
                total_followers = sum([stats['by_company_size'][size]['organic'] for size in stats['by_company_size']])
            
            # Préparer les données pour la taille d'entreprise
            company_size_data = []
            # Ajouter l'en-tête
            company_size_data.append(['Entreprise jusqu\'à X employés', 'Nombre de Followers', 'Pourcentage'])
            
            # Convertir et trier par taille numérique
            size_data = []
            for size, stats_data in stats['by_company_size'].items():
                numeric_size = stats_data.get('numeric_size', 0)
                followers = stats_data['organic']
                percentage = (followers / total_followers) if total_followers > 0 else 0  # Valeur décimale pour Looker
                size_data.append((numeric_size, size, followers, percentage))
            
            # Trier par taille d'entreprise (ordre croissant)
            size_data.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for _, size_name, followers, percentage in size_data:
                company_size_data.append([size_name, followers, percentage])  # Pourcentage en décimal
            
            # Ajouter le total
            company_size_data.append(['Total', total_followers, 1.0])  # 100% en décimal
            
            # Mettre à jour la feuille avec les données
            safe_sheets_operation(sheet.update, company_size_data, 'A1')
            time.sleep(1)
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, "A1:C1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne des nombres (B) comme NUMBER
            safe_sheets_operation(sheet.format, "B2:B" + str(len(company_size_data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            time.sleep(1)
            
            # Formater la colonne des pourcentages (C) comme PERCENT
            safe_sheets_operation(sheet.format, "C2:C" + str(len(company_size_data)), {
                "numberFormat": {"type": "PERCENT", "pattern": "0.0%"}
            })
            time.sleep(1)
            
            # Formater la ligne de total
            safe_sheets_operation(sheet.format, f"A{len(company_size_data)}:C{len(company_size_data)}", {
                "textFormat": {"bold": True}
            })
            
            return sheet
        except Exception as e:
            print(f"   Erreur lors de la préparation de la feuille de résumé: {e}")
            return None
    
    def prepare_and_update_detail_sheets(self, stats):
        """Prépare et met à jour les feuilles détaillées pour chaque catégorie"""
        try:
            # Ajouter un délai plus long entre les mises à jour pour éviter les problèmes de quota
            print("   📊 Mise à jour des feuilles détaillées...")
            
            self._update_company_size_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_seniority_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_function_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_industry_sheet(stats)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour des feuilles détaillées: {e}")
    
    def _update_company_size_sheet(self, stats):
        """Met à jour la feuille des statistiques par taille d'entreprise avec formatage optimisé"""
        try:
            print("   🏢 Mise à jour de la feuille 'Par Taille'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Taille")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Feuille2")
                    safe_sheets_operation(sheet.update_title, "Par Taille")
                    print("   Feuille par défaut 'Feuille2' renommée en 'Par Taille'")
                except gspread.exceptions.WorksheetNotFound:
                    try:
                        sheet = self.spreadsheet.worksheet("Sheet2")
                        safe_sheets_operation(sheet.update_title, "Par Taille")
                        print("   Feuille par défaut 'Sheet2' renommée en 'Par Taille'")
                    except gspread.exceptions.WorksheetNotFound:
                        sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Taille", rows=100, cols=6)
                        print("   Nouvelle feuille 'Par Taille' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            data.append(['Date', 'Taille_Max_Employés', 'Description', 'Followers_Organiques', 'Followers_Payants', 'Total_Followers'])
            
            # Utiliser la date comme chaîne de caractères
            date_str = stats['date']
            size_entries = []
            
            for size, values in stats['by_company_size'].items():
                organic = values['organic']
                paid = values['paid']
                total = organic + paid
                numeric_size = values.get('numeric_size', 0)
                size_entries.append((numeric_size, size, organic, paid, total))
            
            # Trier par taille (ordre croissant)
            size_entries.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for numeric_size, size_name, organic, paid, total in size_entries:
                data.append([date_str, numeric_size, size_name, organic, paid, total])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:F1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne Date (A) comme DATE
            safe_sheets_operation(sheet.format, 'A2:A' + str(len(data)), {
                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
            })
            time.sleep(1)
            
            # Formater la colonne Taille_Max_Employés (B) comme NUMBER
            safe_sheets_operation(sheet.format, 'B2:B' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            time.sleep(1)
            
            # Formater les colonnes de followers (D, E, F) comme NUMBER
            safe_sheets_operation(sheet.format, 'D2:F' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des tailles d'entreprise: {e}")
    
    def _update_seniority_sheet(self, stats):
        """Met à jour la feuille des statistiques par séniorité avec formatage optimisé"""
        try:
            print("   👔 Mise à jour de la feuille 'Par Séniorité'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Séniorité")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Feuille3")
                    safe_sheets_operation(sheet.update_title, "Par Séniorité")
                    print("   Feuille par défaut 'Feuille3' renommée en 'Par Séniorité'")
                except gspread.exceptions.WorksheetNotFound:
                    try:
                        sheet = self.spreadsheet.worksheet("Sheet3")
                        safe_sheets_operation(sheet.update_title, "Par Séniorité")
                        print("   Feuille par défaut 'Sheet3' renommée en 'Par Séniorité'")
                    except gspread.exceptions.WorksheetNotFound:
                        sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Séniorité", rows=100, cols=6)
                        print("   Nouvelle feuille 'Par Séniorité' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            data.append(['Date', 'Niveau_Séniorité', 'Description', 'Followers_Organiques', 'Followers_Payants', 'Total_Followers'])
            
            # Utiliser la date comme chaîne de caractères
            date_str = stats['date']
            seniority_entries = []
            
            for seniority_id, values in stats['by_seniority'].items():
                seniority_name = values.get('name', f"Niveau {seniority_id}")
                organic = values['organic']
                paid = values['paid']
                total = organic + paid
                
                # Essayer de convertir en nombre pour le tri
                try:
                    numeric_seniority = int(seniority_id)
                except ValueError:
                    numeric_seniority = 999  # Pour "autre"
                
                seniority_entries.append((numeric_seniority, seniority_name, organic, paid, total))
            
            # Trier par niveau (ordre croissant)
            seniority_entries.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for level, description, organic, paid, total in seniority_entries:
                data.append([date_str, level, description, organic, paid, total])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:F1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne Date (A) comme DATE
            safe_sheets_operation(sheet.format, 'A2:A' + str(len(data)), {
                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
            })
            time.sleep(1)
            
            # Formater la colonne Niveau_Séniorité (B) comme NUMBER
            safe_sheets_operation(sheet.format, 'B2:B' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "0"}
            })
            time.sleep(1)
            
            # Formater les colonnes de followers (D, E, F) comme NUMBER
            safe_sheets_operation(sheet.format, 'D2:F' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des séniorités: {e}")
            
    def _update_function_sheet(self, stats):
        """Met à jour la feuille des statistiques par fonction avec formatage optimisé"""
        try:
            print("   💼 Mise à jour de la feuille 'Par Fonction'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Fonction")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Feuille4")
                    safe_sheets_operation(sheet.update_title, "Par Fonction")
                    print("   Feuille par défaut 'Feuille4' renommée en 'Par Fonction'")
                except gspread.exceptions.WorksheetNotFound:
                    try:
                        sheet = self.spreadsheet.worksheet("Sheet4")
                        safe_sheets_operation(sheet.update_title, "Par Fonction")
                        print("   Feuille par défaut 'Sheet4' renommée en 'Par Fonction'")
                    except gspread.exceptions.WorksheetNotFound:
                        sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Fonction", rows=100, cols=6)
                        print("   Nouvelle feuille 'Par Fonction' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            data.append(['Date', 'Fonction_ID', 'Nom_Fonction', 'Followers_Organiques', 'Followers_Payants', 'Total_Followers'])
            
            # Utiliser la date comme chaîne de caractères
            date_str = stats['date']
            
            # Trier par nombre de followers (ordre décroissant)
            function_entries = []
            for function_id, values in stats['by_function'].items():
                function_name = values.get('name', f"Fonction {function_id}")
                organic = values['organic']
                paid = values['paid']
                total = organic + paid
                
                # Convertir l'ID en nombre pour Looker
                try:
                    numeric_id = int(function_id)
                except ValueError:
                    numeric_id = 0  # Pour "autre"
                
                function_entries.append((numeric_id, function_name, organic, paid, total))
                
            # Trier par nombre de followers décroissant
            function_entries.sort(key=lambda x: x[4], reverse=True)
            
            # Ajouter à la liste de données
            for function_id, function_name, organic, paid, total in function_entries:
                data.append([date_str, function_id, function_name, organic, paid, total])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:F1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne Date (A) comme DATE
            safe_sheets_operation(sheet.format, 'A2:A' + str(len(data)), {
                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
            })
            time.sleep(1)
            
            # Formater la colonne Fonction_ID (B) comme NUMBER
            safe_sheets_operation(sheet.format, 'B2:B' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "0"}
            })
            time.sleep(1)
            
            # Formater les colonnes de followers (D, E, F) comme NUMBER
            safe_sheets_operation(sheet.format, 'D2:F' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des fonctions: {e}")
    
    def _update_industry_sheet(self, stats):
        """Met à jour la feuille des statistiques par industrie avec formatage optimisé"""
        try:
            print("   🏭 Mise à jour de la feuille 'Par Industrie'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Industrie")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Feuille5")
                    safe_sheets_operation(sheet.update_title, "Par Industrie")
                    print("   Feuille par défaut 'Feuille5' renommée en 'Par Industrie'")
                except gspread.exceptions.WorksheetNotFound:
                    try:
                        sheet = self.spreadsheet.worksheet("Sheet5")
                        safe_sheets_operation(sheet.update_title, "Par Industrie")
                        print("   Feuille par défaut 'Sheet5' renommée en 'Par Industrie'")
                    except gspread.exceptions.WorksheetNotFound:
                        sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Industrie", rows=1000, cols=6)
                        print("   Nouvelle feuille 'Par Industrie' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            data.append(['Date', 'Industrie_ID', 'Nom_Industrie', 'Followers_Organiques', 'Followers_Payants', 'Total_Followers'])
            
            # Utiliser la date comme chaîne de caractères
            date_str = stats['date']
            
            # Trier par nombre de followers (ordre décroissant)
            industry_entries = []
            for industry_id, values in stats['by_industry'].items():
                industry_name = values.get('name', f"Industrie {industry_id}")
                organic = values['organic']
                paid = values['paid']
                total = organic + paid
                
                # Convertir l'ID en nombre pour Looker
                try:
                    numeric_id = int(industry_id)
                except ValueError:
                    numeric_id = 0  # Pour "autre"
                
                industry_entries.append((numeric_id, industry_name, organic, paid, total))
                
            # Trier par nombre de followers décroissant
            industry_entries.sort(key=lambda x: x[4], reverse=True)
            
            # Ajouter à la liste de données
            for industry_id, industry_name, organic, paid, total in industry_entries:
                data.append([date_str, industry_id, industry_name, organic, paid, total])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # FORMATAGE OPTIMISÉ POUR LOOKER STUDIO
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:F1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater la colonne Date (A) comme DATE
            safe_sheets_operation(sheet.format, 'A2:A' + str(len(data)), {
                "numberFormat": {"type": "DATE", "pattern": "yyyy-mm-dd"}
            })
            time.sleep(1)
            
            # Formater la colonne Industrie_ID (B) comme NUMBER
            safe_sheets_operation(sheet.format, 'B2:B' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "0"}
            })
            time.sleep(1)
            
            # Formater les colonnes de followers (D, E, F) comme NUMBER
            safe_sheets_operation(sheet.format, 'D2:F' + str(len(data)), {
                "numberFormat": {"type": "NUMBER", "pattern": "#,##0"}
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des industries: {e}")
    
    def add_follower_statistics(self, stats):
        """Ajoute les statistiques de followers"""
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


class MultiOrganizationFollowerStatsTracker:
    """Gestionnaire pour les statistiques de followers de plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        self.follower_stats_mapping_file = 'follower_stats_mapping.json'
        
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
            if os.path.exists(self.follower_stats_mapping_file):
                with open(self.follower_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                print(f"   📂 Réutilisation du Google Sheet existant")
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Follower_Stats_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(self.follower_stats_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Follower_Stats_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        try:
            if os.path.exists(self.follower_stats_mapping_file):
                with open(self.follower_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(self.follower_stats_mapping_file, 'w', encoding='utf-8') as f:
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
        print("RÉSUMÉ DU TRAITEMENT - STATISTIQUES DE FOLLOWERS")
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
            print("\n📊 Google Sheets de statistiques de followers créés/mis à jour:")
            if os.path.exists(self.follower_stats_mapping_file):
                with open(self.follower_stats_mapping_file, 'r', encoding='utf-8') as f:
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
        tracker = LinkedInFollowerStatisticsTracker(self.access_token, org_id, sheet_name)
        
        # Obtention des statistiques
        print("\n1. Récupération des statistiques de followers...")
        raw_stats = tracker.get_follower_statistics()
        
        if raw_stats:
            # Traitement des données
            print("\n2. Analyse des données statistiques...")
            stats = tracker.parse_follower_statistics(raw_stats)
            
            # Afficher un aperçu
            print("\n📈 Aperçu des statistiques:")
            print(f"   Total followers: {stats.get('total_followers', 0)}")
            print(f"   Tailles d'entreprises représentées: {len(stats.get('by_company_size', {}))}")
            print(f"   Fonctions représentées: {len(stats.get('by_function', {}))}")
            print(f"   Niveaux de séniorité: {len(stats.get('by_seniority', {}))}")
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
            success = exporter.add_follower_statistics(stats)
            
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
            print("   ❌ Impossible de récupérer les statistiques de followers")
            return None


def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION FOLLOWER STATISTICS TRACKER")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationFollowerStatsTracker()
    
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
    print(f"   - Type de données: Statistiques détaillées des followers")
    print(f"   - Formatage: Optimisé pour Looker Studio")
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
        response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement des statistiques de followers...")
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
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()