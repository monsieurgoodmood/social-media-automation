#!/usr/bin/env python3
"""
Collecteur de métriques LinkedIn via l'API Pages Data Portability (DMA)
Version complète avec support de débogage et exportation améliorée
"""

import requests
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from dotenv import load_dotenv
from pathlib import Path
import json
import urllib.parse
import sys
import time

# Chargement des variables d'environnement
load_dotenv()

class LinkedInMetricsExporter:
    """Classe pour gérer l'exportation des métriques LinkedIn vers Google Sheets"""
    
    def __init__(self, spreadsheet_id, credentials_path):
        """Initialise l'exportateur avec l'ID du spreadsheet et le chemin des credentials"""
        self.spreadsheet_id = spreadsheet_id
        self.credentials_path = credentials_path
        self.client = None
        self.spreadsheet = None
        
    def connect(self):
        """Établit la connexion avec Google Sheets API"""
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(str(self.credentials_path), scope)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
            return True
        except Exception as e:
            print(f"Erreur de connexion à Google Sheets: {e}")
            return False

    def clear_sheets(self):
        """Supprime les feuilles existantes et crée des nouvelles feuilles organisées"""
        try:
            # Supprimer toutes les feuilles sauf la première
            all_sheets = self.spreadsheet.worksheets()
            for sheet in all_sheets[1:]:
                self.spreadsheet.del_worksheet(sheet)
            
            # Renommer la première feuille
            main_sheet = all_sheets[0]
            main_sheet.update_title("Résumé des métriques")
            
            # Créer des nouvelles feuilles
            self.spreadsheet.add_worksheet(title="Followers", rows=1000, cols=20)
            self.spreadsheet.add_worksheet(title="Engagement", rows=1000, cols=20)
            self.spreadsheet.add_worksheet(title="Page Views", rows=1000, cols=20)
            self.spreadsheet.add_worksheet(title="Raw Data", rows=1000, cols=20)
            
            # Nettoyer la feuille principale
            main_sheet.clear()
            
            return main_sheet
        except Exception as e:
            print(f"Erreur lors de la création des feuilles: {e}")
            return None

    def export_metrics(self, metrics):
        """Exporte les métriques formatées vers Google Sheets"""
        # Se connecter et préparer les feuilles
        if not self.connect():
            return False
        
        # Récupérer ou créer les feuilles
        try:
            main_sheet = self.spreadsheet.worksheet("Résumé des métriques")
        except:
            main_sheet = self.clear_sheets()
            if not main_sheet:
                return False
        
        # Exporter les données vers la feuille principale
        self._export_summary(main_sheet, metrics)
        
        # Exporter les données vers les feuilles spécifiques
        self._export_followers(metrics)
        self._export_engagement(metrics)
        self._export_page_views(metrics)
        self._export_raw_data(metrics)
        
        # Afficher l'URL
        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
        print(f"\nExport vers Google Sheets réussi!")
        print(f"URL du tableau: {sheet_url}")
        
        return True
    
    def _export_summary(self, sheet, metrics):
        """Exporte un résumé des métriques principales"""
        # Titre et date
        sheet.update_cell(1, 1, "RÉSUMÉ DES MÉTRIQUES LINKEDIN")
        sheet.update_cell(2, 1, f"Date d'extraction: {metrics.get('date', datetime.now().strftime('%Y-%m-%d'))}")
        
        # Formatage du titre
        sheet.format("A1", {
            "textFormat": {"bold": True, "fontSize": 14},
            "horizontalAlignment": "CENTER"
        })
        
        # Section Followers
        sheet.update_cell(4, 1, "FOLLOWERS")
        sheet.update_cell(5, 1, "Total Followers:")
        sheet.update_cell(5, 2, metrics.get('followers', 0))
        
        # Section Engagement
        sheet.update_cell(7, 1, "ENGAGEMENT")
        engagement = metrics.get('engagement', {})
        
        row = 8
        sheet.update_cell(row, 1, "Likes:")
        sheet.update_cell(row, 2, engagement.get('likes', 0))
        row += 1
        sheet.update_cell(row, 1, "Comments:")
        sheet.update_cell(row, 2, engagement.get('comments', 0))
        row += 1
        sheet.update_cell(row, 1, "Shares:")
        sheet.update_cell(row, 2, engagement.get('shares', 0))
        row += 1
        sheet.update_cell(row, 1, "Mentions:")
        sheet.update_cell(row, 2, engagement.get('mentions', 0))
        row += 1
        sheet.update_cell(row, 1, "Total Engagements:")
        sheet.update_cell(row, 2, engagement.get('total_engagements', 0))
        
        # Section Impressions
        row += 2
        sheet.update_cell(row, 1, "IMPRESSIONS")
        row += 1
        sheet.update_cell(row, 1, "Total Impressions:")
        sheet.update_cell(row, 2, metrics.get('impressions', {}).get('total_impressions', 0))
        
        # Section Page Views
        row += 2
        sheet.update_cell(row, 1, "PAGE VIEWS")
        
        page_views = metrics.get('page_views_by_section', {})
        row += 1
        sheet.update_cell(row, 1, "Overview Page:")
        sheet.update_cell(row, 2, page_views.get('OVERVIEW', 0))
        row += 1
        sheet.update_cell(row, 1, "About Page:")
        sheet.update_cell(row, 2, page_views.get('ABOUT', 0))
        row += 1
        sheet.update_cell(row, 1, "Jobs Page:")
        sheet.update_cell(row, 2, page_views.get('JOBS', 0))
        row += 1
        sheet.update_cell(row, 1, "Life Page:")
        sheet.update_cell(row, 2, page_views.get('LIFE', 0))
        
        # Formatage des sections
        for row in [4, 7, 13, 16]:
            sheet.format(f"A{row}", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
        
        # Instructions
        row += 3
        sheet.update_cell(row, 1, "Pour consulter les données détaillées, voir les autres onglets du classeur.")
    
    def _export_followers(self, metrics):
        """Exporte les données de followers dans un onglet dédié"""
        try:
            sheet = self.spreadsheet.worksheet("Followers")
            sheet.clear()
            
            # En-tête
            sheet.update_cell(1, 1, "HISTORIQUE DES FOLLOWERS")
            sheet.update_cell(3, 1, "Date")
            sheet.update_cell(3, 2, "Nombre de Followers")
            
            # Données
            sheet.update_cell(4, 1, metrics.get('date', datetime.now().strftime('%Y-%m-%d')))
            sheet.update_cell(4, 2, metrics.get('followers', 0))
            
            # Formatage
            sheet.format("A1", {
                "textFormat": {"bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER"
            })
            sheet.format("A3:B3", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
        except Exception as e:
            print(f"Erreur lors de l'export des followers: {e}")
    
    def _export_engagement(self, metrics):
        """Exporte les données d'engagement dans un onglet dédié"""
        try:
            sheet = self.spreadsheet.worksheet("Engagement")
            sheet.clear()
            
            # En-tête
            sheet.update_cell(1, 1, "MÉTRIQUES D'ENGAGEMENT")
            sheet.update_cell(3, 1, "Date")
            sheet.update_cell(3, 2, "Likes")
            sheet.update_cell(3, 3, "Comments")
            sheet.update_cell(3, 4, "Shares")
            sheet.update_cell(3, 5, "Mentions")
            sheet.update_cell(3, 6, "Total Engagements")
            
            # Données
            engagement = metrics.get('engagement', {})
            sheet.update_cell(4, 1, metrics.get('date', datetime.now().strftime('%Y-%m-%d')))
            sheet.update_cell(4, 2, engagement.get('likes', 0))
            sheet.update_cell(4, 3, engagement.get('comments', 0))
            sheet.update_cell(4, 4, engagement.get('shares', 0))
            sheet.update_cell(4, 5, engagement.get('mentions', 0))
            sheet.update_cell(4, 6, engagement.get('total_engagements', 0))
            
            # Formatage
            sheet.format("A1", {
                "textFormat": {"bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER"
            })
            sheet.format("A3:F3", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
        except Exception as e:
            print(f"Erreur lors de l'export des engagements: {e}")
    
    def _export_page_views(self, metrics):
        """Exporte les données de vues de page dans un onglet dédié"""
        try:
            sheet = self.spreadsheet.worksheet("Page Views")
            sheet.clear()
            
            # En-tête
            sheet.update_cell(1, 1, "VUES DE PAGE PAR SECTION")
            sheet.update_cell(3, 1, "Date")
            sheet.update_cell(3, 2, "Overview")
            sheet.update_cell(3, 3, "About")
            sheet.update_cell(3, 4, "Jobs")
            sheet.update_cell(3, 5, "Life")
            sheet.update_cell(3, 6, "Total Page Views")
            sheet.update_cell(3, 7, "Unique Visitors")
            
            # Données
            page_views = metrics.get('page_views_by_section', {})
            page_analytics = metrics.get('page_analytics', {})
            total_views = 0
            unique_visitors = 0
            
            try:
                analytics_data = page_analytics.get("elements", [{}])[0]
                total_views = analytics_data.get('totalPageStatistics', {}).get('views', 0)
                unique_visitors = analytics_data.get('totalPageStatistics', {}).get('uniqueVisitors', 0)
            except (IndexError, KeyError):
                pass
            
            sheet.update_cell(4, 1, metrics.get('date', datetime.now().strftime('%Y-%m-%d')))
            sheet.update_cell(4, 2, page_views.get('OVERVIEW', 0))
            sheet.update_cell(4, 3, page_views.get('ABOUT', 0))
            sheet.update_cell(4, 4, page_views.get('JOBS', 0))
            sheet.update_cell(4, 5, page_views.get('LIFE', 0))
            sheet.update_cell(4, 6, total_views)
            sheet.update_cell(4, 7, unique_visitors)
            
            # Formatage
            sheet.format("A1", {
                "textFormat": {"bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER"
            })
            sheet.format("A3:G3", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
        except Exception as e:
            print(f"Erreur lors de l'export des vues de page: {e}")
    
    def _export_raw_data(self, metrics):
        """Exporte les données brutes dans un onglet dédié"""
        try:
            sheet = self.spreadsheet.worksheet("Raw Data")
            sheet.clear()
            
            # En-tête
            sheet.update_cell(1, 1, "DONNÉES BRUTES")
            sheet.update_cell(2, 1, "Ces données sont au format JSON et peuvent être utilisées pour des analyses personnalisées.")
            
            # Données
            row = 4
            for section, data in metrics.items():
                if section == 'date':
                    continue
                    
                sheet.update_cell(row, 1, section.upper())
                row += 1
                
                if isinstance(data, dict):
                    sheet.update_cell(row, 1, json.dumps(data, ensure_ascii=False, indent=2))
                else:
                    sheet.update_cell(row, 1, str(data))
                    
                row += 2
            
            # Formatage
            sheet.format("A1", {
                "textFormat": {"bold": True, "fontSize": 14},
                "horizontalAlignment": "CENTER"
            })
        except Exception as e:
            print(f"Erreur lors de l'export des données brutes: {e}")


class LinkedInDMAMetricsCollector:
    def __init__(self, access_token, organization_id, debug=False):
        self.access_token = access_token
        self.organization_id = organization_id
        self.debug = debug
        self.base_url = "https://api.linkedin.com/rest"
        
        # L'URN sera déterminé par auto-détection
        self.organization_urn = self._detect_valid_urn()
        
        if self.debug:
            print(f"URN utilisé: {self.organization_urn}")

    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202310"
        }
        
    def _detect_valid_urn(self):
        """Détecte automatiquement le format d'URN valide pour cette organisation"""
        if self.debug:
            print("\n--- Détection du format d'URN valide ---")
        
        # Formats d'URN potentiels à tester
        urn_formats = [
            f"urn:li:organization:{self.organization_id}",
            f"urn:li:organizationalPage:{self.organization_id}",
            f"urn:li:company:{self.organization_id}",
            f"urn:li:page:{self.organization_id}",
            self.organization_id  # Tel quel, sans préfixe
        ]
        
        # Tester chaque format avec un endpoint de base
        for urn in urn_formats:
            if self.debug:
                print(f"Test avec URN: {urn}")
                
            encoded_urn = urllib.parse.quote(urn)
            url = f"{self.base_url}/dmaOrganizationFollowers?q=organization&organization={encoded_urn}"
            
            try:
                response = requests.get(url, headers=self.get_headers())
                if response.status_code == 200:
                    if self.debug:
                        print(f"URN valide trouvé: {urn}")
                    return urn
                else:
                    if self.debug:
                        print(f"Échec avec statut {response.status_code}")
            except Exception as e:
                if self.debug:
                    print(f"Exception: {e}")
        
        # Si aucun format ne fonctionne, retourner le format standard
        if self.debug:
            print("Aucun URN valide trouvé, utilisation du format par défaut")
        return f"urn:li:organization:{self.organization_id}"
    
    def _make_api_request(self, endpoint, params=None):
        """Effectue une requête API avec gestion des erreurs et retry"""
        if not params:
            params = {"q": "organization", "organization": self.organization_urn}
        
        # Encoder l'URN si présent dans les paramètres
        if "organization" in params:
            params["organization"] = urllib.parse.quote(params["organization"])
            
        # Construire l'URL avec les paramètres
        url_params = "&".join([f"{k}={v}" for k, v in params.items()])
        url = f"{self.base_url}/{endpoint}?{url_params}"
        
        if self.debug:
            print(f"Requête API: {url}")
        
        # Tentatives multiples avec backoff exponentiel
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    # Rate limit, attendre avant de réessayer
                    if self.debug:
                        print(f"Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Backoff exponentiel
                else:
                    if self.debug:
                        print(f"Erreur {endpoint}: {response.status_code} - {response.text}")
                    return None
                    
            except Exception as e:
                if self.debug:
                    print(f"Exception lors de la requête {endpoint}: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return None  # Échec après toutes les tentatives

    def get_follower_count(self):
        """Obtient le nombre total d'abonnés"""
        # Essayer avec les deux endpoints possibles pour les followers
        data = self._make_api_request("dmaOrganizationFollowers")
        
        if not data or len(data.get("elements", [])) == 0:
            # Si le premier endpoint ne fonctionne pas, essayer un autre
            data = self._make_api_request("dmaFollowers")
            
            # Si toujours pas de données, essayer l'API standard (non-DMA)
            if not data or len(data.get("elements", [])) == 0:
                try:
                    # Tenter d'utiliser l'API standard
                    url = f"{self.base_url}/organizations/{self.organization_id}?fields=followingInfo"
                    response = requests.get(url, headers=self.get_headers())
                    
                    if response.status_code == 200:
                        data = response.json()
                        return data.get("followingInfo", {}).get("followerCount", 0)
                    else:
                        # Utiliser une méthode alternative via l'API Marketing Analytics
                        url = f"{self.base_url}/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity={urllib.parse.quote(self.organization_urn)}"
                        response = requests.get(url, headers=self.get_headers())
                        
                        if response.status_code == 200:
                            data = response.json()
                            if "elements" in data and len(data["elements"]) > 0:
                                return data["elements"][0].get("totalFollowerCount", 0)
                except Exception as e:
                    if self.debug:
                        print(f"Exception lors de la récupération des followers via API alternative: {e}")
            
        # Si données trouvées, compter les éléments
        if data and "elements" in data:
            return len(data.get("elements", []))
        
        # Si aucune des méthodes ne fonctionne, demander l'entrée manuelle
        print("\nImpossible de récupérer le nombre d'abonnés via l'API.")
        try:
            count = input("Veuillez entrer manuellement le nombre d'abonnés (ou 'skip' pour ignorer): ")
            if count.lower() != 'skip':
                return int(count)
        except:
            pass
        
        return 0

    def get_engagement_metrics(self):
        """Récupère les métriques d'engagement (reactions, partages, commentaires)"""
        data = self._make_api_request("dmaOrganizationSocialActions")
        if not data:
            return {}
        
        # Analyse des éléments pour compter les différents types d'engagement
        metrics = {
            "likes": 0,
            "comments": 0,
            "shares": 0,
            "mentions": 0,
            "total_engagements": 0
        }
        
        for item in data.get("elements", []):
            action_type = item.get("socialActionType", "")
            if action_type == "LIKE":
                metrics["likes"] += 1
            elif action_type == "COMMENT":
                metrics["comments"] += 1
            elif action_type == "SHARE":
                metrics["shares"] += 1
            elif action_type == "MENTION":
                metrics["mentions"] += 1
        
        metrics["total_engagements"] = sum([
            metrics["likes"], 
            metrics["comments"], 
            metrics["shares"],
            metrics["mentions"]
        ])
        
        return metrics

    def get_page_analytics(self):
        """Récupère les statistiques de page (impressions, vues)"""
        return self._make_api_request("dmaOrganizationPageStatistics") or {}

    def get_page_views_by_section(self):
        """Récupère les vues par section de la page"""
        data = self._make_api_request("dmaOrganizationPageViews")
        if not data:
            return {}
        
        # Organiser les vues par section
        views_by_section = {}
        for item in data.get("elements", []):
            section = item.get("pageSection", "unknown")
            if section in views_by_section:
                views_by_section[section] += 1
            else:
                views_by_section[section] = 1
                
        return views_by_section

    def get_life_at_page_views(self):
        """Récupère les statistiques de la page Life at Company"""
        return self._make_api_request("dmaOrganizationLifePageStatistics") or {}
    
    def get_post_analytics(self):
        """Récupère les statistiques des publications"""
        return self._make_api_request("dmaOrganizationPostAnalytics") or {}
    
    def get_impression_metrics(self):
        """Récupère les métriques d'impressions"""
        data = self._make_api_request("dmaOrganizationImpressions")
        if not data:
            return {}
        
        total_impressions = len(data.get("elements", []))
        return {"total_impressions": total_impressions}

    def collect_lifetime_metrics(self):
        """Collecte toutes les métriques disponibles"""
        print("\n--- Collecte des métriques LinkedIn (mode lifetime) ---")
        today = datetime.now().strftime('%Y-%m-%d')

        # Récupération de toutes les métriques
        followers = self.get_follower_count()
        engagement = self.get_engagement_metrics()
        page_analytics = self.get_page_analytics()
        page_views_by_section = self.get_page_views_by_section()
        life_at_page = self.get_life_at_page_views()
        post_analytics = self.get_post_analytics()
        impression_metrics = self.get_impression_metrics()

        metrics = {
            'date': today,
            'followers': followers,
            'engagement': engagement,
            'page_analytics': page_analytics,
            'page_views_by_section': page_views_by_section,
            'life_at_page': life_at_page,
            'post_analytics': post_analytics,
            'impressions': impression_metrics
        }

        # Affichage des résultats
        print(f"Date: {metrics['date']}")
        print(f"Total followers: {metrics['followers']}")
        
        if engagement:
            print("\nEngagement metrics:")
            print(f"- Likes: {engagement.get('likes', 0)}")
            print(f"- Comments: {engagement.get('comments', 0)}")
            print(f"- Shares: {engagement.get('shares', 0)}")
            print(f"- Mentions: {engagement.get('mentions', 0)}")
            print(f"- Total engagements: {engagement.get('total_engagements', 0)}")
        
        if impression_metrics:
            print("\nImpression metrics:")
            print(f"- Total impressions: {impression_metrics.get('total_impressions', 0)}")
        
        if page_views_by_section:
            print("\nPage views by section:")
            for section, count in page_views_by_section.items():
                print(f"- {section}: {count}")
        
        if page_analytics:
            print("\nPage analytics summary:")
            # Extraire et afficher les données pertinentes
            try:
                analytics_data = page_analytics.get("elements", [{}])[0]
                print(f"- Total page views: {analytics_data.get('totalPageStatistics', {}).get('views', 0)}")
                print(f"- Unique visitors: {analytics_data.get('totalPageStatistics', {}).get('uniqueVisitors', 0)}")
            except (IndexError, KeyError):
                print("- Données de page analytics non disponibles")
        
        # Données brutes pour débogage si nécessaire
        if self.debug:
            print("\nDonnées brutes pour débogage:")
            print(f"Page analytics: {json.dumps(page_analytics, indent=2)}")
            print(f"Life at page: {json.dumps(life_at_page, indent=2)}")
            print(f"Post analytics: {json.dumps(post_analytics, indent=2)}")

        return metrics


def verify_token(access_token):
    """Vérifie si le token d'accès est valide"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version": "202310"
    }
    
    url = "https://api.linkedin.com/rest/me"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"Erreur {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)


def debug_mode():
    """Mode débogage pour tester l'accès à l'API"""
    print("\n=== MODE DÉBOGAGE LINKEDIN API ===")
    
    # Récupération des variables d'environnement
    access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
    organization_id = os.getenv("LINKEDIN_ORGANIZATION_ID", "")

    if not access_token:
        print("Erreur: Variable d'environnement LINKEDIN_ACCESS_TOKEN manquante")
        return
    
    if not organization_id:
        print("Erreur: Variable d'environnement LINKEDIN_ORGANIZATION_ID manquante")
        return
    
    print(f"ID d'organisation: {organization_id}")
    print(f"Token (premiers caractères): {access_token[:10]}...")
    
    # Vérification du token
    print("\n--- Vérification du token ---")
    is_valid, result = verify_token(access_token)
    
    if is_valid:
        print("✅ Token valide!")
        print(f"Profil: {json.dumps(result, indent=2)}")
    else:
        print(f"❌ Token invalide: {result}")
        return
    
    # Test des différents formats d'URN
    print("\n--- Test des formats d'URN ---")
    collector = LinkedInDMAMetricsCollector(access_token, organization_id, debug=True)
    
    # Test des endpoints
    print("\n--- Test des endpoints DMA ---")
    endpoints = [
        "dmaOrganizationFollowers",
        "dmaOrganizationSocialActions", 
        "dmaOrganizationPageStatistics",
        "dmaOrganizationPageViews",
        "dmaOrganizationLifePageStatistics",
        "dmaOrganizationPostAnalytics",
        "dmaOrganizationImpressions"
    ]
    
    for endpoint in endpoints:
        print(f"\nTest de {endpoint}:")
        params = {"q": "organization", "organization": collector.organization_urn}
        result = collector._make_api_request(endpoint, params)
        
        if result:
            print(f"✅ Succès! Nombre d'éléments: {len(result.get('elements', []))}")
        else:
            print(f"❌ Échec")
    
    # Tester aussi les API standards
    print("\n--- Test des API standards LinkedIn ---")
    try:
        url = f"https://api.linkedin.com/rest/organizations/{organization_id}?fields=followingInfo"
        response = requests.get(url, headers=collector.get_headers())
        
        # Suite du code de debug_mode()
        if response.status_code == 200:
            data = response.json()
            print(f"✅ API standard: Succès!")
            print(f"Followers: {data.get('followingInfo', {}).get('followerCount', 0)}")
        else:
            print(f"❌ API standard: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Exception lors du test de l'API standard: {e}")
    
    print("\n=== FIN DU MODE DÉBOGAGE ===")


if __name__ == "__main__":
    # Vérifier si le mode débogage est demandé
    if len(sys.argv) > 1 and sys.argv[1] == "--debug":
        debug_mode()
        sys.exit(0)
    
    # Récupération des variables d'environnement
    access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
    organization_id = os.getenv("LINKEDIN_ORGANIZATION_ID", "")
    spreadsheet_id = os.getenv("GOOGLE_SPREADSHEET_ID", "")

    if not access_token or not organization_id:
        print("Erreur: Variables d'environnement LINKEDIN_ACCESS_TOKEN ou LINKEDIN_ORGANIZATION_ID manquantes")
        print("Créez un fichier .env avec les variables:")
        print("LINKEDIN_ACCESS_TOKEN='votre_token'")
        print("LINKEDIN_ORGANIZATION_ID='votre_id_organisation'")
        print("GOOGLE_SPREADSHEET_ID='votre_id_spreadsheet'  # Optionnel")
        sys.exit(1)

    # Création de l'instance du collecteur
    collector = LinkedInDMAMetricsCollector(access_token, organization_id)
    
    # Collecte des métriques
    metrics = collector.collect_lifetime_metrics()

    # Export vers Google Sheets si l'ID est fourni
    if spreadsheet_id:
        try:
            # Chemin vers les credentials
            base_dir = Path(__file__).resolve().parent.parent.parent  # Remontez d'un niveau supplémentaire
            credentials_path = base_dir / 'credentials' / 'service_account_credentials.json'
            
            if not credentials_path.exists():
                print(f"Erreur: Fichier de credentials Google non trouvé à {credentials_path}")
                print("Métriques collectées mais non exportées vers Google Sheets.")
                print(f"Créez le dossier et le fichier: {credentials_path}")
            else:
                # Utiliser l'exportateur amélioré
                exporter = LinkedInMetricsExporter(spreadsheet_id, credentials_path)
                exporter.export_metrics(metrics)
        except Exception as e:
            print(f"Erreur lors de l'export vers Google Sheets: {e}")
            print("Métriques collectées mais non exportées.")
    else:
        print("\nAucun ID de Google Sheets fourni, les données ne seront pas exportées.")
    
    print("\nUtilisez l'option --debug pour le mode débogage: python3 linkedin_metrics.py --debug")