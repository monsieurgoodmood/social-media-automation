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
    
    def parse_share_statistics(self, data):
        """Analyse les données de l'API et extrait les statistiques pertinentes"""
        stats = {}
        
        # Date de récupération
        stats['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("Aucune donnée de statistiques valide trouvée.")
            return stats
        
        # Obtenir le premier élément (qui contient les stats d'organisation)
        element = data['elements'][0]
        
        if 'totalShareStatistics' not in element:
            print("Aucune statistique de partage trouvée dans les données.")
            return stats
        
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
    
    def prepare_and_update_stats_sheet(self, stats):
        """Prépare et met à jour la feuille principale des statistiques"""
        try:
            # Utiliser la feuille Sheet1 existante ou en créer une nouvelle
            try:
                sheet = self.spreadsheet.worksheet("Sheet1")
                sheet.update_title("Statistiques de partage")
                print("Feuille 'Sheet1' renommée en 'Statistiques de partage'")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Statistiques de partage")
                    print("Feuille 'Statistiques de partage' utilisée pour les statistiques")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = self.spreadsheet.add_worksheet(title="Statistiques de partage", rows=1000, cols=15)
                    print("Nouvelle feuille 'Statistiques de partage' créée pour les statistiques")
            
            # Vérifier si nous avons déjà des données dans la feuille
            existing_data = sheet.get_all_values()
            headers_exist = len(existing_data) > 0 and len(existing_data[0]) > 1  # Vérifier que la première ligne a du contenu
            
            # Définir les en-têtes
            headers = [
                "Date", 
                "Total Impressions", 
                "Impressions Uniques", 
                "Taux d'engagement (%)", 
                "Clics", 
                "Likes", 
                "Commentaires", 
                "Partages",
                "Mentions dans partages",
                "Mentions dans commentaires",
                "Total interactions", 
                "Taux de clic (%)", 
                "Taux d'interaction (%)"
            ]
            
            # Si nous n'avons pas d'en-têtes existantes, les ajouter
            if not headers_exist:
                sheet.update([headers], "A1")
                
                # Formater les en-têtes
                sheet.format('A1:M1', {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                })
                
                print("En-têtes ajoutés à la feuille")
            
            # Préparer les nouvelles données
            new_row = [
                stats['date'],
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
                f"{stats['engagement']['interaction_rate']:.2f}"
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
            
            return sheet
        except Exception as e:
            print(f"Erreur lors de la préparation de la feuille de statistiques: {e}")
            return None
    
    def add_share_statistics(self, stats):
        """Ajoute les statistiques de partage"""
        if not self.connect():
            print("Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Mettre à jour la feuille principale
        self.prepare_and_update_stats_sheet(stats)
        
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


# Remarque: Cette fonction a été ajustée pour mettre en évidence les noms de colonnes
# et ne pas utiliser d'onglet Résumé, mais plutôt une seule feuille avec un historique chronologique

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
    print("\n--- Récupération des statistiques de partage ---")
    raw_stats = tracker.get_share_statistics()
    
    if raw_stats:
        # Traitement des données
        print("Analyse des données statistiques...")
        stats = tracker.parse_share_statistics(raw_stats)
        
        # Afficher un aperçu des données
        print("\n--- Aperçu des statistiques ---")
        print(f"Date: {stats['date']}")
        print(f"Impressions totales: {stats['impressions']['total']}")
        print(f"Impressions uniques: {stats['impressions']['unique']}")
        print(f"Taux d'engagement: {stats['engagement']['rate']:.2f}%")
        print(f"Clics: {stats['engagement']['clicks']}")
        print(f"Likes: {stats['engagement']['likes']}")
        print(f"Commentaires: {stats['engagement']['comments']}")
        print(f"Partages: {stats['engagement']['shares']}")
        print(f"Total interactions: {stats['engagement']['total_interactions']}")
        print(f"Taux de clic: {stats['engagement']['click_through_rate']:.2f}%")
        print(f"Taux d'interaction: {stats['engagement']['interaction_rate']:.2f}%")
        
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
        print("❌ Impossible de récupérer les statistiques de partage")