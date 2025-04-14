#!/usr/bin/env python3
"""
LinkedIn Followers Tracker
Ce script collecte le nombre de followers LinkedIn d'une organisation et l'enregistre dans Google Sheets.
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

class LinkedInFollowersTracker:
    """Classe pour suivre le nombre de followers LinkedIn d'une organisation"""
    
    def __init__(self, access_token, organization_id, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.sheet_name = sheet_name or f"LinkedIn_Followers_{organization_id}"
        self.base_url = "https://api.linkedin.com/v2"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202312",
            "Content-Type": "application/json"
        }
    
    def get_follower_count(self):
        """Obtient le nombre de followers pour l'organisation"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Construire l'URL
        url = f"{self.base_url}/networkSizes/{encoded_urn}?edgeType=CompanyFollowedByMember"
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    follower_count = data.get('firstDegreeSize', 0)
                    print(f"Nombre de followers: {follower_count}")
                    return follower_count
                    
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
        
        print("Échec après plusieurs tentatives pour obtenir le nombre de followers.")
        return None


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
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée (les droits de propriétaire nécessitent des autorisations spéciales)
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
    
    def prepare_sheet(self):
        """Prépare la feuille de données"""
        try:
            # Vérifier si le Sheet1 existe et le renommer si nécessaire
            worksheets = self.spreadsheet.worksheets()
            
            # Vérifier si Followers Historique existe déjà
            followers_sheet = None
            for worksheet in worksheets:
                if worksheet.title == "Followers Historique":
                    followers_sheet = worksheet
                    break
            
            # Si Followers Historique n'existe pas, on vérifie si Sheet1 existe pour le renommer
            if not followers_sheet:
                sheet1 = None
                for worksheet in worksheets:
                    if worksheet.title == "Sheet1" or worksheet.title == "Feuille1":
                        sheet1 = worksheet
                        break
                
                if sheet1:
                    # Renommer Sheet1 en Followers Historique
                    sheet1.update_title("Followers Historique")
                    followers_sheet = sheet1
                    print("Feuille par défaut renommée en 'Followers Historique'")
                else:
                    # Créer une nouvelle feuille si aucune n'existe
                    followers_sheet = self.spreadsheet.add_worksheet(title="Followers Historique", rows=1000, cols=5)
                    print("Nouvelle feuille 'Followers Historique' créée")
            
            # Vérifier si la première ligne contient les en-têtes corrects
            headers = ["Date de relevé", "Total followers"]
            
            # Récupérer les valeurs actuelles
            values = followers_sheet.get_all_values()
            
            # Vérifier si nous avons besoin de corriger les en-têtes
            needs_headers = True
            if values and len(values) > 0:
                first_row = values[0]
                # Vérifier si la première ligne ressemble à des en-têtes
                if first_row and len(first_row) >= 2:
                    if first_row[0] == "Date de relevé" or first_row[0] == "Date":
                        # La première ligne semble être des en-têtes, on met à jour si nécessaire
                        if first_row != headers:
                            followers_sheet.update('A1:B1', [headers])
                            print("En-têtes mis à jour")
                        needs_headers = False
                    elif len(values) == 1 and first_row[0].count('-') == 2 and first_row[1].isdigit():
                        # C'est une ligne de données sans en-têtes, on insère des en-têtes avant
                        followers_sheet.insert_row(headers, 1)
                        print("En-têtes insérés avant les données existantes")
                        needs_headers = False
            
            # Si on a toujours besoin d'en-têtes (feuille vide ou première ligne ambiguë)
            if needs_headers:
                followers_sheet.update('A1:E1', [headers])
                
                # Formater les en-têtes
                followers_sheet.format("A1:B1", {
                    "textFormat": {"bold": True},
                    "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
                })
                print("En-têtes ajoutés à la feuille vide")
            
            return followers_sheet
        except Exception as e:
            print(f"Erreur lors de la préparation de la feuille: {e}")
            return None
    
    def add_follower_count(self, follower_count):
        """Ajoute le nombre de followers avec la date actuelle"""
        if not self.connect():
            print("Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        sheet = self.prepare_sheet()
        if not sheet:
            return False
        
        try:
            # Obtenir la date actuelle
            today = datetime.now().strftime('%Y-%m-%d')
            
            # Vérifier si nous avons déjà un relevé pour aujourd'hui
            date_col = sheet.col_values(1)
            if today in date_col:
                # Mettre à jour la valeur existante
                row_idx = date_col.index(today) + 1
                sheet.update_cell(row_idx, 2, follower_count)
                print(f"Mise à jour du relevé existant pour le {today}")
            else:
                # Ajouter une nouvelle ligne
                # Récupérer les données pour calculer les croissances
                data = sheet.get_all_values()
                
                # Ignorer la ligne d'en-tête
                data = data[1:] if len(data) > 0 else []
                
                # Préparer la nouvelle ligne avec la date et le nombre de followers
                new_row = [today, follower_count]
                
                # Ajouter la nouvelle ligne
                sheet.append_row(new_row)
                print(f"Nouveau relevé ajouté pour le {today}")
            
            # URL du spreadsheet
            sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
            print(f"URL du tableau: {sheet_url}")
            
            return True
        except Exception as e:
            print(f"Erreur lors de l'ajout des données: {e}")
            return False


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
    sheet_name = os.getenv("GOOGLE_SHEET_NAME", "")

    if not access_token or not organization_id:
        print("Erreur: Variables d'environnement LINKEDIN_ACCESS_TOKEN ou LINKEDIN_ORGANIZATION_ID manquantes")
        print("Créez un fichier .env avec les variables:")
        print("LINKEDIN_ACCESS_TOKEN='votre_token'")
        print("LINKEDIN_ORGANIZATION_ID='votre_id_organisation'")
        print("GOOGLE_SHEET_NAME='nom_de_votre_sheet'  # Optionnel")
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
    tracker = LinkedInFollowersTracker(access_token, organization_id, sheet_name)
    
    # Obtention du nombre de followers
    print("\n--- Récupération du nombre de followers ---")
    follower_count = tracker.get_follower_count()
    
    if follower_count is not None:
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
        success = exporter.add_follower_count(follower_count)
        
        if success:
            print("✅ Export réussi!")
        else:
            print("❌ Échec de l'export")