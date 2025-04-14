#!/usr/bin/env python3
"""
Script pour réinitialiser le Google Sheet et y insérer des données au format correct
"""

import os
import sys
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv

# Assurez-vous d'ajuster ces imports selon votre structure de projet
from linkedin_auth import LinkedInAuthManager
from linkedin_metrics import LinkedInMetricsCollector
from google_sheets import GoogleSheetsManager

def reset_and_format_sheet():
    """Réinitialise complètement le Google Sheet et le reformate correctement"""
    # Charger les variables d'environnement
    load_dotenv(find_dotenv())
    
    # Récupérer l'ID du Google Sheet et les credentials
    spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 
                           '/home/arthur/code/social-media-automation/credentials/service_account_credentials.json')
    
    if not spreadsheet_id:
        print("ERREUR: Aucun ID de Google Sheet trouvé dans les variables d'environnement")
        sys.exit(1)
    
    # Initialiser le gestionnaire Google Sheets
    sheets_manager = GoogleSheetsManager(credentials_path)
    
    # 1. Supprimer complètement la feuille existante
    try:
        # Récupérer les informations du spreadsheet
        spreadsheet_info = sheets_manager.sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        # Trouver l'ID de la feuille "Metrics"
        sheet_id = None
        for sheet in spreadsheet_info.get('sheets', []):
            if sheet.get('properties', {}).get('title') == 'Metrics':
                sheet_id = sheet.get('properties', {}).get('sheetId')
                break
        
        if sheet_id is not None:
            # Supprimer la feuille
            delete_request = {
                'requests': [
                    {
                        'deleteSheet': {
                            'sheetId': sheet_id
                        }
                    }
                ]
            }
            
            sheets_manager.sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=delete_request
            ).execute()
            
            print("Feuille 'Metrics' supprimée avec succès")
        else:
            print("Feuille 'Metrics' non trouvée")
        
        # 2. Créer une nouvelle feuille "Metrics"
        add_sheet_request = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': 'Metrics',
                            'gridProperties': {
                                'rowCount': 1000,
                                'columnCount': 15
                            }
                        }
                    }
                }
            ]
        }
        
        sheets_manager.sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=add_sheet_request
        ).execute()
        
        print("Nouvelle feuille 'Metrics' créée avec succès")
        
        # 3. Ajouter les en-têtes de colonnes
        headers = ['Date', 'Total Followers', 'Organic Followers', 'Paid Followers', 
                  'Page Views', 'Unique Visitors', 'Impressions', 'Engagement Rate', 
                  'Clicks', 'Likes', 'Comments', 'Shares']
        
        sheets_manager.sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='Metrics!A1',
            valueInputOption='RAW',
            body={'values': [headers]}
        ).execute()
        
        print("En-têtes de colonnes ajoutés avec succès")
        
        return True
    except Exception as e:
        print(f"ERREUR lors de la réinitialisation du Google Sheet: {e}")
        return False

def collect_and_export_daily_metrics(days=90):
    """Collecte et exporte les métriques quotidiennes LinkedIn"""
    # Charger les variables d'environnement
    load_dotenv(find_dotenv())
    
    # Récupérer les informations nécessaires
    organization_id = os.getenv('LINKEDIN_ORGANIZATION_ID')
    spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
    credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 
                           '/home/arthur/code/social-media-automation/credentials/service_account_credentials.json')
    
    if not organization_id or not spreadsheet_id:
        print("ERREUR: Variables d'environnement manquantes")
        sys.exit(1)
    
    # Initialiser les gestionnaires
    auth_manager = LinkedInAuthManager()
    linkedin = LinkedInMetricsCollector(auth_manager, organization_id)
    sheets_manager = GoogleSheetsManager(credentials_path)
    
    # Collecter les données quotidiennes sur la période demandée
    # Pour récupérer plusieurs tranches de 90 jours
    all_metrics = []
    end_date = datetime.now()
    
    for i in range(0, days, 90):
        segment_end = end_date - timedelta(days=i) if i > 0 else end_date
        segment_start = segment_end - timedelta(days=min(90, days - i))
        
        print(f"Segment {i//90 + 1}: du {segment_start.strftime('%Y-%m-%d')} au {segment_end.strftime('%Y-%m-%d')}")
        
        segment_metrics = linkedin.get_daily_metrics(segment_start, segment_end)
        
        # Convertir en format pour Google Sheets
        for metric in segment_metrics:
            date = metric['date']
            followers_total = metric.get('followers', {}).get('total', 0)
            followers_organic = metric.get('followers', {}).get('organic', 0)
            followers_paid = metric.get('followers', {}).get('paid', 0)
            page_views = metric.get('page_views', {}).get('views', 0)
            unique_visitors = metric.get('page_views', {}).get('unique_visitors', 0)
            impressions = metric.get('engagement', {}).get('impressions', 0)
            engagement_rate = metric.get('engagement', {}).get('engagement_rate', 0)
            clicks = metric.get('engagement', {}).get('clicks', 0)
            likes = metric.get('engagement', {}).get('likes', 0)
            comments = metric.get('engagement', {}).get('comments', 0)
            shares = metric.get('engagement', {}).get('shares', 0)
            
            all_metrics.append([
                date, followers_total, followers_organic, followers_paid, 
                page_views, unique_visitors, impressions, engagement_rate,
                clicks, likes, comments, shares
            ])
        
        # Pause pour éviter les limitations de l'API
        time.sleep(1)
    
    # Trier les métriques par date
    all_metrics.sort(key=lambda x: x[0])
    
    # Exporter les données vers Google Sheets
    if all_metrics:
        print(f"Exportation de {len(all_metrics)} jours de données vers Google Sheets...")
        
        try:
            result = sheets_manager.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range='Metrics!A2',
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': all_metrics}
            ).execute()
            
            updated_range = result.get('updates', {}).get('updatedRange', 'inconnu')
            updated_cells = result.get('updates', {}).get('updatedCells', 0)
            print(f"Exportation réussie: {updated_cells} cellules mises à jour dans la plage {updated_range}")
        except Exception as e:
            print(f"ERREUR lors de l'exportation des données: {e}")
    else:
        print("Aucune donnée quotidienne n'a été récupérée.")

if __name__ == "__main__":
    print("=== RÉINITIALISATION ET REFORMATAGE DU GOOGLE SHEET ===")
    if reset_and_format_sheet():
        print("\n=== COLLECTE ET EXPORTATION DES MÉTRIQUES QUOTIDIENNES ===")
        # Vous pouvez changer cette valeur (jusqu'à 365 jours)
        collect_and_export_daily_metrics(days=90)
        
        # Afficher l'URL du Google Sheet
        spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
        if spreadsheet_id:
            spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            print(f"\nVotre tableau de bord est disponible à l'adresse suivante :\n{spreadsheet_url}")
    else:
        print("Impossible de réinitialiser le Google Sheet. Opération abandonnée.")