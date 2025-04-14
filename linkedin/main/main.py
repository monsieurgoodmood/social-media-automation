#!/usr/bin/env python3
"""
Script principal pour la collecte des métriques LinkedIn
et export vers Google Sheets pour tableaux de bord Looker Studio
"""
import sys
import traceback
import os
import argparse
from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv

# Importer les modules personnalisés
from env_setup import initialize_env_file, update_redirect_uri
from linkedin_auth import LinkedInAuthManager
from linkedin_metrics import LinkedInMetricsCollector
from google_sheets import GoogleSheetsManager

def main():
    try:
        # Parser pour les arguments en ligne de commande
        parser = argparse.ArgumentParser(description='Collecter et exporter les métriques LinkedIn vers Google Sheets')
        parser.add_argument('--days', type=int, default=90, help='Nombre de jours à collecter (max 90 par requête)')
        parser.add_argument('--mode', choices=['daily', 'lifetime', 'both'], default='daily', 
                            help='Mode de collecte: daily (quotidien), lifetime (global), ou both (les deux)')
        parser.add_argument('--force-auth', action='store_true', help='Forcer une nouvelle authentification LinkedIn')
        args = parser.parse_args()
        
        # 1. S'assurer que le fichier .env est correctement configuré
        print("=== Configuration de l'environnement ===")
        initialize_env_file()
        update_redirect_uri()  # S'assurer que l'URL de redirection est correcte
        
        # Recharger les variables d'environnement après la configuration
        load_dotenv(find_dotenv(), override=True)
        
        # 2. Définir le chemin du fichier de credentials Google
        credentials_path = os.getenv('GOOGLE_CREDENTIALS_PATH', 
                               '/home/arthur/code/social-media-automation/credentials/service_account_credentials.json')
        
        # 3. Initialiser le gestionnaire d'authentification LinkedIn
        print("\n=== Authentification LinkedIn ===")
        auth_manager = LinkedInAuthManager()
        
        # Forcer une nouvelle authentification si demandé
        if args.force_auth:
            print("Authentification forcée demandée...")
            auth_manager._prompt_for_manual_authentication()
        
        # 4. Récupérer l'ID d'organisation depuis le fichier .env
        organization_id = os.getenv('LINKEDIN_ORGANIZATION_ID')
        
        # Vérification des variables requises
        if not organization_id:
            print("ERREUR: Variable d'environnement LINKEDIN_ORGANIZATION_ID manquante dans le fichier .env")
            sys.exit(1)

        # 5. Initialisation des collecteurs
        linkedin = LinkedInMetricsCollector(auth_manager, organization_id)
        sheets_manager = GoogleSheetsManager(credentials_path)
        
        # 6. Collecter et exporter les données
        print("\n=== Collecte et export des métriques ===")
        
        # Récupérer ou créer un spreadsheet
        spreadsheet_id = sheets_manager.get_or_create_spreadsheet()
        
        # Formater le spreadsheet si nécessaire
        sheets_manager.format_spreadsheet(spreadsheet_id)
        
        # Collecter les données selon le mode choisi
        if args.mode in ['daily', 'both']:
            print("\nCollecte des métriques quotidiennes...")
            
            # Pour récupérer 365 jours, nous devons faire plusieurs appels (max 90 jours par appel)
            days_to_collect = 365  # Objectif : 365 jours
            days_per_call = 90     # Maximum autorisé par LinkedIn
            
            all_metrics = []
            end_date = datetime.now()
            
            for i in range(0, days_to_collect, days_per_call):
                # Calculer les dates pour chaque segment
                segment_end = end_date - timedelta(days=i) if i > 0 else end_date
                segment_start = segment_end - timedelta(days=min(days_per_call, days_to_collect - i))
                
                print(f"Segment {i//days_per_call + 1}: du {segment_start.strftime('%Y-%m-%d')} au {segment_end.strftime('%Y-%m-%d')}")
                
                # Collecter les métriques pour ce segment
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
                    
                    # Ajouter la ligne
                    all_metrics.append([
                        date, followers_total, followers_organic, followers_paid, 
                        page_views, unique_visitors, impressions, engagement_rate,
                        clicks, likes, comments, shares
                    ])
                
                # Attendre un peu pour éviter de dépasser les limites de l'API LinkedIn
                import time
                time.sleep(1)
            
            # Trier les métriques par date
            all_metrics.sort(key=lambda x: x[0])
            
            # Exporter les données
            if all_metrics:
                print(f"Exportation de {len(all_metrics)} jours de données vers Google Sheets...")
                sheets_manager.export_data(spreadsheet_id, all_metrics)
            else:
                print("Aucune donnée quotidienne n'a été récupérée.")
        
        if args.mode in ['lifetime', 'both']:
            print("\nCollecte des métriques lifetime...")
            lifetime_metrics = linkedin.collect_lifetime_metrics()
            
            # Exporter les données lifetime comme une ligne supplémentaire
            if lifetime_metrics:
                sheets_manager.export_data(spreadsheet_id, [lifetime_metrics])
        
        # 7. Afficher l'URL du spreadsheet
        spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        print(f"\nVotre tableau de bord est disponible à l'adresse suivante :\n{spreadsheet_url}")
        
        print("\nTraitement terminé avec succès!")
        
    except Exception as e:
        print(f"\nERREUR CRITIQUE: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()