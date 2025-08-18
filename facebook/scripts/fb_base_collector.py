"""
Classe de base pour la collecte de données Facebook - Version optimisée pour les quotas
AVEC CORRECTION AUTOMATIQUE DES EN-TÊTES ET APPEND_SHEET_DATA
"""
import os
import json
import logging
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import numpy as np
import time
import requests
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

class FacebookBaseCollector:
    """
    Classe de base pour la collecte de données Facebook avec gestion optimisée des quotas
    ET CORRECTION AUTOMATIQUE DES EN-TÊTES
    """
    # Variables de classe pour partager l'état des quotas entre toutes les instances
    _last_api_call_time = 0
    _api_calls_count = 0
    _quota_reset_time = 0
    
    def __init__(self, page_token):
        self.page_token = page_token
        self.api_version = "v21.0"
        self.base_url = f"https://graph.facebook.com/{self.api_version}"
        self.token_manager = None
        
        # Paramètres de gestion des quotas
        self.min_delay_between_calls = 1.5  # Délai minimum entre les appels API
        self.quota_calls_limit = 90  # Limite d'appels par 100 secondes (on garde une marge)
        self.quota_window = 100  # Fenêtre de quota en secondes
        
        # Initialiser les services Google
        try:
            # Essayer d'abord les Application Default Credentials
            from google.auth import default
            credentials, project = default(
                scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
            )
            logger.info("Utilisation des Application Default Credentials")
        except:
            # Fallback vers le fichier si disponible
            credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/workspace/credentials/service_account_credentials.json")
            if os.path.exists(credentials_path):
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info(f"Utilisation des credentials depuis {credentials_path}")
            else:
                # Dernier recours - essayer sans credentials explicites
                credentials = None
                logger.warning("Aucune credential trouvée, utilisation des credentials implicites")
        
        self.drive_service = build('drive', 'v3', credentials=credentials)
        self.sheets_service = build('sheets', 'v4', credentials=credentials)
        
        # Utiliser ConfigManager si disponible
        try:
            from utils.config_manager import ConfigManager
            self.config_manager = ConfigManager()
            self.use_cloud_storage = True
        except:
            self.config_manager = None
            self.use_cloud_storage = False
            os.makedirs("configs", exist_ok=True)
    
    def get_header_corrections_mapping(self):
        """
        Retourne les mappings de correction des en-têtes - VERSION COMPLÈTE
        """
        return {
            # Corrections communes à tous les types
            "Nombre de fans": "Nbre de fans",
            "Nombre d'abonnés": "Nbre d'abonnés",
            "Fréquence des impressions": "Fréquence des affichages",
            "Fréquence impressions": "Fréquence des affichages",
            
            # 🔥 CORRECTIONS SPÉCIFIQUES pour posts_lifetime:
            "Clics totaux": "Nbre de clics",
            "post_activity_by_action_type_comment": "Nombre de commentaires",
            "post_activity_by_action_type_unique_comment": "Commentaires uniques",
            
            # Corrections supplémentaires pour posts_metadata:
            "Nombre de commentaires": "Nbre de commentaires",
            "Nombre de J'aime": "Nbre de J'aime",
            "Nombre de partages": "Nbre de partages",
            
            # Autres corrections communes
            "Nombre de \"J'aime\"": "Nbre de \"J'aime\"",
            "Nombre de \"J'adore\"": "Nbre de \"J'adore\"",
            "Nombre de \"Wow\"": "Nbre de \"Wow\"",
            "Nombre de \"Haha\"": "Nbre de \"Haha\"",
            "Nombre de \"Triste\"": "Nbre de \"Triste\"",
            "Nombre de \"En colère\"": "Nbre de \"En colère\"",
        }
    
    def get_expected_headers_by_type(self, metric_type):
        """
        Retourne les en-têtes attendus selon le type de métrique - VERSION CORRIGÉE
        """
        if metric_type == "page_metrics":
            return [
                "Date",
                "Affichages de la page",
                "Visiteurs de la page", 
                "Affichages non viraux",
                "Affichages viraux",
                "Affichages des publications",
                "Visiteurs de la publication",
                "Affichages publicitaires",
                "Affichages organiques",
                "Visiteurs uniques organiques",
                "Vues totales de la page",
                "Nbre de fans",
                "Nouveaux fans",
                "Fans perdus",
                "Total nouveaux fans (payants + organiques)",
                "Nouveaux fans via pub",
                "Nouveaux fans organiques",
                "Nbre d'abonnés",
                "Nouveaux abonnés",
                "Désabonnements",
                "Abonnés uniques du jour",
                "Vues de vidéos",
                "Vues uniques de vidéos",
                "Vues vidéos via pub",
                "Vues vidéos organiques",
                "Relectures vidéos",
                "Temps de visionnage (sec)",
                "Vues complètes (30s)",
                "Vues complètes uniques (30s)",
                "Vues complètes via pub (30s)",
                "Vues complètes organiques (30s)",
                "Vues complètes auto (30s)",
                "Relectures complètes (30s)",
                "Interactions sur publications",
                "Actions totales",
                "Nbre de \"J'aime\"",
                "Nbre de \"J'adore\"",
                "Nbre de \"Wow\"",
                "Nbre de \"Haha\"",
                "Nbre de \"Triste\"",
                "Nbre de \"En colère\"",
                "Tx d'engagement (%)",
                "Fréquence des affichages",
                "Actions calculées",
                "VTR %"
            ]
        
        elif metric_type == "posts_lifetime":
            # 🎯 BASÉ SUR VOS EN-TÊTES RÉELS:
            return [
                "Date de publication",
                "ID publication",
                "URL média",
                "Lien média",
                "Message",
                "Affichages publication",
                "Affichages organiques",
                "Affichages sponsorisés",
                "Affichages viraux",
                "Affichages par fans",
                "Affichages non viraux",
                "Visiteurs de la publication",
                "Visiteurs organiques",
                "Visiteurs via pub",
                "Visiteurs viraux",
                "Visiteurs non viraux",  # 🔄 Repositionné ici selon vos en-têtes
                "Nbre de \"J'aime\"",
                "Nbre de \"J'adore\"",
                "Nbre de \"Wow\"",
                "Nbre de \"Haha\"",
                "Nbre de \"Triste\"",
                "Nbre de \"En colère\"",
                "Réactions J'aime",
                "Réactions J'adore",
                "Nbre de clics",  # 🔥 CORRIGÉ: était "Clics totaux"
                "Autres clics",
                "Clics sur liens",
                "Clics sur photos",
                "Vues vidéo",
                "Vues vidéo organiques",
                "Vues vidéo sponsorisées",
                "Visiteurs vidéo uniques",
                "Visiteurs vidéo organiques",
                "Visiteurs vidéo sponsorisés",
                "Vues avec son",
                "Vues complètes (30s)",
                "Temps moyen visionné",
                "Vues sur la page",
                "Vues via partages",
                "Portée fans",
                "Durée totale visionnage",
                "Partages",
                "J'aime sur activité",
                "Partages uniques",
                "J'aime uniques",
                "Nouveaux abonnés vidéo",
                "Interactions vidéo",
                "Interactions totales",
                "Nbre de commentaires",  # 🔥 CORRIGÉ: était "post_activity_by_action_type_comment"
                "Commentaires uniques",    # ➕ AJOUTÉ pour "post_activity_by_action_type_unique_comment"
                "Tx de clic (%)",
                "Tx d'engagement (%)",
                "Réactions positives",
                "Réactions négatives",
                "Total réactions"
            ]
        
        elif metric_type == "posts_metadata":
            return [
                "ID publication",
                "Date de publication",
                "Type de publication",
                "Message",
                "Lien permanent",
                "Image",
                "Auteur",
                "ID auteur",
                "Nbre de commentaires",  # 🔥 CORRIGÉ: cohérent avec les autres
                "Nbre de J'aime",        # 🔥 CORRIGÉ: cohérent avec les autres
                "Nbre de partages"       # 🔥 CORRIGÉ: cohérent avec les autres
            ]
        
        else:
            # Type par défaut ou autres types
            return []
    
    def correct_sheet_headers(self, spreadsheet_id, expected_headers, metric_type):
        """Corrige automatiquement les en-têtes du Google Sheet - VERSION CORRIGÉE"""
        try:
            # Lire les en-têtes actuels
            self._wait_for_quota()
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="Metrics Data!1:1"  # Première ligne uniquement
            ).execute()
            
            current_headers = result.get('values', [[]])[0] if result.get('values') else []
            
            if not current_headers:
                logger.info("Aucun en-tête existant, pas de correction nécessaire")
                return False
            
            logger.info(f"📋 En-têtes actuels: {current_headers}")
            
            # Obtenir le mapping de correction
            correction_mapping = self.get_header_corrections_mapping()
            
            # Corriger les en-têtes
            corrected_headers = []
            headers_changed = False
            
            for header in current_headers:
                if header in correction_mapping:
                    corrected_header = correction_mapping[header]
                    corrected_headers.append(corrected_header)
                    headers_changed = True
                    logger.info(f"🔧 Correction: '{header}' → '{corrected_header}'")
                else:
                    corrected_headers.append(header)
            
            # Vérifier s'il manque des en-têtes attendus
            for expected_header in expected_headers:
                if expected_header not in corrected_headers:
                    # Ajouter seulement si on n'a pas déjà trop de colonnes
                    if len(corrected_headers) < 50:  # Limite raisonnable
                        corrected_headers.append(expected_header)
                        headers_changed = True
                        logger.info(f"➕ Ajout en-tête manquant: '{expected_header}'")
            
            # Appliquer les corrections si nécessaire
            if headers_changed:
                logger.info(f"🔄 Mise à jour des en-têtes...")
                
                # Mettre à jour les en-têtes
                update_request = self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range="Metrics Data!1:1",
                    valueInputOption="USER_ENTERED",
                    body={'values': [corrected_headers]}
                )
                self._execute_with_retry(update_request, operation_name="correction des en-têtes")
                
                logger.info(f"✅ En-têtes corrigés avec succès!")
                logger.info(f"🎯 Nouveaux en-têtes: {corrected_headers}")
                
                return True
            else:
                logger.info("✅ En-têtes déjà corrects, aucune modification nécessaire")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la correction des en-têtes: {e}")
            return False

    def _wait_for_quota(self):
        """Attend si nécessaire pour respecter les quotas"""
        current_time = time.time()
        
        # Vérifier si on doit réinitialiser le compteur
        if current_time - FacebookBaseCollector._quota_reset_time > self.quota_window:
            FacebookBaseCollector._api_calls_count = 0
            FacebookBaseCollector._quota_reset_time = current_time
        
        # Si on approche de la limite, attendre
        if FacebookBaseCollector._api_calls_count >= self.quota_calls_limit:
            wait_time = self.quota_window - (current_time - FacebookBaseCollector._quota_reset_time)
            if wait_time > 0:
                logger.warning(f"Approche de la limite de quota ({FacebookBaseCollector._api_calls_count} appels), attente de {wait_time:.1f} secondes...")
                time.sleep(wait_time + 1)
                # Réinitialiser après l'attente
                FacebookBaseCollector._api_calls_count = 0
                FacebookBaseCollector._quota_reset_time = time.time()
        
        # Respecter le délai minimum entre les appels
        time_since_last_call = current_time - FacebookBaseCollector._last_api_call_time
        if time_since_last_call < self.min_delay_between_calls:
            time.sleep(self.min_delay_between_calls - time_since_last_call)
        
        # Mettre à jour les compteurs
        FacebookBaseCollector._last_api_call_time = time.time()
        FacebookBaseCollector._api_calls_count += 1
    
    def get_or_update_spreadsheet(self, page_name, page_id, metric_type):
        """Obtient ou crée un spreadsheet avec gestion optimisée des quotas"""
        mapping_file = f"{metric_type}_mapping.json"
        
        if self.use_cloud_storage:
            mapping_config = self.config_manager.load_config(mapping_file) or {}
        else:
            try:
                with open(f"configs/{mapping_file}", 'r') as f:
                    mapping_config = json.load(f)
            except:
                mapping_config = {}

        # Si le spreadsheet existe déjà
        if page_id in mapping_config:
            spreadsheet_id = mapping_config[page_id]["spreadsheet_id"]
            logger.info(f"Spreadsheet existant trouvé pour {page_name} - {metric_type}")
            
            # NOUVEAU: Vérifier et corriger les en-têtes si nécessaire
            expected_headers = self.get_expected_headers_by_type(metric_type)
            if expected_headers:
                logger.info(f"🔍 Vérification des en-têtes pour {metric_type}...")
                self.correct_sheet_headers(spreadsheet_id, expected_headers, metric_type)
            
            return spreadsheet_id

        # Si pas existant, on crée le spreadsheet
        spreadsheet_id = self._create_spreadsheet(page_name, metric_type)

        # Sauvegarder le mapping
        mapping_config[page_id] = {
            "page_name": page_name,
            "spreadsheet_id": spreadsheet_id,
            "spreadsheet_url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}",
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat()
        }

        if self.use_cloud_storage:
            self.config_manager.save_config(mapping_file, mapping_config)
        else:
            with open(f"configs/{mapping_file}", 'w') as f:
                json.dump(mapping_config, f, indent=2)

        return spreadsheet_id

    def _create_spreadsheet(self, page_name, metric_type):
        """Crée un nouveau spreadsheet avec gestion optimisée des quotas"""
        title = self._get_spreadsheet_title(page_name, metric_type)
        
        # Créer le spreadsheet
        create_request = self.sheets_service.spreadsheets().create(
            body={
                'properties': {'title': title},
                'sheets': [{
                    'properties': {
                        'title': 'Metrics Data',
                        'gridProperties': {'frozenRowCount': 1}
                    }
                }]
            }
        )
        spreadsheet = self._execute_with_retry(create_request, operation_name="création spreadsheet")
        spreadsheet_id = spreadsheet['spreadsheetId']
        
        # Donner accès public en lecture
        permission_request = self.drive_service.permissions().create(
            fileId=spreadsheet_id,
            body={'type': 'anyone', 'role': 'reader'},
            fields='id'
        )
        self._execute_with_retry(permission_request, operation_name="permission publique")
        
        # Donner accès à l'email admin si spécifié (optionnel, ne pas faire échouer si ça ne marche pas)
        admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        if admin_email:
            try:
                admin_permission_request = self.drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body={'type': 'user', 'role': 'writer', 'emailAddress': admin_email},
                    fields='id'
                )
                self._execute_with_retry(admin_permission_request, max_retries=3, operation_name="permission admin")
            except:
                logger.warning(f"Impossible d'ajouter {admin_email} comme éditeur - continuons sans")
                
        # Donner accès au compte de service en writer
        service_account_email = os.getenv("SERVICE_ACCOUNT_EMAIL", f"facebook-automation@authentic-ether-457013-t5.iam.gserviceaccount.com")
        try:
            service_account_permission_request = self.drive_service.permissions().create(
                fileId=spreadsheet_id,
                body={'type': 'user', 'role': 'writer', 'emailAddress': service_account_email},
                fields='id'
            )
            self._execute_with_retry(service_account_permission_request, max_retries=3, operation_name="permission service account")
            logger.info(f"Permission ajoutée pour le compte de service: {service_account_email}")
        except Exception as e:
            logger.warning(f"Impossible d'ajouter le compte de service comme éditeur: {e}")

        logger.info(f"Nouveau spreadsheet créé: {spreadsheet_id}")
        return spreadsheet_id
    
    def append_sheet_data(self, spreadsheet_id, df):
        """
        🆕 NOUVELLE MÉTHODE - Ajoute des données à la fin du sheet SANS écraser l'existant
        """
        if df.empty:
            logger.warning("Aucune donnée à ajouter")
            return
        
        try:
            # 1. D'abord vérifier qu'on a des en-têtes dans le sheet
            self._wait_for_quota()
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="Metrics Data!1:1"  # Première ligne seulement
            ).execute()
            
            existing_headers = result.get('values', [[]])[0] if result.get('values') else []
            
            # 2. Préparer les données SANS les en-têtes (car on ajoute seulement les données)
            df_clean = df.fillna("")
            data_rows = []
            
            for _, row in df_clean.iterrows():
                row_values = []
                for col, val in zip(df_clean.columns, row):
                    if isinstance(val, datetime):
                        row_values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    elif isinstance(val, pd.Timestamp):
                        row_values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    elif isinstance(val, str) and val.startswith('=IMAGE('):
                        url = val.replace('=IMAGE("', '').replace('")', '')
                        row_values.append(url)
                    elif isinstance(val, (int, np.integer)):
                        row_values.append(int(val))
                    elif isinstance(val, (float, np.floating)):
                        row_values.append(float(val))
                    elif isinstance(val, bool):
                        row_values.append('TRUE' if val else 'FALSE')
                    elif val == "" or pd.isna(val):
                        row_values.append("")
                    else:
                        row_values.append(str(val))
                data_rows.append(row_values)
            
            if not data_rows:
                logger.warning("Aucune ligne de données à ajouter")
                return
            
            # 3. Si pas d'en-têtes existants, on ajoute d'abord les en-têtes
            if not existing_headers:
                logger.info("Aucun en-tête existant, ajout des en-têtes d'abord")
                headers = df_clean.columns.tolist()
                
                # Ajouter les en-têtes
                header_request = self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range="Metrics Data!A1",
                    valueInputOption="USER_ENTERED",
                    body={'values': [headers]}
                )
                self._execute_with_retry(header_request, operation_name="ajout en-têtes")
                logger.info(f"✅ En-têtes ajoutés: {len(headers)} colonnes")
                start_row = 2  # Commencer à la ligne 2
            else:
                # 4. Trouver la prochaine ligne vide
                self._wait_for_quota()
                all_data_result = self.sheets_service.spreadsheets().values().get(
                    spreadsheetId=spreadsheet_id,
                    range="Metrics Data!A:A"  # Toute la colonne A
                ).execute()
                
                existing_rows = all_data_result.get('values', [])
                start_row = len(existing_rows) + 1  # Prochaine ligne vide
                
                logger.info(f"📍 Données existantes: {len(existing_rows)} lignes, ajout à partir de la ligne {start_row}")
            
            # 5. Ajouter les nouvelles données
            logger.info(f"📊 Ajout de {len(data_rows)} nouvelles lignes à partir de la ligne {start_row}")
            
            # Pour les gros volumes, utiliser des chunks
            chunk_size = 1000
            total_rows = len(data_rows)
            
            for i in range(0, total_rows, chunk_size):
                chunk_end = min(i + chunk_size, total_rows)
                chunk = data_rows[i:chunk_end]
                current_start_row = start_row + i
                
                if total_rows > chunk_size:
                    logger.info(f"📥 Ajout chunk {i//chunk_size + 1}/{(total_rows-1)//chunk_size + 1} (lignes {current_start_row}-{current_start_row + len(chunk) - 1})")
                
                append_request = self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"Metrics Data!A{current_start_row}",
                    valueInputOption="USER_ENTERED",
                    body={'values': chunk}
                )
                self._execute_with_retry(append_request, operation_name=f"append chunk {i//chunk_size + 1}")
                
                # Petite pause entre les chunks
                if chunk_end < total_rows:
                    time.sleep(1)
            
            logger.info(f"✅ {len(data_rows)} nouvelles lignes ajoutées avec succès!")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'ajout des données: {e}")
            raise
    
    def update_sheet_data(self, spreadsheet_id, df):
        """Met à jour les données avec gestion robuste des gros volumes (ÉCRASE tout)"""
        if df.empty:
            logger.warning("Aucune donnée à uploader")
            return
        
        # Préparer les données
        df = df.fillna("")
        values = [df.columns.tolist()]
        
        for _, row in df.iterrows():
            row_values = []
            for col, val in zip(df.columns, row):
                if isinstance(val, datetime):
                    row_values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(val, pd.Timestamp):
                    row_values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(val, str) and val.startswith('=IMAGE('):
                    url = val.replace('=IMAGE("', '').replace('")', '')
                    row_values.append(url)
                elif isinstance(val, (int, np.integer)):
                    row_values.append(int(val))
                elif isinstance(val, (float, np.floating)):
                    row_values.append(float(val))
                elif isinstance(val, bool):
                    row_values.append('TRUE' if val else 'FALSE')
                elif val == "" or pd.isna(val):
                    row_values.append("")
                else:
                    row_values.append(str(val))
            values.append(row_values)
        
        logger.info(f"Mise à jour des données: {len(values)-1} lignes, {len(values[0])} colonnes")
        
        # Déterminer la meilleure méthode selon la taille des données
        total_cells = len(values) * len(values[0]) if values else 0
        use_batch = total_cells < 10000  # Utiliser batch seulement pour moins de 10k cellules
        
        if use_batch:
            try:
                # Pour les petits datasets, essayer le mode batch
                self._update_sheet_batch(spreadsheet_id, values)
                logger.info("Données mises à jour avec succès en mode batch")
                return
            except Exception as e:
                if "Broken pipe" in str(e) or "timed out" in str(e):
                    logger.warning(f"Mode batch échoué (données trop volumineuses): {e}")
                else:
                    logger.warning(f"Mode batch échoué: {e}")
        
        # Pour les gros datasets ou si batch échoue, utiliser le mode normal
        logger.info("Utilisation du mode normal (plus robuste pour gros volumes)")
        self._update_sheet_normal_chunked(spreadsheet_id, values)
        logger.info(f"Données mises à jour: {len(df)} lignes")

    def _update_sheet_batch(self, spreadsheet_id, values):
        """Mode batch pour petits datasets - VERSION CORRIGÉE"""
        # D'abord effacer
        clear_request = self.sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="Metrics Data!A:ZZ"
        )
        self._execute_with_retry(clear_request, operation_name="clear for batch")
        
        # Puis mettre à jour
        update_request = self.sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Metrics Data!A1",
            valueInputOption="USER_ENTERED",
            body={'values': values}
        )
        self._execute_with_retry(update_request, operation_name="batch update")
        
    def _update_sheet_normal_chunked(self, spreadsheet_id, values):
        """Mode normal avec découpage en chunks pour gros volumes - VERSION CORRIGÉE"""
        # D'abord effacer toute la feuille
        clear_request = self.sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="Metrics Data!A:ZZ"
        )
        self._execute_with_retry(clear_request, operation_name="clear sheet")
        
        # Pour les très gros datasets, uploader par chunks
        chunk_size = 1000  # Lignes par chunk
        total_rows = len(values)
        
        if total_rows > chunk_size:
            logger.info(f"Dataset volumineux ({total_rows} lignes), upload par chunks de {chunk_size}")
            
            # Toujours inclure les headers dans le premier chunk
            for i in range(0, total_rows, chunk_size):
                chunk_end = min(i + chunk_size, total_rows)
                
                if i == 0:
                    # Premier chunk avec headers
                    chunk = values[0:chunk_end]
                    start_row = 1
                else:
                    # Chunks suivants sans headers
                    chunk = values[i:chunk_end]
                    start_row = i + 1  # +1 car on compte les headers
                
                logger.info(f"Upload chunk {i//chunk_size + 1}/{(total_rows-1)//chunk_size + 1} (lignes {start_row}-{start_row + len(chunk) - 1})")
                
                update_request = self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"Metrics Data!A{start_row}",
                    valueInputOption="USER_ENTERED",
                    body={'values': chunk}
                )
                self._execute_with_retry(update_request, operation_name=f"update chunk {i//chunk_size + 1}")
                
                # Petite pause entre les chunks pour éviter les problèmes
                if chunk_end < total_rows:
                    time.sleep(2)
        else:
            # Dataset normal, upload en une fois
            update_request = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="Metrics Data!A1",
                valueInputOption="USER_ENTERED",
                body={'values': values}
            )
            self._execute_with_retry(update_request, operation_name="update values")
            
    def _update_sheet_normal(self, spreadsheet_id, values):
        """Mode normal standard - VERSION CORRIGÉE"""
        # Nettoyer la feuille
        clear_request = self.sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range="Metrics Data!A1:ZZ"
        )
        self._execute_with_retry(clear_request, operation_name="clear sheet")
        
        # Uploader les nouvelles données
        update_request = self.sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Metrics Data!A1",
            valueInputOption="USER_ENTERED",
            body={'values': values}
        )
        self._execute_with_retry(update_request, operation_name="update values")
        
    def _execute_with_retry(self, request, max_retries=7, operation_name=""):
        """Exécute une requête avec gestion des quotas et retry améliorée"""
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Attendre pour respecter les quotas
                self._wait_for_quota()
                
                result = request.execute()
                
                # Si succès, on peut réduire légèrement le délai minimum
                if self.min_delay_between_calls > 1:
                    self.min_delay_between_calls = max(1, self.min_delay_between_calls - 0.1)
                
                return result
                
            except HttpError as e:
                if e.resp.status == 429:  # Quota exceeded
                    # Augmenter le délai minimum pour les prochains appels
                    self.min_delay_between_calls = min(5, self.min_delay_between_calls + 0.5)
                    
                    # Calculer le temps d'attente avec backoff exponentiel
                    wait_time = min(120, base_delay * (2 ** attempt))
                    
                    logger.warning(f"Quota dépassé {operation_name} (tentative {attempt + 1}/{max_retries}), attente de {wait_time} secondes...")
                    time.sleep(wait_time)
                    
                    # Réinitialiser les compteurs après une erreur de quota
                    FacebookBaseCollector._api_calls_count = 0
                    FacebookBaseCollector._quota_reset_time = time.time()
                    continue
                    
                elif e.resp.status == 500:  # Internal server error
                    # Erreur serveur Google, attendre plus longtemps
                    wait_time = min(60, 10 * (attempt + 1))
                    logger.warning(f"Erreur serveur Google 500 {operation_name} (tentative {attempt + 1}/{max_retries}), attente de {wait_time} secondes...")
                    time.sleep(wait_time)
                    continue
                    
                elif e.resp.status == 503:  # Service unavailable
                    # Service temporairement indisponible
                    wait_time = min(60, 5 * (2 ** attempt))
                    logger.warning(f"Service indisponible 503 {operation_name} (tentative {attempt + 1}/{max_retries}), attente de {wait_time} secondes...")
                    time.sleep(wait_time)
                    continue
                    
                else:
                    logger.error(f"Erreur HTTP {e.resp.status} {operation_name}: {e}")
                    raise
                    
            except Exception as e:
                logger.error(f"Erreur inattendue {operation_name}: {e}")
                raise
        
        raise Exception(f"Échec après {max_retries} tentatives {operation_name}")
    
    def _update_spreadsheet_title(self, spreadsheet_id, page_name, metric_type):
        """Met à jour le titre - désactivé pour économiser les quotas"""
        # Commenté pour économiser les quotas
        pass
    
    def _format_columns_for_looker(self, spreadsheet_id, df):
        """Formatage désactivé pour économiser les quotas"""
        # Commenté pour économiser les quotas
        pass
    
    def _get_spreadsheet_title(self, page_name, metric_type):
        """Génère le titre du spreadsheet"""
        titles = {
            "page_metrics": f"Facebook Metrics - {page_name}",
            "posts_lifetime": f"Facebook Lifetime Posts Metrics - {page_name}",
            "posts_metadata": f"Facebook Metadata Posts Metrics - {page_name}"
        }
        
        base_title = titles.get(metric_type, f"Facebook {metric_type} - {page_name}")
        date = datetime.now().strftime('%Y-%m-%d')
        return f"{base_title} - Relevé du {date}"
    
    # Les méthodes Facebook restent identiques
    def handle_api_error(self, error_response, context=""):
        """Gère les erreurs de l'API Facebook"""
        if isinstance(error_response, dict) and "error" in error_response:
            error = error_response["error"]
            error_code = error.get("code", 0)
            error_subcode = error.get("error_subcode", 0)
            
            if error_code == 190 and error_subcode == 463:
                logger.error(f"Token expiré détecté {context}: {error.get('message', '')}")
                return True
            
            if error_code == 190:
                logger.error(f"Erreur de token {context}: {error.get('message', '')}")
                return True
        
        return False
    
    def make_api_request(self, url, params, max_retries=3):
        """Effectue une requête API Facebook avec gestion des erreurs"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if self.handle_api_error(data, f"(tentative {attempt + 1}/{max_retries})"):
                    if self.token_manager and attempt < max_retries - 1:
                        logger.info("Tentative de rafraîchissement du token...")
                        try:
                            from utils.token_manager import FacebookTokenManager
                            tm = FacebookTokenManager()
                            new_token = tm.get_valid_token()
                            
                            params["access_token"] = new_token
                            self.page_token = new_token
                            
                            time.sleep(2)
                            continue
                        except Exception as e:
                            logger.error(f"Impossible de rafraîchir le token: {e}")
                    
                    raise Exception(f"Token expiré: {data['error'].get('message', '')}")
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Erreur réseau (tentative {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise
            except Exception as e:
                logger.error(f"Erreur inattendue: {e}")
                raise
        
        raise Exception(f"Échec après {max_retries} tentatives")
    
    def get_all_spreadsheets_report(self):
        """Génère un rapport consolidé de tous les spreadsheets"""
        all_spreadsheets = []
        metric_types = ["page_metrics", "posts_lifetime", "posts_metadata"]
        
        for metric_type in metric_types:
            mapping_file = f"{metric_type}_mapping.json"
            
            if self.use_cloud_storage:
                mapping_config = self.config_manager.load_config(mapping_file) or {}
            else:
                try:
                    with open(f"configs/{mapping_file}", 'r') as f:
                        mapping_config = json.load(f)
                except:
                    mapping_config = {}
            
            for page_id, info in mapping_config.items():
                spreadsheet_info = {
                    "page_id": page_id,
                    "page_name": info["page_name"],
                    "metric_type": metric_type,
                    "spreadsheet_id": info["spreadsheet_id"],
                    "spreadsheet_url": info.get("spreadsheet_url", f"https://docs.google.com/spreadsheets/d/{info['spreadsheet_id']}"),
                    "created_at": info.get("created_at"),
                    "last_updated": info.get("last_updated")
                }
                all_spreadsheets.append(spreadsheet_info)
        
        return {
            "generated_at": datetime.now().isoformat(),
            "total_spreadsheets": len(all_spreadsheets),
            "spreadsheets": all_spreadsheets
        }
    
    def get_existing_dates(self, spreadsheet_id):
        """
        Récupère la liste des dates déjà présentes dans le Google Sheet - VERSION CORRIGÉE
        """
        try:
            self._wait_for_quota()  # Respecter quotas
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range="Metrics Data!A:A"  # Colonne A = 'Date'
            ).execute()

            values = result.get('values', [])
            if len(values) <= 1:
                # Soit vide, soit uniquement l'en-tête
                return set()

            dates = set()
            for row in values[1:]:  # Skip header
                if row and row[0]:
                    try:
                        parsed_date = pd.to_datetime(row[0]).strftime("%Y-%m-%d")
                        dates.add(parsed_date)
                    except:
                        # Si la conversion échoue, on ignore cette cellule
                        continue

            logger.info(f"Dates existantes récupérées : {len(dates)} dates")
            return dates

        except Exception as e:
            logger.warning(f"Impossible de récupérer les dates existantes : {e}")
            return set()