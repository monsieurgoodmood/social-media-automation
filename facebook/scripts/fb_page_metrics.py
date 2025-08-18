"""
Script adapté pour collecter les métriques de page Facebook dans Cloud Functions
Fait partie du système d'automatisation Facebook sur Google Cloud
VERSION CORRIGÉE - GESTION CORRECTE DE L'HISTORIQUE
"""
import os
import sys
import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import json

# Ajouter le répertoire parent au path pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scripts.fb_base_collector import FacebookBaseCollector
except ImportError:
    # Pour les tests locaux
    from fb_base_collector import FacebookBaseCollector

logger = logging.getLogger(__name__)

# Liste complète des métriques de page Facebook
METRICS = [
    # Page Impressions
    "page_impressions", 
    "page_impressions_unique", 
    "page_impressions_nonviral", 
    "page_impressions_viral",
    
    # Page Posts Impressions
    "page_posts_impressions", 
    "page_posts_impressions_unique", 
    "page_posts_impressions_paid", 
    "page_posts_impressions_organic", 
    "page_posts_impressions_organic_unique",
    
    # Page Views
    "page_views_total",

    # Page Fans and Follows
    "page_fans",
    "page_fan_adds", 
    "page_fan_removes",
    "page_fan_adds_by_paid_non_paid_unique",
    "page_follows",
    "page_daily_follows", 
    "page_daily_unfollows",
    "page_daily_follows_unique",

    # Video Metrics
    "page_video_views", 
    "page_video_views_unique", 
    "page_video_views_paid", 
    "page_video_views_organic",  
    "page_video_repeat_views", 
    "page_video_view_time",
    "page_video_complete_views_30s", 
    "page_video_complete_views_30s_unique", 
    "page_video_complete_views_30s_paid", 
    "page_video_complete_views_30s_organic", 
    "page_video_complete_views_30s_autoplayed", 
    "page_video_complete_views_30s_repeat_views",

    # Engagement Metrics
    "page_post_engagements", 
    "page_total_actions", 
    "page_actions_post_reactions_like_total", 
    "page_actions_post_reactions_love_total", 
    "page_actions_post_reactions_wow_total", 
    "page_actions_post_reactions_haha_total", 
    "page_actions_post_reactions_sorry_total", 
    "page_actions_post_reactions_anger_total"
]

def fetch_page_metrics(access_token, metrics, page_id, start_date, end_date):
    """
    Récupère les métriques pour une page sur toute la période par tranches de 90 jours
    Compatible avec les limites de l'API Facebook
    """
    graph_api_url = f"https://graph.facebook.com/v21.0/{page_id}/insights"
    all_data = []
    current_date = start_date
    
    logger.info(f"Récupération des données pour la page {page_id} du {start_date.strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")
    
    while current_date < end_date:
        # Calculer la fin de la tranche (90 jours maximum)
        chunk_end = min(current_date + timedelta(days=89), end_date)
        
        logger.info(f"- Période du {current_date.strftime('%Y-%m-%d')} au {chunk_end.strftime('%Y-%m-%d')}")
        
        params = {
            "metric": ",".join(metrics),
            "period": "day",
            "access_token": access_token,
            # Convertir les dates en timestamps Unix pour l'API Facebook
            "since": int(current_date.timestamp()),
            "until": int(chunk_end.timestamp())
        }
        
        try:
            response = requests.get(graph_api_url, params=params)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                logger.error(f"Erreur API pour la période: {data['error'].get('message', '')}")
                if data['error'].get('code') == 190:  # Token invalide
                    raise Exception("Token invalide")
                current_date = chunk_end + timedelta(days=1)
                continue

            if "data" in data:
                for metric_data in data["data"]:
                    metric_name = metric_data["name"]
                    for value in metric_data["values"]:
                        date = value.get("end_time", "").split("T")[0]
                        if date:
                            # Ajuster la date pour correspondre au jour réel des métriques
                            adjusted_date = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
                            
                            # Créer ou mettre à jour l'enregistrement
                            existing_record = next((item for item in all_data if item["date"] == adjusted_date), None)
                            
                            if existing_record is None:
                                existing_record = {"date": adjusted_date}
                                all_data.append(existing_record)
                            
                            # Traiter la valeur métrique
                            metric_value = value.get("value", 0)
                            if isinstance(metric_value, dict):
                                for key, sub_value in metric_value.items():
                                    existing_record[f"{metric_name}_{key}"] = sub_value
                            else:
                                existing_record[metric_name] = metric_value

            # Attendre un peu entre les requêtes pour éviter les limites de l'API
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur réseau sur la page {page_id}: {str(e)}")
            logger.warning(f"SKIP de cette période, on continue avec la prochaine...")
            time.sleep(5)

        except Exception as e:
            logger.error(f"Erreur inattendue: {str(e)}")
            if "Token invalide" in str(e):
                raise
        
        # Passer à la prochaine tranche
        current_date = chunk_end + timedelta(days=1)

    if not all_data:
        logger.warning(f"Aucune donnée récupérée pour la page {page_id} sur la période demandée.")
        return pd.DataFrame()
    else:
        logger.info(f"✓ Données récupérées pour la page {page_id}: {len(all_data)} jours")

    # Créer le DataFrame et trier par date
    df = pd.DataFrame(all_data)
    if not df.empty and "date" in df.columns:
        df = df.sort_values(by="date").fillna(0)
        logger.info(f"✓ {len(df)} jours de données récupérés")
    
    return df

def add_calculated_metrics(df):
    """
    Ajoute les métriques calculées au DataFrame
    """
    # Taux d'engagement (page) - en décimal pour le format pourcentage
    df['taux_engagement_page'] = df.apply(
        lambda row: (row.get('page_post_engagements', 0) / row.get('page_impressions', 1)) 
        if row.get('page_impressions', 0) > 0 else 0, 
        axis=1
    )
    
    # Fréquence des Impressions
    df['frequence_impressions'] = df.apply(
        lambda row: row.get('page_impressions', 0) / row.get('page_impressions_unique', 1) 
        if row.get('page_impressions_unique', 0) > 0 else 0, 
        axis=1
    )
    
    # Actions totales sur la page (corrigée)
    df['actions_totales_calculees'] = df.apply(
        lambda row: (
            row.get('page_total_actions', 0) + 
            row.get('page_actions_post_reactions_like_total', 0) + 
            row.get('page_actions_post_reactions_love_total', 0) + 
            row.get('page_actions_post_reactions_wow_total', 0) + 
            row.get('page_actions_post_reactions_haha_total', 0) + 
            row.get('page_actions_post_reactions_sorry_total', 0) + 
            row.get('page_actions_post_reactions_anger_total', 0)
        ), 
        axis=1
    )
    
    # VTR % (page) - en décimal pour le format pourcentage
    df['vtr_percentage_page'] = df.apply(
        lambda row: (row.get('page_video_complete_views_30s', 0) / row.get('page_impressions', 1)) 
        if row.get('page_impressions', 0) > 0 else 0, 
        axis=1
    )
    
    return df

def get_corrected_column_mapping():
    """
    Retourne le mapping corrigé pour les noms de colonnes
    """
    return {
        # Colonnes temporelles
        "date": "Date",
        
        # Métriques de base de la page
        "page_impressions": "Affichages de la page",
        "page_impressions_unique": "Visiteurs de la page",
        "page_impressions_nonviral": "Affichages non viraux",
        "page_impressions_viral": "Affichages viraux",
        
        # Métriques des posts
        "page_posts_impressions": "Affichages des publications",
        "page_posts_impressions_unique": "Visiteurs de la publication",
        "page_posts_impressions_paid": "Affichages publicitaires",
        "page_posts_impressions_organic": "Affichages organiques",
        "page_posts_impressions_organic_unique": "Visiteurs uniques organiques",
        
        # Métriques des vues de page
        "page_views_total": "Vues totales de la page",
        
        # Métriques des fans - CORRIGÉ
        "page_fans": "Nbre de fans",
        "page_fan_adds": "Nouveaux fans",
        "page_fan_removes": "Fans perdus",
        
        # Métriques des abonnements payants/non payants
        "page_fan_adds_by_paid_non_paid_unique_total": "Total nouveaux fans (payants + organiques)",
        "page_fan_adds_by_paid_non_paid_unique_paid": "Nouveaux fans via pub",
        "page_fan_adds_by_paid_non_paid_unique_unpaid": "Nouveaux fans organiques",
        
        # Métriques des follows - CORRIGÉ
        "page_follows": "Nbre d'abonnés",
        "page_daily_follows": "Nouveaux abonnés",
        "page_daily_unfollows": "Désabonnements",
        "page_daily_follows_unique": "Abonnés uniques du jour",
        
        # Métriques des vidéos
        "page_video_views": "Vues de vidéos",
        "page_video_views_unique": "Vues uniques de vidéos",
        "page_video_views_paid": "Vues vidéos via pub",
        "page_video_views_organic": "Vues vidéos organiques",
        "page_video_repeat_views": "Relectures vidéos",
        "page_video_view_time": "Temps de visionnage (sec)",
        
        # Métriques des vues complètes
        "page_video_complete_views_30s": "Vues complètes (30s)",
        "page_video_complete_views_30s_unique": "Vues complètes uniques (30s)",
        "page_video_complete_views_30s_paid": "Vues complètes via pub (30s)",
        "page_video_complete_views_30s_organic": "Vues complètes organiques (30s)",
        "page_video_complete_views_30s_autoplayed": "Vues complètes auto (30s)",
        "page_video_complete_views_30s_repeat_views": "Relectures complètes (30s)",
        
        # Métriques d'engagement
        "page_post_engagements": "Interactions sur publications",
        "page_total_actions": "Actions totales",
        "page_actions_post_reactions_like_total": "Nbre de \"J'aime\"",
        "page_actions_post_reactions_love_total": "Nbre de \"J'adore\"",
        "page_actions_post_reactions_wow_total": "Nbre de \"Wow\"",
        "page_actions_post_reactions_haha_total": "Nbre de \"Haha\"",
        "page_actions_post_reactions_sorry_total": "Nbre de \"Triste\"",
        "page_actions_post_reactions_anger_total": "Nbre de \"En colère\"",
        
        # Métriques calculées - CORRIGÉ
        "taux_engagement_page": "Tx d'engagement (%)",
        "frequence_impressions": "Fréquence des affichages",
        "actions_totales_calculees": "Actions calculées",
        "vtr_percentage_page": "VTR %"
    }

def format_dataframe(df):
    """
    Formate le DataFrame avec les noms de colonnes CORRIGÉS et FORCÉS
    """
    # D'abord ajouter les métriques calculées
    df = add_calculated_metrics(df)
    
    # Obtenir le mapping corrigé
    column_mapping = get_corrected_column_mapping()
    
    try:
        # Convertir la colonne date en datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # Appliquer les types de données pour chaque colonne
        for col_name in df.columns:
            if col_name == "date":
                df[col_name] = pd.to_datetime(df[col_name])
            elif col_name in ["taux_engagement_page", "frequence_impressions", "vtr_percentage_page", "page_video_view_time"]:
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
            else:
                # Toutes les autres colonnes sont des entiers
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)

        # FORCER le renommage des colonnes - même si elles existent déjà avec d'autres noms
        df = df.rename(columns=column_mapping)
        
        # Log pour vérifier les colonnes finales
        logger.info(f"✅ Colonnes après renommage: {list(df.columns)}")
        
        return df

    except Exception as e:
        logger.error(f"Erreur lors du formatage du DataFrame: {str(e)}")
        raise

def get_expected_columns():
    """
    Retourne la liste des colonnes attendues CORRIGÉES
    """
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

def force_correct_column_names(df):
    """
    Force la correction des noms de colonnes MÊME SI elles ont déjà des noms incorrects
    """
    correction_mapping = {
        "Nombre de fans": "Nbre de fans",
        "Nombre d'abonnés": "Nbre d'abonnés", 
        "Fréquence des impressions": "Fréquence des affichages",
        "Fréquence impressions": "Fréquence des affichages",
        "Clics totaux": "Nbre de clics",
        "post_activity_by_action_type_comment": "Nbre de commentaires"
    }
    
    df = df.rename(columns=correction_mapping)
    logger.info(f"🔧 Correction forcée des noms de colonnes appliquée")
    
    return df

def validate_dataframe_columns(df, expected_columns=None):
    """
    Valide que le DataFrame a les bonnes colonnes avant l'envoi vers Google Sheets
    """
    if expected_columns is None:
        expected_columns = get_expected_columns()
    
    current_columns = list(df.columns)
    
    # FORCER la correction des noms AVANT validation
    df = force_correct_column_names(df)
    current_columns = list(df.columns)
    
    # Vérifier les colonnes manquantes
    missing_columns = set(expected_columns) - set(current_columns)
    
    # Vérifier les colonnes supplémentaires 
    extra_columns = set(current_columns) - set(expected_columns)
    
    # Créer une copie du DataFrame pour correction
    corrected_df = df.copy()
    
    errors = []
    warnings = []
    
    # Ajouter les colonnes manquantes avec des valeurs par défaut
    if missing_columns:
        for col in missing_columns:
            if "Date" in col:
                corrected_df[col] = pd.NaT
            elif any(keyword in col.lower() for keyword in ["tx", "vtr", "%", "fréquence"]):
                corrected_df[col] = 0.0
            else:
                corrected_df[col] = 0
        
        warnings.append(f"Colonnes manquantes ajoutées avec valeurs par défaut: {list(missing_columns)}")
    
    # Signaler les colonnes supplémentaires (mais les garder)
    if extra_columns:
        warnings.append(f"Colonnes supplémentaires détectées: {list(extra_columns)}")
    
    # Réorganiser les colonnes dans l'ordre attendu
    final_columns = []
    for col in expected_columns:
        if col in corrected_df.columns:
            final_columns.append(col)
    
    # Ajouter les colonnes supplémentaires à la fin
    for col in extra_columns:
        final_columns.append(col)
    
    corrected_df = corrected_df[final_columns]
    
    # Vérifier les types de données
    type_errors = []
    for col in corrected_df.columns:
        if col == "Date":
            if not pd.api.types.is_datetime64_any_dtype(corrected_df[col]):
                try:
                    corrected_df[col] = pd.to_datetime(corrected_df[col])
                except:
                    type_errors.append(f"Impossible de convertir '{col}' en datetime")
        elif any(keyword in col.lower() for keyword in ["tx", "vtr", "%", "fréquence", "temps"]):
            if not pd.api.types.is_numeric_dtype(corrected_df[col]):
                try:
                    corrected_df[col] = pd.to_numeric(corrected_df[col], errors='coerce').fillna(0.0)
                except:
                    type_errors.append(f"Impossible de convertir '{col}' en float")
        else:
            if not pd.api.types.is_numeric_dtype(corrected_df[col]):
                try:
                    corrected_df[col] = pd.to_numeric(corrected_df[col], errors='coerce').fillna(0).astype(int)
                except:
                    type_errors.append(f"Impossible de convertir '{col}' en int")
    
    if type_errors:
        errors.extend(type_errors)
    
    # Construire le message de résultat
    messages = []
    if warnings:
        messages.extend([f"⚠️ {w}" for w in warnings])
    if errors:
        messages.extend([f"❌ {e}" for e in errors])
    
    is_valid = len(errors) == 0
    error_message = "\n".join(messages) if messages else "✅ Toutes les colonnes sont valides"
    
    logger.info(f"Validation des colonnes: {len(corrected_df.columns)} colonnes, {len(corrected_df)} lignes")
    if messages:
        for msg in messages:
            logger.warning(msg)
    
    return is_valid, error_message, corrected_df

def validate_and_format_for_sheets(df):
    """
    Fonction combinée qui formate ET valide le DataFrame avant envoi vers Google Sheets
    AVEC CORRECTION FORCÉE DES NOMS
    """
    try:
        # 1. Formater d'abord
        formatted_df = format_dataframe(df)
        
        # 2. Ensuite valider (avec correction forcée intégrée)
        is_valid, validation_message, corrected_df = validate_dataframe_columns(formatted_df)
        
        # 3. Vérifications supplémentaires spécifiques à Google Sheets
        sheet_validations = []
        
        # Vérifier qu'on a des données
        if corrected_df.empty:
            return False, "❌ DataFrame vide - aucune donnée à envoyer", corrected_df
        
        # Vérifier la colonne Date
        if "Date" not in corrected_df.columns:
            return False, "❌ Colonne 'Date' manquante - obligatoire pour Google Sheets", corrected_df
        
        # Vérifier qu'on a au moins quelques métriques
        numeric_cols = corrected_df.select_dtypes(include=[int, float]).columns
        if len(numeric_cols) < 5:
            sheet_validations.append("⚠️ Peu de colonnes numériques détectées")
        
        # Vérifier les valeurs nulles dans les colonnes critiques
        critical_cols = ["Date", "Affichages de la page", "Visiteurs de la page"]
        null_checks = []
        for col in critical_cols:
            if col in corrected_df.columns:
                null_count = corrected_df[col].isnull().sum()
                if null_count > 0:
                    null_checks.append(f"'{col}': {null_count} valeurs nulles")
        
        if null_checks:
            sheet_validations.append(f"⚠️ Valeurs nulles détectées: {', '.join(null_checks)}")
        
        # Construire le message final
        final_messages = [validation_message]
        if sheet_validations:
            final_messages.extend(sheet_validations)
        
        final_message = "\n".join(final_messages)
        
        # Le DataFrame est prêt si pas d'erreurs critiques
        success = is_valid and not corrected_df.empty and "Date" in corrected_df.columns
        
        logger.info(f"🎯 Validation finale - Colonnes: {list(corrected_df.columns)}")
        
        return success, final_message, corrected_df
        
    except Exception as e:
        error_msg = f"❌ Erreur lors de la validation: {str(e)}"
        logger.error(error_msg)
        return False, error_msg, df

def get_page_token(user_token, page_id):
    """
    Récupère le token spécifique à une page Facebook
    Version améliorée qui utilise d'abord les tokens sauvegardés
    """
    import json
    import os
    from datetime import datetime
    
    # D'abord essayer de récupérer le token depuis la configuration sauvegardée
    try:
        # Essayer avec ConfigManager si disponible (Cloud Functions)
        try:
            from utils.config_manager import ConfigManager
            config_manager = ConfigManager()
            tokens_config = config_manager.load_config("page_tokens.json")
            
            if tokens_config and page_id in tokens_config.get("tokens", {}):
                saved_token = tokens_config["tokens"][page_id].get("access_token")
                if saved_token:
                    logger.info(f"Token trouvé dans la configuration pour la page {page_id}")
                    return saved_token
        except ImportError:
            # En local, charger depuis le fichier
            if os.path.exists("configs/page_tokens.json"):
                with open("configs/page_tokens.json", 'r') as f:
                    tokens_config = json.load(f)
                    
                if page_id in tokens_config.get("tokens", {}):
                    saved_token = tokens_config["tokens"][page_id].get("access_token")
                    if saved_token:
                        logger.info(f"Token trouvé dans le fichier local pour la page {page_id}")
                        return saved_token
    except Exception as e:
        logger.warning(f"Impossible de charger les tokens sauvegardés: {e}")
    
    # Si pas de token sauvegardé, utiliser l'API Facebook
    logger.info(f"Récupération du token via l'API pour la page {page_id}")
    url = f"https://graph.facebook.com/v21.0/me/accounts"
    params = {"access_token": user_token}
    
    try:
        response = requests.get(url, params=params)
        
        # Vérifier le status code avant de lever une exception
        if response.status_code != 200:
            raise Exception(f"API returned status {response.status_code}: {response.text}")
            
        data = response.json()
        
        if "error" in data:
            raise Exception(f"Erreur API: {data['error'].get('message')}")
        
        for page in data.get("data", []):
            if page["id"] == page_id:
                page_token = page["access_token"]
                
                # Sauvegarder ce token pour les prochaines utilisations
                try:
                    # Essayer de sauvegarder avec ConfigManager
                    try:
                        from utils.config_manager import ConfigManager
                        config_manager = ConfigManager()
                        tokens_config = config_manager.load_config("page_tokens.json") or {"tokens": {}}
                        
                        tokens_config["tokens"][page_id] = {
                            "page_name": page.get("name", "Unknown"),
                            "access_token": page_token
                        }
                        tokens_config["last_updated"] = datetime.now().isoformat()
                        
                        config_manager.save_config("page_tokens.json", tokens_config)
                        logger.info(f"Token sauvegardé pour la page {page_id}")
                    except ImportError:
                        # En local
                        os.makedirs("configs", exist_ok=True)
                        tokens_file = "configs/page_tokens.json"
                        
                        if os.path.exists(tokens_file):
                            with open(tokens_file, 'r') as f:
                                tokens_config = json.load(f)
                        else:
                            tokens_config = {"tokens": {}}
                        
                        tokens_config["tokens"][page_id] = {
                            "page_name": page.get("name", "Unknown"),
                            "access_token": page_token
                        }
                        tokens_config["last_updated"] = datetime.now().isoformat()
                        
                        with open(tokens_file, 'w') as f:
                            json.dump(tokens_config, f, indent=2)
                        logger.info(f"Token sauvegardé localement pour la page {page_id}")
                except Exception as e:
                    logger.warning(f"Impossible de sauvegarder le token: {e}")
                
                return page_token
        
        raise Exception(f"Page {page_id} non trouvée ou non autorisée")
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du token de page: {e}")
        raise

def get_earliest_available_date(access_token, page_id):
    """
    Trouve la date la plus ancienne où des données sont disponibles pour cette page
    """
    logger.info(f"🔍 Recherche de la date la plus ancienne disponible pour la page {page_id}...")
    
    try:
        # Chercher en remontant par tranches d'un an
        current_year = datetime.now().year
        earliest_found = None
        
        for year_offset in range(10):  # Chercher jusqu'à 10 ans en arrière
            test_start_date = datetime(current_year - year_offset, 1, 1)
            test_end_date = datetime(current_year - year_offset, 12, 31)
            
            if test_end_date > datetime.now():
                test_end_date = datetime.now()
            
            logger.info(f"   - Test de l'année {current_year - year_offset}...")
            
            # Tester avec une métrique simple
            test_url = f"https://graph.facebook.com/v21.0/{page_id}/insights"
            test_params = {
                "metric": "page_impressions",
                "period": "day",
                "access_token": access_token,
                "since": int(test_start_date.timestamp()),
                "until": int(test_end_date.timestamp()),
                "limit": 10  # Récupérer quelques points pour trouver la vraie première date
            }
            
            try:
                test_response = requests.get(test_url, params=test_params, timeout=10)
                if test_response.status_code == 200:
                    test_data = test_response.json()
                    if (test_data.get("data") and 
                        len(test_data["data"]) > 0 and 
                        test_data["data"][0].get("values") and
                        len(test_data["data"][0]["values"]) > 0):
                        
                        # Des données existent pour cette année, noter comme candidat
                        earliest_found = test_start_date
                        logger.info(f"✓ Données trouvées pour l'année {current_year - year_offset}")
                        # Continuer à chercher plus loin pour trouver le vrai début
                        
            except Exception as test_error:
                logger.debug(f"Erreur lors du test de l'année {current_year - year_offset}: {test_error}")
            
            time.sleep(0.5)  # Éviter de surcharger l'API
        
        if earliest_found:
            logger.info(f"✅ Date la plus ancienne trouvée: {earliest_found.strftime('%Y-%m-%d')}")
            return earliest_found
        else:
            # Si aucune donnée trouvée, utiliser une date par défaut raisonnable
            fallback_date = datetime.now() - timedelta(days=365)  # 1 an en arrière
            logger.info(f"🔄 Aucune donnée historique trouvée, utilisation de la date de fallback: {fallback_date.strftime('%Y-%m-%d')}")
            return fallback_date
        
    except Exception as e:
        logger.warning(f"Erreur lors de la recherche de la date la plus ancienne: {e}")
        # Dernier recours: 1 an en arrière
        fallback_date = datetime.now() - timedelta(days=365)
        logger.info(f"Utilisation de la date de fallback: {fallback_date.strftime('%Y-%m-%d')}")
        return fallback_date

def get_page_creation_date(access_token, page_id):
    """
    Fonction conservée pour compatibilité - redirige directement vers get_earliest_available_date
    NE TENTE PLUS d'accéder à created_time via l'API Facebook
    """
    logger.info(f"🔄 Utilisation de get_earliest_available_date pour la page {page_id}")
    return get_earliest_available_date(access_token, page_id)

def process_page_metrics(token, page_id, page_name):
    """
    Fonction principale pour traiter les métriques d'une page Facebook
    VERSION CORRIGÉE - GESTION APPROPRIÉE DE L'HISTORIQUE
    
    Args:
        token: Token utilisateur Facebook
        page_id: ID de la page Facebook
        page_name: Nom de la page Facebook
        
    Returns:
        spreadsheet_id: ID du Google Sheet créé/mis à jour (ou None si erreur bloquante)
    """
    try:
        logger.info(f"🚀 Début du traitement des métriques pour {page_name} ({page_id})")
        
        # Récupérer le token de la page
        page_access_token = get_page_token(token, page_id)
        
        # Initialiser le collecteur de base
        collector = FacebookBaseCollector(page_access_token)
        
        # Obtenir ou créer le spreadsheet
        spreadsheet_id = collector.get_or_update_spreadsheet(page_name, page_id, "page_metrics")
        
        # Définir la date de fin (aujourd'hui)
        end_date = datetime.now()

        # Récupérer la date la plus ancienne disponible pour cette page
        earliest_available_date = get_earliest_available_date(page_access_token, page_id)
        
        # Vérifier les dates existantes dans le spreadsheet
        existing_dates = collector.get_existing_dates(spreadsheet_id)
        logger.info(f"Dates existantes trouvées: {len(existing_dates)}")

        # Analyser la situation des données existantes
        update_mode = "complete_overwrite"  # Par défaut, collecte complète
        start_date = earliest_available_date
        
        if len(existing_dates) == 0:
            # CAS 1: Aucune donnée - collecte complète
            logger.info(f"🆕 Aucune donnée existante - collecte complète depuis la date la plus ancienne disponible")
            update_mode = "complete_overwrite"
            
        else:
            # Normaliser et parser les dates existantes
            parsed_dates = []
            for d in existing_dates:
                try:
                    date_str = str(d).strip()
                    if date_str and date_str != '' and date_str.lower() != 'nan':
                        parsed_date = pd.to_datetime(date_str).date()
                        parsed_dates.append(parsed_date)
                except Exception as e:
                    logger.debug(f"Impossible de parser la date '{d}': {e}")
                    continue
            
            if not parsed_dates:
                # CAS 2: Données corrompues - collecte complète
                logger.warning("Données existantes corrompues - collecte complète depuis la date la plus ancienne disponible")
                update_mode = "complete_overwrite"
                # start_date est déjà défini à earliest_available_date
                
            else:
                # Analyser les données existantes
                earliest_existing_date = min(parsed_dates)
                latest_existing_date = max(parsed_dates)
                
                logger.info(f"📊 Analyse des données existantes:")
                logger.info(f"   - Date la plus ancienne disponible (Facebook): {earliest_available_date.strftime('%Y-%m-%d')}")
                logger.info(f"   - Date la plus ancienne en base: {earliest_existing_date}")
                logger.info(f"   - Date la plus récente en base: {latest_existing_date}")
                
                # CAS 3: Vérifier si on a l'historique complet depuis la date disponible
                available_date_only = earliest_available_date.date()
                days_missing_from_start = (earliest_existing_date - available_date_only).days
                
                if days_missing_from_start > 30:  # Tolérance de 30 jours
                    # Il manque trop de données historiques - collecte complète
                    logger.warning(f"⚠️ Historique incomplet: il manque {days_missing_from_start} jours depuis la date la plus ancienne disponible")
                    logger.info(f"🔄 Remplacement complet des données pour avoir l'historique complet")
                    update_mode = "complete_overwrite"
                    
                elif earliest_existing_date < available_date_only:
                    # Les données existantes commencent avant la date disponible - c'est PRÉCIEUX !
                    logger.info(f"✅ Données historiques précieuses détectées: nous avons des données depuis {earliest_existing_date} alors que l'API ne va que jusqu'à {available_date_only}")
                    logger.info(f"🎯 Conservation de l'historique existant et mise à jour incrémentale")
                    
                    # Mise à jour incrémentale depuis la dernière date existante
                    days_to_update = (datetime.now().date() - latest_existing_date).days
                    
                    if days_to_update <= 0:
                        logger.info(f"✅ Les données sont déjà à jour pour {page_name}")
                        return spreadsheet_id
                    
                    logger.info(f"🔄 Mise à jour incrémentale - ajout des {days_to_update} derniers jours")
                    start_date = datetime.combine(latest_existing_date + timedelta(days=1), datetime.min.time())
                    update_mode = "incremental"
                    
                else:
                    # CAS 4: Historique semble correct - mise à jour incrémentale
                    days_to_update = (datetime.now().date() - latest_existing_date).days
                    
                    if days_to_update <= 0:
                        logger.info(f"✅ Les données sont déjà à jour pour {page_name}")
                        return spreadsheet_id
                    
                    logger.info(f"🔄 Mise à jour incrémentale - ajout des {days_to_update} derniers jours")
                    start_date = datetime.combine(latest_existing_date + timedelta(days=1), datetime.min.time())
                    update_mode = "incremental"
        
        # Log de la stratégie choisie
        if update_mode == "complete_overwrite":
            logger.info(f"📋 STRATÉGIE: Collecte complète du {start_date.strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")
        else:
            logger.info(f"📋 STRATÉGIE: Ajout incrémental du {start_date.strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")

        # Récupérer les nouvelles données
        logger.info(f"📊 Récupération des métriques du {start_date.strftime('%Y-%m-%d')} au {end_date.strftime('%Y-%m-%d')}")
        
        try:
            raw_data = fetch_page_metrics(page_access_token, METRICS, page_id, start_date, end_date)

            if raw_data.empty:
                logger.info(f"✅ Aucune nouvelle donnée à récupérer pour {page_name}")
                return spreadsheet_id

            logger.info(f"📈 {len(raw_data)} nouvelles lignes de données récupérées")

            # Formater et valider les données
            success, validation_message, validated_data = validate_and_format_for_sheets(raw_data)
            
            if not success:
                logger.error(f"❌ Validation échouée pour {page_name}: {validation_message}")
                return None
            
            logger.info(f"✅ Validation réussie pour {page_name}")
            if "⚠️" in validation_message:
                logger.warning(f"Avertissements de validation: {validation_message}")

            # Envoyer les données vers Google Sheets selon la stratégie
            if update_mode == "complete_overwrite":
                # Remplacer complètement les données existantes
                logger.info(f"🔄 Remplacement complet des données...")
                collector.update_sheet_data(spreadsheet_id, validated_data)
                logger.info(f"✅ Données remplacées complètement pour {page_name}: {len(validated_data)} lignes")
            else:
                # Ajouter les nouvelles données à la fin
                logger.info(f"📊 Ajout des nouvelles données...")
                collector.append_sheet_data(spreadsheet_id, validated_data)
                logger.info(f"✅ {len(validated_data)} nouvelles lignes ajoutées pour {page_name}")
            
            logger.info(f"📋 Colonnes finales: {list(validated_data.columns)}")

        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération/traitement des données pour {page_name}: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
            return None
        
        return spreadsheet_id
        
    except Exception as e:
        logger.error(f"❌ Erreur générale dans process_page_metrics pour {page_name}: {e}")
        import traceback
        logger.error(f"Stack trace: {traceback.format_exc()}")
        return None


# Pour les tests locaux uniquement
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Configuration de logging pour les tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) != 4:
        print("Usage: python fb_page_metrics.py <token> <page_id> <page_name>")
        sys.exit(1)
    
    test_token = sys.argv[1]
    test_page_id = sys.argv[2]
    test_page_name = sys.argv[3]
    
    try:
        spreadsheet_id = process_page_metrics(test_token, test_page_id, test_page_name)
        if spreadsheet_id:
            print(f"✓ Test réussi! Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        else:
            print("✗ Test échoué - aucun spreadsheet créé")
            sys.exit(1)
    except Exception as e:
        print(f"✗ Erreur lors du test: {e}")
        sys.exit(1)