"""
Script adapté pour collecter les métriques de page Facebook dans Cloud Functions
Fait partie du système d'automatisation Facebook sur Google Cloud
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
    
    logger.info(f"Récupération des données pour la page {page_id}...")
    
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
            logger.error(f"Erreur réseau sur la page {page_id} ({page_id}): {str(e)}")
            logger.warning(f"SKIP de cette période pour la page {page_id}, on continue avec la prochaine période...")
            time.sleep(5)
            continue

        except Exception as e:
            logger.error(f"Erreur inattendue: {str(e)}")
            if "Token invalide" in str(e):
                raise
            continue
        
        # Passer à la prochaine tranche
        current_date = chunk_end + timedelta(days=1)

    if not all_data:
        logger.warning(f"Aucune donnée récupérée pour la page {page_id} sur toute la période demandée.")
        # Retourner un DataFrame vide au lieu de continuer
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

def format_dataframe(df):
    """
    Formate le DataFrame en renommant les colonnes et en ajustant les types de données.
    Inclut maintenant les métriques calculées avec des noms simplifiés.
    """
    # D'abord ajouter les métriques calculées
    df = add_calculated_metrics(df)
    
    # Configuration des colonnes avec leurs types et noms simplifiés
    column_config = {
        # Colonnes temporelles - datetime
        "date": {
            "new_name": "Date",
            "type": "datetime"
        },
        
        # Métriques de base de la page - entiers
        "page_impressions": {
            "new_name": "Affichages de la page",  # Simplifié
            "type": "int"
        },
        "page_impressions_unique": {
            "new_name": "Visiteurs de la page",  # Simplifié
            "type": "int"
        },
        "page_impressions_nonviral": {
            "new_name": "Affichages non viraux",  # Simplifié
            "type": "int"
        },
        "page_impressions_viral": {
            "new_name": "Affichages viraux",  # Simplifié
            "type": "int"
        },
        
        # Métriques des posts - entiers
        "page_posts_impressions": {
            "new_name": "Affichages des publications",  # Simplifié
            "type": "int"
        },
        "page_posts_impressions_unique": {
            "new_name": "Visiteurs de la publication",  # Simplifié
            "type": "int"
        },
        "page_posts_impressions_paid": {
            "new_name": "Affichages publicitaires",  # Simplifié
            "type": "int"
        },
        "page_posts_impressions_organic": {
            "new_name": "Affichages organiques",  # Simplifié
            "type": "int"
        },
        "page_posts_impressions_organic_unique": {
            "new_name": "Visiteurs uniques organiques",  # Simplifié
            "type": "int"
        },
        
        # Métriques des vues de page - entiers
        "page_views_total": {
            "new_name": "Vues totales de la page",  # Simplifié
            "type": "int"
        },
        
        # Métriques des fans - entiers
        "page_fans": {
            "new_name": "Nombre de fans",  # Simplifié
            "type": "int"
        },
        "page_fan_adds": {
            "new_name": "Nouveaux fans",  # Simplifié
            "type": "int"
        },
        "page_fan_removes": {
            "new_name": "Fans perdus",  # Simplifié
            "type": "int"
        },
        
        # Métriques des abonnements payants/non payants - entiers
        "page_fan_adds_by_paid_non_paid_unique_total": {
            "new_name": "Total nouveaux fans (payants + organiques)",  # Simplifié
            "type": "int"
        },
        "page_fan_adds_by_paid_non_paid_unique_paid": {
            "new_name": "Nouveaux fans via pub",  # Simplifié
            "type": "int"
        },
        "page_fan_adds_by_paid_non_paid_unique_unpaid": {
            "new_name": "Nouveaux fans organiques",  # Simplifié
            "type": "int"
        },
        
        # Métriques des follows - entiers
        "page_follows": {
            "new_name": "Nombre d'abonnés",  # Simplifié
            "type": "int"
        },
        "page_daily_follows": {
            "new_name": "Nouveaux abonnés",  # Simplifié
            "type": "int"
        },
        "page_daily_unfollows": {
            "new_name": "Désabonnements",
            "type": "int"
        },
        "page_daily_follows_unique": {
            "new_name": "Abonnés uniques du jour",  # Simplifié
            "type": "int"
        },
        
        # Métriques des vidéos - entiers et floats
        "page_video_views": {
            "new_name": "Vues de vidéos",  # Simplifié
            "type": "int"
        },
        "page_video_views_unique": {
            "new_name": "Vues uniques de vidéos",  # Simplifié
            "type": "int"
        },
        "page_video_views_paid": {
            "new_name": "Vues vidéos via pub",  # Simplifié
            "type": "int"
        },
        "page_video_views_organic": {
            "new_name": "Vues vidéos organiques",  # Simplifié
            "type": "int"
        },
        "page_video_repeat_views": {
            "new_name": "Relectures vidéos",  # Simplifié
            "type": "int"
        },
        "page_video_view_time": {
            "new_name": "Temps de visionnage (sec)",  # Simplifié
            "type": "float"
        },
        
        # Métriques des vues complètes - entiers
        "page_video_complete_views_30s": {
            "new_name": "Vues complètes (30s)",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_video_complete_views_30s_unique": {
            "new_name": "Vues complètes uniques (30s)",  # Simplifié
            "type": "int"
        },
        "page_video_complete_views_30s_paid": {
            "new_name": "Vues complètes via pub (30s)",  # Simplifié
            "type": "int"
        },
        "page_video_complete_views_30s_organic": {
            "new_name": "Vues complètes organiques (30s)",  # Simplifié
            "type": "int"
        },
        "page_video_complete_views_30s_autoplayed": {
            "new_name": "Vues complètes auto (30s)",  # Simplifié
            "type": "int"
        },
        "page_video_complete_views_30s_repeat_views": {
            "new_name": "Relectures complètes (30s)",  # Simplifié
            "type": "int"
        },
        
        # Métriques d'engagement - entiers
        "page_post_engagements": {
            "new_name": "Interactions sur publications",  # Simplifié
            "type": "int"
        },
        "page_total_actions": {
            "new_name": "Actions totales",  # Simplifié
            "type": "int"
        },
        "page_actions_post_reactions_like_total": {
            "new_name": "Nbre de \"J'aime\"",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_actions_post_reactions_love_total": {
            "new_name": "Nbre de \"J'adore\"",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_actions_post_reactions_wow_total": {
            "new_name": "Nbre de \"Wow\"",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_actions_post_reactions_haha_total": {
            "new_name": "Nbre de \"Haha\"",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_actions_post_reactions_sorry_total": {
            "new_name": "Nbre de \"Triste\"",  # Simplifié selon votre demande
            "type": "int"
        },
        "page_actions_post_reactions_anger_total": {
            "new_name": "Nbre de \"En colère\"",  # Simplifié selon votre demande
            "type": "int"
        },
        
        # Métriques calculées - floats
        "taux_engagement_page": {
            "new_name": "Tx d'engagement (%)",  # Simplifié avec Tx
            "type": "float"
        },
        "frequence_impressions": {
            "new_name": "Fréquence impressions",  # Déjà court
            "type": "float"
        },
        "actions_totales_calculees": {
            "new_name": "Actions calculées",  # Simplifié
            "type": "int"
        },
        "vtr_percentage_page": {
            "new_name": "VTR %",  # Simplifié
            "type": "float"
        }
    }

    try:
        # Convertir la colonne date en datetime
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # Appliquer les types et renommages pour chaque colonne
        for col_name, config in column_config.items():
            if col_name in df.columns:
                if config["type"] == "int":
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif config["type"] == "float":
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
                elif config["type"] == "datetime":
                    df[col_name] = pd.to_datetime(df[col_name])

        # Renommer les colonnes
        rename_mapping = {k: v["new_name"] for k, v in column_config.items() if k in df.columns}
        df = df.rename(columns=rename_mapping)

        return df

    except Exception as e:
        logger.error(f"Erreur lors du formatage du DataFrame: {str(e)}")
        raise
    

def get_page_token(user_token, page_id):
    """
    Récupère le token spécifique à une page Facebook
    Version améliorée qui utilise d'abord les tokens sauvegardés
    """
    import json  # Import local pour s'assurer qu'il est disponible
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
        response.raise_for_status()
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
    
    
def process_page_metrics(token, page_id, page_name):
    """
    Fonction principale pour traiter les métriques d'une page Facebook
    Compatible avec Cloud Functions
    
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
        
        # Définir la période de collecte
        end_date = datetime.now()

        # Dates existantes
        existing_dates = collector.get_existing_dates(spreadsheet_id)

        # Si peu de dates → initial load
        if len(existing_dates) < 30:
            logger.info(f"Mode initial load pour {page_name} → récupération de 2 ans de données")
            start_date = end_date.replace(year=end_date.year - 2)
            update_mode = "initial"

        else:
            # Daily update : on récupère à partir de la dernière date existante
            logger.info(f"Mode daily update pour {page_name}")
            
            # Normaliser et parser les dates existantes
            parsed_dates = []
            for d in existing_dates:
                try:
                    parsed_dates.append(pd.to_datetime(d).date())
                except:
                    continue
            
            if parsed_dates:
                # Trouver la date la plus récente
                latest_date = max(parsed_dates)
                # Commencer le lendemain de la dernière date
                start_date = datetime.combine(latest_date + timedelta(days=1), datetime.min.time())
                logger.info(f"→ Dernière date existante: {latest_date}, on récupère depuis {start_date.date()}")
            else:
                # Si on ne peut pas parser les dates, on fait une update des 7 derniers jours
                start_date = end_date - timedelta(days=7)
                logger.info("→ Impossible de parser les dates existantes, on update les 7 derniers jours")
            
            update_mode = "daily"
        
        try:
            # Récupérer les métriques
            raw_data = fetch_page_metrics(page_access_token, METRICS, page_id, start_date, end_date)

            if raw_data.empty:
                logger.info(f"✓ Aucune nouvelle donnée à récupérer pour {page_name} (données déjà à jour)")
                return spreadsheet_id
                
            if update_mode == "daily":
                # En mode daily → ne garder que les dates qui n'existent pas encore
                normalized_existing_dates = set()
                for d in existing_dates:
                    try:
                        parsed = pd.to_datetime(d).strftime("%Y-%m-%d")
                        normalized_existing_dates.add(parsed)
                    except:
                        continue
                
                # Filtrer pour ne garder que les nouvelles dates
                raw_data_dates = set(raw_data['date'].values)
                dates_to_keep = raw_data_dates - normalized_existing_dates
                
                if dates_to_keep:
                    raw_data = raw_data[raw_data['date'].isin(dates_to_keep)]
                    logger.info(f"→ {len(raw_data)} nouvelles dates à insérer")
                else:
                    logger.info(f"✓ Aucune nouvelle date à insérer pour {page_name} (toutes les dates sont à jour)")
                    return spreadsheet_id
            else:
                # En mode initial, on garde toutes les données récupérées
                logger.info(f"→ Mode initial: {len(raw_data)} dates à insérer")

            # Formater les données (inclut maintenant les métriques calculées)
            formatted_data = format_dataframe(raw_data)

            # Mettre à jour le Google Sheet
            collector.update_sheet_data(spreadsheet_id, formatted_data)
            
            logger.info(f"✓ Métriques mises à jour pour {page_name}: {len(formatted_data)} lignes")

        except Exception as e:
            logger.error(f"❌ Erreur bloquante pour la page {page_name} ({page_id}): {e} → on passe à la page suivante.")
            # On ne lève pas l'erreur → on passe à la suivante
            return None
        
        return spreadsheet_id
        
    except Exception as e:
        logger.error(f"❌ Erreur générale dans process_page_metrics pour {page_name}: {e} → on passe à la page suivante.")
        return None


# Pour les tests locaux uniquement
if __name__ == "__main__":
    # Ce bloc ne s'exécute que lors des tests locaux
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
        print(f"✓ Test réussi! Spreadsheet: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
    except Exception as e:
        print(f"✗ Erreur lors du test: {e}")
        sys.exit(1)