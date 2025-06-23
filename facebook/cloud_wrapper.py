"""
Wrapper pour exécuter les scripts dans Cloud Functions
"""
import os
import sys
import json
import logging
from datetime import datetime

# Ajouter le répertoire parent au path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.token_manager import FacebookTokenManager
from utils.config_manager import ConfigManager
from scripts.fb_page_metrics import process_page_metrics
from scripts.fb_posts_lifetime import process_posts_lifetime
from scripts.fb_posts_metadata import process_posts_metadata
from scripts.fb_base_collector import FacebookBaseCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_all_pages(metric_type):
    """Traite toutes les pages pour un type de métrique donné, avec retries jusqu'à convergence"""
    logger.info(f"=== START process_all_pages === {metric_type} ===")
    try:
        # Initialiser les managers
        token_manager = FacebookTokenManager()
        config_manager = ConfigManager()
        
        # Obtenir un token valide (avec rafraîchissement automatique si nécessaire)
        token = token_manager.get_valid_token()
        
        # Charger la configuration des pages
        pages_config = config_manager.load_config("pages_config.json")
        if not pages_config or not pages_config.get("pages"):
            raise Exception("Aucune configuration de pages trouvée")
        
        # Préparer le rapport
        report = {
            "execution_time": datetime.now().isoformat(),
            "metric_type": metric_type,
            "status": "success",
            "pages_processed": 0,
            "pages_failed": 0,
            "results": []
        }
        
        # Liste initiale de pages à traiter
        pages_to_process = [
            {"page_id": page_id, "page_info": page_info}
            for page_id, page_info in pages_config["pages"].items()
            if page_info.get("enabled", True) and page_info.get("metrics", {}).get(metric_type, True)
        ]

        logger.info(f"Nombre total de pages à traiter: {len(pages_to_process)}")

        # Fonction de traitement d'une page
        def process_single_page(page_id, page_info):
            nonlocal token, pages_config, report
            logger.info(f"--- START processing page: {page_info['name']} ({page_id}) ---")
            try:
                if metric_type == "page_metrics":
                    spreadsheet_id = process_page_metrics(token, page_id, page_info["name"])
                elif metric_type == "posts_lifetime":
                    spreadsheet_id = process_posts_lifetime(token, page_id, page_info["name"])
                elif metric_type == "posts_metadata":
                    spreadsheet_id = process_posts_metadata(token, page_id, page_info["name"])
                else:
                    raise Exception(f"Type de métrique inconnu: {metric_type}")
                
                spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                
                logger.info(f"--- DONE processing page: {page_info['name']} ({page_id}) ---")

                # MAJ du report
                report["pages_processed"] += 1
                report["results"].append({
                    "page_id": page_id,
                    "page_name": page_info["name"],
                    "status": "success",
                    "spreadsheet_id": spreadsheet_id,
                    "spreadsheet_url": spreadsheet_url
                })

                # MAJ de la config
                pages_config["pages"][page_id]["last_processed"] = datetime.now().isoformat()
                if "spreadsheet_ids" not in pages_config["pages"][page_id]:
                    pages_config["pages"][page_id]["spreadsheet_ids"] = {}
                pages_config["pages"][page_id]["spreadsheet_ids"][metric_type] = spreadsheet_id

                return True  # succès
            except Exception as e:
                logger.error(f"Erreur pour la page {page_info['name']}: {e}")
                report["pages_failed"] += 1
                report["results"].append({
                    "page_id": page_id,
                    "page_name": page_info["name"],
                    "status": "error",
                    "error": str(e)
                })
                return False  # échec

        # === PASSE INITIALE ===
        pages_failed_in_pass = []
        for page in pages_to_process:
            success = process_single_page(page["page_id"], page["page_info"])
            if not success:
                pages_failed_in_pass.append(page)

        # Sauvegarde après passe initiale
        config_manager.save_config("pages_config.json", pages_config)
        config_manager.save_report(f"{metric_type}_execution", report)

        # === BOUCLE DE RETRY ===
        pass_number = 1
        while pages_failed_in_pass:
            logger.info(f"=== RETRY PASS #{pass_number} === Pages en échec: {len(pages_failed_in_pass)}")
            current_failed_pages = pages_failed_in_pass
            pages_failed_in_pass = []

            # Pour éviter de dupliquer les lignes dans le report → on retire l'ancien statut error pour ces pages
            report["results"] = [r for r in report["results"] if not (
                r["status"] == "error" and any(r["page_id"] == p["page_id"] for p in current_failed_pages)
            )]

            # On retraitera les pages échouées
            for page in current_failed_pages:
                success = process_single_page(page["page_id"], page["page_info"])
                if not success:
                    pages_failed_in_pass.append(page)

            # MAJ du report et config après cette passe
            config_manager.save_config("pages_config.json", pages_config)
            config_manager.save_report(f"{metric_type}_execution", report)

            pass_number += 1

        logger.info("=== Toutes les pages ont convergé ===")

        # Final : rapport consolidé
        generate_consolidated_report(config_manager)

        logger.info(f"Traitement terminé FINAL: {report['pages_processed']} réussies, {report['pages_failed']} échouées")
        return report

    except Exception as e:
        logger.error(f"Erreur dans process_all_pages: {e}")
        error_report = {
            "execution_time": datetime.now().isoformat(),
            "metric_type": metric_type,
            "status": "error",
            "error": str(e)
        }
        config_manager.save_report(f"{metric_type}_execution", error_report)
        raise

def generate_consolidated_report(config_manager):
    """Génère un rapport consolidé de tous les spreadsheets"""
    try:
        all_spreadsheets = []
        metric_types = ["page_metrics", "posts_lifetime", "posts_metadata"]
        
        # Récupérer tous les mappings
        for metric_type in metric_types:
            mapping_file = f"{metric_type}_mapping.json"
            mapping_config = config_manager.load_config(mapping_file) or {}
            
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
        
        # Créer le rapport consolidé
        consolidated_report = {
            "generated_at": datetime.now().isoformat(),
            "total_spreadsheets": len(all_spreadsheets),
            "spreadsheets_by_type": {
                metric_type: len([s for s in all_spreadsheets if s["metric_type"] == metric_type])
                for metric_type in metric_types
            },
            "spreadsheets": all_spreadsheets
        }
        
        # Sauvegarder le rapport
        config_manager.save_report("all_spreadsheets", consolidated_report)
        
        # Créer aussi un rapport simplifié pour Looker Studio
        looker_config = {
            "facebook_analytics": {
                "generated_at": datetime.now().isoformat(),
                "description": "Configuration des sources de données Facebook pour Looker Studio",
                "data_sources": []
            }
        }
        
        # Grouper par type de métrique
        for metric_type in metric_types:
            sheets = [s for s in all_spreadsheets if s["metric_type"] == metric_type]
            if sheets:
                source = {
                    "name": f"Facebook {metric_type.replace('_', ' ').title()}",
                    "type": "Google Sheets",
                    "spreadsheets": [
                        {
                            "page_id": sheet["page_id"],
                            "page_name": sheet["page_name"],
                            "url": sheet["spreadsheet_url"],
                            "sheet_name": "Metrics Data",
                            "range": "A1:ZZ"
                        }
                        for sheet in sheets
                    ]
                }
                looker_config["facebook_analytics"]["data_sources"].append(source)
        
        # Sauvegarder la configuration Looker Studio
        config_manager.save_report("looker_studio_config", looker_config)
        
        logger.info(f"Rapport consolidé généré: {len(all_spreadsheets)} spreadsheets")
        
    except Exception as e:
        logger.error(f"Erreur lors de la génération du rapport consolidé: {e}")