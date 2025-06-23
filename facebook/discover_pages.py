"""
Découverte automatique des pages Facebook et mise à jour de la configuration
Version adaptée pour Cloud Functions avec gestion GCS
"""
import os
import sys
import json
import requests
import logging
from datetime import datetime

# Ajouter le répertoire parent au path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.token_manager import FacebookTokenManager
from utils.config_manager import ConfigManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def discover_facebook_pages(token):
    """Découvre toutes les pages Facebook accessibles avec le token"""
    url = "https://graph.facebook.com/v21.0/me/accounts"
    params = {
        "access_token": token,
        "fields": "name,id,access_token,category,tasks",
        "limit": 100
    }
    
    all_pages = []
    
    try:
        while url:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'error' in data:
                raise Exception(f"Erreur API: {data['error']['message']}")
            
            if 'data' in data:
                for page in data['data']:
                    page_info = {
                        "id": page.get("id"),
                        "name": page.get("name"),
                        "category": page.get("category"),
                        "access_token": page.get("access_token"),
                        "tasks": page.get("tasks", [])
                    }
                    all_pages.append(page_info)
                    logger.info(f"Page trouvée: {page_info['name']} (ID: {page_info['id']})")
            
            # Pagination
            url = data.get("paging", {}).get("next")
            params = {}  # Les paramètres sont déjà dans l'URL de pagination
            
        logger.info(f"✓ {len(all_pages)} pages découvertes au total")
        
        return all_pages
        
    except Exception as e:
        logger.error(f"Erreur lors de la découverte des pages: {e}")
        raise

def update_pages_configuration(pages, config_manager):
    """Met à jour la configuration des pages dans GCS"""
    try:
        # Charger la configuration existante ou créer une nouvelle
        existing_config = config_manager.load_config("pages_config.json") or {
            "last_updated": None,
            "total_pages": 0,
            "pages": {}
        }
        
        # Mettre à jour avec les nouvelles pages
        updated_pages = {}
        new_pages = []
        
        for page in pages:
            page_id = page["id"]
            page_name = page["name"]
            
            # Vérifier si c'est une nouvelle page
            if page_id not in existing_config["pages"]:
                new_pages.append(page_name)
            
            # Conserver les configurations existantes ou créer de nouvelles
            updated_pages[page_id] = existing_config["pages"].get(page_id, {
                "name": page_name,
                "category": page.get("category", "Unknown"),
                "enabled": True,
                "metrics": {
                    "page_metrics": True,
                    "posts_lifetime": True,
                    "posts_metadata": True
                },
                "spreadsheet_ids": {}
            })
            
            # Mettre à jour le nom et la catégorie au cas où ils auraient changé
            updated_pages[page_id]["name"] = page_name
            updated_pages[page_id]["category"] = page.get("category", "Unknown")
            updated_pages[page_id]["last_discovered"] = datetime.now().isoformat()
        
        # Marquer les pages qui ne sont plus accessibles
        for page_id, page_info in existing_config["pages"].items():
            if page_id not in updated_pages:
                updated_pages[page_id] = page_info
                updated_pages[page_id]["enabled"] = False
                updated_pages[page_id]["last_seen"] = page_info.get("last_discovered", "Unknown")
                logger.warning(f"Page {page_info['name']} ({page_id}) n'est plus accessible")
        
        # Créer la nouvelle configuration
        new_config = {
            "last_updated": datetime.now().isoformat(),
            "total_pages": len([p for p in updated_pages.values() if p.get("enabled", True)]),
            "pages": updated_pages
        }
        
        # Sauvegarder dans GCS
        config_manager.save_config("pages_config.json", new_config)
        
        # Log des changements
        if new_pages:
            logger.info(f"✓ {len(new_pages)} nouvelles pages découvertes: {', '.join(new_pages)}")
        else:
            logger.info("✓ Aucune nouvelle page découverte")
        
        return new_config
        
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour de la configuration: {e}")
        raise

def update_page_tokens(pages, config_manager):
    """Met à jour les tokens de page dans GCS"""
    try:
        # Charger la configuration existante ou créer une nouvelle
        existing_tokens = config_manager.load_config("page_tokens.json") or {
            "last_updated": None,
            "tokens": {}
        }
        
        # Mettre à jour avec les nouveaux tokens
        updated_tokens = {}
        
        for page in pages:
            if page.get("access_token"):
                updated_tokens[page["id"]] = {
                    "page_name": page["name"],
                    "access_token": page["access_token"],
                    "updated_at": datetime.now().isoformat()
                }
        
        # Créer la nouvelle configuration
        new_tokens_config = {
            "last_updated": datetime.now().isoformat(),
            "tokens": updated_tokens
        }
        
        # Sauvegarder dans GCS
        config_manager.save_config("page_tokens.json", new_tokens_config)
        
        logger.info(f"✓ {len(updated_tokens)} tokens de page mis à jour")
        
        return new_tokens_config
        
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour des tokens: {e}")
        raise

def main():
    """Fonction principale pour la découverte des pages"""
    logger.info("=== DÉCOUVERTE DES PAGES FACEBOOK ===")
    
    try:
        # Initialiser les managers
        token_manager = FacebookTokenManager()
        config_manager = ConfigManager()
        
        # S'assurer que le bucket existe
        config_manager.ensure_bucket_exists()
        
        # Obtenir un token valide
        logger.info("Obtention du token Facebook...")
        token = token_manager.get_valid_token()
        
        # Découvrir les pages
        logger.info("Découverte des pages...")
        pages = discover_facebook_pages(token)
        
        # Mettre à jour la configuration des pages
        logger.info("Mise à jour de la configuration des pages...")
        pages_config = update_pages_configuration(pages, config_manager)
        
        # Mettre à jour les tokens de page
        logger.info("Mise à jour des tokens de page...")
        tokens_config = update_page_tokens(pages, config_manager)
        
        # Générer un rapport
        report = {
            "execution_time": datetime.now().isoformat(),
            "status": "success",
            "pages_discovered": len(pages),
            "pages_enabled": pages_config["total_pages"],
            "tokens_saved": len(tokens_config["tokens"]),
            "pages": [
                {
                    "id": page["id"],
                    "name": page["name"],
                    "category": page.get("category", "Unknown"),
                    "has_token": page.get("access_token") is not None
                }
                for page in pages
            ]
        }
        
        # Sauvegarder le rapport
        config_manager.save_report("pages_discovery", report)
        
        logger.info("✓ Découverte des pages terminée avec succès")
        logger.info(f"  - Pages découvertes: {len(pages)}")
        logger.info(f"  - Pages actives: {pages_config['total_pages']}")
        logger.info(f"  - Tokens sauvegardés: {len(tokens_config['tokens'])}")
        
        return report
        
    except Exception as e:
        logger.error(f"Erreur lors de la découverte des pages: {e}")
        
        # Créer un rapport d'erreur
        error_report = {
            "execution_time": datetime.now().isoformat(),
            "status": "error",
            "error": str(e)
        }
        
        try:
            config_manager.save_report("pages_discovery", error_report)
        except:
            pass
        
        raise

# Pour les tests locaux uniquement
if __name__ == "__main__":
    # Configuration de logging pour les tests
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        report = main()
        print(json.dumps(report, indent=2))
    except Exception as e:
        print(f"❌ Erreur: {e}")
        sys.exit(1)