"""
Cloud Function pour le monitoring du token Facebook
À exécuter toutes les heures pour vérifier l'état du token
"""
import functions_framework
import json
from utils.token_manager import FacebookTokenManager
from utils.config_manager import ConfigManager
from datetime import datetime

@functions_framework.http
def token_monitor(request):
    """Monitore l'état du token Facebook et envoie des alertes si nécessaire"""
    try:
        # Initialiser les managers
        token_manager = FacebookTokenManager()
        config_manager = ConfigManager()
        
        # Vérifier le statut du token
        status = token_manager.check_and_notify_token_status()
        
        # Sauvegarder le rapport
        config_manager.save_report("token_status", status)
        
        # Si action urgente requise, tenter un rafraîchissement automatique
        if status.get("requires_action", False):
            try:
                token = token_manager.get_token_from_secret_manager()
                if token:
                    new_token = token_manager.exchange_for_long_lived_token(token)
                    if new_token:
                        status["refresh_attempted"] = True
                        status["refresh_success"] = True
                        status["message"] += " - Token rafraîchi automatiquement"
                    else:
                        status["refresh_attempted"] = True
                        status["refresh_success"] = False
            except Exception as e:
                status["refresh_error"] = str(e)
        
        # Retourner le statut
        return status, 200
        
    except Exception as e:
        error_status = {
            "status": "error",
            "message": f"Erreur lors du monitoring: {str(e)}",
            "timestamp": datetime.now().isoformat(),
            "urgent": True
        }
        
        # Sauvegarder l'erreur
        try:
            config_manager = ConfigManager()
            config_manager.save_report("token_monitor_error", error_status)
        except:
            pass
        
        return error_status, 500

@functions_framework.cloud_event
def token_monitor_pubsub(cloud_event):
    """Version Pub/Sub du monitoring"""
    # Appeler la fonction HTTP
    from flask import Request
    fake_request = Request.from_values()
    token_monitor(fake_request)