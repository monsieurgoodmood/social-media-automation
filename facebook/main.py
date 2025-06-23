"""
Point d'entrée principal pour Cloud Functions
"""
import functions_framework
import os
from cloud_wrapper import process_all_pages
from discover_pages import main as discover_pages_main
from token_monitor import token_monitor as monitor_token
import json

@functions_framework.http
def facebook_automation(request):
    """Point d'entrée HTTP pour Cloud Functions"""
   
    # Récupérer l'action depuis la requête
    request_json = request.get_json(silent=True)
    request_args = request.args
   
    action = None
    if request_json and 'action' in request_json:
        action = request_json['action']
    elif request_args and 'action' in request_args:
        action = request_args['action']
   
    try:
        # Vérifier le token avant toute action (sauf monitoring)
        if action != 'token_monitor':
            from utils.token_manager import FacebookTokenManager
            tm = FacebookTokenManager()
            token_status = tm.check_and_notify_token_status()
            
            # Si le token est critique, ne pas continuer
            if token_status.get("urgent", False) and token_status.get("status") != "valid":
                return {
                    'status': 'error',
                    'message': 'Token Facebook invalide ou expiré',
                    'token_status': token_status
                }, 503
        
        if action == 'discover_pages':
            # Découverte hebdomadaire des pages
            discover_pages_main()
            return {'status': 'success', 'message': 'Pages discovered successfully'}, 200
           
        elif action == 'page_metrics':
            # Métriques quotidiennes des pages
            report = process_all_pages('page_metrics')
            return report, 200
           
        elif action == 'posts_lifetime':
            # Métriques lifetime des posts
            report = process_all_pages('posts_lifetime')
            return report, 200
           
        elif action == 'posts_metadata':
            # Métadonnées des posts
            report = process_all_pages('posts_metadata')
            return report, 200
        
        elif action == 'token_monitor':
            # Monitoring du token
            return monitor_token(request)
           
        else:
            return {
                'status': 'error',
                'message': 'Action not specified or invalid. Valid actions: discover_pages, page_metrics, posts_lifetime, posts_metadata, token_monitor'
            }, 400
           
    except Exception as e:
        return {
            'status': 'error',
            'message': str(e)
        }, 500

@functions_framework.cloud_event
def facebook_automation_pubsub(cloud_event):
    """Point d'entrée Pub/Sub pour Cloud Functions"""
   
    # Récupérer l'action depuis le message Pub/Sub
    import base64
    message_data = base64.b64decode(cloud_event.data["message"]["data"]).decode()
   
    try:
        message = json.loads(message_data)
        action = message.get('action')
       
        if action == 'discover_pages':
            discover_pages_main()
        elif action in ['page_metrics', 'posts_lifetime', 'posts_metadata']:
            process_all_pages(action)
        elif action == 'token_monitor':
            from flask import Request
            fake_request = Request.from_values()
            monitor_token(fake_request)
        else:
            raise Exception(f"Action inconnue: {action}")
           
    except Exception as e:
        print(f"Erreur: {e}")
        raise