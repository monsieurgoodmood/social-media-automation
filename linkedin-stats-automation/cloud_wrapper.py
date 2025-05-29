#!/usr/bin/env python3
"""
Wrappers pour exécuter les scripts LinkedIn dans Cloud Functions sans interaction
"""

import os
import sys
import importlib.util
import json
import builtins

def run_script_without_interaction(script_name, module_name):
    """Exécute un script en désactivant les confirmations"""
    
    # Définir une variable d'environnement pour indiquer qu'on est en mode automatique
    os.environ['AUTOMATED_MODE'] = 'true'
    
    # Charger le module dynamiquement
    spec = importlib.util.spec_from_file_location(module_name, f'/workspace/{script_name}.py')
    module = importlib.util.module_from_spec(spec)
    
    # Sauvegarder la fonction input originale
    original_input = builtins.input
    
    # Remplacer input() par une fonction qui retourne toujours 'o'
    def auto_yes(prompt=""):
        print(f"Auto-réponse à: {prompt}")
        return 'o'
    
    builtins.input = auto_yes
    
    try:
        # Exécuter le module
        spec.loader.exec_module(module)
        return True
    except SystemExit as e:
        # Capturer les exit() et retourner le code
        return e.code == 0 if e.code is not None else True
    except Exception as e:
        print(f"Erreur lors de l'exécution du script: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Restaurer input original
        builtins.input = original_input

def run_discover_organizations():
    """Exécute le script de découverte des organisations"""
    print("Exécution de discover_organizations.py...")
    return run_script_without_interaction('discover_organizations', 'discover_organizations')

def run_follower_statistics():
    """Exécute le script des statistiques de followers"""
    print("Exécution de linkedin_multi_follower_stats.py...")
    return run_script_without_interaction('linkedin_multi_follower_stats', 'follower_stats')

def run_share_statistics():
    """Exécute le script des statistiques de partage"""
    print("Exécution de linkedin_multi_org_share_tracker.py...")
    return run_script_without_interaction('linkedin_multi_org_share_tracker', 'share_stats')

def run_page_statistics():
    """Exécute le script des statistiques de pages"""
    print("Exécution de linkedin_multi_page_stats.py...")
    return run_script_without_interaction('linkedin_multi_page_stats', 'page_stats')

def run_post_metrics():
    """Exécute le script des métriques de posts"""
    print("Exécution de linkedin_multi_post_metrics.py...")
    return run_script_without_interaction('linkedin_multi_post_metrics', 'post_metrics')

def run_daily_statistics():
    """Exécute le script des statistiques quotidiennes"""
    print("Exécution de linkedin_multi_org_tracker.py...")
    return run_script_without_interaction('linkedin_multi_org_tracker', 'daily_stats')

# Dictionnaire des fonctions disponibles
AVAILABLE_SCRIPTS = {
    'discover_organizations': run_discover_organizations,
    'follower_statistics': run_follower_statistics,
    'share_statistics': run_share_statistics,
    'page_statistics': run_page_statistics,
    'post_metrics': run_post_metrics,
    'daily_statistics': run_daily_statistics
}

def execute_script(script_name):
    """Exécute le script demandé"""
    if script_name in AVAILABLE_SCRIPTS:
        return AVAILABLE_SCRIPTS[script_name]()
    else:
        raise ValueError(f"Script inconnu: {script_name}")
