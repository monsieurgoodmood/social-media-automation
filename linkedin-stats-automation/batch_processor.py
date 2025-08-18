#!/usr/bin/env python3
"""
Version simplifiée qui exécute directement les scripts comme le test local
"""

import os
import sys
import subprocess
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def execute_batch_script(script_name: str, force_run: bool = True) -> bool:
    """Exécute un script directement comme le test local"""
    logger.info(f"🚀 Exécution directe de {script_name}")
    
    # Mapping des scripts
    script_mapping = {
        'follower_statistics': 'linkedin_multi_follower_stats.py',
        'share_statistics': 'linkedin_multi_org_share_tracker.py', 
        'page_statistics': 'linkedin_multi_page_stats.py',
        'post_metrics': 'linkedin_multi_post_metrics.py',
        'daily_statistics': 'linkedin_multi_org_tracker.py'
    }
    
    script_file = script_mapping.get(script_name)
    if not script_file:
        logger.error(f"❌ Script inconnu: {script_name}")
        return False
    
    # Chercher le script
    script_path = None
    for directory in ['/workspace', '/tmp', '.']:
        test_path = os.path.join(directory, script_file)
        if os.path.exists(test_path):
            script_path = test_path
            break
    
    if not script_path:
        logger.error(f"❌ Script non trouvé: {script_file}")
        return False
    
    logger.info(f"📂 Script trouvé: {script_path}")
    
    # Préparer l'environnement
    env = os.environ.copy()
    env['AUTOMATED_MODE'] = 'true'
    env['PYTHONPATH'] = '/workspace:/tmp'
    
    try:
        # Exécuter le script directement (comme le test local)
        logger.info(f"▶️ Lancement de {script_file}...")
        
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            cwd=os.path.dirname(script_path) if os.path.dirname(script_path) else '/workspace',
            timeout=3000  # 50 minutes max
        )
        
        # Logger la sortie
        if result.stdout:
            for line in result.stdout.split('\n')[-20:]:  # Dernières 20 lignes
                if line.strip():
                    logger.info(f"STDOUT: {line}")
        
        if result.stderr:
            for line in result.stderr.split('\n')[-10:]:  # Dernières 10 lignes d'erreur
                if line.strip():
                    logger.warning(f"STDERR: {line}")
        
        success = result.returncode == 0
        
        if success:
            logger.info(f"✅ {script_name} terminé avec succès")
        else:
            logger.error(f"❌ {script_name} a échoué (code: {result.returncode})")
        
        return success
        
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Timeout - {script_name} a pris plus de 50 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution de {script_name}: {e}")
        return False

def process_organizations_batch(script_name: str, batch_size: int = 3, force_run: bool = True) -> Dict[str, Any]:
    """Fonction de compatibilité"""
    success = execute_batch_script(script_name, force_run)
    return {
        'status': 'success' if success else 'error',
        'script': script_name,
        'timestamp': datetime.now().isoformat()
    }