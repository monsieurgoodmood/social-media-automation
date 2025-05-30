# test_execution.py
#!/usr/bin/env python3
"""Test d'exécution dans Cloud Functions"""

import os
import sys
import subprocess
import json
from datetime import datetime

def test_script_execution():
    """Teste l'exécution du script daily_statistics"""
    
    print("=== TEST D'EXÉCUTION ===")
    print(f"Répertoire actuel: {os.getcwd()}")
    print(f"Contenu du répertoire /workspace:")
    
    try:
        files = os.listdir('/workspace')
        for f in files:
            print(f"  - {f}")
    except Exception as e:
        print(f"Erreur listdir: {e}")
    
    # Vérifier que le script existe
    script_path = '/workspace/linkedin_multi_org_tracker.py'
    print(f"\nVérification de {script_path}:")
    print(f"  Existe: {os.path.exists(script_path)}")
    
    if os.path.exists(script_path):
        print(f"  Taille: {os.path.getsize(script_path)} octets")
        print(f"  Permissions: {oct(os.stat(script_path).st_mode)}")
    
    # Essayer d'exécuter le script
    print("\nTentative d'exécution du script...")
    
    env = os.environ.copy()
    env['AUTOMATED_MODE'] = 'true'
    
    try:
        # Méthode 1: subprocess
        print("Méthode 1: subprocess.run")
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            env=env,
            timeout=30  # 30 secondes max pour le test
        )
        
        print(f"Code retour: {result.returncode}")
        print(f"STDOUT (100 premiers caractères): {result.stdout[:100]}")
        print(f"STDERR (100 premiers caractères): {result.stderr[:100]}")
        
    except subprocess.TimeoutExpired:
        print("TIMEOUT après 30 secondes")
    except Exception as e:
        print(f"Erreur subprocess: {e}")
        
    # Méthode 2: import direct
    print("\nMéthode 2: import direct")
    try:
        sys.path.insert(0, '/workspace')
        import linkedin_multi_org_tracker
        print("Import réussi!")
    except Exception as e:
        print(f"Erreur import: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_script_execution()