#!/usr/bin/env python3
# streamlit_app.py
# ================
# 📱 POINT D'ENTRÉE STREAMLIT SIMPLIFIÉ
# Lance l'interface de test robuste

import sys
import os
from pathlib import Path

# Configuration du path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """Lance l'interface Streamlit de test"""
    
    # Vérifier que le fichier de test existe
    test_interface_path = project_root / "app" / "test_interface.py"
    
    if not test_interface_path.exists():
        print("❌ Erreur: app/test_interface.py non trouvé")
        print("👉 Créez d'abord le fichier app/test_interface.py")
        sys.exit(1)
    
    try:
        print("🚀 Lancement WhatsTheData (Interface de Test)")
        print("📁 Fichier: app/test_interface.py")
        print("🌐 URL: http://localhost:8501")
        print()
        
        # Import de l'interface de test
        from app.test_interface import main as test_app
        
        # Lancer l'application
        test_app()
        
    except ImportError as e:
        print(f"❌ Erreur d'import: {e}")
        print("💡 Solutions possibles:")
        print("   • pip install -r requirements.txt")
        print("   • Vérifiez la structure des dossiers")
        print("   • python quick_start.py pour diagnostic complet")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Erreur lors du lancement: {e}")
        print("💡 Essayez:")
        print("   • python quick_start.py")
        print("   • python start_services.py")
        sys.exit(1)

if __name__ == "__main__":
    main()