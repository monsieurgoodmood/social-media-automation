#!/usr/bin/env python3
# streamlit_app.py
# ================
# 📱 POINT D'ENTRÉE STREAMLIT - Alias vers app/main_enhanced.py
# Pour faciliter le déploiement et la compatibilité

import sys
import os
from pathlib import Path

# Ajouter le dossier racine au path pour les imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    """Lance l'application Streamlit principale"""
    
    # Vérifier que le fichier principal existe
    main_enhanced_path = project_root / "app" / "main_enhanced.py"
    
    if not main_enhanced_path.exists():
        print("❌ Erreur: app/main_enhanced.py non trouvé")
        print("👉 Assurez-vous que le fichier existe")
        sys.exit(1)
    
    try:
        # Importer et lancer l'application principale
        print("🚀 Lancement WhatsTheData (Streamlit)")
        print("📁 Fichier principal: app/main_enhanced.py")
        
        # Import dynamique de l'application principale
        from app.main_enhanced import main as main_app
        
        # Lancer l'application
        main_app()
        
    except ImportError as e:
        print(f"❌ Erreur d'import: {e}")
        print("💡 Suggestions:")
        print("   • Vérifiez que tous les modules sont installés: pip install -r requirements.txt")
        print("   • Vérifiez la configuration: python test_database.py")
        print("   • Lancez depuis la racine du projet")
        sys.exit(1)
        
    except Exception as e:
        print(f"❌ Erreur lors du lancement: {e}")
        print("💡 Essayez: python start_services.py")
        sys.exit(1)

if __name__ == "__main__":
    main()