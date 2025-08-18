#!/usr/bin/env python3
# start_services.py
# =================
# 🚀 SCRIPT DE LANCEMENT - Démarre FastAPI + Streamlit ensemble

import subprocess
import sys
import time
import threading
import signal
import os
from pathlib import Path

# Configuration
FASTAPI_PORT = 8000
STREAMLIT_PORT = 8501
PROJECT_ROOT = Path(__file__).parent

class ServiceManager:
    def __init__(self):
        self.processes = []
        self.running = True
    
    def start_fastapi(self):
        """Démarre le serveur FastAPI"""
        print("🚀 Démarrage FastAPI (port 8000)...")
        
        try:
            process = subprocess.Popen([
                sys.executable, "-m", "uvicorn",
                "app.main:app",
                "--host", "0.0.0.0",
                "--port", str(FASTAPI_PORT),
                "--reload",
                "--log-level", "info"
            ], cwd=PROJECT_ROOT)
            
            self.processes.append(("FastAPI", process))
            print("✅ FastAPI démarré")
            return process
            
        except Exception as e:
            print(f"❌ Erreur démarrage FastAPI: {e}")
            return None
    
    def start_streamlit(self):
        """Démarre l'application Streamlit"""
        print("🎨 Démarrage Streamlit (port 8501)...")
        
        # Déterminer le fichier Streamlit à lancer
        streamlit_file = "streamlit_app.py"
        if (PROJECT_ROOT / "app" / "main_enhanced.py").exists():
            streamlit_file = "app/main_enhanced.py"
        elif (PROJECT_ROOT / "streamlit_app.py").exists():
            streamlit_file = "streamlit_app.py"
        else:
            print("❌ Fichier Streamlit non trouvé")
            return None
        
        try:
            process = subprocess.Popen([
                sys.executable, "-m", "streamlit", "run",
                streamlit_file,
                "--server.port", str(STREAMLIT_PORT),
                "--server.headless", "false",
                "--browser.gatherUsageStats", "false"
            ], cwd=PROJECT_ROOT)
            
            self.processes.append(("Streamlit", process))
            print("✅ Streamlit démarré")
            return process
            
        except Exception as e:
            print(f"❌ Erreur démarrage Streamlit: {e}")
            return None
    
    def check_dependencies(self):
        """Vérifie que les dépendances sont installées"""
        print("🔍 Vérification des dépendances...")
        
        required_packages = [
            "fastapi", "uvicorn", "streamlit", "sqlalchemy", 
            "psycopg2-binary", "python-dotenv"
        ]
        
        missing = []
        for package in required_packages:
            try:
                __import__(package.replace("-", "_"))
            except ImportError:
                missing.append(package)
        
        if missing:
            print(f"❌ Packages manquants: {', '.join(missing)}")
            print("👉 Installez avec: pip install " + " ".join(missing))
            return False
        
        print("✅ Toutes les dépendances sont installées")
        return True
    
    def check_environment(self):
        """Vérifie la configuration d'environnement"""
        print("🔧 Vérification de l'environnement...")
        
        env_file = PROJECT_ROOT / ".env"
        if not env_file.exists():
            print("❌ Fichier .env manquant")
            print("👉 Copiez .env.example vers .env et configurez vos variables")
            return False
        
        # Vérifier quelques variables critiques
        from dotenv import load_dotenv
        load_dotenv()
        
        critical_vars = ["DATABASE_URL", "SECRET_KEY"]
        missing_vars = []
        
        for var in critical_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            print(f"❌ Variables d'environnement manquantes: {', '.join(missing_vars)}")
            return False
        
        print("✅ Configuration environnement OK")
        return True
    
    def test_database_connection(self):
        """Test rapide de connexion base de données"""
        print("🗄️ Test de connexion base de données...")
        
        try:
            from app.database.connection import test_database_connection
            if test_database_connection():
                print("✅ Base de données connectée")
                return True
            else:
                print("❌ Connexion base de données échouée")
                return False
        except Exception as e:
            print(f"❌ Erreur test base de données: {e}")
            return False
    
    def wait_for_services(self):
        """Attend que les services soient prêts"""
        import requests
        
        print("⏳ Attente que les services soient prêts...")
        
        # Attendre FastAPI
        for i in range(30):
            try:
                response = requests.get(f"http://localhost:{FASTAPI_PORT}/health", timeout=1)
                if response.status_code == 200:
                    print("✅ FastAPI prêt")
                    break
            except:
                pass
            time.sleep(1)
        else:
            print("⚠️ FastAPI semble lent à démarrer")
        
        # Attendre Streamlit (plus délicat à tester)
        time.sleep(3)
        print("✅ Streamlit devrait être prêt")
    
    def show_urls(self):
        """Affiche les URLs d'accès"""
        print("\n" + "="*60)
        print("🎉 WHATSTHEDATA DÉMARRÉ AVEC SUCCÈS!")
        print("="*60)
        print(f"🌐 API FastAPI:           http://localhost:{FASTAPI_PORT}")
        print(f"📚 Documentation API:     http://localhost:{FASTAPI_PORT}/docs")
        print(f"❤️ Health Check:          http://localhost:{FASTAPI_PORT}/health")
        print(f"📊 Interface Streamlit:   http://localhost:{STREAMLIT_PORT}")
        print("="*60)
        print("🔗 CONNECTEUR LOOKER STUDIO:")
        print(f"   Endpoint principal:    http://localhost:{FASTAPI_PORT}/api/v1/looker-data")
        print(f"   Validation token:      http://localhost:{FASTAPI_PORT}/api/v1/validate-token")
        print("="*60)
        print("💡 CONSEILS:")
        print("   • Utilisez Streamlit pour l'interface utilisateur")
        print("   • L'API FastAPI gère Looker Studio et les webhooks")
        print("   • Ctrl+C pour arrêter tous les services")
        print("="*60)
    
    def monitor_processes(self):
        """Monitor les processus en cours"""
        while self.running:
            for name, process in self.processes:
                if process.poll() is not None:
                    print(f"⚠️ {name} s'est arrêté (code: {process.returncode})")
            time.sleep(5)
    
    def stop_all(self):
        """Arrête tous les services"""
        print("\n🛑 Arrêt des services...")
        self.running = False
        
        for name, process in self.processes:
            try:
                print(f"🛑 Arrêt {name}...")
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} arrêté")
            except subprocess.TimeoutExpired:
                print(f"⚠️ Force kill {name}...")
                process.kill()
            except Exception as e:
                print(f"❌ Erreur arrêt {name}: {e}")
        
        print("✅ Tous les services sont arrêtés")
    
    def run(self):
        """Lance tous les services"""
        print("🚀 WHATSTHEDATA - DÉMARRAGE DES SERVICES")
        print("="*50)
        
        # Vérifications préalables
        if not self.check_dependencies():
            return False
        
        if not self.check_environment():
            return False
        
        if not self.test_database_connection():
            print("⚠️ Problème base de données, continuons quand même...")
        
        # Démarrage des services
        fastapi_process = self.start_fastapi()
        if not fastapi_process:
            return False
        
        # Attendre un peu avant Streamlit
        time.sleep(2)
        
        streamlit_process = self.start_streamlit()
        if not streamlit_process:
            return False
        
        # Attendre que les services soient prêts
        self.wait_for_services()
        
        # Afficher les informations
        self.show_urls()
        
        # Démarrer le monitoring
        monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
        monitor_thread.start()
        
        return True

def signal_handler(sig, frame):
    """Gestionnaire de signal pour arrêt propre"""
    print("\n🛑 Signal d'arrêt reçu...")
    manager.stop_all()
    sys.exit(0)

def main():
    global manager
    manager = ServiceManager()
    
    # Gestionnaire de signaux pour arrêt propre
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        success = manager.run()
        
        if success:
            print("\n⏳ Services en cours d'exécution... (Ctrl+C pour arrêter)")
            
            # Boucle principale
            while manager.running:
                time.sleep(1)
        else:
            print("❌ Échec du démarrage")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Interruption clavier détectée")
        manager.stop_all()
    except Exception as e:
        print(f"❌ Erreur critique: {e}")
        manager.stop_all()
        sys.exit(1)

if __name__ == "__main__":
    main()