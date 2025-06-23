"""
Gestionnaire de configuration hybride pour Facebook
Fonctionne à la fois en local et sur Google Cloud Functions
"""
import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class ConfigManager:
    def __init__(self):
        self.is_cloud = os.getenv("K_SERVICE") is not None  # Variable d'env présente sur Cloud Functions
        self.bucket_name = os.getenv("GCP_BUCKET_NAME", "authentic-ether-457013-t5-facebook-configs")
        
        if self.is_cloud:
            # Mode Cloud : utiliser GCS
            from google.cloud import storage
            from google.cloud.exceptions import NotFound
            self.storage_client = storage.Client()
            self.bucket = self.storage_client.bucket(self.bucket_name)
            self.NotFound = NotFound
            logger.info("ConfigManager en mode CLOUD (GCS)")
        else:
            # Mode Local : utiliser les fichiers locaux
            self.storage_client = None
            self.bucket = None
            logger.info("ConfigManager en mode LOCAL")
        
        # Cache pour éviter les lectures répétées
        self._cache = {}
    
    def load_config(self, filename):
        """Charge une configuration depuis GCS ou local selon l'environnement"""
        # Vérifier le cache d'abord
        if filename in self._cache:
            logger.debug(f"Configuration {filename} chargée depuis le cache")
            return self._cache[filename]
        
        if self.is_cloud:
            return self._load_from_gcs(filename)
        else:
            return self._load_from_local(filename)
    
    def _load_from_gcs(self, filename):
        """Charge depuis Google Cloud Storage"""
        try:
            blob = self.bucket.blob(f"configs/{filename}")
            
            if not blob.exists():
                logger.warning(f"Le fichier {filename} n'existe pas dans GCS")
                
                # Pour les fichiers de mapping, essayer de charger depuis local (déployés avec la fonction)
                if filename in ["page_metrics_mapping.json", "posts_lifetime_mapping.json", "posts_metadata_mapping.json"]:
                    local_path = f"configs/{filename}"
                    if os.path.exists(local_path):
                        logger.info(f"Chargement du mapping depuis le fichier déployé: {local_path}")
                        with open(local_path, 'r') as f:
                            data = json.load(f)
                            self._cache[filename] = data
                            return data
                
                return None
            
            # Télécharger et parser
            content = blob.download_as_text()
            data = json.loads(content)
            
            # Mettre en cache
            self._cache[filename] = data
            
            logger.info(f"Configuration {filename} chargée depuis GCS")
            return data
            
        except self.NotFound:
            logger.warning(f"Fichier {filename} non trouvé dans GCS")
            return None
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {filename} depuis GCS: {e}")
            return None
    
    def _load_from_local(self, filename):
        """Charge depuis le système de fichiers local"""
        try:
            path = f"configs/{filename}"
            if not os.path.exists(path):
                logger.warning(f"Le fichier {path} n'existe pas localement")
                return None
            
            with open(path, 'r') as f:
                data = json.load(f)
            
            # Mettre en cache
            self._cache[filename] = data
            
            logger.info(f"Configuration {filename} chargée depuis le disque local")
            return data
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement de {filename} en local: {e}")
            return None
    
    def save_config(self, filename, data):
        """Sauvegarde une configuration dans GCS ou local selon l'environnement"""
        # Mettre à jour le cache
        self._cache[filename] = data
        
        # Ajouter des métadonnées
        if isinstance(data, dict):
            data["_last_updated"] = datetime.now().isoformat()
            data["_environment"] = "cloud" if self.is_cloud else "local"
        
        if self.is_cloud:
            return self._save_to_gcs(filename, data)
        else:
            return self._save_to_local(filename, data)
    
    def _save_to_gcs(self, filename, data):
        """Sauvegarde dans Google Cloud Storage"""
        try:
            blob = self.bucket.blob(f"configs/{filename}")
            
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type="application/json"
            )
            
            logger.info(f"Configuration {filename} sauvegardée dans GCS")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde de {filename} dans GCS: {e}")
            return False
    
    def _save_to_local(self, filename, data):
        """Sauvegarde dans le système de fichiers local"""
        try:
            path = f"configs/{filename}"
            
            # Créer le dossier si nécessaire
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # Sauvegarder
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Configuration {filename} sauvegardée localement")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde de {filename} en local: {e}")
            return False
    
    def save_report(self, report_name, data):
        """Sauvegarde un rapport dans GCS ou local"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{report_name}_{timestamp}.json"
        
        if self.is_cloud:
            return self._save_report_to_gcs(filename, data, report_name)
        else:
            return self._save_report_to_local(filename, data, report_name)
    
    def _save_report_to_gcs(self, filename, data, report_name):
        """Sauvegarde un rapport dans GCS"""
        try:
            # Version timestampée
            blob = self.bucket.blob(f"reports/{filename}")
            blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type="application/json"
            )
            
            # Version "latest"
            latest_blob = self.bucket.blob(f"reports/{report_name}_latest.json")
            latest_blob.upload_from_string(
                json.dumps(data, indent=2),
                content_type="application/json"
            )
            
            logger.info(f"Rapport {report_name} sauvegardé dans GCS")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport {report_name} dans GCS: {e}")
            return False
    
    def _save_report_to_local(self, filename, data, report_name):
        """Sauvegarde un rapport localement"""
        try:
            # Créer le dossier reports si nécessaire
            os.makedirs("reports", exist_ok=True)
            
            # Version timestampée
            path = f"reports/{filename}"
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Version "latest"
            latest_path = f"reports/{report_name}_latest.json"
            with open(latest_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Rapport {report_name} sauvegardé localement")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport {report_name} en local: {e}")
            return False
    
    def ensure_bucket_exists(self):
        """S'assure que le bucket existe (cloud uniquement)"""
        if not self.is_cloud:
            return True  # En local, pas besoin de bucket
        
        try:
            if not self.bucket.exists():
                logger.info(f"Création du bucket {self.bucket_name}")
                self.bucket.create(location="europe-west1")
            
            # Créer les dossiers nécessaires
            folders = ["configs/", "reports/", "temp/"]
            for folder in folders:
                blob = self.bucket.blob(folder)
                if not blob.exists():
                    blob.upload_from_string("")
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la vérification du bucket: {e}")
            return False
    
    def list_configs(self):
        """Liste toutes les configurations disponibles"""
        if self.is_cloud:
            return self._list_configs_gcs()
        else:
            return self._list_configs_local()
    
    def _list_configs_gcs(self):
        """Liste les configs dans GCS"""
        try:
            configs = []
            for blob in self.bucket.list_blobs(prefix="configs/"):
                if blob.name.endswith(".json"):
                    configs.append(blob.name.replace("configs/", ""))
            return configs
        except Exception as e:
            logger.error(f"Erreur lors du listing des configs GCS: {e}")
            return []
    
    def _list_configs_local(self):
        """Liste les configs locales"""
        try:
            if not os.path.exists("configs"):
                return []
            
            configs = []
            for file in os.listdir("configs"):
                if file.endswith(".json"):
                    configs.append(file)
            return configs
        except Exception as e:
            logger.error(f"Erreur lors du listing des configs locales: {e}")
            return []