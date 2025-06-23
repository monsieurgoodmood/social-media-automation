"""
Gestionnaire de token Facebook pour Cloud Functions
Utilise Google Secret Manager pour TOUS les secrets
"""

import os
import json
import requests
import logging
from datetime import datetime, timedelta
from google.cloud import secretmanager
import time

logger = logging.getLogger(__name__)

class FacebookTokenManager:
    DEBUG_PAGE_TOKEN = False  # False = on ignore debug_token car token de PAGE permanent

    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID", "authentic-ether-457013-t5")
        self.client = secretmanager.SecretManagerServiceClient()
        
        # Récupérer les secrets depuis Secret Manager
        self._client_id = None
        self._client_secret = None
        self._load_app_credentials()
        
        # Cache du token
        self._token_cache = None
        self._cache_expiry = None
    
    def _load_app_credentials(self):
        """Charge les credentials de l'app depuis Secret Manager"""
        try:
            # Récupérer client_id
            client_id_secret = f"projects/{self.project_id}/secrets/facebook-client-id/versions/latest"
            response = self.client.access_secret_version(request={"name": client_id_secret})
            self._client_id = response.payload.data.decode("UTF-8").strip()
            logger.info("Client ID chargé depuis Secret Manager")
            
            # Récupérer client_secret
            client_secret_secret = f"projects/{self.project_id}/secrets/facebook-client-secret/versions/latest"
            response = self.client.access_secret_version(request={"name": client_secret_secret})
            self._client_secret = response.payload.data.decode("UTF-8").strip()
            logger.info("Client Secret chargé depuis Secret Manager")
            
        except Exception as e:
            logger.error(f"Erreur lors du chargement des credentials: {e}")
            # Fallback sur les variables d'environnement si Secret Manager échoue
            self._client_id = os.getenv("FB_CLIENT_ID", "")
            self._client_secret = os.getenv("FB_CLIENT_SECRET", "")
            
            if not self._client_id or not self._client_secret:
                raise Exception("Impossible de charger les credentials Facebook")
    
    @property
    def client_id(self):
        """Getter pour client_id"""
        if not self._client_id:
            self._load_app_credentials()
        return self._client_id
    
    @property
    def client_secret(self):
        """Getter pour client_secret"""
        if not self._client_secret:
            self._load_app_credentials()
        return self._client_secret
    
    def get_valid_token(self):
        """
        Récupère un token valide, avec renouvellement automatique si nécessaire
        C'est la méthode principale à utiliser
        """
        # Vérifier le cache d'abord
        if self._token_cache and self._cache_expiry and datetime.now() < self._cache_expiry:
            return self._token_cache
        
        # Récupérer le token depuis Secret Manager
        token = self.get_token_from_secret_manager()
        
        if not token:
            raise Exception("Aucun token valide trouvé dans Secret Manager")
        
        # Vérifier et rafraîchir si nécessaire
        token = self.ensure_token_validity(token)
        
        # Mettre en cache pour 5 minutes
        self._token_cache = token
        self._cache_expiry = datetime.now() + timedelta(minutes=5)
        
        return token
    
    def get_token_from_secret_manager(self):
        """Récupère le token depuis Secret Manager"""
        try:
            secret_name = f"projects/{self.project_id}/secrets/facebook-access-token/versions/latest"
            response = self.client.access_secret_version(request={"name": secret_name})
            
            # Décoder la payload
            payload = response.payload.data.decode("UTF-8")
            
            # Essayer de parser comme JSON
            try:
                token_data = json.loads(payload)
                # Priorité: access_token si présent, sinon token
                token = token_data.get("access_token") or token_data.get("token")
                
                # Vérifier l'expiration
                if "expiration" in token_data:
                    expiration = datetime.fromtimestamp(token_data["expiration"])
                    safety_margin = timedelta(hours=1)
                    if expiration - safety_margin > datetime.now():
                        logger.info(f"Token valide jusqu'au {expiration.strftime('%Y-%m-%d %H:%M:%S')}")
                        return token
                    else:
                        logger.warning(f"Token expire dans moins d'une heure ou est déjà expiré")
                        return token  # On le retourne quand même, ensure_token_validity décidera
                
                return token
                
            except json.JSONDecodeError:
                # C'est probablement un token simple (string)
                logger.info("Token récupéré comme string simple")
                return payload.strip()
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du token: {e}")
            return None
    
    def save_token_to_secret_manager(self, token, expires_in=5184000):
        """Sauvegarde le token dans Secret Manager"""
        try:
            # Préparer les données du token
            expiration = datetime.now() + timedelta(seconds=expires_in)
            token_data = {
                "token": token,
                "access_token": token,  # Pour compatibilité
                "expiration": expiration.timestamp(),
                "updated_at": datetime.now().isoformat(),
                "expires_in_days": expires_in // 86400,
                "source": "automatic_refresh"
            }
            
            # Créer une nouvelle version du secret
            parent = f"projects/{self.project_id}/secrets/facebook-access-token"
            
            response = self.client.add_secret_version(
                request={
                    "parent": parent,
                    "payload": {
                        "data": json.dumps(token_data, indent=2).encode("UTF-8")
                    }
                }
            )
            
            logger.info(f"Token sauvegardé dans Secret Manager (expire le {expiration.strftime('%Y-%m-%d %H:%M:%S')})")
            
            # Invalider le cache
            self._token_cache = None
            self._cache_expiry = None
            
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du token: {e}")
            return False
    
    def ensure_token_validity(self, token):
        """
        S'assure que le token est valide et le rafraîchit si nécessaire
        Pour l'instant, retourne le token tel quel (tokens de page permanents)
        """
        # Les tokens de page Facebook sont permanents, pas besoin de les rafraîchir
        return token
    
    def exchange_for_long_lived_token(self, short_token):
        """Échange un token court contre un token long terme"""
        url = "https://graph.facebook.com/v21.0/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "fb_exchange_token": short_token
        }
        
        try:
            logger.info("Échange du token contre un token longue durée...")
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'access_token' in data:
                expires_in = data.get('expires_in', 5184000)  # 60 jours par défaut
                self.save_token_to_secret_manager(data['access_token'], expires_in)
                logger.info(f"Token longue durée obtenu ({expires_in // 86400} jours)")
                return data['access_token']
            else:
                error_msg = data.get('error', {}).get('message', 'Erreur inconnue')
                logger.warning(f"Impossible d'obtenir un token longue durée : {error_msg}")
                return None
                
        except Exception as e:
            logger.error(f"Erreur lors de l'échange : {e}")
            return None
    
    def check_and_notify_token_status(self):
        """Vérifie le statut du token et envoie des notifications si nécessaire"""
        try:
            # Récupérer le token et ses métadonnées
            secret_name = f"projects/{self.project_id}/secrets/facebook-access-token/versions/latest"
            response = self.client.access_secret_version(request={"name": secret_name})
            payload = response.payload.data.decode("UTF-8")
            
            try:
                token_data = json.loads(payload)
                token = token_data.get("access_token") or token_data.get("token")
                
                if not token:
                    return {
                        "status": "missing",
                        "message": "Aucun token trouvé dans Secret Manager",
                        "days_left": 0,
                        "requires_action": True,
                        "urgent": True
                    }
                
                # Vérifier l'expiration
                if "expiration" in token_data:
                    expiration = datetime.fromtimestamp(token_data["expiration"])
                    days_left = (expiration - datetime.now()).days
                    hours_left = (expiration - datetime.now()).total_seconds() / 3600
                    
                    status = {
                        "status": "valid" if days_left > 0 else "expired",
                        "message": f"Token {'valide' if days_left > 0 else 'expiré'}, expire {'dans' if days_left > 0 else 'depuis'} {abs(days_left)} jours",
                        "days_left": days_left,
                        "hours_left": hours_left,
                        "expiration_date": expiration.strftime('%Y-%m-%d %H:%M:%S'),
                        "requires_action": days_left <= 7,
                        "urgent": days_left <= 3
                    }
                    
                    # Ajouter des infos supplémentaires
                    if "updated_at" in token_data:
                        status["last_updated"] = token_data["updated_at"]
                    if "source" in token_data:
                        status["source"] = token_data["source"]
                    
                    return status
                else:
                    # Token sans expiration (probablement un token de page permanent)
                    return {
                        "status": "valid",
                        "message": "Token valide (token de page permanent)",
                        "days_left": -1,
                        "requires_action": False,
                        "urgent": False,
                        "type": "page_token"
                    }
                    
            except json.JSONDecodeError:
                # Token simple sans métadonnées
                return {
                    "status": "valid",
                    "message": "Token valide (format simple)",
                    "days_left": -1,
                    "requires_action": False,
                    "urgent": False
                }
                
        except Exception as e:
            logger.error(f"Erreur lors de la vérification du statut: {e}")
            return {
                "status": "error",
                "message": f"Erreur: {str(e)}",
                "requires_action": True,
                "urgent": True
            }

# Fonction utilitaire pour utilisation simplifiée
def get_valid_facebook_token():
    """Fonction helper pour obtenir rapidement un token valide"""
    manager = FacebookTokenManager()
    return manager.get_valid_token()