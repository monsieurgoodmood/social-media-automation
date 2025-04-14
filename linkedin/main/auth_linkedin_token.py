import requests
import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv, set_key

class LinkedInAuthManager:
    def __init__(self, env_file_path='.env'):
        """
        Gestionnaire d'authentification LinkedIn qui gère le renouvellement automatique des tokens
        
        Args:
            env_file_path: Chemin vers le fichier .env
        """
        self.env_file_path = os.path.abspath(env_file_path)
        load_dotenv(self.env_file_path)
        
        # Récupérer les configurations depuis le fichier .env
        self.client_id = os.getenv('LINKEDIN_CLIENT_ID')
        self.client_secret = os.getenv('LINKEDIN_CLIENT_SECRET')
        self.redirect_uri = os.getenv('LINKEDIN_REDIRECT_URI', 'http://localhost:8000/callback')
        self.access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.refresh_token = os.getenv('LINKEDIN_REFRESH_TOKEN')
        self.token_expiry = os.getenv('LINKEDIN_TOKEN_EXPIRY')
        
        # Vérifier les configurations requises
        if not self.client_id or not self.client_secret:
            raise Exception("Les identifiants client LinkedIn (LINKEDIN_CLIENT_ID et LINKEDIN_CLIENT_SECRET) sont requis dans le fichier .env")
        
        # Fichier pour sauvegarder les données d'authentification
        self.auth_file = 'linkedin_auth.json'

    def get_valid_access_token(self):
        """
        Récupère un token d'accès valide, le renouvelle si nécessaire
        
        Returns:
            str: Token d'accès valide
        """
        # Si nous avons un refresh token, vérifier si le token d'accès est expiré
        if self.refresh_token and self.token_expiry:
            # Convertir l'horodatage en datetime
            expiry_time = datetime.fromtimestamp(float(self.token_expiry))
            
            # Si le token expire dans moins de 1 heure, le renouveler
            if expiry_time <= datetime.now() + timedelta(hours=1):
                print("Le token d'accès expire bientôt. Tentative de renouvellement...")
                self._refresh_access_token()
        
        # Si nous n'avons pas de token d'accès valide et pas de refresh token,
        # demander une authentification manuelle
        if not self.access_token:
            if not self.refresh_token:
                self._prompt_for_manual_authentication()
            else:
                self._refresh_access_token()
        
        return self.access_token

    def _refresh_access_token(self):
        """
        Utilise le refresh token pour obtenir un nouveau token d'accès
        """
        if not self.refresh_token:
            print("Aucun refresh token disponible. Impossible de renouveler automatiquement.")
            self._prompt_for_manual_authentication()
            return
        
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = requests.post(url, data=payload, headers=headers)
            
            if response.status_code == 200:
                token_data = response.json()
                
                # Mettre à jour les tokens
                self.access_token = token_data.get("access_token")
                
                # Certaines implémentations renvoient un nouveau refresh token
                if "refresh_token" in token_data:
                    self.refresh_token = token_data.get("refresh_token")
                
                # Calculer l'expiration
                expires_in = token_data.get("expires_in", 3600)  # Par défaut 1 heure
                self.token_expiry = str(time.time() + expires_in)
                
                # Sauvegarder dans le fichier .env
                self._save_tokens_to_env()
                
                print("Token d'accès renouvelé avec succès")
            else:
                print(f"Échec du renouvellement du token: {response.status_code} - {response.text}")
                
                # Si le refresh token est invalide, demander une nouvelle authentification
                if response.status_code == 400 and "invalid_grant" in response.text:
                    print("Le refresh token est invalide ou expiré.")
                    self.refresh_token = None
                    self._prompt_for_manual_authentication()
                
        except Exception as e:
            print(f"Erreur lors du renouvellement du token: {e}")

    def _prompt_for_manual_authentication(self):
        """
        Affiche les instructions pour l'authentification manuelle
        """
        auth_url = (
            f"https://www.linkedin.com/oauth/v2/authorization"
            f"?response_type=code"
            f"&client_id={self.client_id}"
            f"&redirect_uri={self.redirect_uri}"
            f"&scope=r_organization_followers%20r_organization_social%20rw_organization_admin%20r_organization_social_feed%20w_member_social%20w_organization_social%20r_basicprofile%20w_organization_social_feed%20w_member_social_feed%20r_1st_connections_size"
        )
        
        print("\n" + "="*80)
        print("AUTHENTIFICATION LINKEDIN REQUISE")
        print("="*80)
        print("\n1. Visitez l'URL suivante dans votre navigateur pour vous authentifier:")
        print(f"\n{auth_url}\n")
        print("2. Après avoir autorisé l'application, vous serez redirigé vers une URL.")
        print("3. Copiez le paramètre 'code' de cette URL (après '?code=' et avant tout '&').")
        
        auth_code = input("\nEntrez le code d'autorisation: ").strip()
        
        if auth_code:
            self._exchange_code_for_tokens(auth_code)
        else:
            print("Aucun code fourni. Impossible de continuer sans authentification.")
            exit(1)

    def _exchange_code_for_tokens(self, auth_code):
        """
        Échange le code d'autorisation contre des tokens d'accès et de rafraîchissement
        
        Args:
            auth_code: Code d'autorisation obtenu après authentification
        """
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        payload = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        try:
            response = requests.post(url, data=payload, headers=headers)
            
            if response.status_code == 200:
                token_data = response.json()
                
                # Mettre à jour les tokens
                self.access_token = token_data.get("access_token")
                self.refresh_token = token_data.get("refresh_token")
                
                # Calculer l'expiration
                expires_in = token_data.get("expires_in", 3600)  # Par défaut 1 heure
                self.token_expiry = str(time.time() + expires_in)
                
                # Sauvegarder dans le fichier .env
                self._save_tokens_to_env()
                
                print("Authentification réussie!")
            else:
                print(f"Échec de l'échange du code: {response.status_code} - {response.text}")
                exit(1)
                
        except Exception as e:
            print(f"Erreur lors de l'échange du code: {e}")
            exit(1)

    def _save_tokens_to_env(self):
        """
        Sauvegarde les tokens dans le fichier .env
        """
        # Mise à jour du fichier .env
        set_key(self.env_file_path, "LINKEDIN_ACCESS_TOKEN", self.access_token)
        set_key(self.env_file_path, "LINKEDIN_REFRESH_TOKEN", self.refresh_token or "")
        set_key(self.env_file_path, "LINKEDIN_TOKEN_EXPIRY", self.token_expiry or "")
        
        # Recharger les variables d'environnement
        load_dotenv(self.env_file_path, override=True)
        
        # Également sauvegarder dans un fichier JSON pour plus de détails
        try:
            auth_data = {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "token_expiry": self.token_expiry,
                "last_updated": datetime.now().isoformat()
            }
            
            with open(self.auth_file, 'w') as f:
                json.dump(auth_data, f, indent=2)
        except Exception as e:
            print(f"Note: Impossible de sauvegarder les détails d'authentification dans {self.auth_file}: {e}")

    def verify_token(self):
        """
        Vérifie si le token d'accès est valide
        
        Returns:
            bool: True si le token est valide, False sinon
        """
        # URL de test pour vérifier si le token est valide
        test_url = "https://api.linkedin.com/v2/me"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0"
        }
        
        try:
            response = requests.get(test_url, headers=headers)
            return response.status_code == 200
        except:
            return False