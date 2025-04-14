#!/usr/bin/env python3
"""
Module de gestion de l'authentification LinkedIn
"""

import os
import sys
import time
import traceback
import requests
from dotenv import load_dotenv, set_key, find_dotenv

class LinkedInAuthManager:
    def __init__(self):
        """Gestionnaire d'authentification LinkedIn avec renouvellement automatique des tokens"""
        self.env_path = find_dotenv()
        if not self.env_path:
            print("ERREUR: Fichier .env introuvable.")
            sys.exit(1)
            
        # Charger les configurations
        load_dotenv(self.env_path)
        self.client_id = os.getenv('LINKEDIN_CLIENT_ID')
        self.client_secret = os.getenv('LINKEDIN_CLIENT_SECRET')
        self.redirect_uri = os.getenv('LINKEDIN_REDIRECT_URI', 'http://localhost:8080/')
        self.access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.refresh_token = os.getenv('LINKEDIN_REFRESH_TOKEN')
        self.token_expiry = os.getenv('LINKEDIN_TOKEN_EXPIRY')
        
    def get_valid_access_token(self):
        """Récupère un token d'accès valide, en le renouvelant si nécessaire"""
        # Vérifier si le token actuel est valide
        if self.access_token and self.is_token_valid(self.access_token):
            return self.access_token
            
        # Si nous avons un refresh token, essayer de renouveler le token
        if self.refresh_token:
            try:
                self._refresh_access_token()
                if self.access_token and self.is_token_valid(self.access_token):
                    return self.access_token
            except Exception as e:
                print(f"Erreur lors du renouvellement du token: {e}")
        
        # Si nous n'avons toujours pas de token valide, demander une authentification manuelle
        self._prompt_for_manual_authentication()
        return self.access_token
    
    def is_token_valid(self, token):
        """Vérifie si un token d'accès est valide"""
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Restli-Protocol-Version": "2.0.0"
        }
        
        try:
            response = requests.get("https://api.linkedin.com/v2/me", headers=headers)
            return response.status_code == 200
        except Exception:
            return False
    
    def _refresh_access_token(self):
        """Utilise le refresh token pour obtenir un nouveau token d'accès"""
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        response = requests.post(url, data=payload, headers=headers)
        
        if response.status_code == 200:
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            
            # Certaines implémentations renvoient un nouveau refresh token
            if "refresh_token" in token_data:
                self.refresh_token = token_data.get("refresh_token")
            
            # Calculer l'expiration
            expires_in = token_data.get("expires_in", 3600)
            self.token_expiry = str(time.time() + expires_in)
            
            # Sauvegarder dans le fichier .env
            self._save_tokens_to_env()
            print("Token d'accès renouvelé avec succès")
        else:
            error_msg = f"Échec du renouvellement du token: {response.status_code} - {response.text}"
            print(error_msg)
            raise Exception(error_msg)
    
    def _prompt_for_manual_authentication(self):
        """Guide l'utilisateur à travers le processus d'authentification manuelle"""
        # Nous définissons l'URL complète directement ici pour éviter tout problème de formatage
        hardcoded_auth_url = "https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=77ni0sserlveku&redirect_uri=http://localhost:8080/&scope=r_organization_followers%20r_organization_social%20rw_organization_admin%20r_organization_social_feed%20w_member_social%20w_organization_social%20r_basicprofile%20w_organization_social_feed%20w_member_social_feed%20r_1st_connections_size"
        
        # Message d'authentification
        print("\n" + "="*80)
        print("AUTHENTIFICATION LINKEDIN REQUISE")
        print("="*80)
        print("\n1. Copiez et collez l'URL suivante dans votre navigateur pour vous authentifier:")
        print("\nhttps://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id=77ni0sserlveku&redirect_uri=http://localhost:8080/&scope=r_organization_followers%20r_organization_social%20rw_organization_admin%20r_organization_social_feed%20w_member_social%20w_organization_social%20r_basicprofile%20w_organization_social_feed%20w_member_social_feed%20r_1st_connections_size\n")
        print("2. Après avoir autorisé l'application, vous serez redirigé vers une URL (qui peut ne pas s'ouvrir).")
        print("3. Copiez le paramètre 'code' de cette URL (après '?code=' et avant tout '&').")
        
        auth_code = input("\nEntrez le code d'autorisation: ").strip()
        
        if auth_code:
            self._exchange_code_for_tokens(auth_code)
        else:
            print("Aucun code fourni. Impossible de continuer sans authentification.")
            sys.exit(1)
    
    def _exchange_code_for_tokens(self, auth_code):
        """Échange le code d'autorisation contre des tokens d'accès et de rafraîchissement"""
        url = "https://www.linkedin.com/oauth/v2/accessToken"
        
        # Afficher les informations de débogage
        print(f"\nDébogage - Client ID: {self.client_id}")
        print(f"Débogage - Redirect URI: {self.redirect_uri}")
        
        # Vérifier si l'ID client contient des caractères problématiques
        if '"' in self.client_id or '\\' in self.client_id:
            print("ATTENTION: L'ID client contient des caractères spéciaux qui peuvent causer des problèmes")
            # Nettoyage de l'ID client (suppression des guillemets et backslashes)
            clean_client_id = self.client_id.replace('"', '').replace('\\', '')
            print(f"ID client nettoyé: {clean_client_id}")
            self.client_id = clean_client_id
        
        payload = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        
        try:
            print("\nTentative d'échange du code d'autorisation...")
            response = requests.post(url, data=payload, headers=headers)
            
            print(f"Statut de la réponse: {response.status_code}")
            
            if response.status_code == 200:
                token_data = response.json()
                
                self.access_token = token_data.get("access_token")
                self.refresh_token = token_data.get("refresh_token")
                
                expires_in = token_data.get("expires_in", 3600)
                self.token_expiry = str(time.time() + expires_in)
                
                # Sauvegarder dans le fichier .env
                self._save_tokens_to_env()
                print("Authentification réussie!")
            else:
                print(f"Échec de l'échange du code: {response.status_code} - {response.text}")
                
                # Solution de contournement avec hardcoding des valeurs (en dernier recours)
                if response.status_code == 400 and "invalid_client_id" in response.text:
                    print("\nTentative avec l'ID client hardcodé...")
                    payload["client_id"] = "77ni0sserlveku"  # Utiliser l'ID client hardcodé
                    response = requests.post(url, data=payload, headers=headers)
                    
                    if response.status_code == 200:
                        token_data = response.json()
                        
                        self.access_token = token_data.get("access_token")
                        self.refresh_token = token_data.get("refresh_token")
                        
                        expires_in = token_data.get("expires_in", 3600)
                        self.token_expiry = str(time.time() + expires_in)
                        
                        # Sauvegarder dans le fichier .env avec l'ID client corrigé
                        set_key(self.env_path, "LINKEDIN_CLIENT_ID", "77ni0sserlveku")
                        self._save_tokens_to_env()
                        print("Authentification réussie avec l'ID client hardcodé!")
                    else:
                        print(f"Échec de la deuxième tentative: {response.status_code} - {response.text}")
                        sys.exit(1)
                else:
                    sys.exit(1)
                    
        except Exception as e:
            print(f"Erreur lors de l'échange du code: {e}")
            traceback.print_exc()
            sys.exit(1)
        
    def _save_tokens_to_env(self):
        """Sauvegarde les tokens dans le fichier .env"""
        set_key(self.env_path, "LINKEDIN_ACCESS_TOKEN", self.access_token)
        set_key(self.env_path, "LINKEDIN_REFRESH_TOKEN", self.refresh_token or "")
        set_key(self.env_path, "LINKEDIN_TOKEN_EXPIRY", self.token_expiry or "")
        
        # Recharger les variables d'environnement
        load_dotenv(self.env_path, override=True)