#!/usr/bin/env python3
"""
Script unifié pour découvrir automatiquement toutes les organisations LinkedIn
Version optimisée pour Google Cloud Functions (sans interaction)
"""

import os
import json
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Chargement des variables d'environnement
load_dotenv()

class LinkedInOrganizationDiscovery:
    """Classe pour découvrir automatiquement les organisations LinkedIn"""
    
    def __init__(self, access_token):
        """Initialise la découverte avec le token d'accès"""
        self.access_token = access_token.strip().strip("'").strip('"')
        self.base_url = "https://api.linkedin.com/rest"
        
        print("🔍 DÉCOUVERTE AUTOMATIQUE D'ORGANISATIONS LINKEDIN")
        print("="*50)
        
        # Test du token au démarrage
        if not self.test_token():
            raise ValueError("Token d'accès invalide")
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202505",
            "Content-Type": "application/json"
        }
    
    def test_token(self):
        """Teste la validité du token"""
        try:
            print("🔑 Test du token d'accès...")
            url = f"{self.base_url}/me"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                user_data = response.json()
                user_name = f"{user_data.get('localizedFirstName', 'N/A')} {user_data.get('localizedLastName', 'N/A')}"
                print(f"✅ Token valide pour: {user_name}")
                return True
            else:
                print(f"❌ Token invalide - Status: {response.status_code}")
                if response.status_code == 401:
                    print("   Le token a expiré ou est incorrect")
                return False
                
        except Exception as e:
            print(f"❌ Erreur lors du test du token: {e}")
            return False
    
    def get_organization_acls(self):
        """Récupère les permissions d'organisation"""
        print("\n📋 Récupération des permissions d'organisation...")
        
        try:
            url = f"{self.base_url}/organizationAcls?q=roleAssignee"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                acls = data.get('elements', [])
                print(f"✅ {len(acls)} permission(s) trouvée(s)")
                return acls
            elif response.status_code == 403:
                print("⚠️  Accès refusé aux ACLs - Utilisation de la méthode alternative")
                return []
            else:
                print(f"❌ Erreur API: {response.status_code}")
                return []
                
        except Exception as e:
            print(f"❌ Erreur lors de la récupération: {e}")
            return []
    
    def get_organization_details(self, org_id):
        """Récupère les détails d'une organisation"""
        try:
            url = f"{self.base_url}/organizations/{org_id}"
            response = requests.get(url, headers=self.get_headers(), timeout=30)
            
            if response.status_code == 200:
                return response.json()
            else:
                return None
                
        except Exception:
            return None
    
    def extract_org_id_from_urn(self, urn):
        """Extrait l'ID d'organisation depuis l'URN"""
        if not urn:
            return None
        
        parts = urn.split(':')
        if len(parts) >= 4 and parts[2] == 'organization':
            return parts[3]
        
        return parts[-1] if parts else None
    
    def extract_organization_name(self, org_details):
        """Extrait le nom de l'organisation"""
        if not org_details:
            return "Sans nom"
        
        # Essayer différents formats
        if 'localizedName' in org_details:
            return org_details['localizedName']
        
        if 'name' in org_details:
            name_data = org_details['name']
            if isinstance(name_data, dict) and 'localized' in name_data:
                localized = name_data['localized']
                if localized:
                    return list(localized.values())[0]
            elif isinstance(name_data, str):
                return name_data
        
        return "Sans nom"
    
    def parse_organizations_from_acls(self, acls_data):
        """Parse les ACLs pour extraire les organisations (avec déduplication)"""
        organizations_dict = {}  # Utiliser un dict pour dédupliquer par ID
        
        if not acls_data:
            return []
        
        print(f"\n🔍 Analyse de {len(acls_data)} permission(s)...")
        
        for i, element in enumerate(acls_data, 1):
            try:
                role = element.get('role', 'UNKNOWN')
                state = element.get('state', 'UNKNOWN')
                org_urn = element.get('organization', '')
                
                # Filtrer les ACLs non approuvées
                if state != 'APPROVED':
                    continue
                
                # Extraire l'ID
                org_id = self.extract_org_id_from_urn(org_urn)
                if not org_id:
                    continue
                
                # Si on a déjà traité cette organisation, passer
                if org_id in organizations_dict:
                    continue
                
                # Récupérer les détails
                print(f"  📊 Récupération des détails pour l'organisation {org_id}...")
                org_details = self.get_organization_details(org_id)
                org_name = self.extract_organization_name(org_details)
                
                vanity_name = org_details.get('vanityName', '') if org_details else ''
                
                org_info = {
                    'id': str(org_id),
                    'name': org_name,
                    'vanity_name': vanity_name
                }
                
                organizations_dict[org_id] = org_info
                print(f"     ✅ {org_name} (ID: {org_id})")
                
            except Exception as e:
                print(f"     ❌ Erreur: {e}")
                continue
        
        # Convertir le dictionnaire en liste
        organizations = list(organizations_dict.values())
        return organizations
    
    def discover_organizations_fallback(self):
        """Méthode alternative si les ACLs ne fonctionnent pas"""
        print("\n🔄 Méthode alternative: test d'organisations connues...")
        
        # IDs d'organisations connus depuis vos logs
        known_org_ids = [
            '105209218',  # C4E Mamirolle
            '51699835',   # RSP Intelligence collective
            '26838413',   # REI INDUSTRY
            '92916610',   # PS Intelligence
            '77047847',   # Maison Pernet
            '26920700',   # CONVERGENCE PATRIMOINE
            '82576459',   # Humtastic™
            '107401447',  # Le Défi du Réemploi Industriel
            '18399284',   # Le Labo Food
            '34608734',   # KR DRIVING EXPERIENCE
            '105870018',  # Valdheve
        ]
        
        organizations = []
        
        for org_id in known_org_ids:
            print(f"  🧪 Test de l'organisation {org_id}...")
            org_details = self.get_organization_details(org_id)
            
            if org_details:
                org_name = self.extract_organization_name(org_details)
                vanity_name = org_details.get('vanityName', '')
                
                org_info = {
                    'id': str(org_id),
                    'name': org_name,
                    'vanity_name': vanity_name
                }
                
                organizations.append(org_info)
                print(f"     ✅ {org_name}")
            else:
                print(f"     ❌ Pas d'accès")
        
        return organizations
    
    def discover_all_organizations(self):
        """Découvre toutes les organisations accessibles"""
        # Méthode principale: ACLs
        acls_data = self.get_organization_acls()
        organizations = self.parse_organizations_from_acls(acls_data)
        
        # Si échec, méthode alternative
        if not organizations:
            organizations = self.discover_organizations_fallback()
        
        if organizations:
            organizations.sort(key=lambda x: x['name'])
            print(f"\n✅ {len(organizations)} organisation(s) découverte(s)")
        else:
            print("\n❌ Aucune organisation trouvée")
        
        return organizations
    
    def save_to_config_file(self, organizations, filename='organizations_config.json'):
        """Sauvegarde les organisations dans le fichier de configuration"""
        if not organizations:
            print("❌ Aucune organisation à sauvegarder")
            return False
        
        # Trier par nom pour un fichier plus lisible
        organizations.sort(key=lambda x: x['name'])
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(organizations, f, indent=2, ensure_ascii=False)
            
            print(f"\n💾 Configuration sauvegardée dans '{filename}'")
            print(f"📊 {len(organizations)} organisations uniques sauvegardées")
            
            # Afficher la liste finale
            print("\n📋 Organisations dans le fichier de configuration:")
            for i, org in enumerate(organizations, 1):
                vanity_info = f" - {org['vanity_name']}" if org.get('vanity_name') else ""
                print(f"  {i}. {org['name']} (ID: {org['id']}){vanity_info}")
            
            return True
            
        except Exception as e:
            print(f"❌ Erreur lors de la sauvegarde: {e}")
            return False


def main():
    """Fonction principale - Version automatique sans interaction"""
    try:
        # Mode automatique activé
        print("🤖 MODE AUTOMATIQUE ACTIVÉ - Aucune interaction requise")
        print("")
        
        # Récupération du token
        access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
        
        if not access_token:
            print("❌ LINKEDIN_ACCESS_TOKEN manquant dans les variables d'environnement")
            return False
        
        # Découverte
        discovery = LinkedInOrganizationDiscovery(access_token)
        organizations = discovery.discover_all_organizations()
        
        if organizations:
            # Affichage du résumé
            print("\n" + "="*50)
            print("📊 ORGANISATIONS DÉCOUVERTES")
            print("="*50)
            
            for i, org in enumerate(organizations, 1):
                print(f"\n{i}. {org['name']}")
                print(f"   ID: {org['id']}")
                if org.get('vanity_name'):
                    print(f"   URL: https://linkedin.com/company/{org['vanity_name']}")
            
            # Sauvegarde automatique sans demander confirmation
            success = discovery.save_to_config_file(organizations)
            
            if success:
                print(f"\n🎉 Terminé! Fichier organizations_config.json créé/mis à jour")
                print("📁 Le fichier est maintenant prêt pour vos scripts d'automatisation")
                
                # Créer aussi un fichier de résumé pour Google Cloud
                summary = {
                    'discovery_date': datetime.now().isoformat(),
                    'total_organizations': len(organizations),
                    'organizations': organizations
                }
                
                with open('discovery_summary.json', 'w', encoding='utf-8') as f:
                    json.dump(summary, f, indent=2, ensure_ascii=False)
                
            return success
        else:
            print("\n❌ Aucune organisation trouvée")
            return False
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False


if __name__ == "__main__":
    # En mode automatique, on sort avec le bon code
    success = main()
    exit(0 if success else 1)