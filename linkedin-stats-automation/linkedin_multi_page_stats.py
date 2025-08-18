#!/usr/bin/env python3
"""
LinkedIn Multi-Organization Follower Statistics Tracker
Ce script collecte les statistiques des followers LinkedIn par catégorie
pour plusieurs organisations et les enregistre dans Google Sheets.
"""

import os
import requests
import urllib.parse
import json
from datetime import datetime
import time
from pathlib import Path
import sys
from dotenv import load_dotenv
import random

# Pour Google Sheets
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

# Chargement des variables d'environnement
load_dotenv()

def ensure_percentage_as_decimal(value):
    """
    Convertit une valeur en décimal pour Google Sheets PERCENT
    
    Args:
        value: La valeur à convertir (peut être 5 pour 5% ou 0.05 pour 5%)
    
    Returns:
        float: Valeur en décimal (0.05 pour 5%)
    """
    if value is None:
        return 0.0
    
    if isinstance(value, str):
        # Enlever le symbole % si présent
        value = value.replace('%', '').strip()
        try:
            value = float(value)
        except:
            return 0.0
    
    if isinstance(value, (int, float)):
        # Si la valeur est > 1, on assume que c'est un pourcentage
        if value > 1:
            return float(value / 100)
        else:
            return float(value)
    
    return 0.0


def get_column_letter(col_idx):
    """Convertit un indice de colonne (0-based) en lettre de colonne pour Google Sheets (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    col_idx = col_idx + 1  # Convertir de 0-based à 1-based
    while col_idx > 0:
        col_idx, remainder = divmod(col_idx - 1, 26)
        result = chr(65 + remainder) + result
    return result

def safe_sheets_operation(operation, *args, max_retries=5, **kwargs):
    """
    Exécute une opération Google Sheets avec gestion des erreurs de quota
    """
    for attempt in range(max_retries):
        try:
            return operation(*args, **kwargs)
        except APIError as e:
            if '429' in str(e) or 'Quota exceeded' in str(e):
                # Calcul du délai d'attente avec backoff exponentiel + jitter
                base_delay = min(60, (2 ** attempt) * 5)  # Maximum 60 secondes
                jitter = random.uniform(0.5, 1.5)  # Ajouter du jitter pour éviter la synchronisation
                delay = base_delay * jitter
                
                print(f"   ⏳ Quota dépassé (tentative {attempt + 1}/{max_retries}), attente de {delay:.1f}s...")
                time.sleep(delay)
                
                if attempt == max_retries - 1:
                    print(f"   ❌ Échec après {max_retries} tentatives: {e}")
                    raise
            else:
                print(f"   ❌ Erreur API non liée au quota: {e}")
                raise
        except Exception as e:
            print(f"   ❌ Erreur inattendue: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)

class LinkedInFollowerStatisticsTracker:
    """Classe pour suivre les statistiques des followers LinkedIn par catégorie"""
    
    def __init__(self, access_token, organization_id, sheet_name=None):
        """Initialise le tracker avec le token d'accès et l'ID de l'organisation"""
        self.access_token = access_token
        self.organization_id = organization_id
        self.sheet_name = sheet_name or f"LinkedIn_Follower_Stats_{organization_id}"
        self.base_url = "https://api.linkedin.com/rest"
        
    def get_headers(self):
        """Retourne les en-têtes pour les requêtes API"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "LinkedIn-Version": "202505",
            "Content-Type": "application/json"
        }
    
    def get_page_statistics(self):
        """Obtient les statistiques de followers pour l'organisation (lifetime stats)"""
        # Encoder l'URN de l'organisation
        organization_urn = f"urn:li:organization:{self.organization_id}"
        encoded_urn = urllib.parse.quote(organization_urn)
        
        # Construire l'URL pour les statistiques lifetime (sans timeIntervals)
        url = f"{self.base_url}/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity={encoded_urn}"
        
        # Effectuer la requête avec gestion des erreurs et retry
        max_retries = 3
        retry_delay = 2  # secondes
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    print(f"   Données de statistiques de followers récupérées avec succès")
                    return data
                    
                elif response.status_code == 429:
                    # Rate limit, attendre avant de réessayer
                    print(f"   Rate limit atteint, attente de {retry_delay} secondes...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Backoff exponentiel
                else:
                    print(f"   Erreur API: {response.status_code} - {response.text}")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    
            except Exception as e:
                print(f"   Exception lors de la requête: {e}")
                time.sleep(retry_delay)
                retry_delay *= 2
        
        print("   Échec après plusieurs tentatives pour obtenir les statistiques de followers.")
        return None
    
    def parse_page_statistics(self, data):
        """Analyse les données de l'API et extrait les statistiques pertinentes"""
        stats = {}
        
        # Date de récupération
        stats['date'] = datetime.now().strftime('%Y-%m-%d')
        
        # S'assurer que les données sont valides
        if not data or 'elements' not in data or len(data['elements']) == 0:
            print("   Aucune donnée de statistiques valide trouvée.")
            return stats
        
        # Obtenir le premier élément (qui contient toutes les stats)
        element = data['elements'][0]
        
        # D'abord, calculer les totaux pour chaque catégorie
        # Car LinkedIn peut avoir des followers comptés dans plusieurs catégories
        totals_by_category = {
            'country': 0,
            'function': 0,
            'seniority': 0,
            'industry': 0,
            'staff_count': 0
        }
        
        # 1. Statistiques par pays - AVEC DÉDUPLICATION
        stats['by_country'] = {}
        country_followers_by_name = {}  # Pour fusionner les doublons
        
        if 'followerCountsByGeoCountry' in element:
            for item in element['followerCountsByGeoCountry']:
                geo = item.get('geo', 'unknown')
                country_code = geo.split(':')[-1] if ':' in geo else geo
                country_name = self._get_country_name(country_code)
                
                follower_counts = item.get('followerCounts', {})
                
                # Pour les statistiques lifetime, utiliser organicFollowerCount
                # qui contient le total (organique + payé selon la doc)
                organic_count = follower_counts.get('organicFollowerCount', 0)
                paid_count = follower_counts.get('paidFollowerCount', 0)
                total_followers = organic_count  # La doc indique que organicFollowerCount contient déjà le total
                
                # Fusionner les followers par nom de pays (déduplication)
                if country_name in country_followers_by_name:
                    country_followers_by_name[country_name]['total_followers'] += total_followers
                    # Garder une trace des codes multiples pour debugging
                    if 'codes' not in country_followers_by_name[country_name]:
                        country_followers_by_name[country_name]['codes'] = [country_followers_by_name[country_name].get('original_code', '')]
                    country_followers_by_name[country_name]['codes'].append(country_code)
                else:
                    country_followers_by_name[country_name] = {
                        'name': country_name,
                        'total_followers': total_followers,
                        'original_code': country_code  # Garder le premier code rencontré
                    }
        
        # Convertir le dictionnaire fusionné vers le format attendu
        for country_name, country_data in country_followers_by_name.items():
            # Utiliser le premier code rencontré comme clé principale
            primary_code = country_data['original_code']
            stats['by_country'][primary_code] = {
                'name': country_name,
                'total_followers': country_data['total_followers']
            }
            totals_by_category['country'] += country_data['total_followers']
            
            # Afficher un message si des doublons ont été fusionnés
            if 'codes' in country_data:
                codes_list = ', '.join(country_data['codes'])
                print(f"   🔗 Fusion détectée pour {country_name}: codes {codes_list} -> {country_data['total_followers']} followers")
        
        # 2. Statistiques par niveau de séniorité
        stats['by_seniority'] = {}
        if 'followerCountsBySeniority' in element:
            for item in element['followerCountsBySeniority']:
                seniority = item.get('seniority', 'unknown')
                seniority_id = seniority.split(':')[-1] if ':' in seniority else seniority
                seniority_name = self._get_seniority_description(seniority_id)
                
                follower_counts = item.get('followerCounts', {})
                total_followers = follower_counts.get('organicFollowerCount', 0)
                
                stats['by_seniority'][seniority_id] = {
                    'name': seniority_name,
                    'total_followers': total_followers
                }
                totals_by_category['seniority'] += total_followers
        
        # 3. Statistiques par industrie
        stats['by_industry'] = {}
        if 'followerCountsByIndustry' in element:
            for item in element['followerCountsByIndustry']:
                industry = item.get('industry', 'unknown')
                industry_id = industry.split(':')[-1] if ':' in industry else industry
                industry_name = self._get_industry_description(industry_id)
                
                follower_counts = item.get('followerCounts', {})
                total_followers = follower_counts.get('organicFollowerCount', 0)
                
                stats['by_industry'][industry_id] = {
                    'name': industry_name,
                    'total_followers': total_followers
                }
                totals_by_category['industry'] += total_followers
        
        # 4. Statistiques par fonction
        stats['by_function'] = {}
        if 'followerCountsByFunction' in element:
            for item in element['followerCountsByFunction']:
                function = item.get('function', 'unknown')
                function_id = function.split(':')[-1] if ':' in function else function
                function_name = self._get_function_description(function_id)
                
                follower_counts = item.get('followerCounts', {})
                total_followers = follower_counts.get('organicFollowerCount', 0)
                
                stats['by_function'][function_id] = {
                    'name': function_name,
                    'total_followers': total_followers
                }
                totals_by_category['function'] += total_followers
        
        # 5. Statistiques par taille d'entreprise
        stats['by_staff_count'] = {}
        if 'followerCountsByStaffCountRange' in element:
            for item in element['followerCountsByStaffCountRange']:
                staff_range = item.get('staffCountRange', 'unknown')
                staff_name = self._get_staff_count_description(staff_range)
                
                follower_counts = item.get('followerCounts', {})
                total_followers = follower_counts.get('organicFollowerCount', 0)
                
                stats['by_staff_count'][staff_range] = {
                    'name': staff_name,
                    'total_followers': total_followers
                }
                totals_by_category['staff_count'] += total_followers
        
        # 6. Statistiques par type d'association (employés)
        stats['by_association'] = {}
        if 'followerCountsByAssociationType' in element:
            for item in element['followerCountsByAssociationType']:
                association_type = item.get('associationType', 'unknown')
                
                follower_counts = item.get('followerCounts', {})
                total_followers = follower_counts.get('organicFollowerCount', 0)
                
                stats['by_association'][association_type] = {
                    'name': 'Employés' if association_type == 'EMPLOYEE' else association_type,
                    'total_followers': total_followers
                }
                
        # 7. Calcul des totaux globaux
        # Le total calculé est le maximum des totaux par catégorie
        # Car un follower peut être dans toutes les catégories
        calculated_total = max(totals_by_category.values()) if totals_by_category.values() else 0
        
        # Essayer de récupérer le nombre total réel de followers
        total_followers = self._get_total_followers()
        
        # Si on n'a pas pu récupérer le total réel, utiliser le total calculé
        if total_followers == 0:
            total_followers = calculated_total
            print(f"   ⚠️  Utilisation du total calculé depuis les catégories: {total_followers}")
        
        stats['totals'] = {
            'total_followers': total_followers,
            'countries_count': len(stats['by_country']),
            'industries_count': len(stats['by_industry']),
            'functions_count': len(stats['by_function']),
            'seniorities_count': len(stats['by_seniority'])
        }
        
        # Ajouter les followers non catégorisés pour chaque dimension
        # Pays non spécifiés
        uncategorized_countries = max(0, total_followers - totals_by_category['country'])
        if uncategorized_countries > 0:
            stats['by_country']['unknown'] = {
                'name': 'Non spécifié',
                'total_followers': uncategorized_countries
            }
        
        # Séniorité non spécifiée
        uncategorized_seniority = max(0, total_followers - totals_by_category['seniority'])
        if uncategorized_seniority > 0 or len(stats['by_seniority']) == 0:
            stats['by_seniority']['0'] = {
                'name': 'Non spécifié',
                'total_followers': uncategorized_seniority
            }
        
        # Industrie non spécifiée
        uncategorized_industry = max(0, total_followers - totals_by_category['industry'])
        if uncategorized_industry > 0 or len(stats['by_industry']) == 0:
            stats['by_industry']['0'] = {
                'name': 'Non spécifié',
                'total_followers': uncategorized_industry
            }
        
        # Fonction non spécifiée
        uncategorized_function = max(0, total_followers - totals_by_category['function'])
        if uncategorized_function > 0 or len(stats['by_function']) == 0:
            stats['by_function']['0'] = {
                'name': 'Non spécifié',
                'total_followers': uncategorized_function
            }
        
        # Taille d'entreprise non spécifiée
        uncategorized_staff = max(0, total_followers - totals_by_category['staff_count'])
        if uncategorized_staff > 0 or len(stats['by_staff_count']) == 0:
            stats['by_staff_count']['unknown'] = {
                'name': 'Non spécifié',
                'total_followers': uncategorized_staff
            }
        
        # Afficher un résumé des catégories
        print(f"\n   📊 Résumé des followers:")
        print(f"      Total réel/calculé: {total_followers}")
        print(f"      Total par pays: {totals_by_category['country']}")
        print(f"      Total par fonction: {totals_by_category['function']}")
        print(f"      Total par séniorité: {totals_by_category['seniority']}")
        print(f"      Total par industrie: {totals_by_category['industry']}")
        print(f"      Total par taille d'entreprise: {totals_by_category['staff_count']}")
        
        return stats
    
    def _get_total_followers(self):
        """Récupère le nombre total de followers via plusieurs méthodes"""
        
        # Méthode 1: Via networkSizes (méthode documentée)
        total_followers = self._get_total_via_network_sizes()
        if total_followers > 0:
            return total_followers
            
        # Méthode 2: Via organizations (si on a les droits admin)
        total_followers = self._get_total_via_organizations()
        if total_followers > 0:
            return total_followers
            
        # Méthode 3: Calculer depuis les statistiques existantes (moins précis)
        print("   ⚠️  Impossible de récupérer le total réel, utilisation du calcul approximatif")
        return 0
    
    def _get_total_via_network_sizes(self):
        """Récupère le nombre total via l'API networkSizes (méthode documentée)"""
        try:
            # Selon la doc, l'URL doit être exactement comme ça
            organization_urn = f"urn:li:organization:{self.organization_id}"
            encoded_urn = urllib.parse.quote(organization_urn, safe='')
            
            # URL exacte selon la documentation
            url = f"{self.base_url}/networkSizes/{encoded_urn}?edgeType=COMPANY_FOLLOWED_BY_MEMBER"
            
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                data = response.json()
                followers = data.get('firstDegreeSize', 0)
                if followers > 0:
                    print(f"   Nombre total de followers: {followers}")
                    return followers
            else:
                # Si ça ne marche pas, essayer l'ancienne notation
                url_old = f"{self.base_url}/networkSizes/{encoded_urn}?edgeType=CompanyFollowedByMember"
                response = requests.get(url_old, headers=self.get_headers())
                
                if response.status_code == 200:
                    data = response.json()
                    followers = data.get('firstDegreeSize', 0)
                    if followers > 0:
                        print(f"   Nombre total de followers: {followers}")
                        return followers
                
        except Exception as e:
            print(f"   Exception avec networkSizes: {e}")
            
        return 0
    
    def _get_total_via_organizations(self):
        """Récupère le nombre total via l'API organizations (nécessite droits admin)"""
        try:
            # Essayer avec l'ID direct (nécessite droits admin)
            url = f"{self.base_url}/organizations/{self.organization_id}"
            
            response = requests.get(url, headers=self.get_headers())
            
            if response.status_code == 200:
                data = response.json()
                # Chercher followerCount dans la réponse
                # Note: Ce champ n'est pas documenté dans la doc fournie, mais peut exister
                followers = data.get('followerCount', 0)
                if followers > 0:
                    print(f"   Nombre total de followers (via organizations): {followers}")
                    return followers
            
        except Exception as e:
            print(f"   Exception avec organizations: {e}")
            
        return 0
    
    def _get_country_name(self, country_code):
        """Obtient le nom du pays à partir du code pays LinkedIn"""
        countries = {
            # Codes ISO en minuscules
            'ae': 'Émirats arabes unis',
            'ar': 'Argentine',
            'at': 'Autriche',
            'au': 'Australie',
            'be': 'Belgique',
            'bf': 'Burkina Faso',
            'br': 'Brésil',
            'ca': 'Canada',
            'ch': 'Suisse',
            'ci': 'Côte d\'Ivoire',
            'cl': 'Chili',
            'cm': 'Cameroun',
            'cn': 'Chine',
            'co': 'Colombie',
            'cz': 'République tchèque',
            'de': 'Allemagne',
            'dk': 'Danemark',
            'dz': 'Algérie',
            'eg': 'Égypte',
            'es': 'Espagne',
            'fi': 'Finlande',
            'fr': 'France',
            'gb': 'Royaume-Uni',
            'gr': 'Grèce',
            'hk': 'Hong Kong',
            'hu': 'Hongrie',
            'id': 'Indonésie',
            'ie': 'Irlande',
            'il': 'Israël',
            'in': 'Inde',
            'it': 'Italie',
            'jp': 'Japon',
            'kr': 'Corée du Sud',
            'lu': 'Luxembourg',
            'ma': 'Maroc',
            'mg': 'Madagascar',
            'mx': 'Mexique',
            'my': 'Malaisie',
            'ng': 'Nigeria',
            'nl': 'Pays-Bas',
            'no': 'Norvège',
            'nz': 'Nouvelle-Zélande',
            'ph': 'Philippines',
            'pl': 'Pologne',
            'pt': 'Portugal',
            'ro': 'Roumanie',
            'ru': 'Russie',
            'sa': 'Arabie saoudite',
            'se': 'Suède',
            'sg': 'Singapour',
            'th': 'Thaïlande',
            'tn': 'Tunisie',
            'tr': 'Turquie',
            'tw': 'Taïwan',
            'ua': 'Ukraine',
            'us': 'États-Unis',
            'vn': 'Vietnam',
            'za': 'Afrique du Sud',
            
            # Codes numériques LinkedIn complets (codes existants)
            '103644278': 'États-Unis',
            '105015875': 'France', 
            '101165590': 'Royaume-Uni',
            '103883259': 'Allemagne',
            '106693272': 'Suisse',
            '102890719': 'Canada',
            '101452733': 'Australie',
            '102890883': 'Espagne',
            '103350503': 'Italie',
            '102095887': 'Pays-Bas',
            '100565514': 'Chine',
            '101355337': 'Japon',
            '102264497': 'Inde',
            '106057199': 'Brésil',
            '100710459': 'Mexique',
            '105646813': 'Corée du Sud',
            '102927786': 'Russie',
            '106808692': 'Argentine',
            '104621616': 'Émirats arabes unis',
            '105076658': 'Singapour',
            '106373116': 'Afrique du Sud',
            '104508036': 'Suède',
            '104630756': 'Norvège',
            '100506914': 'Belgique',
            '106246626': 'Pologne',
            '105495012': 'Thaïlande',
            '103121230': 'Hong Kong',
            '101282230': 'Irlande',
            '101620260': 'Israël',
            '105763397': 'Portugal',
            '102800292': 'Turquie',
            '106615570': 'Nouvelle-Zélande',
            '104359928': 'Grèce',
            '105072658': 'Autriche',
            '105333783': 'Hongrie',
            '100830449': 'République tchèque',
            '105117694': 'Danemark',
            '100994331': 'Finlande',
            '106774456': 'Roumanie',
            '102095383': 'Ukraine',
            '102748797': 'Maroc',
            '102886501': 'Algérie',
            '100961665': 'Tunisie',
            '102928006': 'Égypte',
            '106542645': 'Nigeria',
            '102886832': 'Chili',
            '100867946': 'Colombie',
            '100877388': 'Vietnam',
            '103588947': 'Philippines',
            '105146439': 'Malaisie',
            '102221843': 'Indonésie',
            '100625338': 'Arabie saoudite',
            '104232339': 'Madagascar',
            '101929829': 'Côte d\'Ivoire',
            '105763554': 'Burkina Faso',
            '100364837': 'Cameroun',
            '101022257': 'Taïwan',
            '103323778': 'Luxembourg',
            
            # NOUVEAUX CODES MANQUANTS - Ajout basé sur les logs d'erreur
            '106395874': 'Estonie',  # Basé sur la séquence géographique européenne
            '103295271': 'Lettonie',  # Pays balte voisin de l'Estonie
            '106315325': 'Lituanie',  # Troisième pays balte
            '100961908': 'Slovénie',  # Pays d'Europe centrale
            '100770782': 'Croatie',  # Pays des Balkans
            '104514075': 'Bosnie-Herzégovine',  # Pays des Balkans
            '103550069': 'Serbie',  # Pays des Balkans
            '102454443': 'Monténégro',  # Pays des Balkans
            '101519029': 'Albanie',  # Pays des Balkans
            '106931611': 'Macédoine du Nord',  # Pays des Balkans
            '103119917': 'Bulgarie',  # Europe du Sud-Est
            '104725424': 'Moldavie',  # Europe de l'Est
            '103291313': 'Biélorussie',  # Europe de l'Est
            '103239229': 'Kazakhstan',  # Asie centrale
            '105745966': 'Ouzbékistan',  # Asie centrale
            '104379274': 'Kirghizistan',  # Asie centrale
            '100800406': 'Tadjikistan',  # Asie centrale
            '106215326': 'Turkménistan',  # Asie centrale
            '102974008': 'Azerbaïdjan',  # Caucase
            '105535747': 'Arménie',  # Caucase
            '100587095': 'Géorgie',  # Caucase
            '101271829': 'Islande',  # Europe du Nord
            
            # Autres codes supplémentaires découverts
            '100446943': 'Panama',
            '106155005': 'Équateur',
            '102571732': 'Pérou',
            '104738515': 'Uruguay',
            '102713980': 'Sénégal',
            '105365761': 'Kenya',
            '104035573': 'Zimbabwe',
            '101855366': 'Tanzanie',
            '104069274': 'Ouganda',
            '105015274': 'Bangladesh',
            '104444292': 'Biélorussie',
            '100446352': 'Venezuela',
            '100874388': 'Pakistan',
            '106149361': 'Sri Lanka',
            '102713854': 'Kazakhstan',
            '104369375': 'Jordanie',
            '100459316': 'Liban',
            '101739942': 'Ghana',
            '106774592': 'Angola',
            '104170880': 'Mozambique',
            '100931694': 'Namibie',
            '105252663': 'Botswana',
            '103810918': 'Malte',
            '107006278': 'Chypre',
            '104640522': 'Qatar',
            '104305776': 'Koweït',
            '104889540': 'Bahreïn',
            '105541707': 'Islande',
            '105214217': 'Népal',
            '109919345': 'Bhoutan',
            '104195383': 'Andorre',
            '106670623': 'Gibraltar',
            '103810579': 'Vatican',
            '103372930': 'Saint-Marin',
            '104460893': 'Albanie',
            '103728760': 'Macédoine du Nord',
            '108988376': 'Kosovo',
            '103372814': 'Mali',
            '101957298': 'Guinée',
            '105028804': 'Moldavie',
            '106796623': 'Bulgarie',
            '104688944': 'Serbie',
            '108734194': 'Monténégro',
            '103419092': 'Bosnie-Herzégovine',
            '104901016': 'Croatie',
            '104558166': 'Slovénie',
            '110343561': 'Slovaquie',
            '105490917': 'Corée du Nord',
            '105246709': 'Macao',
            '104042105': 'Maurice',
            '104586159': 'Réunion',
            '109512725': 'Mayotte',
            '106934271': 'Seychelles',
            '105072945': 'Comores',
            '101022442': 'Éthiopie',
            '102105699': 'Rwanda',
            '105587166': 'Gabon',
            '101834488': 'Tchad',
            '102478259': 'Guinée équatoriale',
            '102787409': 'Érythrée',
            '105146118': 'Soudan du Sud',
            '103587512': 'Soudan',
            '101464403': 'Burundi',
            '101174742': 'Djibouti',
            '101352147': 'République centrafricaine',
            '104677530': 'Congo-Brazzaville',
            '103350119': 'République démocratique du Congo',
            '104265812': 'Somalie',
            '105149562': 'Mauritanie',
            '105072130': 'Gambie',
            '102134353': 'Cap-Vert',
        }
        
        # Recherche dans le dictionnaire
        # D'abord vérifier le code exact
        if country_code in countries:
            return countries[country_code]
        # Ensuite vérifier en minuscules
        elif country_code.lower() in countries:
            return countries[country_code.lower()]
        # Si c'est "unknown" ou vide, retourner "Non spécifié"
        elif country_code in ['unknown', '0', '']:
            return 'Non spécifié'
        # Si on ne trouve pas, loguer le code manquant pour future référence
        else:
            print(f"   ⚠️  Code pays non reconnu: {country_code}")
            return f'Inconnu ({country_code})'
        
    def _get_seniority_description(self, seniority_id):
        """Fournit une description pour les niveaux de séniorité avec numérotation"""
        seniority_map = {
            "1": "01 - Stagiaire",
            "2": "02 - Débutant",
            "3": "03 - Junior",
            "4": "04 - Intermédiaire",
            "5": "05 - Senior",
            "6": "06 - Chef d'équipe",
            "7": "07 - Directeur",
            "8": "08 - Vice-président",
            "9": "09 - Cadre supérieur (C-level)",
            "10": "10 - Cadre dirigeant"
        }
        return seniority_map.get(seniority_id, f"Niveau {seniority_id}")
    
    def _get_industry_description(self, industry_id):
        """Fournit une description pour les identifiants d'industrie"""
        industry_map = {
            "1": "Agriculture",
            "2": "Élevage",
            "3": "Pêche et aquaculture",
            "4": "Banque",
            "5": "Services de télécommunications",
            "6": "Construction",
            "7": "Coopératives",
            "8": "Éducation préscolaire et primaire",
            "9": "Éducation collégiale",
            "10": "Éducation secondaire",
            "11": "Éducation",
            "12": "Divertissement",
            "13": "Environnement",
            "14": "Finance",
            "15": "Gouvernement",
            "16": "Santé",
            "17": "Industrie manufacturière",
            "18": "Médias",
            "19": "Mines et métaux",
            "20": "Commerce de détail",
            "21": "Transport",
            "22": "Sports",
            "23": "Électronique",
            "24": "Énergie",
            "25": "Hôpitaux et santé",
            "26": "Assurance",
            "27": "Technologies et services de l'information",
            "28": "Luxe et bijoux",
            "29": "Machinerie",
            "30": "Maritime",
            "31": "Santé et bien-être",
            "32": "Services juridiques",
            "33": "Bibliothèques",
            "34": "Logistique et chaîne d'approvisionnement",
            "35": "Matériaux",
            "36": "Industrie militaire",
            "37": "Industrie musicale",
            "38": "Nanotechnologie",
            "39": "Journaux",
            "40": "Organisation à but non lucratif",
            "41": "Pétrole et énergie",
            "42": "Services en ligne",
            "43": "Outsourcing/Offshoring",
            "44": "Emballage et conteneurs",
            "45": "Papier et produits forestiers",
            "46": "Services philanthropiques",
            "47": "Photographie",
            "48": "Marketing et publicité",
            "49": "Imprimerie",
            "50": "Biens de consommation",
            "51": "Édition",
            "52": "Chemins de fer",
            "53": "Organisation à but non lucratif",
            "54": "Recherche",
            "55": "Restaurants",
            "56": "Sécurité et investigations",
            "57": "Semi-conducteurs",
            "58": "Transport maritime",
            "59": "Textiles",
            "60": "Mode et vêtements",
            "61": "Arts",
            "62": "Fabrication de moteurs de véhicules",
            "63": "Vins et spiritueux",
            "64": "Industrie du fil et de la fibre",
            "65": "Industrie aéronautique et aérospatiale",
            "66": "Automobile",
            "67": "Génie civil",
            "68": "Comptabilité",
            "69": "Services financiers",
            "70": "Aviation",
            "71": "Architecture et urbanisme",
            "72": "Biotechnologie",
            "73": "Construction navale",
            "74": "Produits chimiques",
            "75": "Internet",
            "76": "Équipements électriques/électroniques",
            "77": "Gestion de l'environnement",
            "78": "Produits alimentaires",
            "79": "Collecte de fonds",
            "80": "Jeux vidéo et électroniques",
            "81": "Hôtellerie",
            "82": "Mobilier d'entreprise",
            "83": "Intelligence artificielle",
            "84": "Industrie pharmaceutique",
            "85": "Plastiques",
            "86": "Commerce international et développement",
            "87": "Industrie du vin et des spiritueux",
            "88": "Commerce de gros",
            "89": "Élevage d'animaux",
            "90": "Gestion des marchés",
            "91": "Affaires politiques",
            "92": "Ressources humaines",
            "93": "Industrie laitière",
            "94": "Design",
            "95": "Services aux consommateurs",
            "96": "Logiciels informatiques",
            "97": "Événements",
            "98": "Arts et artisanat",
            "99": "Formation professionnelle et coaching",
            "100": "Affaires gouvernementales",
            "101": "Meubles",
            "102": "Équipement de loisir",
            "103": "Matériel de bureau",
            "104": "Réseaux informatiques",
            "105": "Relations publiques et communications",
            "106": "Activités de loisirs",
            "107": "Immobilier",
            "108": "Équipement sportif",
            "109": "Vente en gros",
            "110": "Services publics",
            "111": "Animation",
            "112": "Produits cosmétiques",
            "113": "Œuvres de bienfaisance",
            "114": "Industrie du bétail",
            "115": "Arts de la scène",
            "116": "Gestion des installations",
            "117": "Design graphique",
            "118": "Équipement médical",
            "119": "Conseil",
            "120": "Industrie du bois",
            "121": "Industrie de la construction",
            "122": "Enseignement supérieur",
            "123": "Promotion immobilière",
            "124": "Gestion de placements",
            "125": "Services médicaux",
            "126": "Services vétérinaires",
            "127": "Commerce de détail",
            "128": "Génie électrique",
            "129": "Informatique et réseau de sécurité",
            "130": "Matériel informatique",
            "131": "Bâtiment et travaux publics",
            "132": "Traduction et localisation",
            "133": "Programmation informatique",
            "134": "Architecture",
            "135": "Relations publiques et communications",
            "136": "Exploitation minière",
            "137": "Design",
            "138": "Santé, bien-être et fitness",
            "139": "E-learning",
            "140": "Services bancaires d'investissement",
            "141": "Musées et institutions",
            "142": "Vétérinaire",
            "143": "Industrie du voyage",
            "144": "Technologies de l'information et services",
            "145": "Gestion de l'éducation",
            "147": "Développement Web",
            "148": "Technologies et services de l'information"
        }
        return industry_map.get(industry_id, f"Industrie {industry_id}")
    
    def _get_function_description(self, function_id):
        """Fournit une description pour les identifiants de fonction"""
        function_map = {
            "1": "Comptabilité",
            "2": "Services administratifs",
            "3": "Arts et Design",
            "4": "Développement commercial",
            "5": "Services communautaires et sociaux",
            "6": "Conseil",
            "7": "Éducation",
            "8": "Ingénierie",
            "9": "Entrepreneuriat",
            "10": "Finance",
            "11": "Santé",
            "12": "Ressources humaines",
            "13": "Technologies de l'information",
            "14": "Juridique",
            "15": "Marketing",
            "16": "Médias et communication",
            "17": "Opérations militaires et protection",
            "18": "Opérations",
            "19": "Gestion de produit",
            "20": "Gestion de programme et de projet",
            "21": "Achats",
            "22": "Assurance qualité",
            "23": "Immobilier",
            "24": "Recherche",
            "25": "Ventes",
            "26": "Support"
        }
        return function_map.get(function_id, f"Fonction {function_id}")
    
    def _get_staff_count_description(self, staff_range):
        """Fournit une description pour les tailles d'entreprise"""
        staff_map = {
            "SIZE_1": "1 employé",
            "SIZE_2_TO_10": "2-10 employés",
            "SIZE_11_TO_50": "11-50 employés",
            "SIZE_51_TO_200": "51-200 employés",
            "SIZE_201_TO_500": "201-500 employés",
            "SIZE_501_TO_1000": "501-1000 employés",
            "SIZE_1001_TO_5000": "1001-5000 employés",
            "SIZE_5001_TO_10000": "5001-10000 employés",
            "SIZE_10001_OR_MORE": "10001+ employés"
        }
        return staff_map.get(staff_range, staff_range)


class GoogleSheetsExporter:
    """Classe pour exporter les données vers Google Sheets"""
    
    def __init__(self, spreadsheet_name, credentials_path, admin_email="byteberry.analytics@gmail.com"):
        """Initialise l'exportateur avec le nom du spreadsheet et le chemin des credentials"""
        self.spreadsheet_name = spreadsheet_name
        self.credentials_path = credentials_path
        self.admin_email = admin_email
        self.client = None
        self.spreadsheet = None
        
    def connect(self):
        """Établit la connexion avec Google Sheets API"""
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(str(self.credentials_path), scope)
            self.client = gspread.authorize(creds)
            
            # Vérifier si le spreadsheet existe déjà, sinon le créer
            try:
                self.spreadsheet = self.client.open(self.spreadsheet_name)
                print(f"   Spreadsheet existant trouvé: {self.spreadsheet_name}")
            except gspread.exceptions.SpreadsheetNotFound:
                self.spreadsheet = safe_sheets_operation(self.client.create, self.spreadsheet_name)
                print(f"   Nouveau spreadsheet créé: {self.spreadsheet_name}")
                
                # Donner l'accès en édition à l'adresse e-mail spécifiée
                safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
            
            # Vérifier si Sheet1 existe et le renommer en "Résumé"
            try:
                sheet1 = self.spreadsheet.worksheet("Sheet1")
                safe_sheets_operation(sheet1.update_title, "Résumé")
                print("   Feuille 'Sheet1' renommée en 'Résumé'")
            except gspread.exceptions.WorksheetNotFound:
                pass  # Sheet1 n'existe pas, pas besoin de la renommer
            
            return True
        except Exception as e:
            print(f"   Erreur de connexion à Google Sheets: {e}")
            return False
    
    def ensure_admin_access(self):
        """Vérifie et garantit que l'admin a toujours accès au document"""
        try:
            # Récupérer les permissions actuelles
            permissions = safe_sheets_operation(self.spreadsheet.list_permissions)
            
            # Vérifier si l'email admin est déjà dans les permissions
            admin_has_access = False
            for permission in permissions:
                if 'emailAddress' in permission and permission['emailAddress'] == self.admin_email:
                    admin_has_access = True
                    # Vérifier si le rôle est au moins "writer"
                    if permission.get('role') not in ['writer', 'owner']:
                        # Mettre à jour le rôle si nécessaire
                        safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                        print(f"   Rôle mis à jour pour {self.admin_email} (writer)")
                    break
            
            # Si l'admin n'a pas encore accès, lui donner
            if not admin_has_access:
                safe_sheets_operation(self.spreadsheet.share, self.admin_email, perm_type="user", role="writer")
                print(f"   Accès en édition accordé à {self.admin_email}")
                
        except Exception as e:
            print(f"   Erreur lors de la vérification des permissions: {e}")
    
    def format_sheet_for_looker(self, sheet, headers, data_start_row=2):
        """Applique le formatage approprié aux colonnes pour que Looker détecte correctement les types"""
        try:
            # Définir les indices des colonnes selon leur type
            date_columns = [0]  # Colonne A (Date)
            text_columns = [1, 2]  # Colonnes B et C (Pays/Niveau/Industrie et Code/Description)
            number_columns = list(range(3, len(headers)))  # Toutes les autres colonnes sont numériques
            
            # Formater la colonne date
            for col_idx in date_columns:
                col_letter = get_column_letter(col_idx)
                range_name = f"{col_letter}{data_start_row}:{col_letter}"
                safe_sheets_operation(sheet.format, range_name, {
                    "numberFormat": {
                        "type": "DATE",
                        "pattern": "yyyy-mm-dd"
                    }
                })
            
            # Formater les colonnes de texte
            for col_idx in text_columns:
                if col_idx < len(headers):
                    col_letter = get_column_letter(col_idx)
                    range_name = f"{col_letter}{data_start_row}:{col_letter}"
                    safe_sheets_operation(sheet.format, range_name, {
                        "numberFormat": {
                            "type": "TEXT"
                        }
                    })
            
            # Formater les colonnes numériques
            for col_idx in number_columns:
                col_letter = get_column_letter(col_idx)
                range_name = f"{col_letter}{data_start_row}:{col_letter}"
                safe_sheets_operation(sheet.format, range_name, {
                    "numberFormat": {
                        "type": "NUMBER",
                        "pattern": "#,##0"
                    }
                })
            
            print("   ✓ Formatage appliqué pour Looker")
            
        except Exception as e:
            print(f"   Erreur lors du formatage des colonnes: {e}")
        
        
    def prepare_and_update_summary_sheet(self, stats):
        """Prépare et met à jour la feuille de résumé des statistiques"""
        try:
            # Vérifier si la feuille Résumé existe et l'utiliser
            try:
                sheet = self.spreadsheet.worksheet("Résumé")
                print("   Feuille 'Résumé' utilisée pour le résumé")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet1")
                    safe_sheets_operation(sheet.update_title, "Résumé")
                    print("   Feuille par défaut 'Sheet1' renommée en 'Résumé'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Résumé", rows=100, cols=10)
                    print("   Nouvelle feuille 'Résumé' créée")
            
            # Nettoyer la feuille existante
            safe_sheets_operation(sheet.clear)
            
            # Attendre un peu pour éviter les problèmes de quota
            time.sleep(2)
            
            # Préparer les données pour le résumé
            data = []
            
            # En-têtes avec les nouveaux noms
            headers = ["Date", "Nombre d'abonnés", "Pays", "Industries", "Fonctions", "Niveaux", "Employés"]
            data.append(headers)
            
            # Données totales
            totals = stats.get('totals', {})
            association_stats = stats.get('by_association', {})
            employee_followers = association_stats.get('EMPLOYEE', {}).get('total_followers', 0) if 'EMPLOYEE' in association_stats else 0
            
            data.append([
                stats.get('date'),
                totals.get('total_followers', 0),
                totals.get('countries_count', 0),
                totals.get('industries_count', 0),
                totals.get('functions_count', 0),
                totals.get('seniorities_count', 0),
                employee_followers
            ])
            
            # Mettre à jour la feuille avec les données
            safe_sheets_operation(sheet.update, data, 'A1')
            
            # Attendre un peu avant le formatage
            time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:G1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            
            # Attendre un peu avant le prochain formatage
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
            return sheet
        except Exception as e:
            print(f"   Erreur lors de la préparation de la feuille de résumé: {e}")
            return None
    
    def prepare_and_update_detail_sheets(self, stats):
        """Prépare et met à jour les feuilles détaillées pour chaque catégorie"""
        try:
            # Ajouter un délai plus long entre les mises à jour pour éviter les problèmes de quota
            print("   📊 Mise à jour des feuilles détaillées...")
            
            self._update_country_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)  # Attendre plus longtemps entre chaque mise à jour
            
            self._update_seniority_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_industry_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_function_sheet(stats)
            print("   ⏳ Attente de 10 secondes pour éviter les quotas...")
            time.sleep(10)
            
            self._update_staff_count_sheet(stats)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour des feuilles détaillées: {e}")
    
    def _update_country_sheet(self, stats):
        """Met à jour la feuille des statistiques par pays avec déduplication ISO"""
        try:
            print("   📍 Mise à jour de la feuille 'Par Pays'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Pays")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet2")
                    safe_sheets_operation(sheet.update_title, "Par Pays")
                    print("   Feuille par défaut 'Sheet2' renommée en 'Par Pays'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Pays", rows=100, cols=10)
                    print("   Nouvelle feuille 'Par Pays' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Code_Pays_ISO", "Pays", "Nbre d'abonnés"]
            data.append(headers)
            
            date = stats['date']
            
            # Mapper les codes pays LinkedIn vers ISO
            linkedin_to_iso = self._get_linkedin_to_iso_mapping()
            
            # NOUVELLE LOGIQUE: Fusionner par code ISO pour éviter les doublons
            iso_aggregated = {}
            
            for country_code, values in stats['by_country'].items():
                # Convertir le code LinkedIn en code ISO
                iso_code = linkedin_to_iso.get(country_code, country_code.upper())
                
                # Si c'est "unknown" ou "Non spécifié", ne pas mettre de code ISO
                if country_code in ['unknown', '0'] or values['name'] == 'Non spécifié':
                    iso_code = ''
                
                country_name = values['name']
                followers = values['total_followers']
                
                # Clé pour l'agrégation : utiliser le code ISO ou le nom du pays si pas de code ISO
                aggregation_key = iso_code if iso_code else country_name
                
                if aggregation_key in iso_aggregated:
                    # Fusionner les followers
                    iso_aggregated[aggregation_key]['total_followers'] += followers
                    print(f"   🔗 Fusion ISO détectée pour {country_name} ({iso_code}): +{followers} followers")
                else:
                    iso_aggregated[aggregation_key] = {
                        'iso_code': iso_code,
                        'name': country_name,
                        'total_followers': followers
                    }
            
            # Trier par nombre de followers (décroissant)
            country_entries = []
            for agg_key, country_data in iso_aggregated.items():
                country_entries.append((
                    country_data['iso_code'],
                    country_data['name'],
                    country_data['total_followers']
                ))
            
            country_entries.sort(key=lambda x: x[2], reverse=True)
            
            # Ajouter à la liste de données
            for iso_code, country_name, followers in country_entries:
                data.append([date, iso_code, country_name, followers])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:D1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Formater spécifiquement pour Looker
            # Colonne A (Date) - Format DATE
            safe_sheets_operation(sheet.format, 'A2:A', {
                "numberFormat": {
                    "type": "DATE",
                    "pattern": "yyyy-mm-dd"
                }
            })
            time.sleep(1)
            
            # Colonne B (Code_Pays_ISO) - Format TEXT
            safe_sheets_operation(sheet.format, 'B2:B', {
                "numberFormat": {
                    "type": "TEXT"
                }
            })
            time.sleep(1)
            
            # Colonne C (Pays) - Format TEXT
            safe_sheets_operation(sheet.format, 'C2:C', {
                "numberFormat": {
                    "type": "TEXT"
                }
            })
            time.sleep(1)
            
            # Colonne D (Nbre d'abonnés) - Format NUMBER
            safe_sheets_operation(sheet.format, 'D2:D', {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "#,##0"
                }
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des pays: {e}")
            
            
    def _get_linkedin_to_iso_mapping(self):
        """Mapping des codes LinkedIn vers codes ISO pour Looker avec déduplication"""
        return {
            # Codes alphabétiques déjà en ISO
            'ae': 'AE', 'ar': 'AR', 'at': 'AT', 'au': 'AU', 'be': 'BE',
            'bf': 'BF', 'br': 'BR', 'ca': 'CA', 'ch': 'CH', 'ci': 'CI',
            'cl': 'CL', 'cm': 'CM', 'cn': 'CN', 'co': 'CO', 'cz': 'CZ',
            'de': 'DE', 'dk': 'DK', 'dz': 'DZ', 'eg': 'EG', 'es': 'ES',
            'fi': 'FI', 'fr': 'FR', 'gb': 'GB', 'gr': 'GR', 'hk': 'HK',
            'hu': 'HU', 'id': 'ID', 'ie': 'IE', 'il': 'IL', 'in': 'IN',
            'it': 'IT', 'jp': 'JP', 'kr': 'KR', 'lu': 'LU', 'ma': 'MA',
            'mg': 'MG', 'mx': 'MX', 'my': 'MY', 'ng': 'NG', 'nl': 'NL',
            'no': 'NO', 'nz': 'NZ', 'ph': 'PH', 'pl': 'PL', 'pt': 'PT',
            'ro': 'RO', 'ru': 'RU', 'sa': 'SA', 'se': 'SE', 'sg': 'SG',
            'th': 'TH', 'tn': 'TN', 'tr': 'TR', 'tw': 'TW', 'ua': 'UA',
            'us': 'US', 'vn': 'VN', 'za': 'ZA',
            
            # Codes numériques LinkedIn vers ISO
            '103644278': 'US',  # États-Unis
            '105015875': 'FR',  # France
            '101165590': 'GB',  # Royaume-Uni
            '103883259': 'DE',  # Allemagne
            '106693272': 'CH',  # Suisse
            '102890719': 'CA',  # Canada
            '101452733': 'AU',  # Australie
            '102890883': 'ES',  # Espagne
            '103350503': 'IT',  # Italie
            '102095887': 'NL',  # Pays-Bas
            '100565514': 'CN',  # Chine
            '101355337': 'JP',  # Japon
            '102264497': 'IN',  # Inde
            '106057199': 'BR',  # Brésil
            '100710459': 'MX',  # Mexique
            '105646813': 'KR',  # Corée du Sud
            '102927786': 'RU',  # Russie
            '106808692': 'AR',  # Argentine
            '104621616': 'AE',  # Émirats arabes unis
            '105076658': 'SG',  # Singapour
            '106373116': 'ZA',  # Afrique du Sud
            '104508036': 'SE',  # Suède
            '104630756': 'NO',  # Norvège
            '100506914': 'BE',  # Belgique
            '106246626': 'PL',  # Pologne
            '105495012': 'TH',  # Thaïlande
            '103121230': 'HK',  # Hong Kong
            '101282230': 'IE',  # Irlande
            '101620260': 'IL',  # Israël
            '105763397': 'PT',  # Portugal
            '102800292': 'TR',  # Turquie
            '106615570': 'NZ',  # Nouvelle-Zélande
            '104359928': 'GR',  # Grèce
            '105072658': 'AT',  # Autriche
            '105333783': 'HU',  # Hongrie
            '100830449': 'CZ',  # République tchèque
            '105117694': 'DK',  # Danemark
            '100994331': 'FI',  # Finlande
            '106774456': 'RO',  # Roumanie
            '102095383': 'UA',  # Ukraine
            '102748797': 'MA',  # Maroc
            '102886501': 'DZ',  # Algérie
            '100961665': 'TN',  # Tunisie
            '102928006': 'EG',  # Égypte
            '106542645': 'NG',  # Nigeria
            '102886832': 'CL',  # Chili
            '100867946': 'CO',  # Colombie
            '100877388': 'VN',  # Vietnam
            '103588947': 'PH',  # Philippines
            '105146439': 'MY',  # Malaisie
            '102221843': 'ID',  # Indonésie
            '100625338': 'SA',  # Arabie saoudite
            '104232339': 'MG',  # Madagascar
            '101929829': 'CI',  # Côte d'Ivoire
            '105763554': 'BF',  # Burkina Faso
            '100364837': 'CM',  # Cameroun
            '104042105': 'MU',  # Maurice
            '102134353': 'CV',  # Cap-Vert
            '102787409': 'ER',  # Érythrée
            '103587512': 'SD',  # Soudan
            '103350119': 'CD',  # République démocratique du Congo
            '102713980': 'SN',  # Sénégal
            '101174742': 'DJ',  # Djibouti
            
            # NOUVEAUX CODES AJOUTÉS
            '106395874': 'EE',  # Estonie
            '103295271': 'LV',  # Lettonie
            '106315325': 'LT',  # Lituanie
            '100961908': 'SI',  # Slovénie
            '100770782': 'HR',  # Croatie
            '104514075': 'BA',  # Bosnie-Herzégovine
            '103550069': 'RS',  # Serbie
            '102454443': 'ME',  # Monténégro
            '101519029': 'AL',  # Albanie
            '106931611': 'MK',  # Macédoine du Nord
            '103119917': 'BG',  # Bulgarie
            '104725424': 'MD',  # Moldavie
            '103291313': 'BY',  # Biélorussie
            '103239229': 'KZ',  # Kazakhstan
            '105745966': 'UZ',  # Ouzbékistan
            '104379274': 'KG',  # Kirghizistan
            '100800406': 'TJ',  # Tadjikistan
            '106215326': 'TM',  # Turkménistan
            '102974008': 'AZ',  # Azerbaïdjan
            '105535747': 'AM',  # Arménie
            '100587095': 'GE',  # Géorgie
            '101271829': 'IS',  # Islande
        }
    
    def _update_seniority_sheet(self, stats):
        """Met à jour la feuille des statistiques par séniorité"""
        try:
            print("   👔 Mise à jour de la feuille 'Par Séniorité'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Séniorité")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet3")
                    safe_sheets_operation(sheet.update_title, "Par Séniorité")
                    print("   Feuille par défaut 'Sheet3' renommée en 'Par Séniorité'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Séniorité", rows=100, cols=10)
                    print("   Nouvelle feuille 'Par Séniorité' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Niveau", "Ancienneté professionnelle", "Nbre d'abonnés"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par niveau de séniorité (ordre croissant)
            seniority_entries = []
            for seniority_id, values in stats['by_seniority'].items():
                try:
                    level = int(seniority_id)
                except ValueError:
                    level = 999  # Pour les valeurs non numériques
                
                seniority_entries.append((
                    level,
                    values['name'],
                    values['total_followers']
                ))
            
            # Trier par niveau
            seniority_entries.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for entry in seniority_entries:
                data.append([date, entry[0], entry[1], entry[2]])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:D1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker (la colonne Niveau est numérique ici)
            headers_modified = headers.copy()
            self.format_sheet_for_looker(sheet, headers_modified)
            
            # Formater spécifiquement la colonne Niveau comme nombre
            safe_sheets_operation(sheet.format, 'B2:B', {
                "numberFormat": {
                    "type": "NUMBER",
                    "pattern": "0"
                }
            })
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des séniorités: {e}")
    
    def _update_industry_sheet(self, stats):
        """Met à jour la feuille des statistiques par industrie"""
        try:
            print("   🏭 Mise à jour de la feuille 'Par Industrie'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Industrie")
            except gspread.exceptions.WorksheetNotFound:
                try:
                    sheet = self.spreadsheet.worksheet("Sheet4")
                    safe_sheets_operation(sheet.update_title, "Par Industrie")
                    print("   Feuille par défaut 'Sheet4' renommée en 'Par Industrie'")
                except gspread.exceptions.WorksheetNotFound:
                    sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Industrie", rows=1000, cols=10)
                    print("   Nouvelle feuille 'Par Industrie' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Industrie_ID", "Nom_Industrie", "Nbre d'abonnés"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par nombre de followers (décroissant)
            industry_entries = []
            for industry_id, values in stats['by_industry'].items():
                industry_entries.append((
                    industry_id,
                    values['name'],
                    values['total_followers']
                ))
            
            # Trier par nombre de followers
            industry_entries.sort(key=lambda x: x[2], reverse=True)
            
            # Ajouter à la liste de données
            for entry in industry_entries:
                data.append([date] + list(entry))
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:D1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des industries: {e}")
    
    def _update_function_sheet(self, stats):
        """Met à jour la feuille des statistiques par fonction"""
        try:
            print("   💼 Mise à jour de la feuille 'Par Fonction'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Fonction")
            except gspread.exceptions.WorksheetNotFound:
                sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Fonction", rows=100, cols=10)
                print("   Nouvelle feuille 'Par Fonction' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Fonction_ID", "Fonction professionnelle", "Nbre d'abonnés"]
            data.append(headers)
            
            date = stats['date']
            
            # Trier par nombre de followers (décroissant)
            function_entries = []
            for function_id, values in stats['by_function'].items():
                function_entries.append((
                    function_id,
                    values['name'],
                    values['total_followers']
                ))
            
            # Trier par nombre de followers
            function_entries.sort(key=lambda x: x[2], reverse=True)
            
            # Ajouter à la liste de données
            for entry in function_entries:
                data.append([date] + list(entry))
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:D1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des fonctions: {e}")
    
    def _update_staff_count_sheet(self, stats):
        """Met à jour la feuille des statistiques par taille d'entreprise"""
        try:
            print("   🏢 Mise à jour de la feuille 'Par Taille Entreprise'...")
            
            # Vérifier si la feuille existe déjà
            try:
                sheet = self.spreadsheet.worksheet("Par Taille Entreprise")
            except gspread.exceptions.WorksheetNotFound:
                sheet = safe_sheets_operation(self.spreadsheet.add_worksheet, title="Par Taille Entreprise", rows=100, cols=10)
                print("   Nouvelle feuille 'Par Taille Entreprise' créée")
            
            # Nettoyer la feuille
            safe_sheets_operation(sheet.clear)
            time.sleep(2)
            
            # Préparer les données
            data = []
            headers = ["Date", "Taille", "Taille de l’entreprise", "Nbre d'abonnés"]
            data.append(headers)
            
            date = stats['date']
            
            # Définir l'ordre des tailles
            size_order = ["SIZE_1", "SIZE_2_TO_10", "SIZE_11_TO_50", "SIZE_51_TO_200", 
                         "SIZE_201_TO_500", "SIZE_501_TO_1000", "SIZE_1001_TO_5000", 
                         "SIZE_5001_TO_10000", "SIZE_10001_OR_MORE"]
            
            # Trier par ordre de taille
            staff_entries = []
            for staff_range, values in stats['by_staff_count'].items():
                order = size_order.index(staff_range) if staff_range in size_order else 999
                staff_entries.append((
                    order,
                    staff_range,
                    values['name'],
                    values['total_followers']
                ))
            
            # Trier par ordre
            staff_entries.sort(key=lambda x: x[0])
            
            # Ajouter à la liste de données
            for entry in staff_entries:
                data.append([date, entry[1], entry[2], entry[3]])
            
            # Mettre à jour la feuille avec les données
            if data:
                safe_sheets_operation(sheet.update, data, 'A1')
                time.sleep(1)
            
            # Formater les en-têtes
            safe_sheets_operation(sheet.format, 'A1:D1', {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
            })
            time.sleep(1)
            
            # Appliquer le formatage pour Looker
            self.format_sheet_for_looker(sheet, headers)
            
        except Exception as e:
            print(f"   Erreur lors de la mise à jour de la feuille des tailles d'entreprise: {e}")
    
    def add_page_statistics(self, stats):
        """Ajoute les statistiques de followers"""
        if not self.connect():
            print("   Impossible de se connecter à Google Sheets. Vérifiez vos credentials.")
            return False
            
        # Vérifier les permissions de partage pour s'assurer que l'admin a toujours accès
        self.ensure_admin_access()
        
        # Attendre un peu avant de commencer les mises à jour
        time.sleep(2)
        
        # Mettre à jour les feuilles
        summary_sheet = self.prepare_and_update_summary_sheet(stats)
        if summary_sheet:
            print("   ✅ Feuille de résumé mise à jour avec succès")
            # Attendre avant de passer aux feuilles détaillées
            print("   ⏳ Attente de 5 secondes avant les feuilles détaillées...")
            time.sleep(5)
            
            self.prepare_and_update_detail_sheets(stats)
        else:
            print("   ❌ Échec de la mise à jour de la feuille de résumé")
            return False
        
        # URL du spreadsheet
        sheet_url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet.id}"
        print(f"   URL du tableau: {sheet_url}")
        
        return True


def verify_token(access_token):
    """Vérifie si le token d'accès est valide"""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "X-Restli-Protocol-Version": "2.0.0",
        "LinkedIn-Version": "202505"
    }
    
    url = "https://api.linkedin.com/rest/me"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return True, response.json()
        else:
            return False, f"Erreur {response.status_code}: {response.text}"
    except Exception as e:
        return False, str(e)


class MultiOrganizationPageStatsTracker:
    """Gestionnaire pour les statistiques de followers de plusieurs organisations LinkedIn"""
    
    def __init__(self, config_file='organizations_config.json'):
        """Initialise le tracker multi-organisations"""
        self.config_file = config_file
        self.organizations = self.load_organizations()
        self.access_token = os.getenv("LINKEDIN_ACCESS_TOKEN", "").strip("'")
        self.admin_email = os.getenv("GOOGLE_ADMIN_EMAIL", "byteberry.analytics@gmail.com")
        self.page_stats_mapping_file = 'page_stats_mapping.json'
        
    def load_organizations(self):
        """Charge la configuration des organisations depuis un fichier JSON"""
        try:
            # D'abord essayer de charger depuis un fichier local
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Sinon, charger depuis une variable d'environnement
            orgs_json = os.getenv("LINKEDIN_ORGANIZATIONS_CONFIG")
            if orgs_json:
                return json.loads(orgs_json)
            
            # Configuration par défaut
            print("Aucune configuration trouvée, utilisation de la configuration par défaut")
            return []
            
        except Exception as e:
            print(f"Erreur lors du chargement de la configuration: {e}")
            return []
    
    def get_sheet_info_for_org(self, org_id, org_name):
        """Récupère ou crée l'ID et le nom du Google Sheet pour une organisation"""
        try:
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            # Si l'organisation a déjà un sheet ID, le retourner
            if org_id in mapping:
                print(f"   📂 Réutilisation du Google Sheet existant")
                return mapping[org_id]['sheet_id'], mapping[org_id]['sheet_name']
            
            # Sinon, utiliser le nom par défaut
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Follower_Stats_{clean_name}_{org_id}"
            
            # Stocker le mapping pour la prochaine fois
            mapping[org_id] = {
                'sheet_name': sheet_name,
                'sheet_id': None,  # Sera mis à jour après création
                'org_name': org_name
            }
            
            with open(self.page_stats_mapping_file, 'w', encoding='utf-8') as f:
                json.dump(mapping, f, indent=2, ensure_ascii=False)
            
            return None, sheet_name
            
        except Exception as e:
            print(f"Erreur dans la gestion du mapping: {e}")
            clean_name = org_name.replace(' ', '_').replace('™', '').replace('/', '_')
            sheet_name = f"LinkedIn_Follower_Stats_{clean_name}_{org_id}"
            return None, sheet_name
    
    def update_sheet_mapping(self, org_id, sheet_id):
        """Met à jour le mapping avec l'ID du sheet créé"""
        try:
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
            else:
                mapping = {}
            
            if org_id in mapping:
                mapping[org_id]['sheet_id'] = sheet_id
                mapping[org_id]['sheet_url'] = f"https://docs.google.com/spreadsheets/d/{sheet_id}"
                
                with open(self.page_stats_mapping_file, 'w', encoding='utf-8') as f:
                    json.dump(mapping, f, indent=2, ensure_ascii=False)
                    
        except Exception as e:
            print(f"Erreur lors de la mise à jour du mapping: {e}")
    
    def process_all_organizations(self):
        """Traite toutes les organisations configurées"""
        if not self.access_token:
            print("Erreur: LINKEDIN_ACCESS_TOKEN manquant")
            return False
        
        # Vérifier le token une seule fois
        print("\n--- Vérification du token ---")
        is_valid, result = verify_token(self.access_token)
        
        if not is_valid:
            print(f"❌ Token invalide: {result}")
            return False
        
        print("✅ Token valide!")
        user_info = result
        print(f"   Utilisateur: {user_info.get('localizedFirstName', '')} {user_info.get('localizedLastName', '')}")
        
        # Traiter chaque organisation
        results = []
        total_orgs = len(self.organizations)
        successful_urls = []
        
        for idx, org in enumerate(self.organizations, 1):
            org_id = org['id']
            org_name = org['name']
            
            print(f"\n{'='*60}")
            print(f"[{idx}/{total_orgs}] Traitement de: {org_name}")
            print(f"ID: {org_id}")
            print(f"{'='*60}")
            
            try:
                sheet_url = self.process_single_organization(org_id, org_name)
                if sheet_url:
                    results.append({
                        'org_id': org_id,
                        'org_name': org_name,
                        'success': True,
                        'sheet_url': sheet_url,
                        'timestamp': datetime.now().isoformat()
                    })
                    successful_urls.append({
                        'name': org_name,
                        'url': sheet_url
                    })
                else:
                    results.append({
                        'org_id': org_id,
                        'org_name': org_name,
                        'success': False,
                        'timestamp': datetime.now().isoformat()
                    })
                    
                # Attendre entre chaque organisation pour éviter les problèmes de quota
                if idx < total_orgs:  # Ne pas attendre après la dernière organisation
                    print(f"   ⏳ Attente de 15 secondes avant la prochaine organisation...")
                    time.sleep(15)
                    
            except Exception as e:
                print(f"❌ Erreur lors du traitement de {org_name}: {e}")
                results.append({
                    'org_id': org_id,
                    'org_name': org_name,
                    'success': False,
                    'error': str(e),
                    'timestamp': datetime.now().isoformat()
                })
        
        # Résumé
        print(f"\n{'='*60}")
        print("RÉSUMÉ DU TRAITEMENT - STATISTIQUES DE FOLLOWERS")
        print(f"{'='*60}")
        
        successful = sum(1 for r in results if r['success'])
        failed = len(results) - successful
        
        print(f"✅ Organisations traitées avec succès: {successful}/{total_orgs}")
        if failed > 0:
            print(f"❌ Organisations en échec: {failed}/{total_orgs}")
        
        if failed > 0:
            print("\nDétail des échecs:")
            for r in results:
                if not r['success']:
                    error_msg = r.get('error', 'Erreur inconnue')
                    print(f"  - {r['org_name']}: {error_msg}")
        
        # Afficher les URLs des sheets créés
        if successful > 0:
            print("\n📊 Google Sheets de statistiques de followers créés/mis à jour:")
            if os.path.exists(self.page_stats_mapping_file):
                with open(self.page_stats_mapping_file, 'r', encoding='utf-8') as f:
                    mapping = json.load(f)
                
                for r in results:
                    if r['success'] and r['org_id'] in mapping:
                        sheet_info = mapping[r['org_id']]
                        if sheet_info.get('sheet_id'):
                            print(f"  - {r['org_name']}:")
                            url = sheet_info.get('sheet_url') or f"https://docs.google.com/spreadsheets/d/{sheet_info['sheet_id']}"
                            print(f"    {url}")
        
        return successful > 0
    
    def process_single_organization(self, org_id, org_name):
        """Traite une organisation unique"""
        # Obtenir le nom du sheet pour cette organisation
        sheet_id, sheet_name = self.get_sheet_info_for_org(org_id, org_name)
        
        print(f"\n📊 Google Sheet: {sheet_name}")
        
        # Initialisation du tracker
        tracker = LinkedInFollowerStatisticsTracker(self.access_token, org_id, sheet_name)
        
        # Obtention des statistiques
        print("\n1. Récupération des statistiques de followers...")
        raw_stats = tracker.get_page_statistics()
        
        if raw_stats:
            # Traitement des données
            print("\n2. Analyse des données statistiques...")
            stats = tracker.parse_page_statistics(raw_stats)
            
            # Afficher un aperçu
            print("\n📈 Aperçu des statistiques:")
            totals = stats.get('totals', {})
            print(f"   Nbre d'abonnés: {totals.get('total_followers', 0)}")
            print(f"   Pays uniques: {totals.get('countries_count', 0)}")
            print(f"   Industries représentées: {totals.get('industries_count', 0)}")
            print(f"   Fonctions représentées: {totals.get('functions_count', 0)}")
            print(f"   Niveaux de séniorité: {totals.get('seniorities_count', 0)}")
            
            # Afficher le top 3 des pays
            if stats.get('by_country'):
                print("\n   Top 3 des pays:")
                country_list = [(code, data['name'], data['total_followers']) 
                               for code, data in stats['by_country'].items()]
                country_list.sort(key=lambda x: x[2], reverse=True)
                for i, (code, name, followers) in enumerate(country_list[:3], 1):
                    print(f"     {i}. {name}: {followers} abonnés")
            
            # Chemin vers les credentials
            # Déterminer le chemin des credentials selon l'environnement
            if os.getenv('K_SERVICE'):  # Cloud Run/Functions
                credentials_path = Path('/tmp/credentials/service_account_credentials.json')
            else:  # Local
                credentials_path = Path(__file__).resolve().parent / 'credentials' / 'service_account_credentials.json'
            
            if not credentials_path.exists():
                # Essayer de créer les credentials depuis une variable d'environnement
                creds_json = os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON')
                if creds_json:
                    # Créer le dossier si nécessaire et si on n'est pas dans /app
                    if not str(credentials_path).startswith('/app'):
                        credentials_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(credentials_path, 'w') as f:
                        f.write(creds_json)
                    print("   ✅ Credentials créés depuis la variable d'environnement")
                else:
                    print(f"   ❌ Erreur: Fichier de credentials non trouvé: {credentials_path}")
                    return None
            else:
                print("   ✅ Credentials trouvés")
            
            # Export vers Google Sheets
            print("\n3. Export vers Google Sheets...")
            exporter = GoogleSheetsExporter(tracker.sheet_name, credentials_path, self.admin_email)
            success = exporter.add_page_statistics(stats)
            
            if success and exporter.spreadsheet:
                # Mettre à jour le mapping avec l'ID du sheet
                self.update_sheet_mapping(org_id, exporter.spreadsheet.id)
                sheet_url = f"https://docs.google.com/spreadsheets/d/{exporter.spreadsheet.id}"
                print(f"\n✅ Export réussi pour {org_name}!")
                print(f"📊 URL du Sheet: {sheet_url}")
                return sheet_url
            else:
                print(f"\n❌ Échec de l'export pour {org_name}")
                return None
        else:
            print("   ❌ Impossible de récupérer les statistiques de followers")
            return None


def main():
    """Fonction principale"""
    print("="*60)
    print("LINKEDIN MULTI-ORGANISATION FOLLOWER STATISTICS TRACKER")
    print("="*60)
    print(f"Date d'exécution: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Créer le tracker
    tracker = MultiOrganizationPageStatsTracker()
    
    if not tracker.organizations:
        print("\n❌ Aucune organisation configurée!")
        print("\nPour configurer des organisations:")
        print("1. Lancez d'abord: python3 discover_organizations.py")
        print("2. Ou créez manuellement 'organizations_config.json' avec le format:")
        print(json.dumps([
            {"id": "123456", "name": "Entreprise A"},
            {"id": "789012", "name": "Entreprise B"}
        ], indent=2))
        sys.exit(1)
    
    print(f"\n📋 Organisations configurées: {len(tracker.organizations)}")
    for org in tracker.organizations:
        print(f"   - {org['name']} (ID: {org['id']})")
    
    print(f"\n⚙️  Configuration:")
    print(f"   - Email admin: {tracker.admin_email}")
    print(f"   - Type de données: Statistiques de followers par pays, séniorité, fonction et industrie")
    
    # Demander confirmation si plus de 5 organisations
    if len(tracker.organizations) > 5:
        print(f"\n⚠️  Attention: {len(tracker.organizations)} organisations à traiter.")
        print("   Cela peut prendre du temps et consommer des quotas API.")
        if os.getenv('AUTOMATED_MODE', 'false').lower() == 'true':
            response = 'o'
            print('🤖 Mode automatisé: réponse automatique \'o\'')
        else:
            response = input("   Continuer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            sys.exit(0)
    
    print("\n🚀 Démarrage du traitement des statistiques de followers...")
    print("⏳ Note: Le traitement inclut des délais pour respecter les quotas Google Sheets")
    
    # Lancer le traitement
    start_time = datetime.now()
    success = tracker.process_all_organizations()
    end_time = datetime.now()
    
    # Afficher le temps d'exécution
    duration = end_time - start_time
    minutes = int(duration.total_seconds() // 60)
    seconds = int(duration.total_seconds() % 60)
    
    print(f"\n⏱️  Temps d'exécution: {minutes}m {seconds}s")
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()