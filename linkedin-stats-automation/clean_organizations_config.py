#!/usr/bin/env python3
"""
Script pour nettoyer et dédupliquer le fichier organizations_config.json
"""

import json

def clean_organizations_config():
    """Nettoie le fichier de configuration des organisations"""
    
    # Lire le fichier actuel
    with open('organizations_config.json', 'r', encoding='utf-8') as f:
        organizations = json.load(f)
    
    print(f"📋 {len(organizations)} entrées trouvées dans le fichier original")
    
    # Dédupliquer par ID
    seen_ids = set()
    cleaned_orgs = []
    
    for org in organizations:
        org_id = org['id']
        
        if org_id not in seen_ids:
            seen_ids.add(org_id)
            # Garder seulement les champs essentiels
            cleaned_org = {
                'id': org['id'],
                'name': org['name']
            }
            # Ajouter vanity_name si disponible (utile pour les URLs)
            if 'vanity_name' in org:
                cleaned_org['vanity_name'] = org['vanity_name']
            
            cleaned_orgs.append(cleaned_org)
        else:
            print(f"  ⚠️  Doublon supprimé: {org['name']} (ID: {org_id})")
    
    # Trier par nom
    cleaned_orgs.sort(key=lambda x: x['name'])
    
    # Sauvegarder le fichier nettoyé
    with open('organizations_config.json', 'w', encoding='utf-8') as f:
        json.dump(cleaned_orgs, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Fichier nettoyé: {len(cleaned_orgs)} organisations uniques")
    
    # Afficher la liste finale
    print("\n📊 Organisations configurées pour l'automatisation:")
    for i, org in enumerate(cleaned_orgs, 1):
        print(f"{i}. {org['name']} (ID: {org['id']})")
    
    return cleaned_orgs

if __name__ == "__main__":
    clean_organizations_config()