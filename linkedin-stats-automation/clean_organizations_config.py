#!/usr/bin/env python3
"""
Script pour nettoyer et dédupliquer le fichier organizations_config.json
"""

import json
import os
from datetime import datetime

def clean_organizations_config(filename='organizations_config.json'):
    """Nettoie le fichier de configuration des organisations"""
    
    if not os.path.exists(filename):
        print(f"❌ Le fichier {filename} n'existe pas")
        return False
    
    print(f"🧹 Nettoyage du fichier {filename}")
    print("="*50)
    
    # Lire le fichier actuel
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            organizations = json.load(f)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier: {e}")
        return False
    
    print(f"📋 {len(organizations)} entrées trouvées dans le fichier original")
    
    # Dédupliquer par ID
    seen_ids = set()
    cleaned_orgs = []
    duplicates_removed = 0
    
    for org in organizations:
        org_id = org.get('id', '')
        org_name = org.get('name', 'Sans nom')
        
        if not org_id:
            print(f"  ⚠️  Entrée ignorée (pas d'ID): {org_name}")
            continue
        
        if org_id not in seen_ids:
            seen_ids.add(org_id)
            
            # Garder seulement les champs essentiels
            cleaned_org = {
                'id': str(org_id),
                'name': str(org_name)
            }
            
            # Ajouter vanity_name si disponible (utile pour les URLs)
            if 'vanity_name' in org and org['vanity_name']:
                cleaned_org['vanity_name'] = str(org['vanity_name'])
            
            cleaned_orgs.append(cleaned_org)
            print(f"  ✅ Conservé: {org_name} (ID: {org_id})")
        else:
            duplicates_removed += 1
            print(f"  🗑️  Doublon supprimé: {org_name} (ID: {org_id})")
    
    # Trier par nom pour un fichier plus lisible
    cleaned_orgs.sort(key=lambda x: x['name'])
    
    # Créer une sauvegarde du fichier original
    backup_filename = f"{filename}.backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        with open(backup_filename, 'w', encoding='utf-8') as f:
            json.dump(organizations, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Sauvegarde créée: {backup_filename}")
    except Exception as e:
        print(f"\n⚠️  Impossible de créer la sauvegarde: {e}")
    
    # Sauvegarder le fichier nettoyé
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(cleaned_orgs, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Fichier nettoyé: {len(cleaned_orgs)} organisations uniques")
        if duplicates_removed > 0:
            print(f"🗑️  {duplicates_removed} doublon(s) supprimé(s)")
        
        # Afficher la liste finale
        print("\n📊 Organisations configurées pour l'automatisation:")
        for i, org in enumerate(cleaned_orgs, 1):
            vanity_info = f" - {org['vanity_name']}" if 'vanity_name' in org else ""
            print(f"  {i}. {org['name']} (ID: {org['id']}){vanity_info}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erreur lors de la sauvegarde: {e}")
        return False

def validate_organizations_config(filename='organizations_config.json'):
    """Valide le fichier de configuration des organisations"""
    
    if not os.path.exists(filename):
        print(f"❌ Le fichier {filename} n'existe pas")
        return False
    
    print(f"🔍 Validation du fichier {filename}")
    print("="*40)
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            organizations = json.load(f)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier: {e}")
        return False
    
    if not isinstance(organizations, list):
        print("❌ Le fichier doit contenir une liste d'organisations")
        return False
    
    issues = []
    ids = set()
    
    for i, org in enumerate(organizations):
        if not isinstance(org, dict):
            issues.append(f"Entrée {i+1}: n'est pas un objet valide")
            continue
        
        # Vérifier les champs obligatoires
        if 'id' not in org:
            issues.append(f"Entrée {i+1}: champ 'id' manquant")
        elif org['id'] in ids:
            issues.append(f"Entrée {i+1}: ID '{org['id']}' en doublon")
        else:
            ids.add(org['id'])
        
        if 'name' not in org:
            issues.append(f"Entrée {i+1}: champ 'name' manquant")
        elif not org['name'].strip():
            issues.append(f"Entrée {i+1}: nom vide")
    
    if issues:
        print(f"❌ {len(issues)} problème(s) détecté(s):")
        for issue in issues:
            print(f"  • {issue}")
        return False
    else:
        print(f"✅ Fichier valide: {len(organizations)} organisations configurées")
        return True

def main():
    """Fonction principale"""
    print("🧹 NETTOYAGE DU FICHIER ORGANIZATIONS_CONFIG.JSON")
    print("="*60)
    
    # Valider d'abord
    if validate_organizations_config():
        print("\n🤔 Le fichier semble déjà propre.")
        response = input("Voulez-vous quand même le nettoyer ? (o/N): ")
        if response.lower() != 'o':
            print("Annulé.")
            return
    
    # Nettoyer
    success = clean_organizations_config()
    
    if success:
        print("\n🎉 Nettoyage terminé avec succès!")
        print("Vous pouvez maintenant utiliser le fichier avec vos scripts d'automatisation.")
    else:
        print("\n❌ Échec du nettoyage.")

if __name__ == "__main__":
    main()