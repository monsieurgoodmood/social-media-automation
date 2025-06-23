#!/usr/bin/env python3
"""
Script d'initialisation pour migrer les configurations locales vers Google Cloud Storage
À exécuter une fois avant le déploiement
"""
import os
import sys
import json
from google.cloud import storage
from datetime import datetime

# Configuration
PROJECT_ID = "authentic-ether-457013-t5"
BUCKET_NAME = f"{PROJECT_ID}-facebook-configs"

def create_bucket_if_not_exists(client, bucket_name):
    """Crée le bucket s'il n'existe pas"""
    try:
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            bucket = client.create_bucket(bucket_name, location="europe-west1")
            print(f"✅ Bucket créé: gs://{bucket_name}")
        else:
            print(f"✅ Bucket existant: gs://{bucket_name}")
        return bucket
    except Exception as e:
        print(f"❌ Erreur lors de la création du bucket: {e}")
        return None

def upload_config_file(bucket, local_path, gcs_path):
    """Upload un fichier de configuration vers GCS"""
    try:
        blob = bucket.blob(gcs_path)
        
        # Lire le fichier local
        with open(local_path, 'r') as f:
            data = json.load(f)
        
        # Ajouter des métadonnées
        data["_migrated_at"] = datetime.now().isoformat()
        data["_source"] = "local_migration"
        
        # Upload vers GCS
        blob.upload_from_string(
            json.dumps(data, indent=2),
            content_type="application/json"
        )
        
        print(f"  ✓ {local_path} → gs://{bucket.name}/{gcs_path}")
        return True
        
    except FileNotFoundError:
        print(f"  ⚠️  {local_path} non trouvé - ignoré")
        return False
    except Exception as e:
        print(f"  ❌ Erreur pour {local_path}: {e}")
        return False

def main():
    print("🚀 Initialisation des configurations Facebook dans GCS")
    print(f"   Projet: {PROJECT_ID}")
    print(f"   Bucket: gs://{BUCKET_NAME}")
    print()
    
    # Initialiser le client Storage
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/service_account_credentials.json"
    client = storage.Client(project=PROJECT_ID)
    
    # Créer le bucket
    bucket = create_bucket_if_not_exists(client, BUCKET_NAME)
    if not bucket:
        sys.exit(1)
    
    # Fichiers à migrer
    config_files = {
        "configs/pages_config.json": "configs/pages_config.json",
        "configs/page_tokens.json": "configs/page_tokens.json",
        "configs/page_metrics_mapping.json": "configs/page_metrics_mapping.json",
        "configs/posts_lifetime_mapping.json": "configs/posts_lifetime_mapping.json",
        "configs/posts_metadata_mapping.json": "configs/posts_metadata_mapping.json",
    }
    
    print("\n📦 Migration des fichiers de configuration:")
    success_count = 0
    for local_path, gcs_path in config_files.items():
        if upload_config_file(bucket, local_path, gcs_path):
            success_count += 1
    
    print(f"\n✅ Migration terminée: {success_count}/{len(config_files)} fichiers migrés")
    
    # Créer les dossiers vides
    print("\n📁 Création des dossiers:")
    for folder in ["reports/", "temp/"]:
        blob = bucket.blob(folder)
        blob.upload_from_string("")
        print(f"  ✓ {folder}")
    
    # Vérifier le contenu du bucket
    print(f"\n📋 Contenu du bucket gs://{BUCKET_NAME}:")
    for blob in bucket.list_blobs():
        print(f"  - {blob.name}")
    
    print("\n✅ Initialisation terminée!")
    print("\n⚠️  IMPORTANT: Assurez-vous que le service account a les permissions nécessaires:")
    print(f"   - {BUCKET_NAME}: Storage Object Admin")
    print("   - Secret Manager: Secret Manager Secret Accessor")
    print("\n💡 Prochaine étape: lancer deploy_facebook_whatsthedata.sh")

if __name__ == "__main__":
    main()