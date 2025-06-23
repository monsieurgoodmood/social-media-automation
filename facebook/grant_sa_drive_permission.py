#!/usr/bin/env python3

"""
Ajoute automatiquement le service account Facebook Automation
sur tous les Google Sheets listés dans les configs :

- configs/pages_config.json
- configs/page_metrics_mapping.json
- configs/posts_lifetime_mapping.json
- configs/posts_metadata_mapping.json
"""

import json
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Configuration
SERVICE_ACCOUNT_EMAIL = 'facebook-automation-sa@authentic-ether-457013-t5.iam.gserviceaccount.com'

CONFIG_FILES = [
    'configs/pages_config.json',
    'configs/page_metrics_mapping.json',
    'configs/posts_lifetime_mapping.json',
    'configs/posts_metadata_mapping.json'
]

# Scopes Drive nécessaires
SCOPES = ['https://www.googleapis.com/auth/drive']

# Charger les credentials du compte admin (owner des Google Sheets)
creds = service_account.Credentials.from_service_account_file(
    'credentials/service_account_credentials.json', scopes=SCOPES
)

drive_service = build('drive', 'v3', credentials=creds)

# Fonction pour ajouter la permission
def add_permission(spreadsheet_id, page_name):
    permission = {
        'type': 'user',
        'role': 'writer',
        'emailAddress': SERVICE_ACCOUNT_EMAIL
    }

    try:
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission,
            sendNotificationEmail=False
        ).execute()
        print(f"✅ OK - Sheet {spreadsheet_id} ({page_name})")
        return True
    except Exception as e:
        print(f"❌ Erreur - Sheet {spreadsheet_id} ({page_name}): {e}")
        return False

# Process global
total_sheets = 0
total_success = 0
total_error = 0

for config_file in CONFIG_FILES:
    print(f"\n🔍 Traitement de {config_file}...")

    with open(config_file, 'r') as f:
        config = json.load(f)

    # pages_config.json a un format particulier : { "pages": { ... } }
    if 'pages' in config:
        pages = config['pages']
    else:
        pages = config  # les autres JSON ont un format directement { page_id: { ... } }

    print(f"  ➜ {len(pages)} pages détectées.")

    for page_id, page_info in pages.items():
        spreadsheet_id = page_info.get('spreadsheet_id')
        page_name = page_info.get('page_name', 'Page inconnue')

        if not spreadsheet_id:
            print(f"⚠️  Pas de spreadsheet_id pour la page {page_id} ({page_name}) — on passe")
            continue

        total_sheets += 1

        success = add_permission(spreadsheet_id, page_name)
        if success:
            total_success += 1
        else:
            total_error += 1

# Résultat final
print("\n🎉 Process terminé.")
print(f"📊 Résumé:")
print(f"  - Total sheets traitées : {total_sheets}")
print(f"  - Succès : {total_success}")
print(f"  - Erreurs : {total_error}")
