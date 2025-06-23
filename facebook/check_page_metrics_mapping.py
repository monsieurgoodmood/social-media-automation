# check_page_metrics_mapping.py
import json
import os

# Chemin local ou Cloud Storage
json_path = "/home/arthur/code/social-media-automation/facebook/configs/page_metrics_mapping.json"

with open(json_path, "r") as f:
    data = json.load(f)

print(f"✅ Pages dans page_metrics_mapping.json: {len(data)}")
print("-" * 50)

for page_id, info in data.items():
    page_name = info.get("page_name", "Unknown")
    spreadsheet_id = info.get("spreadsheet_id", "MISSING")
    if not spreadsheet_id or spreadsheet_id == "MISSING":
        print(f"❌ MISSING spreadsheet_id → {page_name} ({page_id})")
    else:
        print(f"✓ {page_name} ({page_id}) → {spreadsheet_id}")

print("-" * 50)
print("Fin de la vérification.")
