import json
import csv
from datetime import datetime

# Charger les données JSON
with open("metrics_365_days.json", "r") as file:
    data = json.load(file)

# Préparer les données pour le CSV
csv_data = [["Date", "Total Followers", "Sponsored Followers", "Organic Followers"]]

for entry in data:
    elements = entry.get("elements", [])
    for element in elements:
        time_range = element.get("timeIntervals", {}).get("timeRange", {})
        start_timestamp = time_range.get("start", 0)
        date = datetime.utcfromtimestamp(start_timestamp / 1000).strftime('%Y-%m-%d')
        
        value = element.get("value", {})
        total_count = value.get("totalCount", {}).get("long", 0)
        sponsored_value = value.get("typeSpecificValue", {}).get("followerEdgeAnalyticsValue", {}).get("sponsoredValue", 0)
        organic_value = value.get("typeSpecificValue", {}).get("followerEdgeAnalyticsValue", {}).get("organicValue", 0)
        
        csv_data.append([date, total_count, sponsored_value, organic_value])

# Écrire dans un fichier CSV
with open("metrics_365_days.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(csv_data)

print("Fichier CSV généré : metrics_365_days.csv")
