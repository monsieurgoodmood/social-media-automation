# scheduler.py

import schedule
import time
from main import process_data, update_page_metrics
from facebook_api import refresh_access_token

def job():
    """Mise à jour quotidienne des données Facebook et nettoyage des anciennes données."""
    process_data()
    update_page_metrics()
    print("Mise à jour des données Facebook effectuée.")

def token_refresh_job():
    """Vérifie et met à jour le token d'accès si nécessaire."""
    try:
        new_token = refresh_access_token()
        if new_token:
            print("Token d'accès mis à jour.")
    except Exception as e:
        print(f"Erreur lors de la mise à jour du token: {e}")

# Planification quotidienne à 10h
schedule.every().day.at("10:00").do(job)

# Rafraîchir le token toutes les semaines
schedule.every().week.do(token_refresh_job)

while True:
    schedule.run_pending()
    time.sleep(1)