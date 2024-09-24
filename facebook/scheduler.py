# scheduler.py

import schedule
import time
from main import process_data, update_page_metrics

def job():
    """Mise à jour quotidienne des données Facebook."""
    process_data()
    update_page_metrics()
    print("Mise à jour des données Facebook effectuée.")

# Planification quotidienne
schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
