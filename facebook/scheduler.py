# scheduler.py

import schedule
import time
from main import process_data, update_page_metrics
from db_operations import purge_old_data

def job():
    """Mise à jour quotidienne des données Facebook et purge des anciennes données."""
    process_data()
    update_page_metrics()
    purge_old_data()

# Planification quotidienne à 10h00
schedule.every().day.at("10:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
