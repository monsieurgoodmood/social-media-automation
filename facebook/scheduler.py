# scheduler.py

import schedule
import time
import threading
import logging
from post_metrics_processing import process_post_data
from facebook_api import refresh_access_token

logging.basicConfig(filename='scheduler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def job():
    """Tâche quotidienne pour mettre à jour les données des posts."""
    try:
        logging.info("Début de la mise à jour quotidienne des posts.")
        process_post_data()  # Mise à jour des posts
        logging.info("Mise à jour quotidienne terminée.")
    except Exception as e:
        logging.error(f"Erreur dans la tâche quotidienne : {e}")

def token_refresh_job():
    """Rafraîchit le token d'accès de manière hebdomadaire."""
    try:
        new_token = refresh_access_token()
        if new_token:
            logging.info("Token d'accès rafraîchi avec succès.")
    except Exception as e:
        logging.error(f"Erreur de rafraîchissement du token : {e}")

# Planification des tâches
schedule.every().day.at("10:00").do(job)
schedule.every().week.do(token_refresh_job)

if __name__ == "__main__":
    logging.info("Démarrage du scheduler.")
    while True:
        schedule.run_pending()
        time.sleep(1)
