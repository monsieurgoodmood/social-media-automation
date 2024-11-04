# main.py

import logging
from flask import Flask, jsonify
import os
from post_metrics_processing import process_post_data
from page_metrics_processing import process_page_metrics
import threading

app = Flask(__name__)

log_file = 'app.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

status = {
    'refresh_token': 'pending',
    'data_processing': 'pending',
    'page_metrics_update': 'pending'
}

@app.route('/')
def home():
    return jsonify({"message": "Facebook automation service is running.", "status": status})

@app.route('/health-check')
def health_check():
    return jsonify({"message": "Health check completed.", "status": status})

def data_processing_task():
    logger.info("Début du traitement des données (posts et métriques de page).")
    status['data_processing'] = 'in_progress'

    try:
        process_post_data()
        process_page_metrics()
        status['data_processing'] = 'success'
        logger.info("Les données ont été traitées avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du traitement des données: {e}")
        status['data_processing'] = 'error'

def start_background_tasks():
    logger.info("Lancement des tâches en arrière-plan.")
    threading.Thread(target=data_processing_task).start()

if __name__ == "__main__":
    start_background_tasks()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))