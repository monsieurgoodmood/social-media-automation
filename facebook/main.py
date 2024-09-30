from flask import Flask
import os
from data_processing import process_data, update_page_metrics
import threading
from facebook_api import refresh_access_token


app = Flask(__name__)

@app.route('/')
def home():
    return "Facebook automation service is running."


# Fonction de traitement en arrière-plan
def background_task():
    try:
        # Rafraîchir le token d'accès
        new_token = refresh_access_token()
        if new_token:
            print("Nouveau token obtenu et mis à jour.")
    except Exception as e:
        print(f"Erreur lors du rafraîchissement du token: {e}")
    
    # Traiter les données après avoir rafraîchi le token
    process_data()
    update_page_metrics()


if __name__ == "__main__":
    thread = threading.Thread(target=background_task)
    thread.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
