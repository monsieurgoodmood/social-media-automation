# main.py

from data_processing import process_data, update_page_metrics

if __name__ == "__main__":
    process_data()  # Mise à jour des données des posts
    update_page_metrics()  # Mise à jour des métriques de la page