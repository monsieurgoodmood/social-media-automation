# db_operations.py

import sqlite3
from datetime import datetime, timedelta

def insert_post_insights(data):
    """Insère les insights des posts dans la base de données."""
    conn = sqlite3.connect('facebook_data.db')
    cursor = conn.cursor()

    for post in data:
        cursor.execute('''
            INSERT OR REPLACE INTO facebook_post_insights 
            (post_id, created_time, content_type, title, post_url, impression_totale, impression_unique, clics, reactions, interactions_totales) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            post['Post ID'], post['Created Time'], post['Content Type'], post['Title'], post['Post URL'],
            post['Impression Totale'], post['Impression Unique'], post['Clics'], post['Réactions'], post['Interactions Totales']
        ))

    conn.commit()
    conn.close()

def insert_page_metrics(data):
    """Insère les métriques globales de la page dans la base de données."""
    conn = sqlite3.connect('facebook_data.db')
    cursor = conn.cursor()

    cursor.execute('''
        INSERT OR REPLACE INTO facebook_page_metrics 
        (date, fan_count, followers, visites_totales_jour, visites_totales_semaine, visites_totales_28_jours, visites_uniques_connectees_jour, visites_uniques_connectees_semaine, visites_uniques_connectees_28_jours, impressions_jour, impressions_semaine, impressions_28_jours) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        data['Date'], data['Fan Count'], data['Followers'], data['Visites Totales (Jour)'],
        data['Visites Totales (Semaine)'], data['Visites Totales (28 Jours)'], 
        data['Visites Uniques Connectées (Jour)'], data['Visites Uniques Connectées (Semaine)'], 
        data['Visites Uniques Connectées (28 Jours)'], data['Impressions (Jour)'], 
        data['Impressions (Semaine)'], data['Impressions (28 Jours)']
    ))

    conn.commit()
    conn.close()


def purge_old_data():
    """Supprime les données de plus de 30 jours de MySQL."""
    connection = get_mysql_connection()
    cursor = connection.cursor()

    # Calculer la date limite (30 jours avant aujourd'hui)
    date_limit = datetime.now() - timedelta(days=30)

    # Supprimer les anciens posts
    cursor.execute("DELETE FROM post_metrics WHERE created_time < %s", (date_limit,))

    # Supprimer les anciennes métriques de page
    cursor.execute("DELETE FROM page_metrics WHERE date < %s", (date_limit,))

    connection.commit()
    cursor.close()
    connection.close()
