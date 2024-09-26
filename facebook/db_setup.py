# db_setup.py

import sqlite3

def setup_database():
    """Configure la base de données et crée les tables nécessaires."""
    conn = sqlite3.connect('facebook_data.db')
    cursor = conn.cursor()

    # Table pour les posts Facebook
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facebook_post_insights (
            post_id TEXT PRIMARY KEY,
            created_time TEXT,
            content_type TEXT,
            title TEXT,
            post_url TEXT,
            impression_totale INTEGER,
            impression_unique INTEGER,
            clics INTEGER,
            reactions INTEGER,
            interactions_totales INTEGER
        )
    ''')

    # Table pour les métriques globales de la page Facebook
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS facebook_page_metrics (
            date TEXT PRIMARY KEY,
            fan_count INTEGER,
            followers INTEGER,
            visites_totales_jour INTEGER,
            visites_totales_semaine INTEGER,
            visites_totales_28_jours INTEGER,
            visites_uniques_connectees_jour INTEGER,
            visites_uniques_connectees_semaine INTEGER,
            visites_uniques_connectees_28_jours INTEGER,
            impressions_jour INTEGER,
            impressions_semaine INTEGER,
            impressions_28_jours INTEGER
        )
    ''')

    conn.commit()
    conn.close()
