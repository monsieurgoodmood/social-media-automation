# db_operations.py

import mysql.connector
from config import DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
from datetime import datetime, timedelta

def get_mysql_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Purge des données de plus de 30 jours
def purge_old_data():
    connection = get_mysql_connection()
    cursor = connection.cursor()
    
    date_limit = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    # Supprimer les anciens posts
    cursor.execute("DELETE FROM post_metrics WHERE created_time < %s", (date_limit,))
    
    # Supprimer les anciennes métriques de page
    cursor.execute("DELETE FROM page_metrics WHERE date < %s", (date_limit,))
    
    connection.commit()
    cursor.close()
    connection.close()

