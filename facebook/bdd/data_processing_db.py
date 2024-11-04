# data_processing.py

import pandas as pd
from config import FACEBOOK_PAGE_ID, DB_HOST, DB_USER, DB_PASSWORD, DB_NAME
from facebook_api import get_facebook_posts, get_post_insights, get_page_metrics
import mysql.connector

# Connexion MySQL
def get_mysql_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

# Identifie le type de contenu (texte, image, vidéo, etc.)
def identify_post_type(attachments):
    if attachments and 'data' in attachments:
        attachment = attachments['data'][0]
        if 'media_type' in attachment:
            return 'Image' if attachment['media_type'] == 'photo' else 'Video'
        elif 'type' in attachment:
            return 'Event' if attachment['type'] == 'event' else 'Link'
    return 'Text'

# Génère un titre basé sur le contenu du post
def generate_title(post, content_type):
    message = post.get('message', '')
    return message.split('.')[0][:50] + "..." if message else f"{content_type} Post"

# Calcule le total des réactions
def calculate_total_reactions(reactions_dict):
    return sum(reactions_dict.values()) if reactions_dict and isinstance(reactions_dict, dict) else 0

# Insère les métriques des posts dans MySQL
def insert_post_metrics_to_db(post_data):
    connection = get_mysql_connection()
    cursor = connection.cursor()

    query = """
    INSERT INTO post_metrics
    (post_id, created_time, content_type, title, post_url, impression_total, impression_unique, clicks, reactions, total_interactions)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data = (
        post_data['Post ID'], post_data['Created Time'], post_data['Content Type'], post_data['Title'],
        post_data['Post URL'], post_data['Impression Totale'], post_data['Impression Unique'],
        post_data['Clics'], post_data['Réactions'], post_data['Interactions Totales']
    )

    cursor.execute(query, data)
    connection.commit()
    cursor.close()
    connection.close()

# Traite et insère les données des posts Facebook dans MySQL
def process_data():
    posts = get_facebook_posts()

    for post in posts:
        post_id = post['id']
        insights = get_post_insights(post_id)

        impressions = organic_impressions = clicks = total_reactions = 0
        reactions_dict = None

        for insight in insights:
            if insight['name'] == 'post_impressions_organic':
                organic_impressions = insight['values'][0]['value'] or 0
            elif insight['name'] == 'post_impressions_unique':
                impressions = insight['values'][0]['value'] or 0
            elif insight['name'] == 'post_clicks':
                clicks = insight['values'][0]['value'] or 0
            elif insight['name'] == 'post_reactions_by_type_total':
                reactions_dict = insight['values'][0]['value']

        total_reactions = calculate_total_reactions(reactions_dict)
        interactions_total = clicks + total_reactions
        content_type = identify_post_type(post.get('attachments', None))
        title = generate_title(post, content_type)
        post_url = f"https://www.facebook.com/{FACEBOOK_PAGE_ID}/posts/{post_id}"

        post_data = {
            'Post ID': post_id,
            'Created Time': post['created_time'],
            'Content Type': content_type,
            'Title': title,
            'Post URL': post_url,
            'Impression Totale': organic_impressions,
            'Impression Unique': impressions,
            'Clics': clicks,
            'Réactions': total_reactions,
            'Interactions Totales': interactions_total
        }

        insert_post_metrics_to_db(post_data)

# Insère les métriques des pages dans MySQL
def insert_page_metrics_to_db(metrics):
    connection = get_mysql_connection()
    cursor = connection.cursor()

    query = """
    INSERT INTO page_metrics
    (date, fan_count, followers_count, page_views_total_day, page_views_total_week, page_views_total_28_days,
    page_views_logged_in_unique_day, page_views_logged_in_unique_week, page_views_logged_in_unique_28_days,
    page_impressions_day, page_impressions_week, page_impressions_28_days)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    data = (
        pd.Timestamp.now().date(),
        metrics.get('fan_count', 0),
        metrics.get('followers_count', 0),
        metrics.get('page_views_total_day', 0),
        metrics.get('page_views_total_week', 0),
        metrics.get('page_views_total_28_days', 0),
        metrics.get('page_views_logged_in_unique_day', 0),
        metrics.get('page_views_logged_in_unique_week', 0),
        metrics.get('page_views_logged_in_unique_28_days', 0),
        metrics.get('page_impressions_day', 0),
        metrics.get('page_impressions_week', 0),
        metrics.get('page_impressions_28_days', 0)
    )

    cursor.execute(query, data)
    connection.commit()
    cursor.close()
    connection.close()

# Récupère et insère les métriques globales de la page dans MySQL
def update_page_metrics():
    metrics = get_page_metrics()
    insert_page_metrics_to_db(metrics)
