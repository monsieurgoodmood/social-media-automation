# data_processing.py

import csv
import os
import pandas as pd
from config import FACEBOOK_PAGE_ID
from facebook_api import get_facebook_posts, get_post_insights, get_page_metrics

def identify_post_type(attachments):
    """Identifie le type de post (texte, image, vidéo, etc.)."""
    if attachments and 'data' in attachments:
        attachment = attachments['data'][0]
        if 'media_type' in attachment:
            media_type = attachment['media_type']
            if media_type == 'photo':
                return 'Image'
            elif media_type == 'video':
                return 'Video'
        elif 'type' in attachment:
            attachment_type = attachment['type']
            if attachment_type == 'event':
                return 'Event'
            elif attachment_type == 'share':
                return 'Link'
    return 'Text'

def generate_title(post, content_type):
    """Génère un titre basé sur le contenu du post."""
    message = post.get('message', '')
    if content_type == 'Text':
        return message.split('.')[0][:50] + "..." if message else "Text Post"
    return f"{content_type} Post"

def calculate_total_reactions(reactions_dict):
    """Calcule le total des réactions."""
    if reactions_dict and isinstance(reactions_dict, dict):
        return sum(reactions_dict.values())
    return 0

def append_to_csv(data, filename='facebook_post_insights.csv'):
    """Ajoute des données dans un fichier CSV en gérant les sauts de ligne et guillemets."""
    folder_path = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, filename)

    # Définir les colonnes avec les nouvelles métriques
    if filename == 'facebook_post_insights.csv':
        # Réordonner pour afficher "Impression Totale" avant "Impression Unique"
        fieldnames = ['Post ID', 'Created Time', 'Content Type', 'Title', 'Post URL', 'Impression Totale', 'Impression Unique', 'Clics', 'Réactions', 'Interactions Totales']
    elif filename == 'facebook_page_metrics.csv':
        # Ajout des colonnes des différentes périodes (day, week, days_28)
        fieldnames = [
            'Date', 'Fan Count', 'Followers', 'Visites Totales (Jour)', 'Visites Totales (Semaine)', 'Visites Totales (28 Jours)',
            'Visites Uniques Connectées (Jour)', 'Visites Uniques Connectées (Semaine)', 'Visites Uniques Connectées (28 Jours)',
            'Impressions (Jour)', 'Impressions (Semaine)', 'Impressions (28 Jours)'
        ]
    else:
        raise ValueError("Unrecognized filename for CSV export")

    # Ouverture du fichier CSV et gestion des sauts de ligne
    with open(file_path, 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_ALL, escapechar='\\')

        # Écrire les en-têtes si le fichier est vide
        if os.stat(file_path).st_size == 0:
            writer.writeheader()

        # Écrire chaque ligne de données
        for row in data:
            if 'Title' in row:
                row['Title'] = clean_text(row['Title'])  # Nettoyer le titre
            writer.writerow(row)

    print(f"Données sauvegardées dans {file_path}")


def process_data():
    """Traite et stocke les données des posts Facebook."""
    posts = get_facebook_posts()
    all_post_insights = []

    for post in posts:
        post_id = post['id']
        insights = get_post_insights(post_id)

        # Initialisation des variables de collecte des métriques
        impressions = organic_impressions = clicks = total_reactions = 0
        reactions_dict = None

        # Parcours des insights du post
        if insights:
            for insight in insights:
                if insight['name'] == 'post_impressions_organic':
                    organic_impressions = insight['values'][0]['value'] or 0
                elif insight['name'] == 'post_impressions_unique':
                    impressions = insight['values'][0]['value'] or 0
                elif insight['name'] == 'post_clicks':
                    clicks = insight['values'][0]['value'] or 0
                elif insight['name'] == 'post_reactions_by_type_total':
                    reactions_dict = insight['values'][0]['value']

        # Calcul des réactions totales et des interactions totales (clics + réactions)
        total_reactions = calculate_total_reactions(reactions_dict)
        interactions_total = clicks + total_reactions

        # Identifier le type de contenu et générer le titre du post
        content_type = identify_post_type(post.get('attachments', None))
        title = generate_title(post, content_type)
        post_url = f"https://www.facebook.com/{FACEBOOK_PAGE_ID}/posts/{post_id}"
        
        # Ajouter les données du post dans le dictionnaire
        post_data = {
            'Post ID': post_id,
            'Created Time': post['created_time'],
            'Content Type': content_type,
            'Title': clean_text(title),
            'Post URL': post_url,
            'Impression Totale': organic_impressions,
            'Impression Unique': impressions,
            'Clics': clicks,
            'Réactions': total_reactions,
            'Interactions Totales': interactions_total,
        }

        all_post_insights.append(post_data)

    # Appel à la fonction pour écrire les données dans le CSV
    append_to_csv(all_post_insights)


def update_page_metrics():
    """Récupère et sauvegarde les métriques globales de la page."""
    metrics = get_page_metrics()
    page_data = {
        'Date': pd.Timestamp.now(),
        'Fan Count': metrics.get('fan_count', 0),
        'Followers': metrics.get('followers_count', 0),
        'Visites Totales (Jour)': metrics.get('page_views_total_day', 0),
        'Visites Totales (Semaine)': metrics.get('page_views_total_week', 0),
        'Visites Totales (28 Jours)': metrics.get('page_views_total_days_28', 0),
        'Visites Uniques Connectées (Jour)': metrics.get('page_views_logged_in_unique_day', 0),
        'Visites Uniques Connectées (Semaine)': metrics.get('page_views_logged_in_unique_week', 0),
        'Visites Uniques Connectées (28 Jours)': metrics.get('page_views_logged_in_unique_days_28', 0),
        'Impressions (Jour)': metrics.get('page_impressions_day', 0),
        'Impressions (Semaine)': metrics.get('page_impressions_week', 0),
        'Impressions (28 Jours)': metrics.get('page_impressions_days_28', 0)
    }
    append_to_csv([page_data], filename='facebook_page_metrics.csv')


def clean_text(text):
    """Nettoie le texte pour supprimer les sauts de ligne et espaces multiples."""
    if isinstance(text, str):
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = ' '.join(text.split())
    return text
