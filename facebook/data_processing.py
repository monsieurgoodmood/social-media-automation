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
    """Ajoute des données dans un fichier CSV."""
    folder_path = os.path.join(os.path.dirname(__file__), 'data')
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    file_path = os.path.join(folder_path, filename)
    df = pd.DataFrame(data)
    
    if not os.path.exists(file_path):
        df.to_csv(file_path, index=False)
    else:
        df.to_csv(file_path, mode='a', header=False, index=False)
    
    print(f"Données sauvegardées dans {file_path}")

def clean_old_posts(sheet_name, days=45):
    """Supprime les posts de plus de 45 jours dans Google Sheets."""
    client = get_google_sheets_client()
    sheet = client.open(sheet_name).sheet1
    
    data = sheet.get_all_records()
    updated_data = [row for row in data if (pd.Timestamp.now() - pd.to_datetime(row['Created Time'])).days <= days]
    
    sheet.clear()
    sheet.append_rows(updated_data)

def process_data():
    """Traite et stocke les données des posts Facebook."""
    posts = get_facebook_posts()
    all_post_insights = []

    for post in posts:
        post_id = post['id']
        insights = get_post_insights(post_id)
        
        impressions = None
        engagements = None
        total_reactions = None
        reactions_dict = None

        if insights:
            for insight in insights:
                if insight['name'] == 'post_impressions':
                    impressions = insight['values'][0]['value']
                elif insight['name'] == 'post_engaged_users':
                    engagements = insight['values'][0]['value']
                elif insight['name'] == 'post_reactions_by_type_total':
                    reactions_dict = insight['values'][0]['value']
        
        total_reactions = calculate_total_reactions(reactions_dict)
        content_type = identify_post_type(post.get('attachments', None))
        title = generate_title(post, content_type)
        post_url = f"https://www.facebook.com/{FACEBOOK_PAGE_ID}/posts/{post_id}"
        
        post_data = {
            'Post ID': post_id,
            'Created Time': post['created_time'],
            'Content Type': content_type,
            'Title': title,
            'Post URL': post_url,
            'Impressions': impressions or 'N/A',
            'Engagements': engagements or 'N/A',
            'Reactions': total_reactions or 'N/A',
        }
        all_post_insights.append(post_data)

    append_to_csv(all_post_insights)

def update_page_metrics():
    """Récupère et sauvegarde les métriques globales de la page."""
    metrics = get_page_metrics()
    page_data = {
        'Date': pd.Timestamp.now(),
        'Fan Count': metrics.get('fan_count', 'N/A'),
        'Followers': metrics.get('followers_count', 'N/A')
    }
    append_to_csv([page_data], filename='facebook_page_metrics.csv')
