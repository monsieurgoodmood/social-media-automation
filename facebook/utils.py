# utils.py

import pandas as pd
import numpy as np
import csv
import os
import logging
from metrics import POST_METRICS  # Assurez-vous que metrics.py définit POST_METRICS


logger = logging.getLogger(__name__)

def convert_to_json_compatible(value):
    """Convertit les types numpy et pandas en types natifs Python pour compatibilité JSON."""
    if isinstance(value, (np.integer, int)):
        return int(value)
    elif isinstance(value, (np.floating, float)):
        return float(value)
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    elif pd.isna(value):
        return None
    return value

def clean_data_for_json(df):
    """Remplace les valeurs non conformes (NaN, inf, -inf) dans le DataFrame pour compatibilité JSON."""
    df = df.replace([np.inf, -np.inf], None)  # Remplace inf et -inf par None
    df = df.fillna(0)  # Remplace NaN par 0, ou utilisez None si vous préférez
    return df

def generate_title(post):
    """Génère un titre pour le post basé sur son message."""
    message = post.get('message', '')
    return message[:10] + "..." if message else "Post"

def save_and_upload(data, filename, google_sheet_name, tab_name, exclude_date_in_sheet=False):
    """Sauvegarde les données dans un fichier CSV et les télécharge sur Google Sheets.
    
    Args:
        data (list): Les données à sauvegarder et uploader.
        filename (str): Nom du fichier CSV pour sauvegarder localement.
        google_sheet_name (str): Nom de la feuille Google Sheets.
        tab_name (str): Nom de l'onglet de la feuille Google Sheets.
        exclude_date_in_sheet (bool): Si True, supprime la colonne 'Date' pour l'upload vers Google Sheets.
    """
    folder_path = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(folder_path, exist_ok=True)
    file_path = os.path.join(folder_path, filename)

    # Crée le DataFrame et le sauvegarde en CSV
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False, quoting=csv.QUOTE_ALL)

    # Si exclude_date_in_sheet est True, on retire la colonne 'Date' avant l'upload vers Google Sheets
    if exclude_date_in_sheet and 'Date' in df.columns:
        df = df.drop(columns=['Date'])

    # Upload vers Google Sheets
    from google_sheets import upload_to_google_sheets
    try:
        upload_to_google_sheets(df, google_sheet_name, tab_name)
        logger.info(f"Upload vers Google Sheets '{google_sheet_name}', onglet '{tab_name}' terminé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de l'upload vers Google Sheets : {e}")


def calculate_metrics(row):
    """Calcule les métriques personnalisées pour les posts, avec le taux d'interaction en pourcentage et une valeur par défaut de zéro pour les métriques absentes."""
    
    # Initialiser toutes les métriques de POST_METRICS à zéro si elles sont absentes
    for metric in POST_METRICS:
        row[metric] = row.get(metric, 0)
    
    # Calculs des métriques agrégées
    row['Impressions Totales'] = (row['post_impressions_organic'] + row['post_impressions_paid'] + row['post_impressions_viral'])
    row['Engagement Total'] = (row['post_reactions_like_total'] + row['post_reactions_love_total'] +
                               row['post_reactions_wow_total'] + row['post_reactions_haha_total'] + row['post_clicks'])
    
    # Calcul du taux d'interaction en pourcentage
    row['Taux d\'interaction'] = (row['Engagement Total'] / row['Impressions Totales'] * 100) if row['Impressions Totales'] > 0 else 0
    
    return row

