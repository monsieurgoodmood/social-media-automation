# utils.py

import pandas as pd
import numpy as np
import csv
import os
import logging
from metrics import POST_METRICS  # Assurez-vous que metrics.py définit POST_METRICS
import json


logger = logging.getLogger(__name__)

# Fonction à appliquer dans les données avant l'upload
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
    return str(value)  # Dernier recours : convertir en chaîne


def clean_data_for_json(df):
    """Remplace les valeurs non conformes (NaN, inf, -inf) dans le DataFrame pour compatibilité JSON."""
    df = df.replace([np.inf, -np.inf], None)  # Remplace inf et -inf par None
    df = df.fillna(0)  # Rempl/home/arthur/code/social-media-automation/linkedinace NaN par 0, ou utilisez None si vous préférez
    return df

def generate_title(post):
    """Génère un titre pour le post basé sur son message."""
    message = post.get('message', '')
    return message[:10] + "..." if message else "Post"

def save_and_upload(data, filename, sheet_name, tab_name, exclude_date_in_sheet=False):
    try:
        # Conversion explicite des Timestamp en string
        for col in data.columns:
            if pd.api.types.is_datetime64_any_dtype(data[col]):
                data[col] = data[col].dt.strftime('%Y-%m-%d')
                
        data_records = data.to_dict(orient="records")
        try:
            json.dumps(data_records)  # Vérifie si la sérialisation fonctionne
        except TypeError as e:
            logger.error(f"Erreur de sérialisation JSON : {e}")
            return
        ...
    except Exception as e:
        logger.error(f"Erreur lors de l'upload vers Google Sheets : {e}")
        raise


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

def normalize_metrics(df, expected_metrics):
    """
    Normalise le DataFrame pour inclure toutes les métriques attendues, même si elles sont manquantes,
    en les remplissant avec des valeurs par défaut (zéros).
    
    Args:
        df (pd.DataFrame): DataFrame contenant les données actuelles.
        expected_metrics (list): Liste des métriques attendues.
    
    Returns:
        pd.DataFrame: DataFrame normalisé.
    """
    # Vérifiez si toutes les métriques attendues sont présentes
    missing_metrics = [metric for metric in expected_metrics if metric not in df.columns]
    
    if missing_metrics:
        for metric in missing_metrics:
            df[metric] = 0  # Ajoutez une colonne pour les métriques manquantes avec des valeurs par défaut

    # Réorganisez les colonnes pour correspondre à l'ordre attendu
    ordered_columns = ["Date"] + expected_metrics
    df = df.reindex(columns=ordered_columns, fill_value=0)

    return df
