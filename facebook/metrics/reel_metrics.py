# reel_metrics.py

from metrics import REEL_METRICS
from api_utils import fetch_insights

def get_reel_insights(reel_id):
    """Récupère les insights d'un reel en utilisant les métriques définies."""
    return fetch_insights(reel_id, REEL_METRICS)
