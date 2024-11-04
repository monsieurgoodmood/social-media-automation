# post_metrics.py

from .metrics import POST_METRICS
from .api_utils import fetch_insights

def get_post_insights(post_id):
    """Récupère les insights d'un post en utilisant les métriques disponibles."""
    return fetch_insights(post_id, POST_METRICS)
