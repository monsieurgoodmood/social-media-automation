# video_metrics.py

from .metrics import VIDEO_METRICS
from api_utils import fetch_insights

def get_video_insights(video_id):
    """Récupère les insights d'une vidéo en utilisant les métriques définies."""
    return fetch_insights(video_id, VIDEO_METRICS)
