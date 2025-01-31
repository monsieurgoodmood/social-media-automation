# page_metrics.py

from .metrics import PAGE_METRICS
from .api_utils import fetch_insights
from main.config import FACEBOOK_PAGE_ID

def get_page_insights():
    """Récupère les insights complets d'une page Facebook en utilisant les métriques disponibles."""
    return fetch_insights(FACEBOOK_PAGE_ID, PAGE_METRICS)
