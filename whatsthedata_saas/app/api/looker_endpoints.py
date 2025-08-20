# app/api/looker_endpoints.py
# Endpoints spécialisés pour le connecteur Looker Studio

from fastapi import APIRouter, Depends, HTTPException, Query, Header
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, date
from pydantic import BaseModel, Field
import logging
import json

from ..auth.user_manager import user_manager
from ..database.connection import db_manager
from ..database.models import User, FacebookAccount, LinkedinAccount
from ..utils.config import Config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["Looker Studio"])

# ========================================
# MODELS PYDANTIC POUR LOOKER
# ========================================

class LookerDataRequest(BaseModel):
    platform: str = Field(default="both", description="Platform: linkedin, facebook, or both")
    date_range: str = Field(default="30", description="Number of days to fetch")
    start_date: Optional[str] = Field(None, description="Start date YYYY-MM-DD")
    end_date: Optional[str] = Field(None, description="End date YYYY-MM-DD")
    include_post_details: bool = Field(False, description="Include detailed post metrics")
    fields: Optional[str] = Field(None, description="Comma-separated list of fields")

class LookerDataResponse(BaseModel):
    data: List[Dict[str, Any]]
    total_records: int
    platform_stats: Dict[str, Any]
    date_range: Dict[str, str]
    generated_at: str

# ========================================
# HELPER FUNCTIONS
# ========================================

def get_user_from_token(authorization: str = Header(None)) -> User:
    """Extraire et valider l'utilisateur depuis le token JWT"""
    
    if not authorization:
        raise HTTPException(status_code=401, detail="Token d'autorisation manquant")
    
    try:
        # Extraire le token (format: "Bearer TOKEN")
        if authorization.startswith("Bearer "):
            token = authorization[7:]
        else:
            token = authorization
        
        # Valider le token
        payload = user_manager.verify_token(token)
        if not payload:
            raise HTTPException(status_code=401, detail="Token invalide")
        
        # Récupérer l'utilisateur
        user = user_manager.get_user_by_id(payload.get('user_id'))
        if not user:
            raise HTTPException(status_code=401, detail="Utilisateur non trouvé")
        
        return user
        
    except Exception as e:
        logger.error(f"Erreur validation token: {e}")
        raise HTTPException(status_code=401, detail="Token invalide")

def calculate_date_range(date_range: str, start_date: str = None, end_date: str = None):
    """Calculer la plage de dates"""
    
    if start_date and end_date:
        # Utiliser les dates fournies
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
            end = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Format de date invalide (YYYY-MM-DD attendu)")
    else:
        # Utiliser le range par défaut
        try:
            days = int(date_range)
            end = date.today()
            start = end - timedelta(days=days)
        except ValueError:
            raise HTTPException(status_code=400, detail="Range de date invalide")
    
    return start, end

def get_linkedin_metrics(user: User, start_date: date, end_date: date, include_posts: bool = False) -> List[Dict]:
    """Récupérer les métriques LinkedIn pour un utilisateur"""
    
    try:
        with db_manager.get_session() as session:
            # Récupérer les comptes LinkedIn de l'utilisateur
            linkedin_accounts = session.query(LinkedinAccount).filter(
                LinkedinAccount.user_id == user.id,
                LinkedinAccount.is_active == True
            ).all()
            
            metrics = []
            
            for account in linkedin_accounts:
                # Métriques de base par jour
                current_date = start_date
                while current_date <= end_date:
                    # TODO: Remplacer par de vraies données depuis votre API LinkedIn
                    base_metric = {
                        'platform': 'linkedin',
                        'date': current_date.strftime('%Y-%m-%d'),
                        'account_name': account.organization_name or 'LinkedIn Page',
                        'account_id': account.organization_id,
                        'followers_total': 1500 + (current_date - start_date).days * 10,  # Simulation
                        'followers_growth': 10,
                        'posts_count': 1,
                        'impressions_total': 2500,
                        'reach_total': 1800,
                        'engagement_total': 150,
                        'engagement_rate': 0.06,  # 6%
                        'likes_total': 80,
                        'comments_total': 25,
                        'shares_total': 45,
                        'clicks_total': 35,
                        'linkedin_reactions_like': 50,
                        'linkedin_reactions_celebrate': 15,
                        'linkedin_reactions_support': 8,
                        'linkedin_reactions_love': 4,
                        'linkedin_reactions_insightful': 2,
                        'linkedin_reactions_funny': 1
                    }
                    
                    metrics.append(base_metric)
                    
                    # Si détails posts demandés, ajouter des lignes par post
                    if include_posts:
                        for i in range(2):  # 2 posts par jour simulés
                            post_metric = base_metric.copy()
                            post_metric.update({
                                'post_id': f'linkedin_post_{current_date.strftime("%Y%m%d")}_{i}',
                                'post_type': 'original' if i == 0 else 'repost',
                                'post_message': f'Post LinkedIn du {current_date} #{i+1}',
                                'post_date': f'{current_date} 10:00:00',
                                'post_impressions': 1200 if i == 0 else 800,
                                'post_reach': 900 if i == 0 else 600,
                                'post_engagement': 75 if i == 0 else 50
                            })
                            metrics.append(post_metric)
                    
                    current_date += timedelta(days=1)
            
            return metrics
            
    except Exception as e:
        logger.error(f"Erreur récupération métriques LinkedIn: {e}")
        return []

def get_facebook_metrics(user: User, start_date: date, end_date: date, include_posts: bool = False) -> List[Dict]:
    """Récupérer les métriques Facebook pour un utilisateur"""
    
    try:
        with db_manager.get_session() as session:
            # Récupérer les comptes Facebook de l'utilisateur
            facebook_accounts = session.query(FacebookAccount).filter(
                FacebookAccount.user_id == user.id,
                FacebookAccount.is_active == True
            ).all()
            
            metrics = []
            
            for account in facebook_accounts:
                # Métriques de base par jour
                current_date = start_date
                while current_date <= end_date:
                    # TODO: Remplacer par de vraies données depuis votre API Facebook
                    base_metric = {
                        'platform': 'facebook',
                        'date': current_date.strftime('%Y-%m-%d'),
                        'account_name': account.page_name or 'Facebook Page',
                        'account_id': account.page_id,
                        'followers_total': 3200 + (current_date - start_date).days * 15,  # Simulation
                        'followers_growth': 15,
                        'posts_count': 2,
                        'impressions_total': 4500,
                        'reach_total': 3200,
                        'engagement_total': 320,
                        'engagement_rate': 0.10,  # 10%
                        'likes_total': 180,
                        'comments_total': 45,
                        'shares_total': 95,
                        'clicks_total': 120
                    }
                    
                    metrics.append(base_metric)
                    
                    # Si détails posts demandés
                    if include_posts:
                        for i in range(3):  # 3 posts par jour simulés
                            post_metric = base_metric.copy()
                            post_metric.update({
                                'post_id': f'facebook_post_{current_date.strftime("%Y%m%d")}_{i}',
                                'post_type': 'photo' if i == 0 else 'link' if i == 1 else 'status',
                                'post_message': f'Post Facebook du {current_date} #{i+1}',
                                'post_date': f'{current_date} {9+i*3}:00:00',
                                'post_impressions': 1500 if i == 0 else 1000,
                                'post_reach': 1100 if i == 0 else 750,
                                'post_engagement': 110 if i == 0 else 75
                            })
                            metrics.append(post_metric)
                    
                    current_date += timedelta(days=1)
            
            return metrics
            
    except Exception as e:
        logger.error(f"Erreur récupération métriques Facebook: {e}")
        return []

# ========================================
# ENDPOINTS PRINCIPAUX
# ========================================

@router.get("/looker-data", response_model=LookerDataResponse)
async def get_looker_data(
    platform: str = Query("both", description="Platform: linkedin, facebook, or both"),
    date_range: str = Query("30", description="Number of days"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    include_post_details: bool = Query(False, description="Include post details"),
    fields: Optional[str] = Query(None, description="Comma-separated fields"),
    user: User = Depends(get_user_from_token)
):
    """
    Endpoint principal pour récupérer les données social media
    Utilisé par le connecteur Looker Studio
    """
    
    try:
        # Calculer la plage de dates
        start, end = calculate_date_range(date_range, start_date, end_date)
        
        # Collecter les métriques selon la plateforme demandée
        all_metrics = []
        
        if platform in ['linkedin', 'both']:
            linkedin_metrics = get_linkedin_metrics(user, start, end, include_post_details)
            all_metrics.extend(linkedin_metrics)
        
        if platform in ['facebook', 'both']:
            facebook_metrics = get_facebook_metrics(user, start, end, include_post_details)
            all_metrics.extend(facebook_metrics)
        
        # Filtrer par champs si spécifié
        if fields:
            requested_fields = [f.strip() for f in fields.split(',')]
            filtered_metrics = []
            
            for metric in all_metrics:
                filtered_metric = {
                    field: metric.get(field, '')
                    for field in requested_fields
                    if field in metric
                }
                if filtered_metric:  # N'ajouter que si au moins un champ est trouvé
                    filtered_metrics.append(filtered_metric)
            
            all_metrics = filtered_metrics
        
        # Statistiques par plateforme
        platform_stats = {}
        
        for platform_name in ['linkedin', 'facebook']:
            platform_metrics = [m for m in all_metrics if m.get('platform') == platform_name]
            if platform_metrics:
                platform_stats[platform_name] = {
                    'total_records': len(platform_metrics),
                    'accounts_count': len(set(m.get('account_id', '') for m in platform_metrics)),
                    'date_range': f"{start} to {end}",
                    'total_impressions': sum(m.get('impressions_total', 0) for m in platform_metrics),
                    'total_engagement': sum(m.get('engagement_total', 0) for m in platform_metrics)
                }
        
        response = LookerDataResponse(
            data=all_metrics,
            total_records=len(all_metrics),
            platform_stats=platform_stats,
            date_range={
                'start_date': start.strftime('%Y-%m-%d'),
                'end_date': end.strftime('%Y-%m-%d'),
                'days': str((end - start).days + 1)
            },
            generated_at=datetime.now().isoformat()
        )
        
        logger.info(f"Données générées pour {user.email}: {len(all_metrics)} enregistrements")
        
        return response
        
    except Exception as e:
        logger.error(f"Erreur génération données Looker: {e}")
        raise HTTPException(status_code=500, detail="Erreur lors de la récupération des données")

@router.get("/validate-token")
async def validate_token(user: User = Depends(get_user_from_token)):
    """Valider un token JWT pour Looker Studio"""
    
    return {
        'valid': True,
        'user_id': user.id,
        'email': user.email,
        'plan_type': user.plan_type,
        'subscription_active': user.subscription_end_date > datetime.now() if user.subscription_end_date else False
    }

@router.get("/schema")
async def get_schema(
    platform: str = Query("both"),
    include_post_details: bool = Query(False),
    user: User = Depends(get_user_from_token)
):
    """Retourner le schéma des données disponibles"""
    
    schema = {
        'dimensions': [
            {'id': 'platform', 'name': 'Plateforme', 'type': 'TEXT'},
            {'id': 'date', 'name': 'Date', 'type': 'DATE'},
            {'id': 'account_name', 'name': 'Nom du compte', 'type': 'TEXT'},
            {'id': 'account_id', 'name': 'ID du compte', 'type': 'TEXT'}
        ],
        'metrics': [
            {'id': 'followers_total', 'name': 'Total Followers', 'type': 'NUMBER'},
            {'id': 'followers_growth', 'name': 'Croissance Followers', 'type': 'NUMBER'},
            {'id': 'posts_count', 'name': 'Nombre de posts', 'type': 'NUMBER'},
            {'id': 'impressions_total', 'name': 'Impressions totales', 'type': 'NUMBER'},
            {'id': 'reach_total', 'name': 'Portée totale', 'type': 'NUMBER'},
            {'id': 'engagement_total', 'name': 'Engagement total', 'type': 'NUMBER'},
            {'id': 'engagement_rate', 'name': 'Taux d\'engagement', 'type': 'PERCENT'},
            {'id': 'likes_total', 'name': 'Likes totaux', 'type': 'NUMBER'},
            {'id': 'comments_total', 'name': 'Commentaires totaux', 'type': 'NUMBER'},
            {'id': 'shares_total', 'name': 'Partages totaux', 'type': 'NUMBER'},
            {'id': 'clicks_total', 'name': 'Clics totaux', 'type': 'NUMBER'}
        ]
    }
    
    # Ajouter les champs spécifiques aux posts si demandé
    if include_post_details:
        schema['dimensions'].extend([
            {'id': 'post_id', 'name': 'ID du post', 'type': 'TEXT'},
            {'id': 'post_type', 'name': 'Type de post', 'type': 'TEXT'},
            {'id': 'post_message', 'name': 'Message du post', 'type': 'TEXT'},
            {'id': 'post_date', 'name': 'Date du post', 'type': 'DATETIME'}
        ])
        
        schema['metrics'].extend([
            {'id': 'post_impressions', 'name': 'Impressions du post', 'type': 'NUMBER'},
            {'id': 'post_reach', 'name': 'Portée du post', 'type': 'NUMBER'},
            {'id': 'post_engagement', 'name': 'Engagement du post', 'type': 'NUMBER'}
        ])
    
    # Ajouter les champs LinkedIn si demandé
    if platform in ['linkedin', 'both']:
        schema['metrics'].extend([
            {'id': 'linkedin_reactions_like', 'name': 'LinkedIn - Reactions Like', 'type': 'NUMBER'},
            {'id': 'linkedin_reactions_celebrate', 'name': 'LinkedIn - Reactions Celebrate', 'type': 'NUMBER'},
            {'id': 'linkedin_reactions_support', 'name': 'LinkedIn - Reactions Support', 'type': 'NUMBER'},
            {'id': 'linkedin_reactions_love', 'name': 'LinkedIn - Reactions Love', 'type': 'NUMBER'},
            {'id': 'linkedin_reactions_insightful', 'name': 'LinkedIn - Reactions Insightful', 'type': 'NUMBER'},
            {'id': 'linkedin_reactions_funny', 'name': 'LinkedIn - Reactions Funny', 'type': 'NUMBER'}
        ])
    
    return schema

@router.get("/health")
async def health_check():
    """Health check pour le connecteur Looker Studio"""
    
    return {
        'status': 'healthy',
        'service': 'WhatsTheData Looker Connector',
        'timestamp': datetime.now().isoformat(),
        'database': 'connected' if db_manager.test_connection() else 'disconnected'
    }
    
    
@router.get("/test-data")
async def get_test_data():
    """Endpoint de test sans authentification"""
    return {
        "data": [
            {
                "platform": "linkedin",
                "date": "2025-08-20",
                "account_name": "Test LinkedIn",
                "followers_total": 1500,
                "impressions_total": 2500,
                "engagement_total": 150
            },
            {
                "platform": "facebook", 
                "date": "2025-08-20",
                "account_name": "Test Facebook",
                "followers_total": 3200,
                "impressions_total": 4500,
                "engagement_total": 320
            }
        ],
        "total_records": 2,
        "status": "test_data"
    }
    
@router.get("/generate-test-token")
async def generate_test_token():
    """Génère un token de test pour Looker Studio"""
    return {
        "token": "test_token_whatsthedata_123",
        "username": "test_user",
        "valid_until": "2025-08-21",
        "message": "Token de test généré - utilisez ce token dans Looker Studio"
    }
    
@router.get("/validate-token")
async def validate_token():
    """Validation simplifiée pour tests Looker Studio"""
    return {
        'valid': True,
        'user_id': 1,
        'email': 'test@whatsthedata.com',
        'plan_type': 'premium',
        'subscription_active': True
    }