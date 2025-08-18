# app/api/looker_endpoints.py
# ============================
# 🎯 PRIORITÉ 2 - API pour votre connecteur Looker Studio
# Met à jour votre fichier existant avec la vraie logique

from fastapi import APIRouter, HTTPException, Depends, Security, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Union
import json
import os
import logging

from app.database.connection import get_db, get_db_session
from app.utils.config import settings

# Configuration des logs
logger = logging.getLogger(__name__)

# Router pour les endpoints Looker Studio
router = APIRouter(prefix="/api/v1", tags=["looker-studio"])
security = HTTPBearer()

# ================================
# 1. AUTHENTIFICATION SIMPLIFIÉE
# ================================

def validate_api_token(token: str) -> Dict[str, Any]:
    """
    Valide le token API - VERSION SIMPLIFIÉE pour commencer
    TODO: Remplacer par votre vraie logique d'authentification
    """
    
    # Pour commencer, on accepte tout token de plus de 10 caractères
    # TODO: Implémenter la vraie validation contre vos API keys
    if not token or len(token) < 10:
        raise HTTPException(status_code=401, detail="Token API invalide")
    
    # Simulation - À remplacer par votre logique
    # En attendant, on retourne toujours user_id=1
    return {
        "user_id": 1,  # TODO: Extraire du vrai token
        "email": "test@example.com",
        "plan_type": "premium"  # TODO: Vérifier le vrai plan
    }

async def get_current_user(credentials: HTTPAuthorizationCredentials = Security(security)):
    """Récupère l'utilisateur actuel depuis le token"""
    token = credentials.credentials
    return validate_api_token(token)

# ================================
# 2. ENDPOINTS PRINCIPAUX
# ================================

@router.post("/validate-token")
async def validate_token_endpoint(
    credentials: HTTPAuthorizationCredentials = Security(security)
):
    """Valide un token API pour le connecteur Looker Studio"""
    try:
        user_info = validate_api_token(credentials.credentials)
        
        # Vérifier que l'utilisateur existe en base
        with get_db_session() as db:
            result = db.execute(
                text("SELECT id, email, is_active, plan_type FROM users WHERE id = :user_id"),
                {"user_id": user_info["user_id"]}
            ).fetchone()
            
            if not result:
                return {"valid": False, "error": "Utilisateur non trouvé"}
            
            if not result.is_active:
                return {"valid": False, "error": "Compte inactif"}
            
            return {
                "valid": True,
                "user_id": result.id,
                "email": result.email,
                "plan_type": result.plan_type or "free"
            }
            
    except HTTPException as e:
        return {"valid": False, "error": e.detail}
    except Exception as e:
        logger.error(f"Erreur validation token: {e}")
        return {"valid": False, "error": "Erreur serveur"}

@router.post("/looker-data")
async def get_looker_data(
    request_data: Dict[str, Any],
    user_info: dict = Depends(get_current_user)
):
    """
    Endpoint principal pour récupérer les données Looker Studio
    Adapté à votre schéma PostgreSQL existant
    """
    try:
        # Extraction des paramètres de la requête
        platforms = request_data.get("platforms", ["linkedin", "facebook"])
        date_range_days = int(request_data.get("dateRange", "30"))
        metrics_type = request_data.get("metricsType", "overview")
        start_date = request_data.get("startDate")
        end_date = request_data.get("endDate")
        
        # Calcul des dates si non fournies
        if not start_date or not end_date:
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=date_range_days)
        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        logger.info(f"Requête Looker: user={user_info['user_id']}, platforms={platforms}, dates={start_date} à {end_date}")
        
        # Vérification des permissions utilisateur
        user_platforms = get_user_accessible_platforms(user_info["user_id"])
        allowed_platforms = [p for p in platforms if p in user_platforms]
        
        if not allowed_platforms:
            raise HTTPException(
                status_code=403, 
                detail=f"Aucune plateforme accessible. Plan actuel: {user_info.get('plan_type', 'free')}"
            )
        
        # Collecte des données
        all_data = []
        
        # Métriques de pages LinkedIn
        if "linkedin" in allowed_platforms and metrics_type in ["overview", "pages"]:
            linkedin_page_data = get_linkedin_page_metrics(user_info["user_id"], start_date, end_date)
            all_data.extend(linkedin_page_data)
            logger.info(f"LinkedIn pages: {len(linkedin_page_data)} enregistrements")
        
        # Métriques de posts LinkedIn
        if "linkedin" in allowed_platforms and metrics_type in ["overview", "posts"]:
            linkedin_post_data = get_linkedin_post_metrics(user_info["user_id"], start_date, end_date)
            all_data.extend(linkedin_post_data)
            logger.info(f"LinkedIn posts: {len(linkedin_post_data)} enregistrements")
        
        # Métriques de pages Facebook
        if "facebook" in allowed_platforms and metrics_type in ["overview", "pages"]:
            facebook_page_data = get_facebook_page_metrics(user_info["user_id"], start_date, end_date)
            all_data.extend(facebook_page_data)
            logger.info(f"Facebook pages: {len(facebook_page_data)} enregistrements")
        
        # Métriques de posts Facebook
        if "facebook" in allowed_platforms and metrics_type in ["overview", "posts"]:
            facebook_post_data = get_facebook_post_metrics(user_info["user_id"], start_date, end_date)
            all_data.extend(facebook_post_data)
            logger.info(f"Facebook posts: {len(facebook_post_data)} enregistrements")
        
        logger.info(f"Total données retournées: {len(all_data)} enregistrements")
        
        return {
            "success": True,
            "data": all_data,
            "total_records": len(all_data),
            "date_range": {
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            },
            "platforms_requested": platforms,
            "platforms_accessible": user_platforms,
            "platforms_returned": allowed_platforms,
            "user_plan": user_info.get("plan_type", "free")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur get_looker_data: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur serveur: {str(e)}")

# ================================
# 3. FONCTIONS EXTRACTION LINKEDIN
# ================================

def get_linkedin_page_metrics(user_id: int, start_date, end_date) -> List[Dict]:
    """Récupère les métriques de pages LinkedIn depuis votre base"""
    
    try:
        with get_db_session() as db:
            query = text("""
                SELECT 
                    'linkedin' as platform,
                    lpd.date,
                    la.organization_id as page_id,
                    COALESCE(la.organization_name, 'Page LinkedIn') as page_name,
                    'page_metrics' as content_type,
                    COALESCE(lpd.followers_count, 0) as followers_total,
                    0 as followers_gained,
                    0 as followers_lost,
                    COALESCE(lpd.impression_count, 0) as impressions,
                    COALESCE(lpd.unique_impression_count, 0) as unique_impressions,
                    COALESCE(lpd.all_page_views, 0) as page_views,
                    COALESCE(lpd.engagement_rate, 0) as engagement_rate,
                    COALESCE(lpd.like_count, 0) as likes,
                    COALESCE(lpd.comment_count, 0) as comments,
                    COALESCE(lpd.share_count, 0) as shares,
                    COALESCE(lpd.click_count, 0) as clicks,
                    COALESCE(lpd.like_count, 0) + COALESCE(lpd.comment_count, 0) + 
                    COALESCE(lpd.share_count, 0) + COALESCE(lpd.click_count, 0) as total_engagement,
                    0 as video_views,
                    0 as video_complete_views,
                    COALESCE(lpd.like_count, 0) as reactions_like,
                    0 as reactions_love,
                    0 as reactions_celebrate,
                    0 as reactions_wow,
                    0 as reactions_sorry,
                    0 as reactions_anger
                FROM linkedin_page_daily lpd
                JOIN linkedin_accounts la ON lpd.organization_id = la.organization_id
                WHERE la.user_id = :user_id 
                AND COALESCE(la.is_active, true) = true
                AND lpd.date BETWEEN :start_date AND :end_date
                ORDER BY lpd.date DESC
            """)
            
            result = db.execute(query, {
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Erreur LinkedIn page metrics: {e}")
        return []

def get_linkedin_post_metrics(user_id: int, start_date, end_date) -> List[Dict]:
    """Récupère les métriques de posts LinkedIn"""
    
    try:
        with get_db_session() as db:
            query = text("""
                SELECT 
                    'linkedin' as platform,
                    lpd.date,
                    la.organization_id as page_id,
                    COALESCE(la.organization_name, 'Page LinkedIn') as page_name,
                    'post_metrics' as content_type,
                    COALESCE(lpm.follower_count, 0) as followers_total,
                    0 as followers_gained,
                    0 as followers_lost,
                    COALESCE(lpd.impressions, 0) as impressions,
                    COALESCE(lpd.unique_impressions, 0) as unique_impressions,
                    0 as page_views,
                    COALESCE(lpd.engagement_rate, 0) as engagement_rate,
                    COALESCE(lpd.likes, 0) as likes,
                    COALESCE(lpd.comments, 0) as comments,
                    COALESCE(lpd.shares, 0) as shares,
                    COALESCE(lpd.clicks, 0) as clicks,
                    COALESCE(lpd.engagement_total, 0) as total_engagement,
                    0 as video_views,
                    0 as video_complete_views,
                    COALESCE(lpd.likes, 0) as reactions_like,
                    COALESCE(lpd.reactions_love, 0) as reactions_love,
                    COALESCE(lpd.reactions_celebrate, 0) as reactions_celebrate,
                    COALESCE(lpd.reactions_interest, 0) as reactions_wow,
                    0 as reactions_sorry,
                    0 as reactions_anger
                FROM linkedin_posts_daily lpd
                JOIN linkedin_accounts la ON lpd.organization_id = la.organization_id
                LEFT JOIN linkedin_pages_metadata lpm ON la.organization_id = lpm.organization_id
                WHERE la.user_id = :user_id 
                AND COALESCE(la.is_active, true) = true
                AND lpd.date BETWEEN :start_date AND :end_date
                ORDER BY lpd.date DESC
            """)
            
            result = db.execute(query, {
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Erreur LinkedIn post metrics: {e}")
        return []

# ================================
# 4. FONCTIONS EXTRACTION FACEBOOK
# ================================

def get_facebook_page_metrics(user_id: int, start_date, end_date) -> List[Dict]:
    """Récupère les métriques de pages Facebook"""
    
    try:
        with get_db_session() as db:
            query = text("""
                SELECT 
                    'facebook' as platform,
                    fpd.date,
                    fa.page_id,
                    COALESCE(fa.page_name, 'Page Facebook') as page_name,
                    'page_metrics' as content_type,
                    COALESCE(fpd.page_fans, 0) as followers_total,
                    COALESCE(fpd.page_fan_adds, 0) as followers_gained,
                    COALESCE(fpd.page_fan_removes, 0) as followers_lost,
                    COALESCE(fpd.page_impressions, 0) as impressions,
                    COALESCE(fpd.page_impressions_unique, 0) as unique_impressions,
                    COALESCE(fpd.page_views_total, 0) as page_views,
                    CASE 
                        WHEN COALESCE(fpd.page_impressions, 0) > 0 
                        THEN (COALESCE(fpd.page_post_engagements, 0)::float / fpd.page_impressions::float) * 100 
                        ELSE 0 
                    END as engagement_rate,
                    COALESCE(fpd.page_actions_post_reactions_like_total, 0) as likes,
                    0 as comments,
                    0 as shares,
                    0 as clicks,
                    COALESCE(fpd.page_post_engagements, 0) as total_engagement,
                    COALESCE(fpd.page_video_views, 0) as video_views,
                    COALESCE(fpd.page_video_complete_views_30s, 0) as video_complete_views,
                    COALESCE(fpd.page_actions_post_reactions_like_total, 0) as reactions_like,
                    COALESCE(fpd.page_actions_post_reactions_love_total, 0) as reactions_love,
                    0 as reactions_celebrate,
                    COALESCE(fpd.page_actions_post_reactions_wow_total, 0) as reactions_wow,
                    COALESCE(fpd.page_actions_post_reactions_sorry_total, 0) as reactions_sorry,
                    COALESCE(fpd.page_actions_post_reactions_anger_total, 0) as reactions_anger
                FROM facebook_page_daily fpd
                JOIN facebook_accounts fa ON fpd.page_id = fa.page_id
                WHERE fa.user_id = :user_id 
                AND COALESCE(fa.is_active, true) = true
                AND fpd.date BETWEEN :start_date AND :end_date
                ORDER BY fpd.date DESC
            """)
            
            result = db.execute(query, {
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Erreur Facebook page metrics: {e}")
        return []

def get_facebook_post_metrics(user_id: int, start_date, end_date) -> List[Dict]:
    """Récupère les métriques de posts Facebook"""
    
    try:
        with get_db_session() as db:
            query = text("""
                SELECT 
                    'facebook' as platform,
                    fpm.created_time::date as date,
                    fa.page_id,
                    COALESCE(fa.page_name, 'Page Facebook') as page_name,
                    'post_metrics' as content_type,
                    0 as followers_total,  -- Pas dans les posts
                    0 as followers_gained,
                    0 as followers_lost,
                    COALESCE(fpl.post_impressions, 0) as impressions,
                    COALESCE(fpl.post_impressions_unique, 0) as unique_impressions,
                    0 as page_views,
                    CASE 
                        WHEN COALESCE(fpl.post_impressions, 0) > 0 
                        THEN ((COALESCE(fpl.post_reactions_like, 0) + COALESCE(fpl.post_reactions_love, 0) + 
                              COALESCE(fpl.post_reactions_wow, 0) + COALESCE(fpl.post_reactions_haha, 0) + 
                              COALESCE(fpl.post_reactions_sorry, 0) + COALESCE(fpl.post_reactions_anger, 0) + 
                              COALESCE(fpm.comments_count, 0) + COALESCE(fpm.shares_count, 0))::float / fpl.post_impressions::float) * 100 
                        ELSE 0 
                    END as engagement_rate,
                    COALESCE(fpl.post_reactions_like, 0) as likes,
                    COALESCE(fpm.comments_count, 0) as comments,
                    COALESCE(fpm.shares_count, 0) as shares,
                    COALESCE(fpl.post_clicks, 0) as clicks,
                    (COALESCE(fpl.post_reactions_like, 0) + COALESCE(fpl.post_reactions_love, 0) + 
                     COALESCE(fpl.post_reactions_wow, 0) + COALESCE(fpl.post_reactions_haha, 0) + 
                     COALESCE(fpl.post_reactions_sorry, 0) + COALESCE(fpl.post_reactions_anger, 0) + 
                     COALESCE(fpm.comments_count, 0) + COALESCE(fpm.shares_count, 0) + COALESCE(fpl.post_clicks, 0)) as total_engagement,
                    COALESCE(fpl.post_video_views, 0) as video_views,
                    COALESCE(fpl.post_video_complete_views, 0) as video_complete_views,
                    COALESCE(fpl.post_reactions_like, 0) as reactions_like,
                    COALESCE(fpl.post_reactions_love, 0) as reactions_love,
                    0 as reactions_celebrate,
                    COALESCE(fpl.post_reactions_wow, 0) as reactions_wow,
                    COALESCE(fpl.post_reactions_sorry, 0) as reactions_sorry,
                    COALESCE(fpl.post_reactions_anger, 0) as reactions_anger
                FROM facebook_posts_metadata fpm
                JOIN facebook_posts_lifetime fpl ON fpm.post_id = fpl.post_id
                JOIN facebook_accounts fa ON fpm.page_id = fa.page_id
                WHERE fa.user_id = :user_id 
                AND COALESCE(fa.is_active, true) = true
                AND fpm.created_time::date BETWEEN :start_date AND :end_date
                ORDER BY fpm.created_time DESC
            """)
            
            result = db.execute(query, {
                "user_id": user_id,
                "start_date": start_date,
                "end_date": end_date
            })
            
            return [dict(row._mapping) for row in result.fetchall()]
            
    except Exception as e:
        logger.error(f"Erreur Facebook post metrics: {e}")
        return []

# ================================
# 5. FONCTIONS UTILITAIRES
# ================================

def get_user_accessible_platforms(user_id: int) -> List[str]:
    """Retourne les plateformes accessibles selon le plan utilisateur"""
    
    try:
        with get_db_session() as db:
            result = db.execute(
                text("SELECT plan_type FROM users WHERE id = :user_id"),
                {"user_id": user_id}
            ).fetchone()
            
            if not result:
                return []
            
            plan_type = (result.plan_type or "free").lower()
            
            if plan_type == "premium":
                return ["linkedin", "facebook"]
            elif plan_type == "linkedin":
                return ["linkedin"]
            elif plan_type == "facebook":
                return ["facebook"]
            else:
                return []  # Plan gratuit
                
    except Exception as e:
        logger.error(f"Erreur get_user_accessible_platforms: {e}")
        return []

# ================================
# 6. ENDPOINTS DE DEBUG
# ================================

@router.get("/test-connection")
async def test_database_connection():
    """Test de connexion à la base de données"""
    try:
        from app.database.connection import test_database_connection, get_table_counts
        
        connection_ok = test_database_connection()
        if not connection_ok:
            raise HTTPException(status_code=500, detail="Connexion base de données échouée")
        
        counts = get_table_counts()
        
        return {
            "database_connected": True,
            "table_counts": counts,
            "status": "OK"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")

@router.get("/sample-data")
async def get_sample_data():
    """Récupère un échantillon de données pour tester"""
    try:
        from app.database.connection import get_sample_data_for_looker
        
        sample_data = get_sample_data_for_looker()
        
        if "error" in sample_data:
            raise HTTPException(status_code=500, detail=sample_data["error"])
        
        return sample_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")

# ================================
# 🎯 EXPORT DU ROUTER
# ================================

# Ce router doit être importé dans votre app/main.py :
# from app.api.looker_endpoints import router as looker_router
# app.include_router(looker_router)