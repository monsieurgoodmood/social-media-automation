# app/utils/session_manager.py
import secrets
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from app.database.connection import get_db_session
from app.database.models import OAuthSession

logger = logging.getLogger(__name__)

class SessionManager:
    """Gestionnaire de sessions OAuth avec ORM SQLAlchemy"""
    
    @staticmethod
    def create_session(data: Dict[Any, Any], expires_minutes: int = 30) -> str:
        """Créer une nouvelle session OAuth"""
        state = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(minutes=expires_minutes)
        
        try:
            with get_db_session() as db:
                # Nettoyer d'abord
                SessionManager._cleanup_expired_sessions(db)
                
                # Créer nouvelle session
                session = OAuthSession(
                    state=state,
                    data=json.dumps(data, default=str),
                    expires_at=expires_at
                )
                db.add(session)
                db.commit()
                
                logger.info(f"Session OAuth créée: {state[:8]}...")
                return state
                
        except Exception as e:
            logger.error(f"Erreur création session OAuth: {e}")
            return f"fallback_{secrets.token_urlsafe(16)}"
    
    @staticmethod
    def get_session(state: str) -> Optional[Dict[Any, Any]]:
        """Récupérer une session OAuth"""
        if not state or state.startswith("fallback_"):
            return None
            
        try:
            with get_db_session() as db:
                session = db.query(OAuthSession).filter(
                    OAuthSession.state == state,
                    OAuthSession.expires_at > datetime.now()
                ).first()
                
                if not session:
                    return None
                    
                return json.loads(session.data)
                
        except Exception as e:
            logger.error(f"Erreur récupération session OAuth: {e}")
            return None
    
    @staticmethod
    def update_session(state: str, data: Dict[Any, Any]) -> bool:
        """Mettre à jour une session OAuth"""
        if not state or state.startswith("fallback_"):
            return False
            
        try:
            with get_db_session() as db:
                session = db.query(OAuthSession).filter(
                    OAuthSession.state == state
                ).first()
                
                if not session:
                    return False
                    
                session.data = json.dumps(data, default=str)
                db.commit()
                return True
                
        except Exception as e:
            logger.error(f"Erreur update session OAuth: {e}")
            return False
    
    @staticmethod
    def delete_session(state: str) -> bool:
        """Supprimer une session OAuth"""
        if not state or state.startswith("fallback_"):
            return True
            
        try:
            with get_db_session() as db:
                deleted = db.query(OAuthSession).filter(
                    OAuthSession.state == state
                ).delete()
                db.commit()
                return deleted > 0
                
        except Exception as e:
            logger.error(f"Erreur suppression session OAuth: {e}")
            return False
    
    @staticmethod
    def _cleanup_expired_sessions(db):
        """Nettoyer les sessions expirées"""
        try:
            deleted = db.query(OAuthSession).filter(
                OAuthSession.expires_at < datetime.now()
            ).delete()
            
            if deleted > 0:
                logger.info(f"Sessions expirées nettoyées: {deleted}")
                
        except Exception as e:
            logger.error(f"Erreur nettoyage sessions expirées: {e}")
    
    @staticmethod
    def emergency_create_user_session(email: str, firstname: str = "", lastname: str = "") -> str:
        """Créer une session d'urgence"""
        emergency_data = {
            'provider': 'emergency_recovery',
            'user_email': email,
            'user_name': f"{firstname} {lastname}".strip() or email.split('@')[0],
            'step': 'plan_selection',
            'created_at': datetime.now(),
            'is_emergency': True
        }
        
        return SessionManager.create_session(emergency_data, expires_minutes=60)