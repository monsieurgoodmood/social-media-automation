# app/utils/session_manager.py
import secrets
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import Column, String, DateTime, Text, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.database.connection import get_db_session, engine

logger = logging.getLogger(__name__)
Base = declarative_base()

class OAuthSession(Base):
    __tablename__ = 'oauth_sessions'
    
    id = Column(Integer, primary_key=True)
    state = Column(String(64), unique=True, index=True)
    data = Column(Text)  # JSON data
    created_at = Column(DateTime, default=datetime.now)
    expires_at = Column(DateTime)

class SessionManager:
    """Gestionnaire de sessions OAuth persistant et robuste"""
    
    @staticmethod
    def init_tables():
        """Créer les tables si elles n'existent pas"""
        try:
            Base.metadata.create_all(bind=engine)
            logger.info("Tables OAuth sessions créées/vérifiées")
        except Exception as e:
            logger.error(f"Erreur création tables OAuth: {e}")
    
    @staticmethod
    def create_session(data: Dict[Any, Any], expires_minutes: int = 30) -> str:
        """Créer une nouvelle session OAuth persistante"""
        state = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(minutes=expires_minutes)
        
        try:
            with get_db_session() as db:
                # Nettoyer les sessions expirées d'abord
                SessionManager._cleanup_expired_sessions(db)
                
                # Créer nouvelle session
                session = OAuthSession(
                    state=state,
                    data=json.dumps(data, default=str),  # default=str pour datetime
                    expires_at=expires_at
                )
                db.add(session)
                db.commit()
                
                logger.info(f"Session OAuth créée: {state[:8]}...")
                return state
                
        except Exception as e:
            logger.error(f"Erreur création session OAuth: {e}")
            # En cas d'erreur, générer un state fallback
            return f"fallback_{secrets.token_urlsafe(16)}"
    
    @staticmethod
    def get_session(state: str) -> Optional[Dict[Any, Any]]:
        """Récupérer une session OAuth"""
        if not state or state.startswith("fallback_"):
            logger.warning(f"Tentative récupération session fallback: {state}")
            return None
            
        try:
            with get_db_session() as db:
                session = db.query(OAuthSession).filter(
                    OAuthSession.state == state,
                    OAuthSession.expires_at > datetime.now()
                ).first()
                
                if not session:
                    logger.warning(f"Session OAuth non trouvée ou expirée: {state[:8]}...")
                    return None
                    
                data = json.loads(session.data)
                logger.info(f"Session OAuth récupérée: {state[:8]}...")
                return data
                
        except Exception as e:
            logger.error(f"Erreur récupération session OAuth {state[:8]}...: {e}")
            return None
    
    @staticmethod
    def update_session(state: str, data: Dict[Any, Any]) -> bool:
        """Mettre à jour une session OAuth"""
        if not state or state.startswith("fallback_"):
            logger.warning(f"Tentative update session fallback: {state}")
            return False
            
        try:
            with get_db_session() as db:
                session = db.query(OAuthSession).filter(
                    OAuthSession.state == state
                ).first()
                
                if not session:
                    logger.warning(f"Session OAuth à updater non trouvée: {state[:8]}...")
                    return False
                    
                session.data = json.dumps(data, default=str)
                db.commit()
                
                logger.info(f"Session OAuth mise à jour: {state[:8]}...")
                return True
                
        except Exception as e:
            logger.error(f"Erreur update session OAuth {state[:8]}...: {e}")
            return False
    
    @staticmethod
    def delete_session(state: str) -> bool:
        """Supprimer une session OAuth"""
        if not state or state.startswith("fallback_"):
            return True  # Les sessions fallback n'existent pas en DB
            
        try:
            with get_db_session() as db:
                deleted = db.query(OAuthSession).filter(
                    OAuthSession.state == state
                ).delete()
                db.commit()
                
                if deleted:
                    logger.info(f"Session OAuth supprimée: {state[:8]}...")
                return deleted > 0
                
        except Exception as e:
            logger.error(f"Erreur suppression session OAuth {state[:8]}...: {e}")
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
        """Créer une session d'urgence pour récupération après perte de session"""
        emergency_data = {
            'provider': 'emergency_recovery',
            'user_email': email,
            'user_name': f"{firstname} {lastname}".strip() or email.split('@')[0],
            'step': 'plan_selection',
            'created_at': datetime.now(),
            'is_emergency': True
        }
        
        return SessionManager.create_session(emergency_data, expires_minutes=60)

# Initialiser les tables au démarrage
SessionManager.init_tables()