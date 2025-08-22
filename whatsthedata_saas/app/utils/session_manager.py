# app/utils/session_manager.py
import secrets
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy import Column, String, DateTime, Text, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from app.database.connection import get_db_session

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
            with get_db_session() as db:
                # Créer la table oauth_sessions si elle n'existe pas
                db.execute(text("""
                    CREATE TABLE IF NOT EXISTS oauth_sessions (
                        id SERIAL PRIMARY KEY,
                        state VARCHAR(64) UNIQUE NOT NULL,
                        data TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL
                    );
                """))
                
                # Créer l'index sur state si nécessaire
                db.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_oauth_sessions_state 
                    ON oauth_sessions(state);
                """))
                
                # Créer l'index sur expires_at pour le nettoyage
                db.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_oauth_sessions_expires_at 
                    ON oauth_sessions(expires_at);
                """))
                
                db.commit()
                logger.info("Tables OAuth sessions créées/vérifiées")
        except Exception as e:
            logger.error(f"Erreur création tables OAuth: {e}")
            # Ne pas faire planter l'app si les tables existent déjà
    
    @staticmethod
    def create_session(data: Dict[Any, Any], expires_minutes: int = 30) -> str:
        """Créer une nouvelle session OAuth persistante"""
        state = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(minutes=expires_minutes)
        
        try:
            with get_db_session() as db:
                # Nettoyer les sessions expirées d'abord
                SessionManager._cleanup_expired_sessions(db)
                
                # Créer nouvelle session avec SQL brut
                db.execute(
                    text("""
                    INSERT INTO oauth_sessions (state, data, expires_at) 
                    VALUES (:state, :data, :expires_at)
                    """),
                    {
                        'state': state,
                        'data': json.dumps(data, default=str),
                        'expires_at': expires_at
                    }
                )
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
                result = db.execute(
                    text("""
                    SELECT data FROM oauth_sessions 
                    WHERE state = :state AND expires_at > :now
                    """),
                    {'state': state, 'now': datetime.now()}
                ).fetchone()
                
                if not result:
                    logger.warning(f"Session OAuth non trouvée ou expirée: {state[:8]}...")
                    return None
                    
                data = json.loads(result[0])
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
                result = db.execute(
                    text("""
                    UPDATE oauth_sessions 
                    SET data = :data 
                    WHERE state = :state
                    """),
                    {
                        'data': json.dumps(data, default=str),
                        'state': state
                    }
                )
                db.commit()
                
                if result.rowcount > 0:
                    logger.info(f"Session OAuth mise à jour: {state[:8]}...")
                    return True
                else:
                    logger.warning(f"Session OAuth à updater non trouvée: {state[:8]}...")
                    return False
                
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
                result = db.execute(
                    text("DELETE FROM oauth_sessions WHERE state = :state"),
                    {'state': state}
                )
                db.commit()
                
                if result.rowcount > 0:
                    logger.info(f"Session OAuth supprimée: {state[:8]}...")
                    return True
                return False
                
        except Exception as e:
            logger.error(f"Erreur suppression session OAuth {state[:8]}...: {e}")
            return False
    
    @staticmethod
    def _cleanup_expired_sessions(db):
        """Nettoyer les sessions expirées"""
        try:
            result = db.execute(
                text("DELETE FROM oauth_sessions WHERE expires_at < :now"),
                {'now': datetime.now()}
            )
            
            if result.rowcount > 0:
                logger.info(f"Sessions expirées nettoyées: {result.rowcount}")
                
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