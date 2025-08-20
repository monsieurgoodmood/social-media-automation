"""
Gestionnaire de connexion à la base de données PostgreSQL
Avec pool de connexions, retry automatique et gestion d'erreurs robuste
"""

import os
import time
import logging
from contextlib import contextmanager
from typing import Optional, Generator
from urllib.parse import quote_plus

from sqlalchemy import create_engine, pool, event, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError, OperationalError
from sqlalchemy.pool import QueuePool
import psycopg2
from psycopg2 import OperationalError as Psycopg2OperationalError

from .models import Base, User, FacebookAccount, LinkedinAccount
from ..utils.config import get_env_var

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Gestionnaire principal de la base de données avec pool de connexions"""
    
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self._connection_string = None
        self.max_retries = 3
        self.retry_delay = 1  # secondes
        
    def _build_connection_string(self) -> str:
        """Construire la chaîne de connexion PostgreSQL"""
        
        # Priorité : DATABASE_URL (pour déploiement) puis variables individuelles
        database_url = get_env_var('DATABASE_URL')
        if database_url:
            logger.info("Utilisation de DATABASE_URL pour la connexion")
            return database_url
            
        # Variables individuelles pour développement local
        db_host = get_env_var('DB_HOST', 'localhost')
        db_port = get_env_var('DB_PORT', '5432')
        db_name = get_env_var('DB_NAME', 'whatsthedata')
        db_user = get_env_var('DB_USER', 'postgres')
        db_password = get_env_var('DB_PASSWORD', '')
        
        # Encoder le mot de passe pour gérer les caractères spéciaux
        encoded_password = quote_plus(db_password)
        
        connection_string = f"postgresql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
        
        logger.info(f"Connexion à PostgreSQL: {db_user}@{db_host}:{db_port}/{db_name}")
        return connection_string
    
    def _create_engine(self):
        """Créer le moteur SQLAlchemy avec pool de connexions optimisé"""
        
        self._connection_string = self._build_connection_string()
        
        # Configuration du pool de connexions
        engine_config = {
            'poolclass': QueuePool,
            'pool_size': int(get_env_var('DB_POOL_SIZE', '10')),
            'max_overflow': int(get_env_var('DB_MAX_OVERFLOW', '20')),
            'pool_pre_ping': True,  # Vérifier les connexions avant utilisation
            'pool_recycle': int(get_env_var('DB_POOL_RECYCLE', '3600')),  # 1h
            'connect_args': {
                'connect_timeout': int(get_env_var('DB_CONNECT_TIMEOUT', '10')),
                'application_name': 'WhatTheData_SaaS',
                'options': '-c timezone=UTC'
            },
            'echo': get_env_var('DB_ECHO', 'false').lower() == 'true',
            'future': True  # SQLAlchemy 2.0 style
        }
        
        try:
            self.engine = create_engine(self._connection_string, **engine_config)
            
            # Événements pour monitoring
            self._setup_engine_events()
            
            logger.info("✅ Moteur de base de données créé avec succès")
            return self.engine
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du moteur: {e}")
            raise
    
    def _setup_engine_events(self):
        """Configurer les événements de monitoring du moteur"""
        
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            """Événement de connexion - configuration initiale"""
            if hasattr(dbapi_connection, 'set_session'):
                # Configuration PostgreSQL
                with dbapi_connection.cursor() as cursor:
                    cursor.execute("SET timezone TO 'UTC'")
                    cursor.execute("SET statement_timeout = '30s'")
        
        @event.listens_for(self.engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Événement quand une connexion est récupérée du pool"""
            logger.debug("Connexion récupérée du pool")
        
        @event.listens_for(self.engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            """Événement quand une connexion est retournée au pool"""
            logger.debug("Connexion retournée au pool")
    
    def initialize(self):
        """Initialiser la connexion et créer les tables si nécessaire"""
        
        if not self.engine:
            self._create_engine()
        
        # Créer la session factory
        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False
        )
        
        # Tester la connexion
        if self.test_connection():
            logger.info("✅ Connexion à la base de données établie")
            
            # Créer les tables si elles n'existent pas
            if get_env_var('AUTO_CREATE_TABLES', 'true').lower() == 'true':
                self.create_tables()
        else:
            raise ConnectionError("❌ Impossible de se connecter à la base de données")
    
    def test_connection(self, retries: int = None) -> bool:
        """Tester la connexion à la base de données avec retry automatique"""
        
        if retries is None:
            retries = self.max_retries
            
        for attempt in range(retries + 1):
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test")).fetchone()
                    if result and result.test == 1:
                        logger.info(f"✅ Test de connexion réussi (tentative {attempt + 1})")
                        return True
                        
            except (SQLAlchemyError, Psycopg2OperationalError) as e:
                if attempt < retries:
                    wait_time = self.retry_delay * (2 ** attempt)  # Backoff exponentiel
                    logger.warning(f"⚠️  Tentative {attempt + 1} échouée, retry dans {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Toutes les tentatives de connexion ont échoué: {e}")
                    return False
        
        return False
    
    def create_tables(self):
        """Créer toutes les tables définies dans les modèles"""
        
        try:
            # Créer toutes les tables
            Base.metadata.create_all(bind=self.engine)
            logger.info("✅ Tables créées/vérifiées avec succès")
            
            # Vérifier que les tables principales existent
            self._verify_core_tables()
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création des tables: {e}")
            raise
    
    def _verify_core_tables(self):
        """Vérifier que les tables essentielles existent"""
        
        core_tables = [
            'users', 'social_access_tokens', 'facebook_accounts', 'linkedin_accounts',
            'facebook_page_daily', 'facebook_posts_metadata', 'linkedin_page_daily',
            'linkedin_posts_metadata', 'looker_templates'
        ]
        
        with self.get_session() as session:
            for table in core_tables:
                try:
                    result = session.execute(text(f"SELECT 1 FROM {table} LIMIT 1"))
                    logger.debug(f"✅ Table '{table}' accessible")
                except Exception as e:
                    logger.error(f"❌ Problème avec la table '{table}': {e}")
                    raise
    
    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Context manager pour obtenir une session de base de données"""
        
        if not self.SessionLocal:
            raise RuntimeError("DatabaseManager non initialisé. Appelez initialize() d'abord.")
        
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Erreur dans la session, rollback effectué: {e}")
            raise
        finally:
            session.close()
    
    def get_session_direct(self) -> Session:
        """Obtenir une session directement (à fermer manuellement)"""
        
        if not self.SessionLocal:
            raise RuntimeError("DatabaseManager non initialisé. Appelez initialize() d'abord.")
        
        return self.SessionLocal()
    
    def execute_raw_query(self, query: str, params: dict = None):
        """Exécuter une requête SQL brute avec paramètres"""
        
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                conn.commit()
                return result
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'exécution de la requête: {e}")
            raise
    
    def get_table_info(self, table_name: str):
        """Obtenir des informations sur une table"""
        
        query = """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_name = :table_name
        AND table_schema = 'public'
        ORDER BY ordinal_position
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), {"table_name": table_name})
                return result.fetchall()
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des infos de table: {e}")
            return []
    
    def get_database_stats(self):
        """Obtenir des statistiques sur la base de données"""
        
        stats_queries = {
            'total_users': "SELECT COUNT(*) FROM users",
            'active_users': "SELECT COUNT(*) FROM users WHERE is_active = true",
            'facebook_accounts': "SELECT COUNT(*) FROM facebook_accounts WHERE is_active = true",
            'linkedin_accounts': "SELECT COUNT(*) FROM linkedin_accounts WHERE is_active = true",
            'facebook_posts': "SELECT COUNT(*) FROM facebook_posts_metadata",
            'linkedin_posts': "SELECT COUNT(*) FROM linkedin_posts_metadata",
            'database_size': """
                SELECT pg_size_pretty(pg_database_size(current_database())) as size
            """
        }
        
        stats = {}
        try:
            with self.engine.connect() as conn:
                for stat_name, query in stats_queries.items():
                    result = conn.execute(text(query)).fetchone()
                    if stat_name == 'database_size':
                        stats[stat_name] = result.size if result else 'Unknown'
                    else:
                        stats[stat_name] = result[0] if result else 0
            
            logger.info("📊 Statistiques de la base de données récupérées")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des statistiques: {e}")
            return {}
    
    def cleanup_old_data(self, days: int = 90):
        """Nettoyer les anciennes données (par exemple logs anciens)"""
        
        cleanup_queries = [
            f"""
            DELETE FROM facebook_page_daily 
            WHERE created_at < NOW() - INTERVAL '{days} days'
            AND date < CURRENT_DATE - INTERVAL '{days} days'
            """,
            f"""
            DELETE FROM linkedin_page_daily 
            WHERE created_at < NOW() - INTERVAL '{days} days'
            AND date < CURRENT_DATE - INTERVAL '{days} days'
            """
        ]
        
        try:
            deleted_total = 0
            with self.engine.connect() as conn:
                for query in cleanup_queries:
                    result = conn.execute(text(query))
                    deleted_count = result.rowcount
                    deleted_total += deleted_count
                    logger.info(f"🧹 Supprimé {deleted_count} lignes anciennes")
                
                conn.commit()
            
            logger.info(f"✅ Nettoyage terminé: {deleted_total} lignes supprimées")
            return deleted_total
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du nettoyage: {e}")
            raise
    
    def close(self):
        """Fermer toutes les connexions"""
        
        if self.engine:
            self.engine.dispose()
            logger.info("🔒 Connexions à la base de données fermées")

# ========================================
# INSTANCE GLOBALE ET FONCTIONS HELPER
# ========================================

# Instance globale du gestionnaire de base de données
db_manager = DatabaseManager()

def init_database():
    """Initialiser la base de données (à appeler au démarrage de l'app)"""
    
    try:
        db_manager.initialize()
        logger.info("🚀 Base de données initialisée avec succès")
        return True
    except Exception as e:
        logger.error(f"💥 Échec de l'initialisation de la base de données: {e}")
        return False

def get_db_session():
    """Fonction helper pour obtenir une session (FastAPI compatible)"""
    return db_manager.get_session()

def get_db_session_direct():
    """Fonction helper pour obtenir une session directe"""
    return db_manager.get_session_direct()

# ========================================
# FONCTIONS MÉTIER SPÉCIFIQUES
# ========================================

def create_user(email: str, firstname: str = None, lastname: str = None, 
                company: str = None, plan_type: str = 'free') -> Optional[User]:
    """Créer un nouvel utilisateur"""
    
    try:
        with db_manager.get_session() as session:
            # Vérifier si l'utilisateur existe déjà
            existing_user = session.query(User).filter(User.email == email).first()
            if existing_user:
                logger.warning(f"⚠️  Utilisateur avec email {email} existe déjà")
                return existing_user
            
            # Créer le nouvel utilisateur
            new_user = User(
                email=email,
                firstname=firstname,
                lastname=lastname,
                company=company,
                plan_type=plan_type,
                is_active=True
            )
            
            session.add(new_user)
            session.flush()  # Pour obtenir l'ID
            
            logger.info(f"✅ Utilisateur créé: {email} (ID: {new_user.id})")
            return new_user
            
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création de l'utilisateur: {e}")
        return None

def get_user_with_accounts(user_id: int) -> Optional[User]:
    """Récupérer un utilisateur avec ses comptes sociaux"""
    
    try:
        with db_manager.get_session() as session:
            user = session.query(User).filter(User.id == user_id).first()
            if user:
                # Charger les relations
                user.facebook_accounts
                user.linkedin_accounts
                user.social_tokens
            return user
    except Exception as e:
        logger.error(f"❌ Erreur lors de la récupération de l'utilisateur: {e}")
        return None

def add_facebook_account(user_id: int, page_id: str, page_name: str = None) -> Optional[FacebookAccount]:
    """Ajouter un compte Facebook à un utilisateur"""
    
    try:
        with db_manager.get_session() as session:
            # Vérifier si le compte existe déjà
            existing = session.query(FacebookAccount).filter(
                FacebookAccount.user_id == user_id,
                FacebookAccount.page_id == page_id
            ).first()
            
            if existing:
                existing.is_active = True
                existing.page_name = page_name or existing.page_name
                logger.info(f"✅ Compte Facebook réactivé: {page_id}")
                return existing
            
            # Créer le nouveau compte
            fb_account = FacebookAccount(
                user_id=user_id,
                page_id=page_id,
                page_name=page_name,
                is_active=True
            )
            
            session.add(fb_account)
            session.flush()
            
            logger.info(f"✅ Compte Facebook ajouté: {page_id} pour user {user_id}")
            return fb_account
            
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'ajout du compte Facebook: {e}")
        return None

def add_linkedin_account(user_id: int, organization_id: str, 
                        organization_name: str = None) -> Optional[LinkedinAccount]:
    """Ajouter un compte LinkedIn à un utilisateur"""
    
    try:
        with db_manager.get_session() as session:
            # Vérifier si le compte existe déjà
            existing = session.query(LinkedinAccount).filter(
                LinkedinAccount.user_id == user_id,
                LinkedinAccount.organization_id == organization_id
            ).first()
            
            if existing:
                existing.is_active = True
                existing.organization_name = organization_name or existing.organization_name
                logger.info(f"✅ Compte LinkedIn réactivé: {organization_id}")
                return existing
            
            # Créer le nouveau compte
            li_account = LinkedinAccount(
                user_id=user_id,
                organization_id=organization_id,
                organization_name=organization_name,
                is_active=True
            )
            
            session.add(li_account)
            session.flush()
            
            logger.info(f"✅ Compte LinkedIn ajouté: {organization_id} pour user {user_id}")
            return li_account
            
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'ajout du compte LinkedIn: {e}")
        return None

def health_check() -> dict:
    """Vérification de santé de la base de données"""
    
    result = {
        'database': 'unknown',
        'connection': False,
        'tables': False,
        'stats': {},
        'timestamp': time.time()
    }
    
    try:
        # Test de connexion
        if db_manager.test_connection(retries=1):
            result['connection'] = True
            result['database'] = 'postgresql'
            
            # Test des tables
            try:
                stats = db_manager.get_database_stats()
                result['tables'] = True
                result['stats'] = stats
            except:
                result['tables'] = False
        
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
    
    return result

# ========================================
# GESTION DES ERREURS SPÉCIFIQUES
# ========================================

class DatabaseConnectionError(Exception):
    """Erreur de connexion à la base de données"""
    pass

class DatabaseQueryError(Exception):
    """Erreur lors de l'exécution d'une requête"""
    pass

def handle_db_error(func):
    """Décorateur pour gérer les erreurs de base de données"""
    
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (DisconnectionError, OperationalError) as e:
            logger.error(f"❌ Erreur de connexion DB dans {func.__name__}: {e}")
            raise DatabaseConnectionError(f"Problème de connexion: {e}")
        except SQLAlchemyError as e:
            logger.error(f"❌ Erreur SQLAlchemy dans {func.__name__}: {e}")
            raise DatabaseQueryError(f"Erreur de requête: {e}")
        except Exception as e:
            logger.error(f"❌ Erreur inattendue dans {func.__name__}: {e}")
            raise
    
    return wrapper

# Auto-initialisation si ce module est importé directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    print("🧪 Test de la connexion à la base de données...")
    
    if init_database():
        print("✅ Connexion réussie !")
        
        # Afficher les statistiques
        stats = db_manager.get_database_stats()
        print(f"📊 Statistiques: {stats}")
        
        # Test de health check
        health = health_check()
        print(f"🏥 Health check: {health}")
        
    else:
        print("❌ Échec de la connexion")
def test_database_connection():
    """Test simple de connexion base de données"""
    try:
        return db_manager.test_connection(retries=1)
    except:
        return False
