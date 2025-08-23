# app/main.py
# WhatsTheData API - Production FastAPI Application
# Connecteurs Looker Studio, Webhooks Stripe, Interface utilisateur

import os
import sys
import time
import logging
import uvicorn
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware

# Imports locaux avec gestion d'erreurs
try:
    from app.utils.config import Config
    from app.database.connection import init_database, test_database_connection, db_manager
    from app.api.looker_endpoints import router as looker_router
    from app.api.connect_routes import router as connect_router
except ImportError as e:
    print(f"ERREUR CRITIQUE: Import manquant - {e}")
    sys.exit(1)

# Import optionnel du router LinkedIn
linkedin_router_available = False
try:
    from app.api.looker_linkedin_endpoint import get_linkedin_router
    linkedin_router_available = True
except ImportError:
    print("Info: Router LinkedIn spécialisé non disponible - utilisation du router combiné uniquement")

# Configuration globale
BASE_URL = os.getenv('BASE_URL', 'https://whats-the-data-d954d4d4cb5f.herokuapp.com')
PORT = int(os.getenv("PORT", 8000))

# Configuration logging professionnelle
def setup_logging():
    """Configuration logging centralisée"""
    
    log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
    
    # Format selon l'environnement
    if Config.ENVIRONMENT == 'production':
        log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    else:
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('whatsthedata.log') if Config.ENVIRONMENT != 'production' else logging.StreamHandler()
        ]
    )
    
    # Réduire le bruit des librairies externes
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)

logger = setup_logging()

# ================================
# CYCLE DE VIE APPLICATION
# ================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestion complète du cycle de vie avec validation robuste"""
    
    startup_start = time.time()
    
    # === STARTUP ===
    logger.info("Démarrage WhatsTheData API...")
    
    try:
        # 1. Validation configuration critique
        config_validation = Config.validate_required_settings()
        if not config_validation['valid']:
            logger.error("Configuration invalide:")
            for error in config_validation['errors']:
                logger.error(f"  - {error}")
            raise Exception("Configuration critique manquante")
        
        logger.info("Configuration validée avec succès")
        
        # 2. Initialisation base de données
        db_initialized = False
        db_retries = 3
        
        for attempt in range(db_retries):
            try:
                if init_database():
                    db_initialized = True
                    logger.info("Base de données initialisée")
                    break
                else:
                    logger.warning(f"Tentative DB {attempt + 1}/{db_retries} échouée")
                    if attempt < db_retries - 1:
                        time.sleep(2)
            except Exception as e:
                logger.error(f"Erreur DB tentative {attempt + 1}: {e}")
                if attempt < db_retries - 1:
                    time.sleep(2)
        
        if not db_initialized:
            logger.error("Impossible d'initialiser la base de données")
            if Config.ENVIRONMENT == 'production':
                raise Exception("Base de données requise en production")
        
        # 3. Test des APIs externes critiques
        await test_external_apis()
        
        # 4. Validation des tokens
        validate_api_tokens()
        
        startup_time = time.time() - startup_start
        logger.info(f"WhatsTheData API démarrée en {startup_time:.2f}s")
        logger.info(f"Environnement: {Config.ENVIRONMENT}")
        logger.info(f"Base URL: {BASE_URL}")
        logger.info("Endpoints disponibles:")
        logger.info("  - Health Check: /health")
        logger.info("  - Looker Studio: /api/v1/looker-data")
        logger.info("  - LinkedIn Connector: /api/v1/linkedin/complete-metrics")
        logger.info("  - Stripe Webhook: /webhooks/stripe")
        
    except Exception as e:
        logger.critical(f"Erreur critique au démarrage: {e}")
        raise
    
    yield
    
    # === SHUTDOWN ===
    logger.info("Arrêt WhatsTheData API...")
    
    try:
        # Nettoyage connexions DB
        if hasattr(db_manager, 'close_connections'):
            db_manager.close_connections()
    except Exception as e:
        logger.error(f"Erreur lors de l'arrêt: {e}")
    
    logger.info("API arrêtée proprement")

async def test_external_apis():
    """Test de connectivité des APIs externes"""
    
    import httpx
    
    apis_to_test = [
        ("LinkedIn API", "https://api.linkedin.com/rest/me", [200, 401, 403]),
        ("Facebook API", "https://graph.facebook.com/me", [200, 400, 401]),
        ("Stripe API", "https://api.stripe.com/v1/charges", [401])  # 401 = accessible mais non authentifié
    ]
    
    async with httpx.AsyncClient(timeout=10) as client:
        for api_name, url, accepted_codes in apis_to_test:
            try:
                response = await client.get(url, timeout=5)
                if response.status_code in accepted_codes:
                    logger.info(f"{api_name}: accessible")
                else:
                    logger.warning(f"{api_name}: réponse inattendue {response.status_code}")
            except Exception as e:
                logger.warning(f"{api_name}: non accessible - {e}")

def validate_api_tokens():
    """Validation des tokens API critiques avec gestion d'attributs manquants"""
    
    # Utiliser getattr avec fallback pour éviter les erreurs d'attributs manquants
    tokens_status = {
        "LinkedIn Community": bool(getattr(Config, 'COMMUNITY_ACCESS_TOKEN', None)),
        "LinkedIn Access": bool(getattr(Config, 'LINKEDIN_ACCESS_TOKEN', None)), 
        "Stripe Secret": bool(getattr(Config, 'STRIPE_SECRET_KEY', None)),
        "Facebook Client": bool(getattr(Config, 'FB_CLIENT_SECRET', None))
    }
    
    for token_name, is_valid in tokens_status.items():
        if is_valid:
            logger.info(f"Token {token_name}: configuré")
        else:
            logger.warning(f"Token {token_name}: manquant")
    
    # Vérifier au moins un token majeur disponible
    major_tokens = [
        getattr(Config, 'COMMUNITY_ACCESS_TOKEN', None),
        getattr(Config, 'LINKEDIN_ACCESS_TOKEN', None),
        getattr(Config, 'STRIPE_SECRET_KEY', None)
    ]
    
    if not any(major_tokens):
        logger.error("Aucun token critique configuré")
        if getattr(Config, 'ENVIRONMENT', 'development') == 'production':
            raise Exception("Tokens API requis en production")
    else:
        logger.info("Au moins un token majeur est configuré")

# ================================
# APPLICATION FASTAPI
# ================================

def create_app() -> FastAPI:
    """Factory pour créer l'application FastAPI"""
    
    # Configuration selon l'environnement
    docs_enabled = Config.DEBUG or Config.ENVIRONMENT != 'production'
    
    app = FastAPI(
        title="WhatsTheData API",
        description="API backend pour connecteurs Looker Studio, webhooks Stripe et gestion utilisateurs",
        version=Config.APP_VERSION,
        lifespan=lifespan,
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json" if docs_enabled else None
    )
    
    return app

app = create_app()

# ================================
# MIDDLEWARES DE SÉCURITÉ
# ================================

# Compression pour les réponses volumineuses
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Hosts de confiance
if Config.ENVIRONMENT == 'production':
    trusted_hosts = [
        "whats-the-data-d954d4d4cb5f.herokuapp.com",
        "*.herokuapp.com",
        "datastudio.google.com",
        "lookerstudio.google.com"
    ]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts)

# CORS optimisé pour Looker Studio
allowed_origins = [
    "https://datastudio.google.com",
    "https://lookerstudio.google.com",
    "https://script.google.com",
    "https://accounts.google.com",
    BASE_URL
]

# Ajout des origines de développement si applicable
if Config.DEBUG:
    allowed_origins.extend([
        "http://localhost:8501",
        "http://localhost:3000",
        "http://127.0.0.1:8501",
        "http://127.0.0.1:3000"
    ])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD"],
    allow_headers=[
        "Authorization",
        "Content-Type",
        "X-Requested-With",
        "Accept",
        "Origin",
        "User-Agent",
        "DNT",
        "Cache-Control",
        "X-Mx-ReqToken",
        "Keep-Alive",
        "X-User-Agent"
    ],
    max_age=3600
)

# Middleware de monitoring avancé
@app.middleware("http")
async def monitoring_middleware(request: Request, call_next):
    """Middleware de monitoring et performance"""
    
    start_time = time.time()
    
    # Log des requêtes importantes
    if any(path in str(request.url.path) for path in ['/api/', '/webhooks/', '/connect/']):
        logger.info(f"Request: {request.method} {request.url.path}")
    
    try:
        response = await call_next(request)
        
        # Calcul du temps de traitement
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = f"{process_time:.4f}"
        response.headers["X-API-Version"] = Config.APP_VERSION
        
        # Log des réponses lentes
        if process_time > 2.0:
            logger.warning(f"Requête lente: {request.method} {request.url.path} - {process_time:.2f}s")
        
        return response
        
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"Erreur middleware: {request.method} {request.url.path} - {e} - {process_time:.2f}s")
        raise

# ================================
# ROUTES PRINCIPALES
# ================================

@app.get("/")
async def root():
    """Page d'accueil API avec informations système"""
    
    try:
        db_status = test_database_connection()
    except:
        db_status = False
    
    return {
        "service": "WhatsTheData API",
        "version": Config.APP_VERSION,
        "status": "online",
        "environment": Config.ENVIRONMENT,
        "purpose": "Backend API pour Looker Studio et gestion SaaS",
        "database_status": "connected" if db_status else "disconnected",
        "timestamp": time.time(),
        "endpoints": {
            "documentation": "/docs" if Config.DEBUG else "disabled",
            "health_check": "/health",
            "system_status": "/status",
            "looker_combined": "/api/v1/combined/metrics",
            "looker_linkedin": "/api/v1/linkedin/complete-metrics", 
            "stripe_webhook": "/webhooks/stripe",
            "user_connection": "/connect"
        },
        "external_integrations": {
            "looker_studio": "active",
            "stripe_payments": "configured" if Config.STRIPE_SECRET_KEY else "pending",
            "linkedin_api": "configured" if Config.COMMUNITY_ACCESS_TOKEN else "pending",
            "facebook_api": "configured" if Config.FB_CLIENT_SECRET else "pending"
        }
    }

@app.get("/health")
async def health_check():
    """Health check standard avec détails système"""
    
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "uptime": time.time(),  # Sera remplacé par un vrai uptime
        "version": Config.APP_VERSION,
        "environment": Config.ENVIRONMENT
    }
    
    try:
        # Test base de données
        db_connected = test_database_connection()
        health_status["database"] = {
            "status": "connected" if db_connected else "disconnected",
            "tested_at": time.time()
        }
        
        # Test configuration critique
        config_validation = Config.validate_required_settings()
        health_status["configuration"] = {
            "valid": config_validation['valid'],
            "errors": config_validation.get('errors', [])
        }
        
        # Déterminer le statut global
        if not db_connected:
            health_status["status"] = "degraded"
            
        if not config_validation['valid']:
            health_status["status"] = "unhealthy"
            
        status_code = 200 if health_status["status"] == "healthy" else 503
        
        return JSONResponse(content=health_status, status_code=status_code)
        
    except Exception as e:
        logger.error(f"Erreur health check: {e}")
        return JSONResponse(
            content={
                "status": "error",
                "error": str(e) if Config.DEBUG else "Health check failed",
                "timestamp": time.time()
            },
            status_code=503
        )

@app.get("/status")
async def system_status():
    """Status système détaillé pour monitoring"""
    
    try:
        status_info = {
            "system": {
                "api_version": getattr(Config, 'APP_VERSION', '1.0.0'),
                "environment": getattr(Config, 'ENVIRONMENT', 'development'),
                "debug_mode": getattr(Config, 'DEBUG', True),
                "log_level": getattr(Config, 'LOG_LEVEL', 'INFO'),
                "timestamp": time.time()
            },
            "database": {
                "connected": False,
                "last_check": time.time()
            },
            "external_apis": {
                "linkedin": {"configured": bool(getattr(Config, 'COMMUNITY_ACCESS_TOKEN', None))},
                "facebook": {"configured": bool(getattr(Config, 'FB_CLIENT_SECRET', None))},
                "stripe": {"configured": bool(getattr(Config, 'STRIPE_SECRET_KEY', None))}
            },
            "features": {
                "looker_studio": "active",
                "stripe_webhooks": "active", 
                "user_management": "active",
                "linkedin_router": "available" if linkedin_router_available else "pending"
            }
        }
        
        # Test DB avec timeout
        try:
            status_info["database"]["connected"] = test_database_connection()
        except Exception as e:
            status_info["database"]["error"] = str(e)
            logger.warning(f"Test DB failed: {e}")
        
        return status_info
        
    except Exception as e:
        logger.error(f"Erreur system status: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "System status error",
                "message": str(e) if Config.DEBUG else "Status unavailable",
                "timestamp": time.time()
            }
        )

# ================================
# INTÉGRATION DES ROUTERS
# ================================

# Router principal Looker Studio (existant)
app.include_router(
    looker_router,
    tags=["Looker Studio - Combined"]
)

# Nouveau router LinkedIn spécialisé (si disponible)
if linkedin_router_available:
    app.include_router(
        get_linkedin_router(),
        tags=["Looker Studio - LinkedIn"]
    )
    logger.info("Router LinkedIn spécialisé activé")
else:
    logger.info("Router LinkedIn spécialisé non disponible - utilisation du router combiné")

# Router de connexion utilisateur
app.include_router(
    connect_router,
    prefix="",
    tags=["User Connection"]
)

# ================================
# WEBHOOKS ET INTÉGRATIONS
# ================================

@app.post("/webhooks/stripe")
async def stripe_webhook_handler(request: Request):
    """Webhook Stripe avec validation et sécurité"""
    
    start_time = time.time()
    
    try:
        # Récupération du payload
        payload = await request.body()
        signature = request.headers.get("stripe-signature", "")
        
        if not signature:
            logger.warning("Webhook Stripe sans signature")
            raise HTTPException(status_code=400, detail="Missing signature")
        
        if len(payload) == 0:
            logger.warning("Webhook Stripe payload vide")
            raise HTTPException(status_code=400, detail="Empty payload")
        
        # Log du webhook (sans données sensibles)
        logger.info(f"Webhook Stripe reçu: {len(payload)} bytes, signature présente")
        
        # TODO: Traitement avec stripe_handler
        # from app.payments.stripe_handler import process_webhook
        # result = process_webhook(payload, signature)
        
        processing_time = time.time() - start_time
        logger.info(f"Webhook Stripe traité en {processing_time:.3f}s")
        
        return {"received": True, "processed_at": time.time()}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur critique webhook Stripe: {e}")
        raise HTTPException(status_code=500, detail="Webhook processing failed")

# ================================
# ROUTES DE DÉVELOPPEMENT
# ================================

if Config.DEBUG:
    
    @app.get("/debug/config")
    async def debug_configuration():
        """Configuration détaillée pour debug"""
        
        return {
            "environment_summary": Config.get_env_summary(),
            "cors_origins": allowed_origins,
            "trusted_hosts": app.user_middleware[0].allowed_hosts if Config.ENVIRONMENT == 'production' else "all",
            "database_url_configured": bool(Config.DATABASE_URL),
            "base_url": BASE_URL,
            "port": PORT,
            "docs_enabled": True,
            "middleware_count": len(app.user_middleware)
        }
    
    @app.get("/debug/test-db")
    async def debug_database():
        """Test détaillé de la base de données"""
        
        try:
            connection_test = test_database_connection()
            
            result = {
                "connection_test": connection_test,
                "database_url_configured": bool(Config.DATABASE_URL),
                "timestamp": time.time()
            }
            
            if hasattr(db_manager, 'get_session'):
                try:
                    with db_manager.get_session() as session:
                        # Test simple query
                        session.execute("SELECT 1")
                        result["query_test"] = True
                except Exception as e:
                    result["query_test"] = False
                    result["query_error"] = str(e)
            
            return result
            
        except Exception as e:
            return JSONResponse(
                content={"error": str(e), "timestamp": time.time()},
                status_code=500
            )

# ================================
# GESTION D'ERREURS GLOBALE
# ================================

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Gestion standardisée des erreurs HTTP"""
    
    error_response = {
        "error": {
            "code": exc.status_code,
            "message": exc.detail,
            "timestamp": time.time(),
            "path": str(request.url.path)
        }
    }
    
    # Log selon la sévérité
    if exc.status_code >= 500:
        logger.error(f"HTTP {exc.status_code}: {exc.detail} - {request.url.path}")
    elif exc.status_code >= 400:
        logger.warning(f"HTTP {exc.status_code}: {exc.detail} - {request.url.path}")
    
    return JSONResponse(content=error_response, status_code=exc.status_code)

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Gestionnaire d'erreurs générales avec logging sécurisé"""
    
    error_id = f"error_{int(time.time())}"
    
    # Log complet côté serveur
    logger.error(f"Erreur non gérée [{error_id}]: {exc}", exc_info=True)
    
    # Réponse utilisateur (sans détails sensibles en production)
    error_response = {
        "error": {
            "code": 500,
            "message": str(exc) if Config.DEBUG else "Erreur interne du serveur",
            "error_id": error_id,
            "timestamp": time.time(),
            "path": str(request.url.path)
        }
    }
    
    return JSONResponse(content=error_response, status_code=500)

# ================================
# POINT D'ENTRÉE PRINCIPAL
# ================================

def main():
    """Point d'entrée principal avec configuration optimisée"""
    
    logger.info("Lancement WhatsTheData API")
    logger.info(f"Environment: {Config.ENVIRONMENT}")
    logger.info(f"Port: {PORT}")
    logger.info(f"Base URL: {BASE_URL}")
    logger.info(f"Debug: {Config.DEBUG}")
    
    # Configuration uvicorn selon l'environnement
    uvicorn_config = {
        "app": "app.main:app",
        "host": "0.0.0.0",
        "port": PORT,
        "log_level": Config.LOG_LEVEL.lower(),
        "access_log": Config.DEBUG,
        "reload": Config.DEBUG and Config.ENVIRONMENT != 'production'
    }
    
    # Configuration production
    if Config.ENVIRONMENT == 'production':
        uvicorn_config.update({
            "workers": 1,  # Heroku préfère 1 worker
            "loop": "uvloop",
            "http": "httptools",
            "reload": False
        })
    
    try:
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        logger.info("Arrêt demandé par l'utilisateur")
    except Exception as e:
        logger.critical(f"Erreur fatale au lancement: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# Export pour déploiement Heroku
application = app