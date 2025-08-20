import os
# app/main.py
# =============
# 🚀 API FASTAPI PURE - Pour Looker Studio et webhooks
# Votre Streamlit reste dans streamlit_app.py

from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from contextlib import asynccontextmanager
import logging
import time
import uvicorn

# Imports de vos modules
from app.utils.config import Config, settings
from app.database.connection import init_database, test_database_connection
# from app.api.looker_endpoints import router as looker_router

# Configuration des logs
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ================================
# 🚀 LIFECYCLE SIMPLIFIÉ
# ================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestion du cycle de vie de l'application"""
    
    # STARTUP
    logger.info("🚀 Démarrage WhatsTheData API (FastAPI)...")
    
    try:
        # Test de configuration
        validation = Config.validate_required_settings()
        if not validation['valid']:
            logger.error("❌ Configuration invalide:")
            for error in validation['errors']:
                logger.error(f"  • {error}")
            raise Exception("Configuration invalide")
        
        logger.info("✅ Configuration validée")
        
        # Test de connexion base de données
        if test_database_connection():
            logger.info("✅ Base de données connectée")
        else:
            logger.warning("⚠️ Problème de connexion base de données")
        
        logger.info("🎉 WhatsTheData API prête!")
        logger.info("📊 Endpoints Looker Studio disponibles")
        
    except Exception as e:
        logger.error(f"❌ Erreur au démarrage: {e}")
        raise
    
    yield
    
    # SHUTDOWN
    logger.info("🛑 Arrêt WhatsTheData API")

# ================================
# 🏗️ APPLICATION FASTAPI PURE
# ================================

app = FastAPI(
    title="WhatsTheData API",
    description="API pour connecteur Looker Studio et webhooks",
    version=Config.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if Config.DEBUG else None,
    redoc_url="/redoc" if Config.DEBUG else None
)

# ================================
# 🛡️ MIDDLEWARES
# ================================

# CORS pour Looker Studio
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://datastudio.google.com",
        "https://lookerstudio.google.com", 
        "https://script.google.com",
        "http://localhost:8501",  # Votre Streamlit
        *Config.get_cors_origins()
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Timing middleware
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

# ================================
# 🎯 ROUTES ESSENTIELLES
# ================================

@app.get("/")
async def root():
    """Page d'accueil API"""
    return {
        "service": "WhatsTheData API",
        "version": Config.APP_VERSION,
        "status": "online",
        "purpose": "API backend pour Looker Studio et webhooks",
        "streamlit_app": "http://localhost:8501",
        "endpoints": {
            "health": "/health",
            "looker_studio": "/api/v1/looker-data",
            "stripe_webhook": "/webhooks/stripe"
        }
    }

@app.get("/health")
async def health_check():
    """Health check rapide"""
    try:
        db_status = test_database_connection()
        return {
            "status": "healthy" if db_status else "degraded",
            "timestamp": time.time(),
            "database": "connected" if db_status else "disconnected",
            "api_version": Config.APP_VERSION
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
        )

# ================================
# 📊 ROUTES LOOKER STUDIO
# ================================

# Router principal pour Looker Studio
# app.include_router(
#     looker_router,
#     tags=["Looker Studio API"]
# )

# ================================
# 💳 WEBHOOK STRIPE
# ================================

@app.post("/webhooks/stripe")
async def stripe_webhook(request: Request):
    """Webhook Stripe pour les événements de paiement"""
    try:
        # TODO: Implémenter avec app/payments/stripe_handler.py
        payload = await request.body()
        signature = request.headers.get("stripe-signature")
        
        logger.info(f"Webhook Stripe reçu: {len(payload)} bytes")
        
        # Ici on traiterait le webhook avec stripe_handler
        return {"received": True}
        
    except Exception as e:
        logger.error(f"Erreur webhook Stripe: {e}")
        raise HTTPException(status_code=400, detail="Webhook error")

# ================================
# 🔗 CALLBACK OAUTH
# ================================

@app.get("/oauth/linkedin/callback")
async def linkedin_oauth_callback(request: Request):
    """Callback OAuth LinkedIn"""
    try:
        # TODO: Implémenter avec app/auth/linkedin_oauth.py
        code = request.query_params.get("code")
        state = request.query_params.get("state")
        
        if not code:
            return RedirectResponse("http://localhost:8501?error=linkedin_auth_failed")
        
        logger.info(f"LinkedIn OAuth callback reçu: code={code[:10]}...")
        
        # Traitement du code OAuth
        # success = linkedin_oauth.handle_callback(code, state)
        # if success:
        #     return RedirectResponse("http://localhost:8501?success=linkedin_connected")
        
        return RedirectResponse("http://localhost:8501?success=linkedin_connected")
        
    except Exception as e:
        logger.error(f"Erreur callback LinkedIn: {e}")
        return RedirectResponse("http://localhost:8501?error=linkedin_internal_error")

@app.get("/oauth/facebook/callback")
async def facebook_oauth_callback(request: Request):
    """Callback OAuth Facebook"""
    try:
        # TODO: Implémenter avec app/auth/facebook_oauth.py
        code = request.query_params.get("code")
        state = request.query_params.get("state")
        
        if not code:
            return RedirectResponse("http://localhost:8501?error=facebook_auth_failed")
        
        logger.info(f"Facebook OAuth callback reçu: code={code[:10]}...")
        
        # Traitement du code OAuth
        return RedirectResponse("http://localhost:8501?success=facebook_connected")
        
    except Exception as e:
        logger.error(f"Erreur callback Facebook: {e}")
        return RedirectResponse("http://localhost:8501?error=facebook_internal_error")

# ================================
# 🧪 ROUTES DEBUG
# ================================

@app.get("/debug/config")
async def debug_config():
    """Configuration debug (dev uniquement)"""
    if not Config.DEBUG:
        raise HTTPException(status_code=404, detail="Not found")
    
    summary = Config.get_env_summary()
    return {
        "environment": Config.ENVIRONMENT,
        "database_configured": summary['database']['configured'],
        "stripe_configured": summary['stripe']['configured'],
        "linkedin_configured": summary['linkedin']['community_configured'],
        "facebook_configured": summary['facebook']['configured'],
        "cors_origins": Config.get_cors_origins()
    }

# ================================
# 🚨 GESTION D'ERREURS
# ================================

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Gestionnaire d'erreurs global"""
    logger.error(f"Erreur non gérée: {exc}")
    
    return JSONResponse(
        status_code=500,
        content={
            "error": "Erreur serveur" if not Config.DEBUG else str(exc),
            "timestamp": time.time(),
            "path": str(request.url)
        }
    )

# ================================
# 🚀 POINT D'ENTRÉE
# ================================

if __name__ == "__main__":
    logger.info("🚀 Lancement direct WhatsTheData API")
    logger.info("🌐 API: http://localhost:8000")
    logger.info("📚 Documentation: http://localhost:8000/docs")
    logger.info("🖥️ Interface Streamlit: http://localhost:8501")
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=Config.DEBUG,
        log_level=Config.LOG_LEVEL.lower()
    )

# Export pour deployment
application = app