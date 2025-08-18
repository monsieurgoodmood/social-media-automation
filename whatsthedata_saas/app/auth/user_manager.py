"""
Gestionnaire d'authentification et d'utilisateurs
Avec intégration Stripe, JWT, et gestion des abonnements
"""

import os
import jwt
import hashlib
import secrets
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum

import bcrypt
import stripe
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from ..database.connection import db_manager, DatabaseManager
from ..database.models import (
    User, SocialAccessToken, FacebookAccount, LinkedinAccount, 
    LookerTemplate, UserTemplateAccess
)
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

# Configuration Stripe
stripe.api_key = get_env_var('STRIPE_SECRET_KEY')

class PlanType(Enum):
    """Types d'abonnements disponibles"""
    FREE = "free"
    LINKEDIN_BASIC = "linkedin_basic"
    FACEBOOK_BASIC = "facebook_basic"
    PREMIUM = "premium"

@dataclass
class PlanFeatures:
    """Fonctionnalités incluses dans chaque plan"""
    name: str
    max_facebook_accounts: int
    max_linkedin_accounts: int
    data_retention_days: int
    looker_templates: List[str]
    api_calls_per_month: int
    price_monthly: float
    stripe_price_id: str

class PlanManager:
    """Gestionnaire des plans et fonctionnalités"""
    
    PLANS = {
        PlanType.FREE: PlanFeatures(
            name="Gratuit",
            max_facebook_accounts=0,
            max_linkedin_accounts=1,
            data_retention_days=30,
            looker_templates=["basic_linkedin"],
            api_calls_per_month=100,
            price_monthly=0.0,
            stripe_price_id=""
        ),
        PlanType.LINKEDIN_BASIC: PlanFeatures(
            name="LinkedIn Basic",
            max_facebook_accounts=0,
            max_linkedin_accounts=3,
            data_retention_days=90,
            looker_templates=["basic_linkedin", "advanced_linkedin"],
            api_calls_per_month=1000,
            price_monthly=19.99,
            stripe_price_id=get_env_var('STRIPE_PRICE_LINKEDIN_BASIC', 'price_linkedin_basic')
        ),
        PlanType.FACEBOOK_BASIC: PlanFeatures(
            name="Facebook Basic",
            max_facebook_accounts=3,
            max_linkedin_accounts=0,
            data_retention_days=90,
            looker_templates=["basic_facebook", "advanced_facebook"],
            api_calls_per_month=1000,
            price_monthly=19.99,
            stripe_price_id=get_env_var('STRIPE_PRICE_FACEBOOK_BASIC', 'price_facebook_basic')
        ),
        PlanType.PREMIUM: PlanFeatures(
            name="Premium",
            max_facebook_accounts=10,
            max_linkedin_accounts=10,
            data_retention_days=365,
            looker_templates=["basic_linkedin", "advanced_linkedin", "basic_facebook", "advanced_facebook", "unified_dashboard"],
            api_calls_per_month=10000,
            price_monthly=49.99,
            stripe_price_id=get_env_var('STRIPE_PRICE_PREMIUM', 'price_premium')
        )
    }
    
    @classmethod
    def get_plan_features(cls, plan_type: str) -> PlanFeatures:
        """Récupérer les fonctionnalités d'un plan"""
        plan_enum = PlanType(plan_type) if isinstance(plan_type, str) else plan_type
        return cls.PLANS.get(plan_enum, cls.PLANS[PlanType.FREE])
    
    @classmethod
    def can_add_facebook_account(cls, user: User) -> bool:
        """Vérifier si l'utilisateur peut ajouter un compte Facebook"""
        features = cls.get_plan_features(user.plan_type)
        current_count = len([acc for acc in user.facebook_accounts if acc.is_active])
        return current_count < features.max_facebook_accounts
    
    @classmethod
    def can_add_linkedin_account(cls, user: User) -> bool:
        """Vérifier si l'utilisateur peut ajouter un compte LinkedIn"""
        features = cls.get_plan_features(user.plan_type)
        current_count = len([acc for acc in user.linkedin_accounts if acc.is_active])
        return current_count < features.max_linkedin_accounts

class AuthenticationError(Exception):
    """Erreur d'authentification"""
    pass

class SubscriptionError(Exception):
    """Erreur liée aux abonnements"""
    pass

class UserManager:
    """Gestionnaire principal des utilisateurs"""
    
    def __init__(self):
        self.jwt_secret = get_env_var('JWT_SECRET_KEY', 'default-jwt-secret-change-me')
        self.jwt_algorithm = get_env_var('JWT_ALGORITHM', 'HS256')
        self.jwt_expiration_hours = int(get_env_var('JWT_EXPIRATION_HOURS', '24'))
        
    def hash_password(self, password: str) -> str:
        """Hasher un mot de passe"""
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
        return hashed.decode('utf-8')
    
    def verify_password(self, password: str, hashed_password: str) -> bool:
        """Vérifier un mot de passe"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed_password.encode('utf-8'))
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification du mot de passe: {e}")
            return False
    
    def generate_jwt_token(self, user_id: int, email: str) -> str:
        """Générer un token JWT pour un utilisateur"""
        
        payload = {
            'user_id': user_id,
            'email': email,
            'exp': datetime.utcnow() + timedelta(hours=self.jwt_expiration_hours),
            'iat': datetime.utcnow(),
            'iss': get_env_var('APP_NAME', 'WhatsTheData')
        }
        
        try:
            token = jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)
            logger.info(f"✅ Token JWT généré pour user {user_id}")
            return token
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération du token: {e}")
            raise AuthenticationError("Impossible de générer le token")
    
    def verify_jwt_token(self, token: str) -> Dict[str, Any]:
        """Vérifier et décoder un token JWT"""
        
        try:
            payload = jwt.decode(token, self.jwt_secret, algorithms=[self.jwt_algorithm])
            return payload
        except jwt.ExpiredSignatureError:
            logger.warning("⚠️  Token expiré")
            raise AuthenticationError("Token expiré")
        except jwt.InvalidTokenError as e:
            logger.warning(f"⚠️  Token invalide: {e}")
            raise AuthenticationError("Token invalide")
    
    def create_user(self, email: str, password: str, firstname: str = None, 
                   lastname: str = None, company: str = None, 
                   plan_type: str = 'free') -> Optional[User]:
        """Créer un nouvel utilisateur avec mot de passe"""
        
        try:
            with db_manager.get_session() as session:
                # Vérifier si l'utilisateur existe déjà
                existing_user = session.query(User).filter(User.email == email).first()
                if existing_user:
                    logger.warning(f"⚠️  Utilisateur avec email {email} existe déjà")
                    raise AuthenticationError("Un compte avec cet email existe déjà")
                
                # Hasher le mot de passe
                password_hash = self.hash_password(password)
                
                # Créer l'utilisateur
                new_user = User(
                    email=email,
                    password_hash=password_hash,
                    firstname=firstname,
                    lastname=lastname,
                    company=company,
                    plan_type=plan_type,
                    is_active=True
                )
                
                session.add(new_user)
                session.flush()  # Pour obtenir l'ID
                
                # Créer un client Stripe pour l'utilisateur
                try:
                    stripe_customer = self.create_stripe_customer(new_user)
                    logger.info(f"✅ Client Stripe créé: {stripe_customer.id}")
                except Exception as e:
                    logger.warning(f"⚠️  Impossible de créer le client Stripe: {e}")
                
                logger.info(f"✅ Utilisateur créé: {email} (ID: {new_user.id})")
                return new_user
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de l'utilisateur: {e}")
            raise
    
    def authenticate_user(self, email: str, password: str) -> Optional[User]:
        """Authentifier un utilisateur avec email/mot de passe"""
        
        try:
            with db_manager.get_session() as session:
                user = session.query(User).filter(
                    and_(User.email == email, User.is_active == True)
                ).first()
                
                if not user:
                    logger.warning(f"⚠️  Utilisateur non trouvé: {email}")
                    raise AuthenticationError("Email ou mot de passe incorrect")
                
                if not self.verify_password(password, user.password_hash):
                    logger.warning(f"⚠️  Mot de passe incorrect pour: {email}")
                    raise AuthenticationError("Email ou mot de passe incorrect")
                
                # Mettre à jour la dernière connexion
                user.last_login = datetime.utcnow()
                session.commit()
                
                logger.info(f"✅ Utilisateur authentifié: {email}")
                return user
                
        except AuthenticationError:
            raise
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'authentification: {e}")
            raise AuthenticationError("Erreur lors de l'authentification")
    
    def get_user_by_id(self, user_id: int) -> Optional[User]:
        """Récupérer un utilisateur par son ID"""
        
        try:
            with db_manager.get_session() as session:
                user = session.query(User).filter(
                    and_(User.id == user_id, User.is_active == True)
                ).first()
                
                if user:
                    # Charger les relations
                    user.facebook_accounts
                    user.linkedin_accounts
                    user.social_tokens
                
                return user
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération de l'utilisateur: {e}")
            return None
    
    def get_user_by_token(self, token: str) -> Optional[User]:
        """Récupérer un utilisateur à partir d'un token JWT"""
        
        try:
            payload = self.verify_jwt_token(token)
            user_id = payload.get('user_id')
            
            if not user_id:
                raise AuthenticationError("Token invalide")
            
            return self.get_user_by_id(user_id)
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération par token: {e}")
            return None
    
    def update_user_plan(self, user_id: int, new_plan: str, 
                         subscription_end_date: datetime = None) -> bool:
        """Mettre à jour le plan d'un utilisateur"""
        
        try:
            with db_manager.get_session() as session:
                user = session.query(User).filter(User.id == user_id).first()
                if not user:
                    logger.error(f"❌ Utilisateur {user_id} non trouvé")
                    return False
                
                old_plan = user.plan_type
                user.plan_type = new_plan
                
                if subscription_end_date:
                    user.subscription_end_date = subscription_end_date
                
                session.commit()
                
                logger.info(f"✅ Plan utilisateur {user_id} mis à jour: {old_plan} → {new_plan}")
                
                # Donner accès aux templates du nouveau plan
                self.grant_template_access(user_id, new_plan)
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour du plan: {e}")
            return False
    
    def check_subscription_validity(self, user: User) -> bool:
        """Vérifier si l'abonnement de l'utilisateur est valide"""
        
        if user.plan_type == PlanType.FREE.value:
            return True
        
        if not user.subscription_end_date:
            return False
        
        return user.subscription_end_date > datetime.utcnow()
    
    # ========================================
    # GESTION DES TOKENS SOCIAUX
    # ========================================
    
    def store_social_token(self, user_id: int, platform: str, access_token: str,
                          refresh_token: str = None, expires_at: datetime = None,
                          scope: str = None) -> bool:
        """Stocker un token social pour un utilisateur"""
        
        try:
            with db_manager.get_session() as session:
                # Désactiver les anciens tokens
                session.query(SocialAccessToken).filter(
                    and_(
                        SocialAccessToken.user_id == user_id,
                        SocialAccessToken.platform == platform
                    )
                ).update({"is_active": False})
                
                # Créer le nouveau token
                social_token = SocialAccessToken(
                    user_id=user_id,
                    platform=platform,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    expires_at=expires_at,
                    scope=scope,
                    is_active=True
                )
                
                session.add(social_token)
                session.commit()
                
                logger.info(f"✅ Token {platform} stocké pour user {user_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du stockage du token: {e}")
            return False
    
    def get_social_token(self, user_id: int, platform: str) -> Optional[SocialAccessToken]:
        """Récupérer le token social actif d'un utilisateur"""
        
        try:
            with db_manager.get_session() as session:
                token = session.query(SocialAccessToken).filter(
                    and_(
                        SocialAccessToken.user_id == user_id,
                        SocialAccessToken.platform == platform,
                        SocialAccessToken.is_active == True
                    )
                ).first()
                
                return token
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du token: {e}")
            return None
    
    def revoke_social_token(self, user_id: int, platform: str) -> bool:
        """Révoquer un token social"""
        
        try:
            with db_manager.get_session() as session:
                session.query(SocialAccessToken).filter(
                    and_(
                        SocialAccessToken.user_id == user_id,
                        SocialAccessToken.platform == platform
                    )
                ).update({"is_active": False})
                
                session.commit()
                
                logger.info(f"✅ Token {platform} révoqué pour user {user_id}")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la révocation du token: {e}")
            return False
    
    # ========================================
    # GESTION DES COMPTES SOCIAUX
    # ========================================
    
    def add_facebook_account(self, user_id: int, page_id: str, 
                           page_name: str = None) -> Optional[FacebookAccount]:
        """Ajouter un compte Facebook avec vérification des limites"""
        
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                raise ValueError("Utilisateur non trouvé")
            
            # Vérifier les limites du plan
            if not PlanManager.can_add_facebook_account(user):
                features = PlanManager.get_plan_features(user.plan_type)
                raise SubscriptionError(
                    f"Limite atteinte: {features.max_facebook_accounts} comptes Facebook max pour le plan {features.name}"
                )
            
            with db_manager.get_session() as session:
                # Vérifier si le compte existe déjà
                existing = session.query(FacebookAccount).filter(
                    and_(
                        FacebookAccount.user_id == user_id,
                        FacebookAccount.page_id == page_id
                    )
                ).first()
                
                if existing:
                    existing.is_active = True
                    existing.page_name = page_name or existing.page_name
                    session.commit()
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
                session.commit()
                
                logger.info(f"✅ Compte Facebook ajouté: {page_id} pour user {user_id}")
                return fb_account
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'ajout du compte Facebook: {e}")
            raise
    
    def add_linkedin_account(self, user_id: int, organization_id: str,
                           organization_name: str = None) -> Optional[LinkedinAccount]:
        """Ajouter un compte LinkedIn avec vérification des limites"""
        
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                raise ValueError("Utilisateur non trouvé")
            
            # Vérifier les limites du plan
            if not PlanManager.can_add_linkedin_account(user):
                features = PlanManager.get_plan_features(user.plan_type)
                raise SubscriptionError(
                    f"Limite atteinte: {features.max_linkedin_accounts} comptes LinkedIn max pour le plan {features.name}"
                )
            
            with db_manager.get_session() as session:
                # Vérifier si le compte existe déjà
                existing = session.query(LinkedinAccount).filter(
                    and_(
                        LinkedinAccount.user_id == user_id,
                        LinkedinAccount.organization_id == organization_id
                    )
                ).first()
                
                if existing:
                    existing.is_active = True
                    existing.organization_name = organization_name or existing.organization_name
                    session.commit()
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
                session.commit()
                
                logger.info(f"✅ Compte LinkedIn ajouté: {organization_id} pour user {user_id}")
                return li_account
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'ajout du compte LinkedIn: {e}")
            raise
    
    def get_user_accounts(self, user_id: int) -> Dict[str, List]:
        """Récupérer tous les comptes actifs d'un utilisateur"""
        
        try:
            with db_manager.get_session() as session:
                facebook_accounts = session.query(FacebookAccount).filter(
                    and_(
                        FacebookAccount.user_id == user_id,
                        FacebookAccount.is_active == True
                    )
                ).all()
                
                linkedin_accounts = session.query(LinkedinAccount).filter(
                    and_(
                        LinkedinAccount.user_id == user_id,
                        LinkedinAccount.is_active == True
                    )
                ).all()
                
                return {
                    'facebook': facebook_accounts,
                    'linkedin': linkedin_accounts
                }
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des comptes: {e}")
            return {'facebook': [], 'linkedin': []}
    
    # ========================================
    # INTÉGRATION STRIPE
    # ========================================
    
    def create_stripe_customer(self, user: User) -> stripe.Customer:
        """Créer un client Stripe pour un utilisateur"""
        
        try:
            customer = stripe.Customer.create(
                email=user.email,
                name=f"{user.firstname or ''} {user.lastname or ''}".strip(),
                metadata={
                    'user_id': user.id,
                    'plan_type': user.plan_type,
                    'app': 'whatsthedata'
                }
            )
            
            logger.info(f"✅ Client Stripe créé: {customer.id} pour user {user.id}")
            return customer
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création du client Stripe: {e}")
            raise
    
    def create_checkout_session(self, user_id: int, plan_type: str, 
                               success_url: str, cancel_url: str) -> stripe.checkout.Session:
        """Créer une session de paiement Stripe"""
        
        try:
            user = self.get_user_by_id(user_id)
            if not user:
                raise ValueError("Utilisateur non trouvé")
            
            plan_features = PlanManager.get_plan_features(plan_type)
            
            if not plan_features.stripe_price_id:
                raise ValueError(f"Plan {plan_type} non disponible pour l'achat")
            
            # Récupérer ou créer le client Stripe
            try:
                # Chercher un client existant
                customers = stripe.Customer.list(email=user.email, limit=1)
                if customers.data:
                    customer = customers.data[0]
                else:
                    customer = self.create_stripe_customer(user)
            except:
                customer = self.create_stripe_customer(user)
            
            # Créer la session de checkout
            session = stripe.checkout.Session.create(
                customer=customer.id,
                payment_method_types=['card'],
                line_items=[{
                    'price': plan_features.stripe_price_id,
                    'quantity': 1,
                }],
                mode='subscription',
                success_url=success_url + '?session_id={CHECKOUT_SESSION_ID}',
                cancel_url=cancel_url,
                metadata={
                    'user_id': user_id,
                    'plan_type': plan_type,
                }
            )
            
            logger.info(f"✅ Session Stripe créée: {session.id} pour user {user_id}")
            return session
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de la session Stripe: {e}")
            raise
    
    def handle_stripe_webhook(self, event_type: str, event_data: Dict) -> bool:
        """Gérer les webhooks Stripe"""
        
        try:
            if event_type == 'checkout.session.completed':
                session = event_data['object']
                user_id = int(session['metadata']['user_id'])
                plan_type = session['metadata']['plan_type']
                
                # Mettre à jour l'abonnement de l'utilisateur
                subscription_end = datetime.utcnow() + timedelta(days=30)  # 1 mois
                success = self.update_user_plan(user_id, plan_type, subscription_end)
                
                if success:
                    logger.info(f"✅ Abonnement activé via Stripe: user {user_id} → {plan_type}")
                    return True
                
            elif event_type == 'invoice.payment_failed':
                # Gérer les échecs de paiement
                customer_id = event_data['object']['customer']
                logger.warning(f"⚠️  Échec de paiement pour customer {customer_id}")
                
            elif event_type == 'customer.subscription.deleted':
                # Gérer l'annulation d'abonnement
                customer_id = event_data['object']['customer']
                logger.info(f"📅 Abonnement annulé pour customer {customer_id}")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement webhook Stripe: {e}")
            return False
    
    # ========================================
    # GESTION DES TEMPLATES LOOKER STUDIO
    # ========================================
    
    def grant_template_access(self, user_id: int, plan_type: str) -> bool:
        """Donner accès aux templates Looker Studio selon le plan"""
        
        try:
            plan_features = PlanManager.get_plan_features(plan_type)
            
            with db_manager.get_session() as session:
                # Récupérer les templates disponibles pour ce plan
                templates = session.query(LookerTemplate).filter(
                    and_(
                        LookerTemplate.name.in_(plan_features.looker_templates),
                        LookerTemplate.is_active == True
                    )
                ).all()
                
                # Donner accès à chaque template
                for template in templates:
                    # Vérifier si l'accès existe déjà
                    existing_access = session.query(UserTemplateAccess).filter(
                        and_(
                            UserTemplateAccess.user_id == user_id,
                            UserTemplateAccess.template_id == template.id
                        )
                    ).first()
                    
                    if not existing_access:
                        access = UserTemplateAccess(
                            user_id=user_id,
                            template_id=template.id,
                            is_active=True
                        )
                        session.add(access)
                    else:
                        existing_access.is_active = True
                
                session.commit()
                
                logger.info(f"✅ Accès templates accordé pour user {user_id}: {len(templates)} templates")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'attribution des templates: {e}")
            return False
    
    def get_user_templates(self, user_id: int) -> List[LookerTemplate]:
        """Récupérer les templates accessibles à un utilisateur"""
        
        try:
            with db_manager.get_session() as session:
                templates = session.query(LookerTemplate).join(UserTemplateAccess).filter(
                    and_(
                        UserTemplateAccess.user_id == user_id,
                        UserTemplateAccess.is_active == True,
                        LookerTemplate.is_active == True
                    )
                ).all()
                
                return templates
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des templates: {e}")
            return []

# ========================================
# INSTANCE GLOBALE
# ========================================

user_manager = UserManager()

# ========================================
# FONCTIONS HELPER
# ========================================

def require_auth(func):
    """Décorateur pour vérifier l'authentification"""
    
    def wrapper(*args, **kwargs):
        # Cette fonction sera utilisée avec FastAPI ou Streamlit
        # L'implémentation dépendra du framework utilisé
        return func(*args, **kwargs)
    
    return wrapper

def get_current_user(token: str) -> Optional[User]:
    """Récupérer l'utilisateur actuel à partir d'un token"""
    return user_manager.get_user_by_token(token)

def create_user_account(email: str, password: str, **kwargs) -> Dict[str, Any]:
    """Créer un compte utilisateur complet"""
    
    try:
        user = user_manager.create_user(email, password, **kwargs)
        if not user:
            return {'success': False, 'error': 'Impossible de créer le compte'}
        
        # Générer un token de connexion
        token = user_manager.generate_jwt_token(user.id, user.email)
        
        # Donner accès aux templates du plan gratuit
        user_manager.grant_template_access(user.id, user.plan_type)
        
        return {
            'success': True,
            'user': {
                'id': user.id,
                'email': user.email,
                'firstname': user.firstname,
                'lastname': user.lastname,
                'company': user.company,
                'plan_type': user.plan_type
            },
            'token': token
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la création du compte: {e}")
        return {'success': False, 'error': str(e)}

def login_user(email: str, password: str) -> Dict[str, Any]:
    """Connecter un utilisateur"""
    
    try:
        user = user_manager.authenticate_user(email, password)
        if not user:
            return {'success': False, 'error': 'Identifiants incorrects'}
        
        # Vérifier la validité de l'abonnement
        subscription_valid = user_manager.check_subscription_validity(user)
        
        # Générer un token
        token = user_manager.generate_jwt_token(user.id, user.email)
        
        # Récupérer les comptes sociaux
        accounts = user_manager.get_user_accounts(user.id)
        
        return {
            'success': True,
            'user': {
                'id': user.id,
                'email': user.email,
                'firstname': user.firstname,
                'lastname': user.lastname,
                'company': user.company,
                'plan_type': user.plan_type,
                'subscription_valid': subscription_valid,
                'subscription_end_date': user.subscription_end_date.isoformat() if user.subscription_end_date else None
            },
            'token': token,
            'accounts': accounts
        }
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de la connexion: {e}")
        return {'success': False, 'error': str(e)}

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    # Test de création d'utilisateur
    print("🧪 Test UserManager...")
    
    try:
        # Initialiser la base de données
        from ..database.connection import init_database
        init_database()
        
        # Test de création d'utilisateur
        result = create_user_account(
            email="test@whatsthedata.com",
            password="testpassword123",
            firstname="Test",
            lastname="User",
            company="Test Company"
        )
        
        print(f"Résultat création: {result}")
        
        if result['success']:
            # Test de connexion
            login_result = login_user("test@whatsthedata.com", "testpassword123")
            print(f"Résultat connexion: {login_result}")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")