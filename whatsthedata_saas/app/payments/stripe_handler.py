"""
Gestionnaire Stripe complet et robuste
Gère les abonnements, paiements, webhooks avec gestion d'erreurs exhaustive
Intégration complète avec UserManager et base de données
"""

import os
import json
import time
import logging
import hmac
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from enum import Enum
from decimal import Decimal

import stripe
from stripe.error import (
    StripeError, CardError, RateLimitError, InvalidRequestError,
    AuthenticationError, APIConnectionError, APIError, IdempotencyError
)

from ..auth.user_manager import user_manager, PlanType, PlanManager
from ..database.connection import db_manager
from ..database.models import User
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

# Configuration Stripe
stripe.api_key = get_env_var('STRIPE_SECRET_KEY')
stripe.api_version = "2023-10-16"  # Version API stable

class StripeWebhookEvent(Enum):
    """Types d'événements webhook Stripe"""
    CHECKOUT_SESSION_COMPLETED = "checkout.session.completed"
    CUSTOMER_SUBSCRIPTION_CREATED = "customer.subscription.created"
    CUSTOMER_SUBSCRIPTION_UPDATED = "customer.subscription.updated"
    CUSTOMER_SUBSCRIPTION_DELETED = "customer.subscription.deleted"
    INVOICE_PAYMENT_SUCCEEDED = "invoice.payment_succeeded"
    INVOICE_PAYMENT_FAILED = "invoice.payment_failed"
    INVOICE_FINALIZED = "invoice.finalized"
    CUSTOMER_CREATED = "customer.created"
    CUSTOMER_UPDATED = "customer.updated"
    CUSTOMER_DELETED = "customer.deleted"
    PAYMENT_METHOD_ATTACHED = "payment_method.attached"
    PAYMENT_INTENT_SUCCEEDED = "payment_intent.succeeded"
    PAYMENT_INTENT_PAYMENT_FAILED = "payment_intent.payment_failed"

class SubscriptionStatus(Enum):
    """Statuts d'abonnement"""
    ACTIVE = "active"
    PAST_DUE = "past_due"
    UNPAID = "unpaid"
    CANCELED = "canceled"
    INCOMPLETE = "incomplete"
    INCOMPLETE_EXPIRED = "incomplete_expired"
    TRIALING = "trialing"
    PAUSED = "paused"

class PaymentStatus(Enum):
    """Statuts de paiement"""
    SUCCEEDED = "succeeded"
    PENDING = "pending"
    FAILED = "failed"
    CANCELED = "canceled"
    REQUIRES_ACTION = "requires_action"
    REQUIRES_PAYMENT_METHOD = "requires_payment_method"

@dataclass
class StripeConfig:
    """Configuration Stripe"""
    public_key: str
    secret_key: str
    webhook_secret: str
    currency: str = "eur"
    country: str = "FR"
    automatic_tax: bool = True
    collect_billing_address: bool = True

@dataclass
class PriceInfo:
    """Information sur un prix Stripe"""
    stripe_price_id: str
    amount: int  # En centimes
    currency: str
    interval: str  # 'month', 'year'
    plan_type: str
    features: List[str]

class StripePaymentError(Exception):
    """Erreur de paiement Stripe personnalisée"""
    
    def __init__(self, message: str, stripe_error: StripeError = None, 
                 error_code: str = None, decline_code: str = None,
                 payment_intent_id: str = None):
        super().__init__(message)
        self.stripe_error = stripe_error
        self.error_code = error_code
        self.decline_code = decline_code
        self.payment_intent_id = payment_intent_id
        self.timestamp = datetime.utcnow()

class StripeWebhookError(Exception):
    """Erreur webhook Stripe"""
    pass

class StripeSubscriptionError(Exception):
    """Erreur d'abonnement Stripe"""
    pass

class StripeHandler:
    """Gestionnaire principal Stripe"""
    
    def __init__(self):
        self.config = self._load_stripe_config()
        self._validate_configuration()
        
        # Cache des clients Stripe (pour éviter les doublons)
        self._customer_cache = {}
        
        # Mapping des plans vers les prix Stripe
        self.plan_prices = self._load_plan_prices()
        
        # Configuration des webhooks
        self.webhook_endpoints = {
            'main': get_env_var('STRIPE_WEBHOOK_ENDPOINT', '/webhooks/stripe'),
            'connect': get_env_var('STRIPE_CONNECT_WEBHOOK_ENDPOINT', '/webhooks/stripe/connect')
        }
        
    def _load_stripe_config(self) -> StripeConfig:
        """Charger la configuration Stripe depuis les variables d'environnement"""
        
        config = StripeConfig(
            public_key=get_env_var('STRIPE_PUBLIC_KEY'),
            secret_key=get_env_var('STRIPE_SECRET_KEY'),
            webhook_secret=get_env_var('STRIPE_WEBHOOK_SECRET', ''),
            currency=get_env_var('STRIPE_CURRENCY', 'eur'),
            country=get_env_var('STRIPE_COUNTRY', 'FR'),
            automatic_tax=get_env_var('STRIPE_AUTOMATIC_TAX', 'true').lower() == 'true',
            collect_billing_address=get_env_var('STRIPE_COLLECT_BILLING_ADDRESS', 'true').lower() == 'true'
        )
        
        logger.info("✅ Configuration Stripe chargée")
        return config
    
    def _validate_configuration(self):
        """Valider la configuration Stripe"""
        
        if not self.config.public_key or not self.config.secret_key:
            raise ValueError("STRIPE_PUBLIC_KEY et STRIPE_SECRET_KEY sont requis")
        
        if not self.config.public_key.startswith('pk_'):
            raise ValueError("STRIPE_PUBLIC_KEY doit commencer par 'pk_'")
        
        if not self.config.secret_key.startswith('sk_'):
            raise ValueError("STRIPE_SECRET_KEY doit commencer par 'sk_'")
        
        # Déterminer l'environnement
        is_test_mode = 'test' in self.config.secret_key
        env_mode = "TEST" if is_test_mode else "LIVE"
        
        logger.info(f"✅ Configuration Stripe validée (Mode: {env_mode})")
        
        # Avertissement pour la production
        if not is_test_mode:
            logger.warning("⚠️  MODE PRODUCTION STRIPE ACTIVÉ - Vérifiez la configuration")
    
    def _load_plan_prices(self) -> Dict[str, PriceInfo]:
        """Charger les prix Stripe pour chaque plan"""
        
        prices = {}
        
        # Plan LinkedIn Basic
        linkedin_price_id = get_env_var('STRIPE_PRICE_LINKEDIN_BASIC')
        if linkedin_price_id:
            prices[PlanType.LINKEDIN_BASIC.value] = PriceInfo(
                stripe_price_id=linkedin_price_id,
                amount=1999,  # 19.99€
                currency=self.config.currency,
                interval='month',
                plan_type=PlanType.LINKEDIN_BASIC.value,
                features=["3 comptes LinkedIn", "90 jours de données", "Templates avancés"]
            )
        
        # Plan Facebook Basic
        facebook_price_id = get_env_var('STRIPE_PRICE_FACEBOOK_BASIC')
        if facebook_price_id:
            prices[PlanType.FACEBOOK_BASIC.value] = PriceInfo(
                stripe_price_id=facebook_price_id,
                amount=1999,  # 19.99€
                currency=self.config.currency,
                interval='month',
                plan_type=PlanType.FACEBOOK_BASIC.value,
                features=["3 pages Facebook", "90 jours de données", "Analytics avancées"]
            )
        
        # Plan Premium
        premium_price_id = get_env_var('STRIPE_PRICE_PREMIUM')
        if premium_price_id:
            prices[PlanType.PREMIUM.value] = PriceInfo(
                stripe_price_id=premium_price_id,
                amount=4999,  # 49.99€
                currency=self.config.currency,
                interval='month',
                plan_type=PlanType.PREMIUM.value,
                features=["10 comptes LinkedIn", "10 pages Facebook", "365 jours de données", "Dashboard unifié"]
            )
        
        logger.info(f"✅ {len(prices)} prix Stripe configurés")
        return prices
    
    # ========================================
    # GESTION DES CLIENTS STRIPE
    # ========================================
    
    def create_or_get_customer(self, user: User, force_create: bool = False) -> stripe.Customer:
        """Créer ou récupérer un client Stripe"""
        
        try:
            # Vérifier le cache d'abord
            cache_key = f"user_{user.id}"
            if not force_create and cache_key in self._customer_cache:
                customer_id = self._customer_cache[cache_key]
                try:
                    customer = stripe.Customer.retrieve(customer_id)
                    if customer and not customer.get('deleted'):
                        logger.debug(f"✅ Client Stripe récupéré du cache: {customer_id}")
                        return customer
                except:
                    # Si erreur, supprimer du cache et continuer
                    del self._customer_cache[cache_key]
            
            # Chercher un client existant par email
            if not force_create:
                try:
                    existing_customers = stripe.Customer.list(
                        email=user.email,
                        limit=1
                    )
                    
                    if existing_customers.data:
                        customer = existing_customers.data[0]
                        self._customer_cache[cache_key] = customer.id
                        logger.info(f"✅ Client Stripe existant trouvé: {customer.id}")
                        return customer
                except Exception as e:
                    logger.warning(f"⚠️  Erreur lors de la recherche de client existant: {e}")
            
            # Créer un nouveau client
            customer_data = {
                'email': user.email,
                'name': f"{user.firstname or ''} {user.lastname or ''}".strip() or None,
                'metadata': {
                    'user_id': str(user.id),
                    'plan_type': user.plan_type,
                    'source': 'whatsthedata',
                    'created_via': 'api'
                }
            }
            
            # Ajouter des informations optionnelles
            if user.company:
                customer_data['description'] = f"Entreprise: {user.company}"
            
            customer = stripe.Customer.create(**customer_data)
            
            # Mettre en cache
            self._customer_cache[cache_key] = customer.id
            
            logger.info(f"✅ Nouveau client Stripe créé: {customer.id} pour user {user.id}")
            return customer
            
        except StripeError as e:
            logger.error(f"❌ Erreur Stripe lors de la création/récupération du client: {e}")
            raise StripePaymentError(
                f"Impossible de créer/récupérer le client: {e.user_message or str(e)}",
                stripe_error=e
            )
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de la gestion du client: {e}")
            raise
    
    def update_customer(self, customer_id: str, user: User) -> stripe.Customer:
        """Mettre à jour un client Stripe"""
        
        try:
            update_data = {
                'email': user.email,
                'name': f"{user.firstname or ''} {user.lastname or ''}".strip() or None,
                'metadata': {
                    'user_id': str(user.id),
                    'plan_type': user.plan_type,
                    'last_updated': datetime.utcnow().isoformat()
                }
            }
            
            if user.company:
                update_data['description'] = f"Entreprise: {user.company}"
            
            customer = stripe.Customer.modify(customer_id, **update_data)
            
            logger.info(f"✅ Client Stripe mis à jour: {customer_id}")
            return customer
            
        except StripeError as e:
            logger.error(f"❌ Erreur lors de la mise à jour du client: {e}")
            raise StripePaymentError(
                f"Impossible de mettre à jour le client: {e.user_message or str(e)}",
                stripe_error=e
            )
    
    # ========================================
    # GESTION DES CHECKOUT SESSIONS
    # ========================================
    
    def create_checkout_session(self, user_id: int, plan_type: str, 
                               success_url: str, cancel_url: str,
                               trial_days: int = None, coupon_code: str = None,
                               allow_promotion_codes: bool = True) -> Dict[str, Any]:
        """Créer une session de checkout Stripe"""
        
        try:
            # Récupérer l'utilisateur
            user = user_manager.get_user_by_id(user_id)
            if not user:
                raise ValueError(f"Utilisateur {user_id} non trouvé")
            
            # Vérifier que le plan existe
            if plan_type not in self.plan_prices:
                raise ValueError(f"Plan {plan_type} non disponible")
            
            plan_info = self.plan_prices[plan_type]
            
            # Créer ou récupérer le client
            customer = self.create_or_get_customer(user)
            
            # Configuration de base de la session
            session_config = {
                'customer': customer.id,
                'payment_method_types': ['card'],
                'line_items': [{
                    'price': plan_info.stripe_price_id,
                    'quantity': 1,
                }],
                'mode': 'subscription',
                'success_url': success_url + ('&' if '?' in success_url else '?') + 'session_id={CHECKOUT_SESSION_ID}',
                'cancel_url': cancel_url,
                'metadata': {
                    'user_id': str(user_id),
                    'plan_type': plan_type,
                    'created_via': 'whatsthedata_api',
                    'timestamp': datetime.utcnow().isoformat()
                },
                'customer_update': {
                    'address': 'auto' if self.config.collect_billing_address else 'never',
                    'name': 'auto'
                },
                'allow_promotion_codes': allow_promotion_codes,
                'billing_address_collection': 'auto' if self.config.collect_billing_address else 'never'
            }
            
            # Ajouter la période d'essai si spécifiée
            if trial_days and trial_days > 0:
                session_config['subscription_data'] = {
                    'trial_period_days': trial_days,
                    'metadata': {
                        'trial_days': str(trial_days)
                    }
                }
            
            # Ajouter un coupon si spécifié
            if coupon_code:
                try:
                    # Vérifier que le coupon existe
                    coupon = stripe.Coupon.retrieve(coupon_code)
                    session_config['discounts'] = [{
                        'coupon': coupon_code
                    }]
                    logger.info(f"✅ Coupon appliqué: {coupon_code}")
                except StripeError as e:
                    logger.warning(f"⚠️  Coupon invalide {coupon_code}: {e}")
                    # Continuer sans le coupon
            
            # Configuration de la taxation automatique
            if self.config.automatic_tax:
                session_config['automatic_tax'] = {'enabled': True}
            
            # Créer la session
            session = stripe.checkout.Session.create(**session_config)
            
            # Informations de retour
            result = {
                'session_id': session.id,
                'checkout_url': session.url,
                'customer_id': customer.id,
                'plan_type': plan_type,
                'amount': plan_info.amount,
                'currency': plan_info.currency,
                'trial_days': trial_days,
                'expires_at': datetime.fromtimestamp(session.expires_at).isoformat(),
                'success': True
            }
            
            logger.info(f"✅ Session checkout créée: {session.id} pour user {user_id} (plan: {plan_type})")
            return result
            
        except ValueError as e:
            logger.error(f"❌ Erreur de validation lors de la création de session: {e}")
            raise
        except StripeError as e:
            logger.error(f"❌ Erreur Stripe lors de la création de session: {e}")
            raise StripePaymentError(
                f"Impossible de créer la session de paiement: {e.user_message or str(e)}",
                stripe_error=e
            )
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de la création de session: {e}")
            raise
    
    def retrieve_checkout_session(self, session_id: str) -> Dict[str, Any]:
        """Récupérer les détails d'une session de checkout"""
        
        try:
            session = stripe.checkout.Session.retrieve(
                session_id,
                expand=['customer', 'subscription', 'line_items']
            )
            
            result = {
                'session_id': session.id,
                'payment_status': session.payment_status,
                'customer_id': session.customer.id if session.customer else None,
                'customer_email': session.customer.email if session.customer else None,
                'subscription_id': session.subscription.id if session.subscription else None,
                'amount_total': session.amount_total,
                'currency': session.currency,
                'metadata': session.metadata,
                'created': datetime.fromtimestamp(session.created).isoformat(),
                'expires_at': datetime.fromtimestamp(session.expires_at).isoformat()
            }
            
            # Ajouter les détails de l'abonnement si présent
            if session.subscription:
                sub = session.subscription
                result['subscription'] = {
                    'id': sub.id,
                    'status': sub.status,
                    'current_period_start': datetime.fromtimestamp(sub.current_period_start).isoformat(),
                    'current_period_end': datetime.fromtimestamp(sub.current_period_end).isoformat(),
                    'trial_start': datetime.fromtimestamp(sub.trial_start).isoformat() if sub.trial_start else None,
                    'trial_end': datetime.fromtimestamp(sub.trial_end).isoformat() if sub.trial_end else None,
                    'cancel_at_period_end': sub.cancel_at_period_end
                }
            
            return result
            
        except StripeError as e:
            logger.error(f"❌ Erreur lors de la récupération de session: {e}")
            raise StripePaymentError(
                f"Impossible de récupérer la session: {e.user_message or str(e)}",
                stripe_error=e
            )
    
    # ========================================
    # GESTION DES ABONNEMENTS
    # ========================================
    
    def get_customer_subscriptions(self, customer_id: str, status: str = None) -> List[Dict[str, Any]]:
        """Récupérer les abonnements d'un client"""
        
        try:
            params = {'customer': customer_id, 'limit': 10}
            if status:
                params['status'] = status
            
            subscriptions = stripe.Subscription.list(**params)
            
            result = []
            for sub in subscriptions.data:
                sub_info = {
                    'id': sub.id,
                    'status': sub.status,
                    'customer_id': sub.customer,
                    'current_period_start': datetime.fromtimestamp(sub.current_period_start).isoformat(),
                    'current_period_end': datetime.fromtimestamp(sub.current_period_end).isoformat(),
                    'trial_start': datetime.fromtimestamp(sub.trial_start).isoformat() if sub.trial_start else None,
                    'trial_end': datetime.fromtimestamp(sub.trial_end).isoformat() if sub.trial_end else None,
                    'cancel_at_period_end': sub.cancel_at_period_end,
                    'canceled_at': datetime.fromtimestamp(sub.canceled_at).isoformat() if sub.canceled_at else None,
                    'items': [],
                    'metadata': sub.metadata
                }
                
                # Ajouter les éléments de l'abonnement
                for item in sub.items.data:
                    item_info = {
                        'id': item.id,
                        'price_id': item.price.id,
                        'quantity': item.quantity,
                        'amount': item.price.unit_amount,
                        'currency': item.price.currency,
                        'interval': item.price.recurring.interval
                    }
                    sub_info['items'].append(item_info)
                
                result.append(sub_info)
            
            return result
            
        except StripeError as e:
            logger.error(f"❌ Erreur lors de la récupération des abonnements: {e}")
            raise StripePaymentError(
                f"Impossible de récupérer les abonnements: {e.user_message or str(e)}",
                stripe_error=e
            )
    
    def cancel_subscription(self, subscription_id: str, at_period_end: bool = True,
                           reason: str = None) -> Dict[str, Any]:
        """Annuler un abonnement"""
        
        try:
            if at_period_end:
                # Annuler à la fin de la période
                subscription = stripe.Subscription.modify(
                    subscription_id,
                    cancel_at_period_end=True,
                    metadata={
                        'cancellation_reason': reason or 'user_requested',
                        'canceled_via': 'whatsthedata_api',
                        'canceled_at': datetime.utcnow().isoformat()
                    }
                )
                
                result = {
                    'subscription_id': subscription.id,
                    'status': subscription.status,
                    'cancel_at_period_end': subscription.cancel_at_period_end,
                    'current_period_end': datetime.fromtimestamp(subscription.current_period_end).isoformat(),
                    'canceled_immediately': False,
                    'access_until': datetime.fromtimestamp(subscription.current_period_end).isoformat()
                }
                
                logger.info(f"✅ Abonnement programmé pour annulation: {subscription_id}")
                
            else:
                # Annuler immédiatement
                subscription = stripe.Subscription.cancel(
                    subscription_id,
                    prorate=True  # Proratiser le remboursement
                )
                
                result = {
                    'subscription_id': subscription.id,
                    'status': subscription.status,
                    'canceled_at': datetime.fromtimestamp(subscription.canceled_at).isoformat(),
                    'canceled_immediately': True,
                    'access_until': datetime.utcnow().isoformat()
                }
                
                logger.info(f"✅ Abonnement annulé immédiatement: {subscription_id}")
            
            return result
            
        except StripeError as e:
            logger.error(f"❌ Erreur lors de l'annulation de l'abonnement: {e}")
            raise StripeSubscriptionError(
                f"Impossible d'annuler l'abonnement: {e.user_message or str(e)}"
            )
    
    def reactivate_subscription(self, subscription_id: str) -> Dict[str, Any]:
        """Réactiver un abonnement annulé (si pas encore expiré)"""
        
        try:
            subscription = stripe.Subscription.modify(
                subscription_id,
                cancel_at_period_end=False,
                metadata={
                    'reactivated_via': 'whatsthedata_api',
                    'reactivated_at': datetime.utcnow().isoformat()
                }
            )
            
            result = {
                'subscription_id': subscription.id,
                'status': subscription.status,
                'cancel_at_period_end': subscription.cancel_at_period_end,
                'current_period_end': datetime.fromtimestamp(subscription.current_period_end).isoformat(),
                'reactivated': True
            }
            
            logger.info(f"✅ Abonnement réactivé: {subscription_id}")
            return result
            
        except StripeError as e:
            logger.error(f"❌ Erreur lors de la réactivation de l'abonnement: {e}")
            raise StripeSubscriptionError(
                f"Impossible de réactiver l'abonnement: {e.user_message or str(e)}"
            )
    
    def change_subscription_plan(self, subscription_id: str, new_plan_type: str,
                                proration_behavior: str = 'always_invoice') -> Dict[str, Any]:
        """Changer le plan d'un abonnement"""
        
        try:
            # Vérifier que le nouveau plan existe
            if new_plan_type not in self.plan_prices:
                raise ValueError(f"Plan {new_plan_type} non disponible")
            
            new_price_id = self.plan_prices[new_plan_type].stripe_price_id
            
            # Récupérer l'abonnement actuel
            subscription = stripe.Subscription.retrieve(subscription_id)
            
            # Modifier l'abonnement
            updated_subscription = stripe.Subscription.modify(
                subscription_id,
                items=[{
                    'id': subscription.items.data[0].id,
                    'price': new_price_id,
                }],
                proration_behavior=proration_behavior,
                metadata={
                    'plan_changed_via': 'whatsthedata_api',
                    'plan_changed_at': datetime.utcnow().isoformat(),
                    'previous_plan': subscription.metadata.get('plan_type'),
                    'new_plan': new_plan_type
                }
            )
            
            result = {
                'subscription_id': updated_subscription.id,
                'status': updated_subscription.status,
                'new_plan_type': new_plan_type,
                'new_price_id': new_price_id,
                'current_period_start': datetime.fromtimestamp(updated_subscription.current_period_start).isoformat(),
                'current_period_end': datetime.fromtimestamp(updated_subscription.current_period_end).isoformat(),
                'proration_applied': proration_behavior != 'none'
            }
            
            logger.info(f"✅ Plan d'abonnement modifié: {subscription_id} → {new_plan_type}")
            return result
            
        except ValueError as e:
            logger.error(f"❌ Erreur de validation lors du changement de plan: {e}")
            raise
        except StripeError as e:
            logger.error(f"❌ Erreur Stripe lors du changement de plan: {e}")
            raise StripeSubscriptionError(
                f"Impossible de changer le plan: {e.user_message or str(e)}"
            )
    
    # ========================================
    # GESTION DES WEBHOOKS
    # ========================================
    
    def verify_webhook_signature(self, payload: bytes, signature: str) -> bool:
        """Vérifier la signature d'un webhook Stripe"""
        
        if not self.config.webhook_secret:
            logger.warning("⚠️  STRIPE_WEBHOOK_SECRET non configuré, signature non vérifiée")
            return True  # En développement, autoriser sans vérification
        
        try:
            # Construire la signature attendue
            timestamp = None
            signatures = {}
            
            for element in signature.split(','):
                key, value = element.split('=', 1)
                if key == 't':
                    timestamp = value
                elif key.startswith('v'):
                    signatures[key] = value
            
            if not timestamp or not signatures:
                logger.error("❌ Format de signature webhook invalide")
                return False
            
            # Vérifier que l'événement n'est pas trop ancien (5 minutes max)
            event_time = int(timestamp)
            current_time = int(time.time())
            if abs(current_time - event_time) > 300:  # 5 minutes
                logger.error("❌ Webhook trop ancien")
                return False
            
            # Construire la payload signée
            signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
            
            # Calculer la signature HMAC
            expected_signature = hmac.new(
                self.config.webhook_secret.encode('utf-8'),
                signed_payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()
            
            # Vérifier la signature
            for version, signature_value in signatures.items():
                if hmac.compare_digest(expected_signature, signature_value):
                    logger.debug("✅ Signature webhook validée")
                    return True
            
            logger.error("❌ Signature webhook invalide")
            return False
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification de signature: {e}")
            return False
    
    def handle_webhook_event(self, event_type: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter un événement webhook Stripe"""
        
        try:
            result = {'success': False, 'processed': False, 'message': ''}
            
            # Router vers le bon handler selon le type d'événement
            if event_type == StripeWebhookEvent.CHECKOUT_SESSION_COMPLETED.value:
                result = self._handle_checkout_completed(event_data)
                
            elif event_type == StripeWebhookEvent.CUSTOMER_SUBSCRIPTION_CREATED.value:
                result = self._handle_subscription_created(event_data)
                
            elif event_type == StripeWebhookEvent.CUSTOMER_SUBSCRIPTION_UPDATED.value:
                result = self._handle_subscription_updated(event_data)
                
            elif event_type == StripeWebhookEvent.CUSTOMER_SUBSCRIPTION_DELETED.value:
                result = self._handle_subscription_deleted(event_data)
                
            elif event_type == StripeWebhookEvent.INVOICE_PAYMENT_SUCCEEDED.value:
                result = self._handle_payment_succeeded(event_data)
                
            elif event_type == StripeWebhookEvent.INVOICE_PAYMENT_FAILED.value:
                result = self._handle_payment_failed(event_data)
                
            else:
                logger.info(f"ℹ️  Événement webhook non traité: {event_type}")
                result = {'success': True, 'processed': False, 'message': f'Événement {event_type} ignoré'}
            
            if result['success']:
                logger.info(f"✅ Webhook traité: {event_type}")
            else:
                logger.error(f"❌ Échec du traitement webhook: {event_type}")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du webhook {event_type}: {e}")
            raise StripeWebhookError(f"Erreur webhook: {e}")
    
    def _handle_checkout_completed(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter la completion d'un checkout"""
        
        try:
            session = event_data['object']
            user_id = int(session['metadata']['user_id'])
            plan_type = session['metadata']['plan_type']
            
            # Récupérer les détails de l'abonnement
            if session.get('subscription'):
                subscription = stripe.Subscription.retrieve(session['subscription'])
                
                # Calculer la date de fin de l'abonnement
                subscription_end = datetime.fromtimestamp(subscription.current_period_end)
                
                # Mettre à jour le plan de l'utilisateur
                success = user_manager.update_user_plan(
                    user_id=user_id,
                    new_plan=plan_type,
                    subscription_end_date=subscription_end
                )
                
                if success:
                    logger.info(f"✅ Abonnement activé: user {user_id} → {plan_type}")
                    return {
                        'success': True,
                        'processed': True,
                        'message': f'Abonnement {plan_type} activé pour user {user_id}',
                        'user_id': user_id,
                        'plan_type': plan_type,
                        'subscription_end': subscription_end.isoformat()
                    }
                else:
                    raise Exception("Échec de la mise à jour du plan utilisateur")
            else:
                raise Exception("Aucun abonnement trouvé dans la session")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement checkout completed: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    def _handle_subscription_created(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter la création d'un abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            # Récupérer l'utilisateur associé au client
            customer = stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                logger.info(f"✅ Abonnement créé: {subscription['id']} pour user {user_id}")
                return {
                    'success': True,
                    'processed': True,
                    'message': f'Abonnement créé pour user {user_id}',
                    'subscription_id': subscription['id'],
                    'user_id': user_id
                }
            else:
                logger.warning("⚠️  Impossible de trouver l'user_id pour l'abonnement")
                return {'success': True, 'processed': False, 'message': 'User ID non trouvé'}
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement subscription created: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    def _handle_subscription_updated(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter la mise à jour d'un abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            # Récupérer l'utilisateur
            customer = stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Mettre à jour la date de fin d'abonnement
                subscription_end = datetime.fromtimestamp(subscription['current_period_end'])
                
                # Déterminer le plan à partir des items de l'abonnement
                plan_type = None
                for item in subscription['items']['data']:
                    price_id = item['price']['id']
                    for plan, price_info in self.plan_prices.items():
                        if price_info.stripe_price_id == price_id:
                            plan_type = plan
                            break
                    if plan_type:
                        break
                
                if plan_type:
                    user_manager.update_user_plan(
                        user_id=user_id,
                        new_plan=plan_type,
                        subscription_end_date=subscription_end
                    )
                
                logger.info(f"✅ Abonnement mis à jour: {subscription['id']} pour user {user_id}")
                return {
                    'success': True,
                    'processed': True,
                    'message': f'Abonnement mis à jour pour user {user_id}',
                    'subscription_id': subscription['id'],
                    'user_id': user_id,
                    'status': subscription['status']
                }
            else:
                return {'success': True, 'processed': False, 'message': 'User ID non trouvé'}
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement subscription updated: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    def _handle_subscription_deleted(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter la suppression d'un abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            # Récupérer l'utilisateur
            customer = stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Rétrograder vers le plan gratuit
                user_manager.update_user_plan(
                    user_id=user_id,
                    new_plan=PlanType.FREE.value,
                    subscription_end_date=None
                )
                
                logger.info(f"✅ Abonnement supprimé: user {user_id} rétrogradé vers plan gratuit")
                return {
                    'success': True,
                    'processed': True,
                    'message': f'User {user_id} rétrogradé vers plan gratuit',
                    'subscription_id': subscription['id'],
                    'user_id': user_id
                }
            else:
                return {'success': True, 'processed': False, 'message': 'User ID non trouvé'}
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement subscription deleted: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    def _handle_payment_succeeded(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter un paiement réussi"""
        
        try:
            invoice = event_data['object']
            customer_id = invoice['customer']
            
            # Log du paiement réussi
            logger.info(f"✅ Paiement réussi: {invoice['id']} pour customer {customer_id}")
            
            return {
                'success': True,
                'processed': True,
                'message': 'Paiement traité avec succès',
                'invoice_id': invoice['id'],
                'amount': invoice['amount_paid'],
                'currency': invoice['currency']
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement payment succeeded: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    def _handle_payment_failed(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter un échec de paiement"""
        
        try:
            invoice = event_data['object']
            customer_id = invoice['customer']
            
            # Récupérer l'utilisateur
            customer = stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            # Log de l'échec
            logger.warning(f"⚠️  Échec de paiement: {invoice['id']} pour customer {customer_id}")
            
            # TODO: Envoyer une notification à l'utilisateur
            # TODO: Marquer le compte comme "payment_failed" après X échecs
            
            return {
                'success': True,
                'processed': True,
                'message': 'Échec de paiement traité',
                'invoice_id': invoice['id'],
                'user_id': user_id,
                'amount': invoice['amount_due'],
                'currency': invoice['currency']
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement payment failed: {e}")
            return {'success': False, 'processed': False, 'message': str(e)}
    
    # ========================================
    # UTILITAIRES ET INFORMATIONS
    # ========================================
    
    def get_user_payment_info(self, user_id: int) -> Dict[str, Any]:
        """Récupérer les informations de paiement d'un utilisateur"""
        
        try:
            user = user_manager.get_user_by_id(user_id)
            if not user:
                return {'error': 'Utilisateur non trouvé'}
            
            # Récupérer le client Stripe
            try:
                customer = self.create_or_get_customer(user)
            except:
                return {
                    'user_id': user_id,
                    'has_stripe_customer': False,
                    'subscriptions': [],
                    'payment_methods': []
                }
            
            # Récupérer les abonnements
            subscriptions = self.get_customer_subscriptions(customer.id)
            
            # Récupérer les méthodes de paiement
            payment_methods = []
            try:
                pm_list = stripe.PaymentMethod.list(
                    customer=customer.id,
                    type='card'
                )
                
                for pm in pm_list.data:
                    pm_info = {
                        'id': pm.id,
                        'type': pm.type,
                        'card': {
                            'brand': pm.card.brand,
                            'last4': pm.card.last4,
                            'exp_month': pm.card.exp_month,
                            'exp_year': pm.card.exp_year
                        }
                    }
                    payment_methods.append(pm_info)
            except:
                pass  # Ignorer les erreurs de récupération des méthodes de paiement
            
            result = {
                'user_id': user_id,
                'customer_id': customer.id,
                'has_stripe_customer': True,
                'subscriptions': subscriptions,
                'payment_methods': payment_methods,
                'default_payment_method': customer.invoice_settings.default_payment_method
            }
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des infos de paiement: {e}")
            return {'error': str(e)}
    
    def get_available_plans(self) -> List[Dict[str, Any]]:
        """Récupérer la liste des plans disponibles"""
        
        plans = []
        for plan_type, price_info in self.plan_prices.items():
            plan_features = PlanManager.get_plan_features(plan_type)
            
            plan_info = {
                'plan_type': plan_type,
                'name': plan_features.name,
                'price': price_info.amount / 100,  # Convertir centimes en euros
                'currency': price_info.currency,
                'interval': price_info.interval,
                'stripe_price_id': price_info.stripe_price_id,
                'features': price_info.features,
                'max_facebook_accounts': plan_features.max_facebook_accounts,
                'max_linkedin_accounts': plan_features.max_linkedin_accounts,
                'data_retention_days': plan_features.data_retention_days,
                'api_calls_per_month': plan_features.api_calls_per_month
            }
            
            plans.append(plan_info)
        
        return sorted(plans, key=lambda x: x['price'])
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du gestionnaire Stripe"""
        
        health = {
            'stripe_handler': 'ok',
            'configuration': {
                'api_key_present': bool(self.config.secret_key),
                'webhook_secret_present': bool(self.config.webhook_secret),
                'test_mode': 'test' in self.config.secret_key,
                'currency': self.config.currency,
                'automatic_tax': self.config.automatic_tax
            },
            'plans_configured': len(self.plan_prices),
            'plans': list(self.plan_prices.keys()),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Test de l'API Stripe
        try:
            # Test simple : récupérer le compte
            account = stripe.Account.retrieve()
            health['api_connection'] = 'ok'
            health['account_id'] = account.id
            health['account_country'] = account.country
        except Exception as e:
            health['api_connection'] = 'error'
            health['api_error'] = str(e)
        
        return health

# ========================================
# INSTANCE GLOBALE
# ========================================

stripe_handler = StripeHandler()

# ========================================
# FONCTIONS HELPER
# ========================================

def create_checkout_for_user(user_id: int, plan_type: str, success_url: str, 
                            cancel_url: str, **kwargs) -> Dict[str, Any]:
    """Fonction helper pour créer un checkout"""
    return stripe_handler.create_checkout_session(
        user_id, plan_type, success_url, cancel_url, **kwargs
    )

def process_stripe_webhook(payload: bytes, signature: str, event_type: str, 
                          event_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fonction helper pour traiter un webhook"""
    
    # Vérifier la signature
    if not stripe_handler.verify_webhook_signature(payload, signature):
        raise StripeWebhookError("Signature webhook invalide")
    
    # Traiter l'événement
    return stripe_handler.handle_webhook_event(event_type, event_data)

def get_user_billing_info(user_id: int) -> Dict[str, Any]:
    """Fonction helper pour obtenir les infos de facturation"""
    return stripe_handler.get_user_payment_info(user_id)

def cancel_user_subscription(user_id: int, at_period_end: bool = True) -> Dict[str, Any]:
    """Fonction helper pour annuler un abonnement utilisateur"""
    
    try:
        user = user_manager.get_user_by_id(user_id)
        if not user:
            return {'success': False, 'error': 'Utilisateur non trouvé'}
        
        customer = stripe_handler.create_or_get_customer(user)
        subscriptions = stripe_handler.get_customer_subscriptions(customer.id, status='active')
        
        if not subscriptions:
            return {'success': False, 'error': 'Aucun abonnement actif trouvé'}
        
        # Annuler le premier abonnement actif
        result = stripe_handler.cancel_subscription(
            subscriptions[0]['id'], 
            at_period_end=at_period_end
        )
        
        return {'success': True, **result}
        
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'annulation d'abonnement: {e}")
        return {'success': False, 'error': str(e)}

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test StripeHandler...")
    
    try:
        # Test de configuration
        handler = StripeHandler()
        health = handler.health_check()
        print(f"Health check: {json.dumps(health, indent=2)}")
        
        # Test des plans disponibles
        plans = handler.get_available_plans()
        print(f"Plans disponibles: {len(plans)}")
        for plan in plans:
            print(f"  - {plan['name']}: {plan['price']}€/{plan['interval']}")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")
        import traceback
        traceback.print_exc()