"""
Gestionnaire d'abonnements complet et robuste
Couche d'abstraction au-dessus de stripe_handler pour la logique métier
Gestion des transitions d'état, validation, accès aux fonctionnalités
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
from decimal import Decimal
import asyncio
from concurrent.futures import ThreadPoolExecutor

from .stripe_handler import (
    stripe_handler, StripePaymentError, StripeSubscriptionError, 
    StripeWebhookError, SubscriptionStatus, PaymentStatus
)
from ..auth.user_manager import user_manager, PlanType, PlanManager
from ..database.connection import db_manager
from ..database.models import User
from ..utils.config import get_env_var
from ..utils.helpers import (
    format_datetime, safe_int, safe_str, 
    validate_email, rate_limit, retry_with_backoff
)

# Configuration du logging
logger = logging.getLogger(__name__)

class SubscriptionEvent(Enum):
    """Types d'événements d'abonnement"""
    CREATED = "created"
    ACTIVATED = "activated"
    UPGRADED = "upgraded"
    DOWNGRADED = "downgraded"
    RENEWED = "renewed"
    CANCELED = "canceled"
    EXPIRED = "expired"
    PAUSED = "paused"
    RESUMED = "resumed"
    PAYMENT_FAILED = "payment_failed"
    PAYMENT_RETRY = "payment_retry"
    TRIAL_STARTED = "trial_started"
    TRIAL_ENDED = "trial_ended"

class SubscriptionValidationError(Exception):
    """Erreur de validation d'abonnement"""
    pass

class SubscriptionAccessError(Exception):
    """Erreur d'accès basée sur l'abonnement"""
    pass

class SubscriptionTransitionError(Exception):
    """Erreur de transition d'état d'abonnement"""
    pass

@dataclass
class SubscriptionLimits:
    """Limites d'un abonnement"""
    max_facebook_accounts: int
    max_linkedin_accounts: int
    max_instagram_accounts: int
    data_retention_days: int
    api_calls_per_month: int
    advanced_analytics: bool
    custom_reports: bool
    priority_support: bool
    white_label: bool

@dataclass
class SubscriptionUsage:
    """Utilisation actuelle d'un abonnement"""
    facebook_accounts: int = 0
    linkedin_accounts: int = 0
    instagram_accounts: int = 0
    api_calls_this_month: int = 0
    storage_used_mb: int = 0
    last_updated: datetime = None

@dataclass
class SubscriptionInfo:
    """Informations complètes d'un abonnement"""
    user_id: int
    stripe_customer_id: str
    stripe_subscription_id: str
    plan_type: str
    status: str
    current_period_start: datetime
    current_period_end: datetime
    trial_start: Optional[datetime] = None
    trial_end: Optional[datetime] = None
    cancel_at_period_end: bool = False
    canceled_at: Optional[datetime] = None
    limits: SubscriptionLimits = None
    usage: SubscriptionUsage = None
    next_billing_date: Optional[datetime] = None
    amount: int = 0  # en centimes
    currency: str = "eur"
    discount: Optional[Dict] = None
    payment_method: Optional[Dict] = None
    created_at: datetime = None
    updated_at: datetime = None

class SubscriptionManager:
    """Gestionnaire principal des abonnements"""
    
    def __init__(self):
        self.stripe_handler = stripe_handler
        self.user_manager = user_manager
        self.plan_manager = PlanManager()
        
        # Configuration des limites par plan
        self.plan_limits = self._initialize_plan_limits()
        
        # Cache des informations d'abonnement
        self._subscription_cache = {}
        self._cache_ttl = 300  # 5 minutes
        
        # Pool de threads pour les opérations asynchrones
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        logger.info("✅ SubscriptionManager initialisé")
    
    def _initialize_plan_limits(self) -> Dict[str, SubscriptionLimits]:
        """Initialiser les limites pour chaque plan"""
        
        limits = {}
        
        # Plan gratuit
        limits[PlanType.FREE.value] = SubscriptionLimits(
            max_facebook_accounts=1,
            max_linkedin_accounts=1,
            max_instagram_accounts=0,
            data_retention_days=30,
            api_calls_per_month=1000,
            advanced_analytics=False,
            custom_reports=False,
            priority_support=False,
            white_label=False
        )
        
        # LinkedIn Basic
        limits[PlanType.LINKEDIN_BASIC.value] = SubscriptionLimits(
            max_facebook_accounts=0,
            max_linkedin_accounts=3,
            max_instagram_accounts=0,
            data_retention_days=90,
            api_calls_per_month=10000,
            advanced_analytics=True,
            custom_reports=False,
            priority_support=False,
            white_label=False
        )
        
        # Facebook Basic
        limits[PlanType.FACEBOOK_BASIC.value] = SubscriptionLimits(
            max_facebook_accounts=3,
            max_linkedin_accounts=0,
            max_instagram_accounts=1,
            data_retention_days=90,
            api_calls_per_month=10000,
            advanced_analytics=True,
            custom_reports=False,
            priority_support=False,
            white_label=False
        )
        
        # Premium
        limits[PlanType.PREMIUM.value] = SubscriptionLimits(
            max_facebook_accounts=10,
            max_linkedin_accounts=10,
            max_instagram_accounts=5,
            data_retention_days=365,
            api_calls_per_month=100000,
            advanced_analytics=True,
            custom_reports=True,
            priority_support=True,
            white_label=True
        )
        
        return limits
    
    # ========================================
    # CRÉATION ET GESTION D'ABONNEMENTS
    # ========================================
    
    @retry_with_backoff(max_retries=3)
    def create_subscription(self, user_id: int, plan_type: str, 
                           success_url: str, cancel_url: str,
                           trial_days: Optional[int] = None,
                           coupon_code: Optional[str] = None,
                           payment_method_id: Optional[str] = None) -> Dict[str, Any]:
        """Créer un nouvel abonnement avec validation complète"""
        
        try:
            # Validation des paramètres
            if not self._validate_subscription_creation(user_id, plan_type):
                raise SubscriptionValidationError("Validation de création échouée")
            
            # Vérifier si l'utilisateur a déjà un abonnement actif
            existing_subscription = self.get_user_subscription(user_id)
            if existing_subscription and existing_subscription.status in ['active', 'trialing']:
                raise SubscriptionValidationError(
                    f"L'utilisateur a déjà un abonnement actif: {existing_subscription.plan_type}"
                )
            
            # Créer la session de checkout
            checkout_result = self.stripe_handler.create_checkout_session(
                user_id=user_id,
                plan_type=plan_type,
                success_url=success_url,
                cancel_url=cancel_url,
                trial_days=trial_days,
                coupon_code=coupon_code
            )
            
            # Enregistrer l'événement
            self._log_subscription_event(
                user_id=user_id,
                event_type=SubscriptionEvent.CREATED,
                metadata={
                    'plan_type': plan_type,
                    'trial_days': trial_days,
                    'session_id': checkout_result['session_id']
                }
            )
            
            logger.info(f"✅ Abonnement en cours de création: user {user_id} → {plan_type}")
            
            return {
                'success': True,
                'checkout_url': checkout_result['checkout_url'],
                'session_id': checkout_result['session_id'],
                'plan_type': plan_type,
                'trial_days': trial_days,
                'amount': checkout_result['amount'],
                'currency': checkout_result['currency']
            }
            
        except SubscriptionValidationError as e:
            logger.error(f"❌ Erreur de validation lors de la création d'abonnement: {e}")
            raise
        except StripePaymentError as e:
            logger.error(f"❌ Erreur Stripe lors de la création d'abonnement: {e}")
            raise SubscriptionValidationError(f"Erreur de paiement: {e}")
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de la création d'abonnement: {e}")
            raise
    
    def _validate_subscription_creation(self, user_id: int, plan_type: str) -> bool:
        """Valider la création d'un abonnement"""
        
        # Vérifier que l'utilisateur existe
        user = self.user_manager.get_user_by_id(user_id)
        if not user:
            raise SubscriptionValidationError(f"Utilisateur {user_id} non trouvé")
        
        # Vérifier que le plan existe
        if plan_type not in self.plan_limits:
            raise SubscriptionValidationError(f"Plan {plan_type} non disponible")
        
        # Vérifier que l'utilisateur n'est pas suspendu
        if not user.is_active:
            raise SubscriptionValidationError("Compte utilisateur désactivé")
        
        # Validation email
        if not validate_email(user.email):
            raise SubscriptionValidationError("Email utilisateur invalide")
        
        return True
    
    @retry_with_backoff(max_retries=3)
    def cancel_subscription(self, user_id: int, at_period_end: bool = True,
                           reason: str = None) -> Dict[str, Any]:
        """Annuler un abonnement utilisateur"""
        
        try:
            # Récupérer l'abonnement actuel
            subscription = self.get_user_subscription(user_id)
            if not subscription:
                raise SubscriptionValidationError("Aucun abonnement trouvé")
            
            if subscription.status not in ['active', 'trialing']:
                raise SubscriptionValidationError(f"Abonnement non annulable (statut: {subscription.status})")
            
            # Annuler via Stripe
            cancel_result = self.stripe_handler.cancel_subscription(
                subscription.stripe_subscription_id,
                at_period_end=at_period_end,
                reason=reason
            )
            
            # Mettre à jour le cache
            self._invalidate_subscription_cache(user_id)
            
            # Si annulation immédiate, mettre à jour le plan utilisateur
            if not at_period_end:
                self.user_manager.update_user_plan(
                    user_id=user_id,
                    new_plan=PlanType.FREE.value,
                    subscription_end_date=None
                )
            
            # Enregistrer l'événement
            self._log_subscription_event(
                user_id=user_id,
                event_type=SubscriptionEvent.CANCELED,
                metadata={
                    'reason': reason,
                    'at_period_end': at_period_end,
                    'canceled_immediately': not at_period_end
                }
            )
            
            logger.info(f"✅ Abonnement annulé: user {user_id} (à la fin de période: {at_period_end})")
            
            return {
                'success': True,
                'canceled_immediately': not at_period_end,
                'access_until': cancel_result.get('access_until'),
                'subscription_id': subscription.stripe_subscription_id
            }
            
        except SubscriptionValidationError as e:
            logger.error(f"❌ Erreur de validation lors de l'annulation: {e}")
            raise
        except StripeSubscriptionError as e:
            logger.error(f"❌ Erreur Stripe lors de l'annulation: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de l'annulation: {e}")
            raise
    
    @retry_with_backoff(max_retries=3)
    def reactivate_subscription(self, user_id: int) -> Dict[str, Any]:
        """Réactiver un abonnement annulé (si pas encore expiré)"""
        
        try:
            subscription = self.get_user_subscription(user_id)
            if not subscription:
                raise SubscriptionValidationError("Aucun abonnement trouvé")
            
            if not subscription.cancel_at_period_end:
                raise SubscriptionValidationError("L'abonnement n'est pas programmé pour annulation")
            
            # Réactiver via Stripe
            reactivate_result = self.stripe_handler.reactivate_subscription(
                subscription.stripe_subscription_id
            )
            
            # Mettre à jour le cache
            self._invalidate_subscription_cache(user_id)
            
            # Enregistrer l'événement
            self._log_subscription_event(
                user_id=user_id,
                event_type=SubscriptionEvent.RESUMED,
                metadata={'reactivated_at': datetime.utcnow().isoformat()}
            )
            
            logger.info(f"✅ Abonnement réactivé: user {user_id}")
            
            return {
                'success': True,
                'subscription_id': subscription.stripe_subscription_id,
                'status': reactivate_result['status']
            }
            
        except SubscriptionValidationError as e:
            logger.error(f"❌ Erreur de validation lors de la réactivation: {e}")
            raise
        except StripeSubscriptionError as e:
            logger.error(f"❌ Erreur Stripe lors de la réactivation: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors de la réactivation: {e}")
            raise
    
    @retry_with_backoff(max_retries=3)
    def upgrade_subscription(self, user_id: int, new_plan_type: str) -> Dict[str, Any]:
        """Changer le plan d'un abonnement (upgrade/downgrade)"""
        
        try:
            # Validation
            subscription = self.get_user_subscription(user_id)
            if not subscription:
                raise SubscriptionValidationError("Aucun abonnement trouvé")
            
            if subscription.status not in ['active', 'trialing']:
                raise SubscriptionValidationError(f"Impossible de modifier un abonnement {subscription.status}")
            
            if new_plan_type not in self.plan_limits:
                raise SubscriptionValidationError(f"Plan {new_plan_type} non disponible")
            
            if subscription.plan_type == new_plan_type:
                raise SubscriptionValidationError("Le plan est déjà identique")
            
            # Déterminer si c'est un upgrade ou downgrade
            current_limits = self.plan_limits[subscription.plan_type]
            new_limits = self.plan_limits[new_plan_type]
            
            is_upgrade = (
                new_limits.max_facebook_accounts > current_limits.max_facebook_accounts or
                new_limits.max_linkedin_accounts > current_limits.max_linkedin_accounts or
                new_limits.data_retention_days > current_limits.data_retention_days
            )
            
            # Vérifier les contraintes pour un downgrade
            if not is_upgrade:
                usage = self.get_subscription_usage(user_id)
                if not self._validate_downgrade_constraints(usage, new_limits):
                    raise SubscriptionValidationError(
                        "Impossible de rétrograder : l'utilisation actuelle dépasse les limites du nouveau plan"
                    )
            
            # Changer le plan via Stripe
            change_result = self.stripe_handler.change_subscription_plan(
                subscription.stripe_subscription_id,
                new_plan_type,
                proration_behavior='always_invoice' if is_upgrade else 'create_prorations'
            )
            
            # Mettre à jour le plan utilisateur
            self.user_manager.update_user_plan(
                user_id=user_id,
                new_plan=new_plan_type,
                subscription_end_date=datetime.fromisoformat(change_result['current_period_end'])
            )
            
            # Mettre à jour le cache
            self._invalidate_subscription_cache(user_id)
            
            # Enregistrer l'événement
            event_type = SubscriptionEvent.UPGRADED if is_upgrade else SubscriptionEvent.DOWNGRADED
            self._log_subscription_event(
                user_id=user_id,
                event_type=event_type,
                metadata={
                    'from_plan': subscription.plan_type,
                    'to_plan': new_plan_type,
                    'proration_applied': change_result['proration_applied']
                }
            )
            
            logger.info(f"✅ Plan modifié: user {user_id} {subscription.plan_type} → {new_plan_type}")
            
            return {
                'success': True,
                'from_plan': subscription.plan_type,
                'to_plan': new_plan_type,
                'is_upgrade': is_upgrade,
                'proration_applied': change_result['proration_applied'],
                'new_period_end': change_result['current_period_end']
            }
            
        except SubscriptionValidationError as e:
            logger.error(f"❌ Erreur de validation lors du changement de plan: {e}")
            raise
        except StripeSubscriptionError as e:
            logger.error(f"❌ Erreur Stripe lors du changement de plan: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Erreur inattendue lors du changement de plan: {e}")
            raise
    
    def _validate_downgrade_constraints(self, usage: SubscriptionUsage, 
                                       new_limits: SubscriptionLimits) -> bool:
        """Valider qu'un downgrade est possible selon l'utilisation actuelle"""
        
        constraints = []
        
        if usage.facebook_accounts > new_limits.max_facebook_accounts:
            constraints.append(f"Facebook: {usage.facebook_accounts} comptes > {new_limits.max_facebook_accounts} autorisés")
        
        if usage.linkedin_accounts > new_limits.max_linkedin_accounts:
            constraints.append(f"LinkedIn: {usage.linkedin_accounts} comptes > {new_limits.max_linkedin_accounts} autorisés")
        
        if constraints:
            logger.warning(f"⚠️  Contraintes de downgrade: {'; '.join(constraints)}")
            return False
        
        return True
    
    # ========================================
    # RÉCUPÉRATION D'INFORMATIONS
    # ========================================
    
    def get_user_subscription(self, user_id: int, use_cache: bool = True) -> Optional[SubscriptionInfo]:
        """Récupérer les informations d'abonnement d'un utilisateur"""
        
        try:
            # Vérifier le cache d'abord
            cache_key = f"subscription_{user_id}"
            if use_cache and cache_key in self._subscription_cache:
                cached_data, timestamp = self._subscription_cache[cache_key]
                if (datetime.utcnow() - timestamp).seconds < self._cache_ttl:
                    return cached_data
            
            # Récupérer l'utilisateur
            user = self.user_manager.get_user_by_id(user_id)
            if not user:
                return None
            
            # Si plan gratuit, retourner une structure minimale
            if user.plan_type == PlanType.FREE.value:
                subscription_info = SubscriptionInfo(
                    user_id=user_id,
                    stripe_customer_id="",
                    stripe_subscription_id="",
                    plan_type=PlanType.FREE.value,
                    status="active",
                    current_period_start=datetime.utcnow(),
                    current_period_end=datetime.utcnow() + timedelta(days=365),
                    limits=self.plan_limits[PlanType.FREE.value],
                    usage=self.get_subscription_usage(user_id),
                    created_at=user.created_at
                )
                
                # Mettre en cache
                self._subscription_cache[cache_key] = (subscription_info, datetime.utcnow())
                return subscription_info
            
            # Récupérer les informations de paiement Stripe
            payment_info = self.stripe_handler.get_user_payment_info(user_id)
            if 'error' in payment_info or not payment_info.get('subscriptions'):
                logger.warning(f"⚠️  Aucune information d'abonnement Stripe pour user {user_id}")
                return None
            
            # Prendre le premier abonnement actif
            stripe_subscription = None
            for sub in payment_info['subscriptions']:
                if sub['status'] in ['active', 'trialing', 'past_due']:
                    stripe_subscription = sub
                    break
            
            if not stripe_subscription:
                logger.warning(f"⚠️  Aucun abonnement actif trouvé pour user {user_id}")
                return None
            
            # Construire l'objet SubscriptionInfo
            subscription_info = SubscriptionInfo(
                user_id=user_id,
                stripe_customer_id=payment_info['customer_id'],
                stripe_subscription_id=stripe_subscription['id'],
                plan_type=user.plan_type,
                status=stripe_subscription['status'],
                current_period_start=datetime.fromisoformat(stripe_subscription['current_period_start']),
                current_period_end=datetime.fromisoformat(stripe_subscription['current_period_end']),
                trial_start=datetime.fromisoformat(stripe_subscription['trial_start']) if stripe_subscription.get('trial_start') else None,
                trial_end=datetime.fromisoformat(stripe_subscription['trial_end']) if stripe_subscription.get('trial_end') else None,
                cancel_at_period_end=stripe_subscription['cancel_at_period_end'],
                canceled_at=datetime.fromisoformat(stripe_subscription['canceled_at']) if stripe_subscription.get('canceled_at') else None,
                limits=self.plan_limits.get(user.plan_type, self.plan_limits[PlanType.FREE.value]),
                usage=self.get_subscription_usage(user_id),
                next_billing_date=datetime.fromisoformat(stripe_subscription['current_period_end']) if not stripe_subscription['cancel_at_period_end'] else None,
                amount=stripe_subscription['items'][0]['amount'] if stripe_subscription.get('items') else 0,
                currency=stripe_subscription['items'][0]['currency'] if stripe_subscription.get('items') else 'eur',
                created_at=user.created_at,
                updated_at=datetime.utcnow()
            )
            
            # Mettre en cache
            self._subscription_cache[cache_key] = (subscription_info, datetime.utcnow())
            
            return subscription_info
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération d'abonnement pour user {user_id}: {e}")
            return None
    
    def get_subscription_usage(self, user_id: int) -> SubscriptionUsage:
        """Calculer l'utilisation actuelle d'un abonnement"""
        
        try:
            usage = SubscriptionUsage(last_updated=datetime.utcnow())
            
            # Compter les comptes connectés
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Comptes Facebook
                    cursor.execute(
                        "SELECT COUNT(*) FROM facebook_accounts WHERE user_id = %s AND is_active = true",
                        (user_id,)
                    )
                    usage.facebook_accounts = cursor.fetchone()[0]
                    
                    # Comptes LinkedIn
                    cursor.execute(
                        "SELECT COUNT(*) FROM linkedin_accounts WHERE user_id = %s AND is_active = true",
                        (user_id,)
                    )
                    usage.linkedin_accounts = cursor.fetchone()[0]
                    
                    # TODO: Compter les appels API du mois actuel
                    # TODO: Calculer l'espace de stockage utilisé
            
            return usage
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du calcul d'utilisation pour user {user_id}: {e}")
            return SubscriptionUsage()
    
    def get_subscription_limits(self, user_id: int) -> SubscriptionLimits:
        """Récupérer les limites d'abonnement d'un utilisateur"""
        
        user = self.user_manager.get_user_by_id(user_id)
        if not user:
            return self.plan_limits[PlanType.FREE.value]
        
        return self.plan_limits.get(user.plan_type, self.plan_limits[PlanType.FREE.value])
    
    # ========================================
    # VÉRIFICATION D'ACCÈS ET LIMITES
    # ========================================
    
    def check_feature_access(self, user_id: int, feature: str) -> bool:
        """Vérifier si un utilisateur a accès à une fonctionnalité"""
        
        try:
            limits = self.get_subscription_limits(user_id)
            
            feature_map = {
                'advanced_analytics': limits.advanced_analytics,
                'custom_reports': limits.custom_reports,
                'priority_support': limits.priority_support,
                'white_label': limits.white_label
            }
            
            return feature_map.get(feature, False)
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification d'accès pour user {user_id}, feature {feature}: {e}")
            return False
    
    def check_account_limit(self, user_id: int, platform: str) -> Dict[str, Any]:
        """Vérifier si l'utilisateur peut ajouter un compte sur une plateforme"""
        
        try:
            limits = self.get_subscription_limits(user_id)
            usage = self.get_subscription_usage(user_id)
            
            if platform.lower() == 'facebook':
                current = usage.facebook_accounts
                max_allowed = limits.max_facebook_accounts
            elif platform.lower() == 'linkedin':
                current = usage.linkedin_accounts
                max_allowed = limits.max_linkedin_accounts
            elif platform.lower() == 'instagram':
                current = usage.instagram_accounts
                max_allowed = limits.max_instagram_accounts
            else:
                return {'allowed': False, 'reason': f'Plateforme {platform} non supportée'}
            
            allowed = current < max_allowed
            
            return {
                'allowed': allowed,
                'current': current,
                'max_allowed': max_allowed,
                'remaining': max(0, max_allowed - current),
                'reason': None if allowed else f'Limite atteinte ({current}/{max_allowed})'
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification de limite pour user {user_id}, platform {platform}: {e}")
            return {'allowed': False, 'reason': 'Erreur de vérification'}
    
    def check_data_access(self, user_id: int, requested_date: datetime) -> bool:
        """Vérifier si l'utilisateur peut accéder aux données d'une date donnée"""
        
        try:
            limits = self.get_subscription_limits(user_id)
            cutoff_date = datetime.utcnow() - timedelta(days=limits.data_retention_days)
            
            return requested_date >= cutoff_date
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification d'accès aux données: {e}")
            return False
    
    def check_api_quota(self, user_id: int) -> Dict[str, Any]:
        """Vérifier le quota d'API d'un utilisateur"""
        
        try:
            limits = self.get_subscription_limits(user_id)
            usage = self.get_subscription_usage(user_id)
            
            quota_used = usage.api_calls_this_month
            quota_limit = limits.api_calls_per_month
            
            return {
                'quota_used': quota_used,
                'quota_limit': quota_limit,
                'quota_remaining': max(0, quota_limit - quota_used),
                'percentage_used': (quota_used / quota_limit * 100) if quota_limit > 0 else 0,
                'quota_exceeded': quota_used >= quota_limit
            }
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification du quota API: {e}")
            return {'quota_exceeded': True}
    
    # ========================================
    # WEBHOOK ET ÉVÉNEMENTS
    # ========================================
    
    def handle_subscription_webhook(self, event_type: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Traiter un webhook d'abonnement avec logique métier supplémentaire"""
        
        try:
            # Déléguer au stripe_handler pour le traitement de base
            result = self.stripe_handler.handle_webhook_event(event_type, event_data)
            
            if not result['success']:
                return result
            
            # Logique métier supplémentaire selon le type d'événement
            if event_type == 'customer.subscription.created':
                self._handle_subscription_created_business_logic(event_data)
                
            elif event_type == 'customer.subscription.updated':
                self._handle_subscription_updated_business_logic(event_data)
                
            elif event_type == 'customer.subscription.deleted':
                self._handle_subscription_deleted_business_logic(event_data)
                
            elif event_type == 'invoice.payment_failed':
                self._handle_payment_failed_business_logic(event_data)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement webhook d'abonnement: {e}")
            raise StripeWebhookError(f"Erreur webhook: {e}")
    
    def _handle_subscription_created_business_logic(self, event_data: Dict[str, Any]):
        """Logique métier lors de la création d'abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            # Récupérer l'utilisateur
            customer = self.stripe_handler.stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Invalider le cache
                self._invalidate_subscription_cache(user_id)
                
                # Enregistrer l'événement
                self._log_subscription_event(
                    user_id=user_id,
                    event_type=SubscriptionEvent.ACTIVATED,
                    metadata={'subscription_id': subscription['id']}
                )
                
                # Envoyer un email de bienvenue (async)
                self.executor.submit(self._send_welcome_email, user_id)
                
                logger.info(f"✅ Logique métier - Abonnement créé pour user {user_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur dans la logique métier de création d'abonnement: {e}")
    
    def _handle_subscription_updated_business_logic(self, event_data: Dict[str, Any]):
        """Logique métier lors de la mise à jour d'abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            customer = self.stripe_handler.stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Invalider le cache
                self._invalidate_subscription_cache(user_id)
                
                # Vérifier si l'abonnement expire bientôt
                if subscription.get('cancel_at_period_end'):
                    period_end = datetime.fromtimestamp(subscription['current_period_end'])
                    days_until_end = (period_end - datetime.utcnow()).days
                    
                    if days_until_end <= 3:  # 3 jours avant expiration
                        self.executor.submit(self._send_expiration_warning, user_id, days_until_end)
                
                logger.info(f"✅ Logique métier - Abonnement mis à jour pour user {user_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur dans la logique métier de mise à jour d'abonnement: {e}")
    
    def _handle_subscription_deleted_business_logic(self, event_data: Dict[str, Any]):
        """Logique métier lors de la suppression d'abonnement"""
        
        try:
            subscription = event_data['object']
            customer_id = subscription['customer']
            
            customer = self.stripe_handler.stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Invalider le cache
                self._invalidate_subscription_cache(user_id)
                
                # Enregistrer l'événement
                self._log_subscription_event(
                    user_id=user_id,
                    event_type=SubscriptionEvent.EXPIRED,
                    metadata={'subscription_id': subscription['id']}
                )
                
                # Nettoyer les données selon la politique de rétention
                self.executor.submit(self._cleanup_user_data, user_id)
                
                # Envoyer un email de fin d'abonnement
                self.executor.submit(self._send_cancellation_email, user_id)
                
                logger.info(f"✅ Logique métier - Abonnement supprimé pour user {user_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur dans la logique métier de suppression d'abonnement: {e}")
    
    def _handle_payment_failed_business_logic(self, event_data: Dict[str, Any]):
        """Logique métier lors d'un échec de paiement"""
        
        try:
            invoice = event_data['object']
            customer_id = invoice['customer']
            
            customer = self.stripe_handler.stripe.Customer.retrieve(customer_id)
            user_id = int(customer.metadata.get('user_id', 0))
            
            if user_id:
                # Enregistrer l'événement
                self._log_subscription_event(
                    user_id=user_id,
                    event_type=SubscriptionEvent.PAYMENT_FAILED,
                    metadata={
                        'invoice_id': invoice['id'],
                        'amount': invoice['amount_due']
                    }
                )
                
                # Envoyer une notification de paiement échoué
                self.executor.submit(self._send_payment_failed_email, user_id, invoice)
                
                logger.warning(f"⚠️  Logique métier - Paiement échoué pour user {user_id}")
            
        except Exception as e:
            logger.error(f"❌ Erreur dans la logique métier d'échec de paiement: {e}")
    
    # ========================================
    # GESTION DU CACHE ET ÉVÉNEMENTS
    # ========================================
    
    def _invalidate_subscription_cache(self, user_id: int):
        """Invalider le cache d'abonnement pour un utilisateur"""
        cache_key = f"subscription_{user_id}"
        if cache_key in self._subscription_cache:
            del self._subscription_cache[cache_key]
    
    def _log_subscription_event(self, user_id: int, event_type: SubscriptionEvent, 
                               metadata: Dict[str, Any] = None):
        """Enregistrer un événement d'abonnement"""
        
        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO subscription_events 
                        (user_id, event_type, metadata, created_at)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        user_id,
                        event_type.value,
                        json.dumps(metadata or {}),
                        datetime.utcnow()
                    ))
                    conn.commit()
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'enregistrement d'événement: {e}")
    
    # ========================================
    # NOTIFICATIONS ASYNCHRONES
    # ========================================
    
    def _send_welcome_email(self, user_id: int):
        """Envoyer un email de bienvenue (implémentation à faire)"""
        # TODO: Intégrer avec un service d'email (SendGrid, etc.)
        logger.info(f"📧 Email de bienvenue envoyé à user {user_id}")
    
    def _send_expiration_warning(self, user_id: int, days_remaining: int):
        """Envoyer un avertissement d'expiration (implémentation à faire)"""
        # TODO: Envoyer email d'avertissement
        logger.info(f"⚠️  Avertissement d'expiration envoyé à user {user_id} ({days_remaining} jours)")
    
    def _send_cancellation_email(self, user_id: int):
        """Envoyer un email de fin d'abonnement (implémentation à faire)"""
        # TODO: Envoyer email de fin d'abonnement
        logger.info(f"📧 Email de fin d'abonnement envoyé à user {user_id}")
    
    def _send_payment_failed_email(self, user_id: int, invoice: Dict):
        """Envoyer un email d'échec de paiement (implémentation à faire)"""
        # TODO: Envoyer email d'échec de paiement
        logger.info(f"📧 Email d'échec de paiement envoyé à user {user_id}")
    
    def _cleanup_user_data(self, user_id: int):
        """Nettoyer les données utilisateur selon la politique de rétention"""
        # TODO: Implémenter le nettoyage des données
        logger.info(f"🧹 Nettoyage des données lancé pour user {user_id}")
    
    # ========================================
    # UTILITAIRES ET INFORMATIONS
    # ========================================
    
    def get_subscription_analytics(self, user_id: int) -> Dict[str, Any]:
        """Récupérer les analytics d'abonnement d'un utilisateur"""
        
        try:
            subscription = self.get_user_subscription(user_id)
            if not subscription:
                return {'error': 'Aucun abonnement trouvé'}
            
            usage = subscription.usage
            limits = subscription.limits
            
            analytics = {
                'subscription_info': {
                    'plan_type': subscription.plan_type,
                    'status': subscription.status,
                    'days_remaining': (subscription.current_period_end - datetime.utcnow()).days,
                    'is_trial': subscription.trial_end is not None and datetime.utcnow() < subscription.trial_end,
                    'cancel_at_period_end': subscription.cancel_at_period_end
                },
                'usage_stats': {
                    'facebook_accounts': {
                        'used': usage.facebook_accounts,
                        'limit': limits.max_facebook_accounts,
                        'percentage': (usage.facebook_accounts / limits.max_facebook_accounts * 100) if limits.max_facebook_accounts > 0 else 0
                    },
                    'linkedin_accounts': {
                        'used': usage.linkedin_accounts,
                        'limit': limits.max_linkedin_accounts,
                        'percentage': (usage.linkedin_accounts / limits.max_linkedin_accounts * 100) if limits.max_linkedin_accounts > 0 else 0
                    },
                    'api_calls': {
                        'used': usage.api_calls_this_month,
                        'limit': limits.api_calls_per_month,
                        'percentage': (usage.api_calls_this_month / limits.api_calls_per_month * 100) if limits.api_calls_per_month > 0 else 0
                    }
                },
                'features_access': {
                    'advanced_analytics': limits.advanced_analytics,
                    'custom_reports': limits.custom_reports,
                    'priority_support': limits.priority_support,
                    'white_label': limits.white_label
                },
                'billing_info': {
                    'next_billing_date': subscription.next_billing_date.isoformat() if subscription.next_billing_date else None,
                    'amount': subscription.amount / 100 if subscription.amount else 0,
                    'currency': subscription.currency
                }
            }
            
            return analytics
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des analytics: {e}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """Vérification de santé du gestionnaire d'abonnements"""
        
        health = {
            'subscription_manager': 'ok',
            'stripe_handler': 'ok',
            'cache_size': len(self._subscription_cache),
            'plans_configured': len(self.plan_limits),
            'available_plans': list(self.plan_limits.keys()),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Test du stripe_handler
        try:
            stripe_health = self.stripe_handler.health_check()
            health['stripe_status'] = stripe_health.get('api_connection', 'unknown')
        except Exception as e:
            health['stripe_status'] = 'error'
            health['stripe_error'] = str(e)
        
        # Test de la base de données
        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    health['database'] = 'ok'
        except Exception as e:
            health['database'] = 'error'
            health['database_error'] = str(e)
        
        return health

# ========================================
# INSTANCE GLOBALE ET FONCTIONS HELPER
# ========================================

subscription_manager = SubscriptionManager()

def create_user_subscription(user_id: int, plan_type: str, success_url: str, 
                           cancel_url: str, **kwargs) -> Dict[str, Any]:
    """Fonction helper pour créer un abonnement"""
    return subscription_manager.create_subscription(
        user_id, plan_type, success_url, cancel_url, **kwargs
    )

def get_user_subscription_info(user_id: int) -> Optional[SubscriptionInfo]:
    """Fonction helper pour récupérer les infos d'abonnement"""
    return subscription_manager.get_user_subscription(user_id)

def check_user_feature_access(user_id: int, feature: str) -> bool:
    """Fonction helper pour vérifier l'accès à une fonctionnalité"""
    return subscription_manager.check_feature_access(user_id, feature)

def validate_account_limit(user_id: int, platform: str) -> Dict[str, Any]:
    """Fonction helper pour valider les limites de compte"""
    return subscription_manager.check_account_limit(user_id, platform)

def process_subscription_webhook(event_type: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
    """Fonction helper pour traiter les webhooks d'abonnement"""
    return subscription_manager.handle_subscription_webhook(event_type, event_data)

# ========================================
# DÉCORATEURS POUR CONTRÔLE D'ACCÈS
# ========================================

def require_subscription(plan_types: List[str] = None):
    """Décorateur pour exiger un abonnement spécifique"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # Récupérer user_id du premier argument ou des kwargs
            user_id = kwargs.get('user_id') or (args[0] if args else None)
            
            if not user_id:
                raise SubscriptionAccessError("user_id requis")
            
            subscription = subscription_manager.get_user_subscription(user_id)
            if not subscription:
                raise SubscriptionAccessError("Aucun abonnement trouvé")
            
            if plan_types and subscription.plan_type not in plan_types:
                raise SubscriptionAccessError(f"Plan {subscription.plan_type} insuffisant")
            
            if subscription.status not in ['active', 'trialing']:
                raise SubscriptionAccessError(f"Abonnement {subscription.status}")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

def require_feature(feature_name: str):
    """Décorateur pour exiger l'accès à une fonctionnalité"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            user_id = kwargs.get('user_id') or (args[0] if args else None)
            
            if not user_id:
                raise SubscriptionAccessError("user_id requis")
            
            if not subscription_manager.check_feature_access(user_id, feature_name):
                raise SubscriptionAccessError(f"Accès à {feature_name} non autorisé")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Tests si exécuté directement
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    print("🧪 Test SubscriptionManager...")
    
    try:
        # Test de santé
        manager = SubscriptionManager()
        health = manager.health_check()
        print(f"Health check: {json.dumps(health, indent=2)}")
        
        # Test des limites
        limits = manager.get_subscription_limits(1)
        print(f"Limites plan gratuit: {asdict(limits)}")
        
    except Exception as e:
        print(f"❌ Erreur lors des tests: {e}")
        import traceback
        traceback.print_exc()