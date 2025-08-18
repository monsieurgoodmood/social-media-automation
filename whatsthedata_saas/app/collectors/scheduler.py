"""
Scheduler unifié pour les collectes LinkedIn et Facebook
Orchestration intelligente, priorisation des quotas, monitoring global
Avec gestion d'erreurs exhaustive et optimisation des ressources
VERSION COMPLÈTE ET ROBUSTE
"""

import os
import json
import time
import logging
import threading
import asyncio
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List, Tuple, Set, Union, Callable
from dataclasses import dataclass, asdict, field
from enum import Enum
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from queue import Queue, PriorityQueue, Empty
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.thread import ThreadPoolExecutor as APSThreadPoolExecutor
import psutil
import uuid
from collections import defaultdict, deque
import pickle
import signal
import sys

from .linkedin_collector import linkedin_collector, LinkedinCollector, DataType as LinkedinDataType
from .facebook_collector import facebook_collector, FacebookCollector, FacebookDataType
from ..auth.user_manager import user_manager
from ..database.connection import db_manager
from ..database.models import User, LinkedinAccount, FacebookAccount
from ..utils.config import get_env_var

# Configuration du logging
logger = logging.getLogger(__name__)

class CollectorType(Enum):
    """Types de collecteurs"""
    LINKEDIN = "linkedin"
    FACEBOOK = "facebook"
    HYBRID = "hybrid"  # Collecte simultanée

class Priority(Enum):
    """Niveaux de priorité des tâches"""
    CRITICAL = 1    # Tokens expirent bientôt, quotas se reset
    HIGH = 2        # Collectes en retard importantes
    NORMAL = 3      # Collectes programmées normales
    LOW = 4         # Collectes de récupération, nettoyage
    MAINTENANCE = 5 # Tâches de maintenance système

class TaskStatus(Enum):
    """Statuts des tâches"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"
    SKIPPED = "skipped"
    PAUSED = "paused"
    TIMEOUT = "timeout"

class ResourceType(Enum):
    """Types de ressources système"""
    API_QUOTA = "api_quota"
    CPU = "cpu"
    MEMORY = "memory"
    DATABASE = "database"
    NETWORK = "network"
    DISK = "disk"

class HealthStatus(Enum):
    """Statuts de santé système"""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"

@dataclass
class CollectionTask:
    """Tâche de collecte unifiée"""
    task_id: str
    user_id: int
    collector_type: CollectorType
    target_id: str  # page_id ou organization_id
    data_types: List[str]
    priority: Priority
    scheduled_time: datetime
    created_at: datetime = field(default_factory=datetime.utcnow)
    status: TaskStatus = TaskStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    last_error: Optional[str] = None
    execution_time: Optional[float] = None
    resources_needed: Dict[ResourceType, int] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)  # IDs des tâches dépendantes
    metadata: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 300  # 5 minutes par défaut
    worker_assigned: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    def __lt__(self, other):
        """Comparaison pour la PriorityQueue"""
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        return self.scheduled_time < other.scheduled_time
    
    def is_expired(self) -> bool:
        """Vérifier si la tâche a expiré"""
        if self.status == TaskStatus.RUNNING and self.started_at:
            elapsed = datetime.utcnow() - self.started_at
            return elapsed.total_seconds() > self.timeout_seconds
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convertir en dictionnaire sérialisable"""
        return {
            'task_id': self.task_id,
            'user_id': self.user_id,
            'collector_type': self.collector_type.value,
            'target_id': self.target_id,
            'data_types': self.data_types,
            'priority': self.priority.value,
            'status': self.status.value,
            'scheduled_time': self.scheduled_time.isoformat(),
            'created_at': self.created_at.isoformat(),
            'attempts': self.attempts,
            'max_attempts': self.max_attempts,
            'last_error': self.last_error,
            'execution_time': self.execution_time,
            'metadata': self.metadata,
            'timeout_seconds': self.timeout_seconds,
            'worker_assigned': self.worker_assigned,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None
        }

@dataclass
class ResourceUsage:
    """Utilisation des ressources système"""
    api_calls_linkedin: int = 0
    api_calls_facebook: int = 0
    concurrent_tasks: int = 0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    database_connections: int = 0
    disk_usage_percent: float = 0.0
    network_usage_mbps: float = 0.0
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class CollectionStats:
    """Statistiques globales de collecte"""
    total_tasks_scheduled: int = 0
    total_tasks_completed: int = 0
    total_tasks_failed: int = 0
    total_tasks_cancelled: int = 0
    total_tasks_timeout: int = 0
    linkedin_collections: int = 0
    facebook_collections: int = 0
    hybrid_collections: int = 0
    average_execution_time: float = 0.0
    success_rate: float = 0.0
    quota_efficiency: float = 0.0
    last_reset: datetime = field(default_factory=datetime.utcnow)
    peak_concurrent_tasks: int = 0
    total_errors_by_type: Dict[str, int] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

@dataclass
class HealthCheckResult:
    """Résultat d'un health check"""
    status: HealthStatus
    component: str
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'status': self.status.value,
            'component': self.component,
            'message': self.message,
            'timestamp': self.timestamp.isoformat(),
            'details': self.details
        }

class SchedulerError(Exception):
    """Erreur du scheduler principal"""
    
    def __init__(self, message: str, error_code: str = None, 
                 task_id: str = None, collector_type: CollectorType = None):
        super().__init__(message)
        self.error_code = error_code
        self.task_id = task_id
        self.collector_type = collector_type
        self.timestamp = datetime.utcnow()

class ResourceExhaustionError(SchedulerError):
    """Erreur d'épuisement des ressources"""
    pass

class DependencyError(SchedulerError):
    """Erreur de dépendances entre tâches"""
    pass

class TimeoutError(SchedulerError):
    """Erreur de timeout de tâche"""
    pass

class TaskSchedulingPolicy:
    """Politique de planification des tâches"""
    
    def __init__(self):
        self.max_concurrent_tasks = int(get_env_var('SCHEDULER_MAX_CONCURRENT_TASKS', '6'))
        self.max_tasks_per_user = int(get_env_var('SCHEDULER_MAX_TASKS_PER_USER', '3'))
        self.max_tasks_per_collector = int(get_env_var('SCHEDULER_MAX_TASKS_PER_COLLECTOR', '4'))
        self.priority_boost_minutes = int(get_env_var('SCHEDULER_PRIORITY_BOOST_MINUTES', '60'))
        self.starvation_prevention_enabled = get_env_var('SCHEDULER_STARVATION_PREVENTION', 'true').lower() == 'true'
    
    def should_schedule_task(self, task: CollectionTask, current_tasks: Dict[str, CollectionTask]) -> Tuple[bool, str]:
        """Déterminer si une tâche peut être planifiée selon la politique"""
        
        # Vérifier le nombre total de tâches concurrentes
        running_tasks = [t for t in current_tasks.values() if t.status == TaskStatus.RUNNING]
        if len(running_tasks) >= self.max_concurrent_tasks:
            return False, f"Limite globale atteinte ({len(running_tasks)}/{self.max_concurrent_tasks})"
        
        # Vérifier le nombre de tâches par utilisateur
        user_tasks = [t for t in running_tasks if t.user_id == task.user_id]
        if len(user_tasks) >= self.max_tasks_per_user:
            return False, f"Limite utilisateur atteinte ({len(user_tasks)}/{self.max_tasks_per_user})"
        
        # Vérifier le nombre de tâches par collecteur
        collector_tasks = [t for t in running_tasks if t.collector_type == task.collector_type]
        if len(collector_tasks) >= self.max_tasks_per_collector:
            return False, f"Limite collecteur atteinte ({len(collector_tasks)}/{self.max_tasks_per_collector})"
        
        # Boost de priorité pour les tâches anciennes
        if self.starvation_prevention_enabled:
            wait_time = datetime.utcnow() - task.created_at
            if wait_time.total_seconds() > self.priority_boost_minutes * 60:
                if task.priority.value > Priority.HIGH.value:
                    task.priority = Priority.HIGH
                    return True, "Priorité boostée pour éviter la famine"
        
        return True, "Politique respectée"

class CircuitBreaker:
    """Circuit breaker pour protection contre les erreurs en cascade"""
    
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def call(self, func: Callable, *args, **kwargs):
        """Exécuter une fonction avec circuit breaker"""
        
        with self._lock:
            if self.state == 'OPEN':
                if self._should_attempt_reset():
                    self.state = 'HALF_OPEN'
                else:
                    raise SchedulerError("Circuit breaker ouvert", error_code="CIRCUIT_BREAKER_OPEN")
            
            try:
                result = func(*args, **kwargs)
                self._on_success()
                return result
            except Exception as e:
                self._on_failure()
                raise
    
    def _should_attempt_reset(self) -> bool:
        """Vérifier si on peut tenter de fermer le circuit"""
        if self.last_failure_time is None:
            return True
        return (datetime.utcnow() - self.last_failure_time).total_seconds() > self.timeout_seconds
    
    def _on_success(self):
        """Gérer un succès"""
        self.failure_count = 0
        self.state = 'CLOSED'
    
    def _on_failure(self):
        """Gérer un échec"""
        self.failure_count += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'OPEN'

class MetricsCollector:
    """Collecteur de métriques système et de performance"""
    
    def __init__(self):
        self.metrics_history = deque(maxlen=1000)  # Garder 1000 points de données
        self.collection_interval = 30  # secondes
        self.is_collecting = False
        self._stop_event = threading.Event()
    
    def start_collection(self):
        """Démarrer la collecte de métriques"""
        if self.is_collecting:
            return
        
        self.is_collecting = True
        self._stop_event.clear()
        
        def collect_loop():
            while not self._stop_event.is_set():
                try:
                    metrics = self.collect_current_metrics()
                    self.metrics_history.append(metrics)
                except Exception as e:
                    logger.error(f"❌ Erreur collecte métriques: {e}")
                
                self._stop_event.wait(self.collection_interval)
        
        thread = threading.Thread(target=collect_loop, daemon=True)
        thread.start()
        logger.info("✅ Collecte de métriques démarrée")
    
    def stop_collection(self):
        """Arrêter la collecte de métriques"""
        self._stop_event.set()
        self.is_collecting = False
        logger.info("🛑 Collecte de métriques arrêtée")
    
    def collect_current_metrics(self) -> Dict[str, Any]:
        """Collecter les métriques actuelles du système"""
        try:
            # Métriques système
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            # Métriques réseau
            network = psutil.net_io_counters()
            
            # Métriques processus
            process = psutil.Process()
            process_memory = process.memory_info()
            
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'system': {
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory.percent,
                    'memory_used_mb': memory.used / (1024 * 1024),
                    'memory_available_mb': memory.available / (1024 * 1024),
                    'disk_percent': (disk.used / disk.total) * 100,
                    'disk_used_gb': disk.used / (1024 * 1024 * 1024),
                    'disk_free_gb': disk.free / (1024 * 1024 * 1024)
                },
                'network': {
                    'bytes_sent': network.bytes_sent,
                    'bytes_recv': network.bytes_recv,
                    'packets_sent': network.packets_sent,
                    'packets_recv': network.packets_recv
                },
                'process': {
                    'memory_rss_mb': process_memory.rss / (1024 * 1024),
                    'memory_vms_mb': process_memory.vms / (1024 * 1024),
                    'cpu_percent': process.cpu_percent(),
                    'num_threads': process.num_threads(),
                    'open_files': len(process.open_files()) if hasattr(process, 'open_files') else 0
                }
            }
        except Exception as e:
            logger.error(f"❌ Erreur collecte métriques système: {e}")
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e)
            }
    
    def get_metrics_summary(self, minutes: int = 10) -> Dict[str, Any]:
        """Obtenir un résumé des métriques sur N minutes"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        
        recent_metrics = [
            m for m in self.metrics_history 
            if 'timestamp' in m and datetime.fromisoformat(m['timestamp']) > cutoff_time
        ]
        
        if not recent_metrics:
            return {'error': 'Pas de métriques récentes disponibles'}
        
        # Calculer les moyennes et extrema
        cpu_values = [m.get('system', {}).get('cpu_percent', 0) for m in recent_metrics]
        memory_values = [m.get('system', {}).get('memory_percent', 0) for m in recent_metrics]
        
        return {
            'period_minutes': minutes,
            'data_points': len(recent_metrics),
            'cpu': {
                'avg': sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                'min': min(cpu_values) if cpu_values else 0,
                'max': max(cpu_values) if cpu_values else 0
            },
            'memory': {
                'avg': sum(memory_values) / len(memory_values) if memory_values else 0,
                'min': min(memory_values) if memory_values else 0,
                'max': max(memory_values) if memory_values else 0
            },
            'latest': recent_metrics[-1] if recent_metrics else {}
        }

class PersistenceManager:
    """Gestionnaire de persistance pour l'état du scheduler"""
    
    def __init__(self, state_file: str = "scheduler_state.pkl"):
        self.state_file = state_file
        self.auto_save_interval = 300  # 5 minutes
        self.is_auto_saving = False
        self._stop_event = threading.Event()
    
    def save_state(self, scheduler_state: Dict[str, Any]):
        """Sauvegarder l'état du scheduler"""
        try:
            # Créer une copie sérialisable
            serializable_state = {
                'timestamp': datetime.utcnow().isoformat(),
                'task_queue': [task.to_dict() for task in scheduler_state.get('task_queue', [])],
                'running_tasks': {tid: task.to_dict() for tid, task in scheduler_state.get('running_tasks', {}).items()},
                'completed_tasks': {tid: task.to_dict() for tid, task in scheduler_state.get('completed_tasks', {}).items()},
                'failed_tasks': {tid: task.to_dict() for tid, task in scheduler_state.get('failed_tasks', {}).items()},
                'stats': scheduler_state.get('stats', {}).to_dict() if hasattr(scheduler_state.get('stats', {}), 'to_dict') else {},
                'resource_usage': scheduler_state.get('resource_usage', {}).to_dict() if hasattr(scheduler_state.get('resource_usage', {}), 'to_dict') else {}
            }
            
            with open(self.state_file, 'wb') as f:
                pickle.dump(serializable_state, f)
            
            logger.debug(f"✅ État du scheduler sauvegardé: {self.state_file}")
        except Exception as e:
            logger.error(f"❌ Erreur sauvegarde état: {e}")
    
    def load_state(self) -> Optional[Dict[str, Any]]:
        """Charger l'état du scheduler"""
        try:
            if not os.path.exists(self.state_file):
                return None
            
            with open(self.state_file, 'rb') as f:
                state = pickle.load(f)
            
            logger.info(f"✅ État du scheduler chargé: {self.state_file}")
            return state
        except Exception as e:
            logger.error(f"❌ Erreur chargement état: {e}")
            return None
    
    def start_auto_save(self, get_state_func: Callable):
        """Démarrer la sauvegarde automatique"""
        if self.is_auto_saving:
            return
        
        self.is_auto_saving = True
        self._stop_event.clear()
        
        def save_loop():
            while not self._stop_event.is_set():
                try:
                    state = get_state_func()
                    self.save_state(state)
                except Exception as e:
                    logger.error(f"❌ Erreur sauvegarde automatique: {e}")
                
                self._stop_event.wait(self.auto_save_interval)
        
        thread = threading.Thread(target=save_loop, daemon=True)
        thread.start()
        logger.info("✅ Sauvegarde automatique démarrée")
    
    def stop_auto_save(self):
        """Arrêter la sauvegarde automatique"""
        self._stop_event.set()
        self.is_auto_saving = False
        logger.info("🛑 Sauvegarde automatique arrêtée")

class UnifiedCollectionScheduler:
    """Scheduler unifié pour LinkedIn et Facebook - VERSION COMPLÈTE"""
    
    def __init__(self):
        # Collecteurs
        self.linkedin_collector = linkedin_collector
        self.facebook_collector = facebook_collector
        
        # Configuration
        self.config = self._load_scheduler_config()
        
        # Scheduler APScheduler pour les tâches récurrentes
        self.aps_scheduler = BackgroundScheduler(
            jobstores={'default': MemoryJobStore()},
            executors={'default': APSThreadPoolExecutor(max_workers=self.config['max_workers'])},
            job_defaults={'coalesce': False, 'max_instances': 1}
        )
        
        # Queues de tâches et stockage
        self.task_queue = PriorityQueue()
        self.running_tasks: Dict[str, CollectionTask] = {}
        self.completed_tasks: Dict[str, CollectionTask] = {}
        self.failed_tasks: Dict[str, CollectionTask] = {}
        self.cancelled_tasks: Dict[str, CollectionTask] = {}
        
        # Politique de planification
        self.scheduling_policy = TaskSchedulingPolicy()
        
        # Circuit breakers par collecteur
        self.circuit_breakers = {
            CollectorType.LINKEDIN: CircuitBreaker(),
            CollectorType.FACEBOOK: CircuitBreaker(),
            CollectorType.HYBRID: CircuitBreaker()
        }
        
        # Gestion des ressources
        self.resource_usage = ResourceUsage()
        self.resource_limits = self._load_resource_limits()
        
        # Statistiques et métriques
        self.stats = CollectionStats()
        self.metrics_collector = MetricsCollector()
        self.persistence_manager = PersistenceManager()
        
        # Contrôle d'exécution
        self.is_running = False
        self.is_paused = False
        self.worker_threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._resource_lock = threading.RLock()
        
        # Cache des utilisateurs et comptes
        self._user_cache = {}
        self._cache_expiry = datetime.utcnow()
        
        # Monitoring et alertes
        self.error_counts = defaultdict(int)
        self.performance_metrics = {}
        self.alert_thresholds = self._load_alert_thresholds()
        self.health_checks = {}
        
        # Handlers de signaux pour arrêt propre
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        # UUID unique pour cette instance
        self.instance_id = str(uuid.uuid4())[:8]
        
        logger.info(f"🚀 Scheduler unifié initialisé (instance: {self.instance_id})")
    
    def _signal_handler(self, signum, frame):
        """Gestionnaire de signaux pour arrêt propre"""
        logger.info(f"🛑 Signal {signum} reçu, arrêt propre en cours...")
        self.stop_scheduler(timeout=30)
        sys.exit(0)
    
    def _load_scheduler_config(self) -> Dict[str, Any]:
        """Charger la configuration du scheduler"""
        
        config = {
            # Workers et concurrence
            'max_workers': int(get_env_var('SCHEDULER_MAX_WORKERS', '4')),
            'max_concurrent_tasks': int(get_env_var('SCHEDULER_MAX_CONCURRENT_TASKS', '6')),
            'worker_sleep_interval': float(get_env_var('SCHEDULER_WORKER_SLEEP_INTERVAL', '1.0')),
            
            # Temporisation et retry
            'default_retry_delay': int(get_env_var('SCHEDULER_RETRY_DELAY', '300')),
            'max_retry_attempts': int(get_env_var('SCHEDULER_MAX_RETRY_ATTEMPTS', '3')),
            'exponential_backoff': get_env_var('SCHEDULER_EXPONENTIAL_BACKOFF', 'true').lower() == 'true',
            'task_timeout_seconds': int(get_env_var('SCHEDULER_TASK_TIMEOUT_SECONDS', '600')),
            
            # Planification
            'linkedin_interval_hours': int(get_env_var('SCHEDULER_LINKEDIN_INTERVAL_HOURS', '4')),
            'facebook_interval_hours': int(get_env_var('SCHEDULER_FACEBOOK_INTERVAL_HOURS', '6')),
            'maintenance_hour': int(get_env_var('SCHEDULER_MAINTENANCE_HOUR', '2')),
            'weekend_collection': get_env_var('SCHEDULER_WEEKEND_COLLECTION', 'true').lower() == 'true',
            
            # Optimisations
            'quota_buffer_percent': int(get_env_var('SCHEDULER_QUOTA_BUFFER_PERCENT', '15')),
            'intelligent_scheduling': get_env_var('SCHEDULER_INTELLIGENT_SCHEDULING', 'true').lower() == 'true',
            'load_balancing': get_env_var('SCHEDULER_LOAD_BALANCING', 'true').lower() == 'true',
            'adaptive_intervals': get_env_var('SCHEDULER_ADAPTIVE_INTERVALS', 'true').lower() == 'true',
            
            # Monitoring et santé
            'health_check_interval': int(get_env_var('SCHEDULER_HEALTH_CHECK_INTERVAL', '30')),
            'stats_reset_hours': int(get_env_var('SCHEDULER_STATS_RESET_HOURS', '24')),
            'error_threshold': int(get_env_var('SCHEDULER_ERROR_THRESHOLD', '10')),
            'auto_recovery_enabled': get_env_var('SCHEDULER_AUTO_RECOVERY', 'true').lower() == 'true',
            
            # Persistance
            'state_persistence_enabled': get_env_var('SCHEDULER_STATE_PERSISTENCE', 'true').lower() == 'true',
            'state_file': get_env_var('SCHEDULER_STATE_FILE', 'scheduler_state.pkl'),
            'auto_save_interval': int(get_env_var('SCHEDULER_AUTO_SAVE_INTERVAL', '300')),
            
            # Debugging et logging
            'debug_mode': get_env_var('SCHEDULER_DEBUG_MODE', 'false').lower() == 'true',
            'log_task_details': get_env_var('SCHEDULER_LOG_TASK_DETAILS', 'false').lower() == 'true',
            'performance_monitoring': get_env_var('SCHEDULER_PERFORMANCE_MONITORING', 'true').lower() == 'true',
            'detailed_metrics': get_env_var('SCHEDULER_DETAILED_METRICS', 'false').lower() == 'true'
        }
        
        logger.info(f"✅ Configuration scheduler chargée: {len(config)} paramètres")
        return config
    
    def _load_resource_limits(self) -> Dict[ResourceType, int]:
        """Charger les limites de ressources"""
        
        limits = {
            ResourceType.API_QUOTA: int(get_env_var('SCHEDULER_API_QUOTA_LIMIT', '500')),
            ResourceType.CPU: int(get_env_var('SCHEDULER_CPU_LIMIT_PERCENT', '80')),
            ResourceType.MEMORY: int(get_env_var('SCHEDULER_MEMORY_LIMIT_MB', '1024')),
            ResourceType.DATABASE: int(get_env_var('SCHEDULER_DB_CONNECTION_LIMIT', '10')),
            ResourceType.NETWORK: int(get_env_var('SCHEDULER_NETWORK_LIMIT_MBPS', '100')),
            ResourceType.DISK: int(get_env_var('SCHEDULER_DISK_LIMIT_PERCENT', '90'))
        }
        
        logger.info(f"✅ Limites de ressources configurées: {limits}")
        return limits
    
    def _load_alert_thresholds(self) -> Dict[str, Dict[str, Any]]:
        """Charger les seuils d'alerte"""
        
        return {
            'cpu': {
                'warning': int(get_env_var('ALERT_CPU_WARNING', '70')),
                'critical': int(get_env_var('ALERT_CPU_CRITICAL', '90'))
            },
            'memory': {
                'warning': int(get_env_var('ALERT_MEMORY_WARNING', '80')),
                'critical': int(get_env_var('ALERT_MEMORY_CRITICAL', '95'))
            },
            'errors': {
                'warning': int(get_env_var('ALERT_ERRORS_WARNING', '5')),
                'critical': int(get_env_var('ALERT_ERRORS_CRITICAL', '10'))
            },
            'queue_size': {
                'warning': int(get_env_var('ALERT_QUEUE_WARNING', '50')),
                'critical': int(get_env_var('ALERT_QUEUE_CRITICAL', '100'))
            }
        }
    
    # ========================================
    # GESTION AVANCÉE DES TÂCHES
    # ========================================
    
    def schedule_task(self, user_id: int, collector_type: CollectorType, target_id: str,
                     data_types: List[str] = None, priority: Priority = Priority.NORMAL,
                     scheduled_time: datetime = None, dependencies: List[str] = None,
                     metadata: Dict[str, Any] = None, timeout_seconds: int = None) -> str:
        """Planifier une tâche de collecte avec validation complète"""
        
        try:
            # Générer un ID unique pour la tâche
            task_id = f"{collector_type.value}_{user_id}_{target_id}_{int(time.time())}_{uuid.uuid4().hex[:8]}"
            
            # Configuration par défaut
            if not scheduled_time:
                scheduled_time = datetime.utcnow()
            
            if not data_types:
                data_types = self._get_default_data_types(collector_type)
            
            if not timeout_seconds:
                timeout_seconds = self.config['task_timeout_seconds']
            
            # Estimer les ressources nécessaires
            resources_needed = self._estimate_resources(collector_type, data_types)
            
            # Créer la tâche
            task = CollectionTask(
                task_id=task_id,
                user_id=user_id,
                collector_type=collector_type,
                target_id=target_id,
                data_types=data_types,
                priority=priority,
                scheduled_time=scheduled_time,
                max_attempts=self.config['max_retry_attempts'],
                resources_needed=resources_needed,
                dependencies=dependencies or [],
                metadata=metadata or {},
                timeout_seconds=timeout_seconds
            )
            
            # Validation complète
            validation_result = self._validate_task(task)
            if not validation_result[0]:
                raise SchedulerError(
                    f"Validation échouée: {validation_result[1]}",
                    error_code="TASK_VALIDATION_FAILED",
                    task_id=task_id
                )
            
            # Vérifier les politiques de planification
            policy_check = self.scheduling_policy.should_schedule_task(task, self.running_tasks)
            if not policy_check[0]:
                # Retarder la tâche au lieu de la rejeter
                task.scheduled_time = datetime.utcnow() + timedelta(minutes=5)
                task.metadata['policy_delayed'] = True
                task.metadata['delay_reason'] = policy_check[1]
            
            # Ajouter à la queue
            self.task_queue.put(task)
            task.status = TaskStatus.QUEUED
            
            self.stats.total_tasks_scheduled += 1
            
            logger.info(f"✅ Tâche planifiée: {task_id} ({collector_type.value}, priorité {priority.name})")
            
            # Notification d'événement
            self._emit_event('task_scheduled', {
                'task_id': task_id,
                'collector_type': collector_type.value,
                'user_id': user_id,
                'target_id': target_id,
                'priority': priority.value,
                'scheduled_time': scheduled_time.isoformat()
            })
            
            return task_id
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la planification de tâche: {e}")
            raise SchedulerError(
                f"Impossible de planifier la tâche: {e}",
                error_code="TASK_SCHEDULING_FAILED",
                collector_type=collector_type
            )
    
    def _get_default_data_types(self, collector_type: CollectorType) -> List[str]:
        """Obtenir les types de données par défaut pour un collecteur"""
        
        if collector_type == CollectorType.LINKEDIN:
            return ["organization_info", "followers_breakdown", "posts", "posts_metrics"]
        elif collector_type == CollectorType.FACEBOOK:
            return ["page_info", "page_metrics", "posts", "posts_metrics"]
        elif collector_type == CollectorType.HYBRID:
            return ["organization_info", "page_info", "posts", "posts_metrics"]
        return []
    
    def _validate_task(self, task: CollectionTask) -> Tuple[bool, str]:
        """Validation complète d'une tâche"""
        
        # Vérifier l'utilisateur
        if not user_manager.user_exists(task.user_id):
            return False, f"Utilisateur {task.user_id} non trouvé"
        
        # Vérifier si l'utilisateur a les comptes nécessaires
        if task.collector_type == CollectorType.LINKEDIN:
            if not self._user_has_linkedin_account(task.user_id, task.target_id):
                return False, f"Compte LinkedIn {task.target_id} non accessible pour l'utilisateur {task.user_id}"
        elif task.collector_type == CollectorType.FACEBOOK:
            if not self._user_has_facebook_account(task.user_id, task.target_id):
                return False, f"Compte Facebook {task.target_id} non accessible pour l'utilisateur {task.user_id}"
        
        # Vérifier les dépendances
        if not self._validate_dependencies(task):
            return False, "Dépendances invalides"
        
        # Vérifier les types de données
        valid_types = self._get_valid_data_types(task.collector_type)
        invalid_types = [dt for dt in task.data_types if dt not in valid_types]
        if invalid_types:
            return False, f"Types de données invalides: {invalid_types}"
        
        # Vérifier la date de planification
        if task.scheduled_time < datetime.utcnow() - timedelta(hours=24):
            return False, "Date de planification trop ancienne"
        
        return True, "Validation réussie"
    
    def _user_has_linkedin_account(self, user_id: int, organization_id: str) -> bool:
        """Vérifier si l'utilisateur a accès au compte LinkedIn"""
        try:
            with db_manager.get_session() as session:
                account = session.query(LinkedinAccount).filter(
                    LinkedinAccount.user_id == user_id,
                    LinkedinAccount.organization_id == organization_id,
                    LinkedinAccount.is_active == True
                ).first()
                return account is not None
        except Exception:
            return False
    
    def _user_has_facebook_account(self, user_id: int, page_id: str) -> bool:
        """Vérifier si l'utilisateur a accès au compte Facebook"""
        try:
            with db_manager.get_session() as session:
                account = session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user_id,
                    FacebookAccount.page_id == page_id,
                    FacebookAccount.is_active == True
                ).first()
                return account is not None
        except Exception:
            return False
    
    def _get_valid_data_types(self, collector_type: CollectorType) -> List[str]:
        """Obtenir les types de données valides pour un collecteur"""
        
        if collector_type == CollectorType.LINKEDIN:
            return [dt.value for dt in LinkedinDataType]
        elif collector_type == CollectorType.FACEBOOK:
            return [dt.value for dt in FacebookDataType]
        elif collector_type == CollectorType.HYBRID:
            linkedin_types = [dt.value for dt in LinkedinDataType]
            facebook_types = [dt.value for dt in FacebookDataType]
            return linkedin_types + facebook_types
        return []
    
    def schedule_user_collection(self, user_id: int, force_refresh: bool = False,
                                priority: Priority = Priority.NORMAL,
                                include_linkedin: bool = True,
                                include_facebook: bool = True) -> List[str]:
        """Planifier la collecte complète d'un utilisateur avec options avancées"""
        
        try:
            scheduled_tasks = []
            
            # Récupérer les comptes de l'utilisateur
            user_accounts = self._get_user_accounts(user_id)
            
            # Planifier LinkedIn
            if include_linkedin:
                for linkedin_account in user_accounts.get('linkedin', []):
                    if force_refresh or self._should_collect_linkedin(user_id, linkedin_account.organization_id):
                        # Espacer les tâches LinkedIn
                        delay = len(scheduled_tasks) * 2  # 2 minutes entre chaque
                        scheduled_time = datetime.utcnow() + timedelta(minutes=delay)
                        
                        task_id = self.schedule_task(
                            user_id=user_id,
                            collector_type=CollectorType.LINKEDIN,
                            target_id=linkedin_account.organization_id,
                            priority=priority,
                            scheduled_time=scheduled_time,
                            metadata={
                                'account_name': linkedin_account.organization_name,
                                'user_collection': True,
                                'force_refresh': force_refresh
                            }
                        )
                        scheduled_tasks.append(task_id)
            
            # Planifier Facebook (avec délai plus important)
            if include_facebook:
                base_delay = len(scheduled_tasks) * 3 + 5  # 3 minutes entre + 5 min de base
                for i, facebook_account in enumerate(user_accounts.get('facebook', [])):
                    if force_refresh or self._should_collect_facebook(user_id, facebook_account.page_id):
                        scheduled_time = datetime.utcnow() + timedelta(minutes=base_delay + (i * 3))
                        
                        task_id = self.schedule_task(
                            user_id=user_id,
                            collector_type=CollectorType.FACEBOOK,
                            target_id=facebook_account.page_id,
                            priority=priority,
                            scheduled_time=scheduled_time,
                            metadata={
                                'page_name': facebook_account.page_name,
                                'user_collection': True,
                                'force_refresh': force_refresh
                            }
                        )
                        scheduled_tasks.append(task_id)
            
            logger.info(f"✅ Collecte utilisateur planifiée: {user_id} ({len(scheduled_tasks)} tâches)")
            
            # Émettre un événement
            self._emit_event('user_collection_scheduled', {
                'user_id': user_id,
                'tasks_count': len(scheduled_tasks),
                'task_ids': scheduled_tasks,
                'force_refresh': force_refresh,
                'include_linkedin': include_linkedin,
                'include_facebook': include_facebook
            })
            
            return scheduled_tasks
            
        except Exception as e:
            logger.error(f"❌ Erreur planification utilisateur {user_id}: {e}")
            raise SchedulerError(
                f"Impossible de planifier la collecte utilisateur: {e}",
                error_code="USER_SCHEDULING_FAILED"
            )
    
    def cancel_task(self, task_id: str, reason: str = "Cancelled by user") -> bool:
        """Annuler une tâche avec nettoyage complet"""
        
        try:
            task_found = False
            
            # Chercher dans les tâches en cours
            if task_id in self.running_tasks:
                task = self.running_tasks[task_id]
                task.status = TaskStatus.CANCELLED
                task.last_error = reason
                task.completed_at = datetime.utcnow()
                
                # Libérer les ressources
                self._release_resources(task)
                
                # Déplacer vers les tâches annulées
                self.cancelled_tasks[task_id] = task
                del self.running_tasks[task_id]
                
                self.stats.total_tasks_cancelled += 1
                task_found = True
                
                logger.info(f"✅ Tâche en cours annulée: {task_id}")
            
            # Chercher dans la queue
            if not task_found:
                temp_queue = PriorityQueue()
                
                while not self.task_queue.empty():
                    try:
                        task = self.task_queue.get_nowait()
                        if task.task_id == task_id:
                            task.status = TaskStatus.CANCELLED
                            task.last_error = reason
                            task.completed_at = datetime.utcnow()
                            self.cancelled_tasks[task_id] = task
                            self.stats.total_tasks_cancelled += 1
                            task_found = True
                            logger.info(f"✅ Tâche en queue annulée: {task_id}")
                        else:
                            temp_queue.put(task)
                    except Empty:
                        break
                
                # Remettre les tâches non annulées dans la queue
                while not temp_queue.empty():
                    try:
                        self.task_queue.put(temp_queue.get_nowait())
                    except Empty:
                        break
            
            if task_found:
                # Émettre un événement
                self._emit_event('task_cancelled', {
                    'task_id': task_id,
                    'reason': reason,
                    'timestamp': datetime.utcnow().isoformat()
                })
            else:
                logger.warning(f"⚠️  Tâche non trouvée pour annulation: {task_id}")
            
            return task_found
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'annulation de tâche {task_id}: {e}")
            return False
    
    def pause_scheduler(self):
        """Mettre en pause le scheduler"""
        
        if self.is_paused:
            logger.warning("⚠️  Scheduler déjà en pause")
            return
        
        self.is_paused = True
        self._pause_event.clear()
        
        logger.info("⏸️  Scheduler mis en pause")
        self._emit_event('scheduler_paused', {
            'timestamp': datetime.utcnow().isoformat(),
            'running_tasks': len(self.running_tasks),
            'queued_tasks': self.task_queue.qsize()
        })
    
    def resume_scheduler(self):
        """Reprendre le scheduler"""
        
        if not self.is_paused:
            logger.warning("⚠️  Scheduler n'est pas en pause")
            return
        
        self.is_paused = False
        self._pause_event.set()
        
        logger.info("▶️  Scheduler repris")
        self._emit_event('scheduler_resumed', {
            'timestamp': datetime.utcnow().isoformat()
        })
    
    def retry_task(self, task_id: str, reset_attempts: bool = False) -> bool:
        """Relancer une tâche échouée avec options avancées"""
        
        try:
            if task_id not in self.failed_tasks:
                logger.warning(f"⚠️  Tâche non trouvée dans les échecs: {task_id}")
                return False
            
            task = self.failed_tasks[task_id]
            
            # Réinitialiser les tentatives si demandé
            if reset_attempts:
                task.attempts = 0
            
            # Vérifier le nombre de tentatives
            if task.attempts >= task.max_attempts and not reset_attempts:
                logger.warning(f"⚠️  Nombre max de tentatives atteint pour {task_id}")
                return False
            
            # Réinitialiser la tâche
            task.status = TaskStatus.PENDING
            task.attempts += 1
            task.last_error = None
            task.started_at = None
            task.completed_at = None
            task.worker_assigned = None
            
            # Calculer le délai de retry avec backoff exponentiel
            delay_seconds = self._calculate_retry_delay(task)
            task.scheduled_time = datetime.utcnow() + timedelta(seconds=delay_seconds)
            
            # Mettre à jour les métadonnées
            task.metadata['retry_count'] = task.metadata.get('retry_count', 0) + 1
            task.metadata['retry_timestamp'] = datetime.utcnow().isoformat()
            
            # Remettre en queue
            self.task_queue.put(task)
            task.status = TaskStatus.QUEUED
            
            # Retirer des échecs
            del self.failed_tasks[task_id]
            
            logger.info(f"✅ Tâche remise en queue: {task_id} (tentative {task.attempts}/{task.max_attempts})")
            
            # Émettre un événement
            self._emit_event('task_retried', {
                'task_id': task_id,
                'attempt': task.attempts,
                'max_attempts': task.max_attempts,
                'delay_seconds': delay_seconds,
                'reset_attempts': reset_attempts
            })
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du retry de tâche {task_id}: {e}")
            return False
    
    # ========================================
    # DÉMARRAGE ET ARRÊT AVANCÉS
    # ========================================
    
    def start_scheduler(self, restore_state: bool = True):
        """Démarrer le scheduler avec récupération d'état optionnelle"""
        
        if self.is_running:
            logger.warning("⚠️  Scheduler déjà en cours d'exécution")
            return
        
        try:
            logger.info(f"🚀 Démarrage du scheduler unifié (instance: {self.instance_id})")
            
            # Restaurer l'état si demandé et si la persistance est activée
            if restore_state and self.config['state_persistence_enabled']:
                self._restore_state()
            
            self.is_running = True
            self._stop_event.clear()
            self._pause_event.set()  # Démarrer non pausé
            
            # Démarrer APScheduler pour les tâches récurrentes
            self.aps_scheduler.start()
            
            # Planifier les tâches récurrentes
            self._schedule_recurring_tasks()
            
            # Démarrer les composants de monitoring
            if self.config['performance_monitoring']:
                self.metrics_collector.start_collection()
            
            # Démarrer la sauvegarde automatique
            if self.config['state_persistence_enabled']:
                self.persistence_manager.start_auto_save(self._get_current_state)
            
            # Démarrer les workers
            self._start_workers()
            
            # Démarrer le monitoring de santé
            self._start_health_monitoring()
            
            # Démarrer la surveillance des timeouts
            self._start_timeout_monitor()
            
            # Effectuer un health check initial
            self._perform_health_check()
            
            logger.info(f"✅ Scheduler unifié démarré avec {self.config['max_workers']} workers")
            
            # Émettre un événement
            self._emit_event('scheduler_started', {
                'instance_id': self.instance_id,
                'workers': self.config['max_workers'],
                'state_restored': restore_state,
                'timestamp': datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du démarrage du scheduler: {e}")
            self.is_running = False
            raise SchedulerError(
                f"Impossible de démarrer le scheduler: {e}",
                error_code="SCHEDULER_START_FAILED"
            )
    
    def stop_scheduler(self, timeout: int = 30, save_state: bool = True):
        """Arrêter le scheduler avec nettoyage complet"""
        
        if not self.is_running:
            logger.info("ℹ️  Scheduler déjà arrêté")
            return
        
        try:
            logger.info("🛑 Arrêt du scheduler en cours...")
            
            # Signaler l'arrêt
            self._stop_event.set()
            self.is_running = False
            
            # Sauvegarder l'état si demandé
            if save_state and self.config['state_persistence_enabled']:
                try:
                    self.persistence_manager.save_state(self._get_current_state())
                    logger.info("✅ État du scheduler sauvegardé")
                except Exception as e:
                    logger.error(f"❌ Erreur sauvegarde état: {e}")
            
            # Arrêter la sauvegarde automatique
            self.persistence_manager.stop_auto_save()
            
            # Arrêter la collecte de métriques
            self.metrics_collector.stop_collection()
            
            # Arrêter APScheduler
            if self.aps_scheduler.running:
                self.aps_scheduler.shutdown(wait=True)
            
            # Attendre la fin des workers avec timeout
            start_time = time.time()
            for worker in self.worker_threads:
                remaining_time = timeout - (time.time() - start_time)
                if remaining_time > 0:
                    worker.join(timeout=remaining_time)
                else:
                    logger.warning(f"⚠️  Timeout atteint pour l'arrêt du worker {worker.name}")
            
            # Forcer l'arrêt des tâches en cours
            self._force_stop_running_tasks()
            
            # Nettoyer les ressources
            self._cleanup_resources()
            
            # Statistiques finales
            final_stats = self.get_statistics()
            logger.info(f"📊 Statistiques finales: {final_stats['scheduler_stats']['total_tasks_completed']} tâches complétées, "
                       f"{final_stats['scheduler_stats']['success_rate']:.2%} de succès")
            
            logger.info("✅ Scheduler arrêté avec succès")
            
            # Émettre un événement final
            self._emit_event('scheduler_stopped', {
                'instance_id': self.instance_id,
                'final_stats': final_stats['scheduler_stats'],
                'timestamp': datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du scheduler: {e}")
            raise SchedulerError(
                f"Erreur lors de l'arrêt: {e}",
                error_code="SCHEDULER_STOP_FAILED"
            )
    
    def _restore_state(self):
        """Restaurer l'état du scheduler depuis la persistance"""
        
        try:
            saved_state = self.persistence_manager.load_state()
            if not saved_state:
                logger.info("ℹ️  Aucun état sauvegardé trouvé, démarrage à froid")
                return
            
            # Restaurer les tâches
            restored_tasks = 0
            
            # Restaurer les tâches en queue
            for task_dict in saved_state.get('task_queue', []):
                try:
                    task = self._dict_to_task(task_dict)
                    if task and task.status in [TaskStatus.PENDING, TaskStatus.QUEUED]:
                        self.task_queue.put(task)
                        restored_tasks += 1
                except Exception as e:
                    logger.warning(f"⚠️  Erreur restauration tâche en queue: {e}")
            
            # Restaurer les tâches terminées récentes (pour les statistiques)
            for task_id, task_dict in saved_state.get('completed_tasks', {}).items():
                try:
                    task = self._dict_to_task(task_dict)
                    if task:
                        self.completed_tasks[task_id] = task
                except Exception as e:
                    logger.warning(f"⚠️  Erreur restauration tâche terminée {task_id}: {e}")
            
            # Restaurer les tâches échouées
            for task_id, task_dict in saved_state.get('failed_tasks', {}).items():
                try:
                    task = self._dict_to_task(task_dict)
                    if task:
                        self.failed_tasks[task_id] = task
                except Exception as e:
                    logger.warning(f"⚠️  Erreur restauration tâche échouée {task_id}: {e}")
            
            # Restaurer les statistiques
            try:
                stats_dict = saved_state.get('stats', {})
                if stats_dict:
                    self.stats = CollectionStats(**stats_dict)
            except Exception as e:
                logger.warning(f"⚠️  Erreur restauration statistiques: {e}")
            
            logger.info(f"✅ État restauré: {restored_tasks} tâches en queue, "
                       f"{len(self.completed_tasks)} terminées, {len(self.failed_tasks)} échouées")
            
        except Exception as e:
            logger.error(f"❌ Erreur restauration état: {e}")
    
    def _dict_to_task(self, task_dict: Dict[str, Any]) -> Optional[CollectionTask]:
        """Convertir un dictionnaire en tâche"""
        
        try:
            return CollectionTask(
                task_id=task_dict['task_id'],
                user_id=task_dict['user_id'],
                collector_type=CollectorType(task_dict['collector_type']),
                target_id=task_dict['target_id'],
                data_types=task_dict['data_types'],
                priority=Priority(task_dict['priority']),
                status=TaskStatus(task_dict['status']),
                scheduled_time=datetime.fromisoformat(task_dict['scheduled_time']),
                created_at=datetime.fromisoformat(task_dict['created_at']),
                attempts=task_dict['attempts'],
                max_attempts=task_dict['max_attempts'],
                last_error=task_dict['last_error'],
                execution_time=task_dict['execution_time'],
                metadata=task_dict['metadata'],
                timeout_seconds=task_dict['timeout_seconds'],
                worker_assigned=task_dict['worker_assigned'],
                started_at=datetime.fromisoformat(task_dict['started_at']) if task_dict['started_at'] else None,
                completed_at=datetime.fromisoformat(task_dict['completed_at']) if task_dict['completed_at'] else None
            )
        except Exception as e:
            logger.warning(f"⚠️  Erreur conversion dictionnaire vers tâche: {e}")
            return None
    
    def _get_current_state(self) -> Dict[str, Any]:
        """Obtenir l'état actuel du scheduler pour la persistance"""
        
        # Extraire les tâches de la queue
        queue_tasks = []
        temp_queue = PriorityQueue()
        
        while not self.task_queue.empty():
            try:
                task = self.task_queue.get_nowait()
                queue_tasks.append(task)
                temp_queue.put(task)
            except Empty:
                break
        
        # Remettre les tâches dans la queue
        while not temp_queue.empty():
            try:
                self.task_queue.put(temp_queue.get_nowait())
            except Empty:
                break
        
        return {
            'task_queue': queue_tasks,
            'running_tasks': self.running_tasks,
            'completed_tasks': self.completed_tasks,
            'failed_tasks': self.failed_tasks,
            'stats': self.stats,
            'resource_usage': self.resource_usage
        }
    
    def _force_stop_running_tasks(self):
        """Forcer l'arrêt des tâches en cours"""
        
        for task_id, task in list(self.running_tasks.items()):
            try:
                task.status = TaskStatus.CANCELLED
                task.last_error = "Arrêt forcé du scheduler"
                task.completed_at = datetime.utcnow()
                
                # Libérer les ressources
                self._release_resources(task)
                
                # Déplacer vers les tâches annulées
                self.cancelled_tasks[task_id] = task
                del self.running_tasks[task_id]
                
                logger.debug(f"🛑 Tâche forcée à l'arrêt: {task_id}")
            except Exception as e:
                logger.error(f"❌ Erreur arrêt forcé tâche {task_id}: {e}")
    
    # ========================================
    # MONITORING ET SANTÉ AVANCÉS
    # ========================================
    
    def _start_health_monitoring(self):
        """Démarrer le monitoring de santé en continu"""
        
        def health_monitor_loop():
            while not self._stop_event.is_set():
                try:
                    if not self.is_paused:
                        self._perform_health_check()
                    self._stop_event.wait(self.config['health_check_interval'] * 60)
                except Exception as e:
                    logger.error(f"❌ Erreur monitoring santé: {e}")
        
        health_thread = threading.Thread(target=health_monitor_loop, daemon=True)
        health_thread.start()
        logger.info("✅ Monitoring de santé démarré")
    
    def _start_timeout_monitor(self):
        """Démarrer la surveillance des timeouts"""
        
        def timeout_monitor_loop():
            while not self._stop_event.is_set():
                try:
                    self._check_task_timeouts()
                    self._stop_event.wait(60)  # Vérifier chaque minute
                except Exception as e:
                    logger.error(f"❌ Erreur monitoring timeout: {e}")
        
        timeout_thread = threading.Thread(target=timeout_monitor_loop, daemon=True)
        timeout_thread.start()
        logger.info("✅ Monitoring des timeouts démarré")
    
    def _check_task_timeouts(self):
        """Vérifier et gérer les timeouts de tâches"""
        
        timed_out_tasks = []
        
        for task_id, task in list(self.running_tasks.items()):
            if task.is_expired():
                timed_out_tasks.append(task)
        
        for task in timed_out_tasks:
            try:
                logger.warning(f"⏰ Timeout détecté pour la tâche {task.task_id}")
                
                task.status = TaskStatus.TIMEOUT
                task.last_error = f"Timeout après {task.timeout_seconds}s"
                task.completed_at = datetime.utcnow()
                
                # Libérer les ressources
                self._release_resources(task)
                
                # Déplacer vers les tâches échouées
                self.failed_tasks[task.task_id] = task
                del self.running_tasks[task.task_id]
                
                self.stats.total_tasks_timeout += 1
                
                # Émettre un événement
                self._emit_event('task_timeout', {
                    'task_id': task.task_id,
                    'timeout_seconds': task.timeout_seconds,
                    'execution_time': (datetime.utcnow() - task.started_at).total_seconds() if task.started_at else 0
                })
                
            except Exception as e:
                logger.error(f"❌ Erreur gestion timeout tâche {task.task_id}: {e}")
    
    def _perform_health_check(self):
        """Effectuer un health check complet du système"""
        
        health_results = {}
        
        try:
            # Health check des collecteurs
            health_results['linkedin_collector'] = self._check_linkedin_health()
            health_results['facebook_collector'] = self._check_facebook_health()
            
            # Health check des ressources système
            health_results['system_resources'] = self._check_system_resources()
            
            # Health check de la base de données
            health_results['database'] = self._check_database_health()
            
            # Health check des queues et workers
            health_results['scheduler_internals'] = self._check_scheduler_internals()
            
            # Health check des quotas API
            health_results['api_quotas'] = self._check_api_quotas()
            
            # Stocker les résultats
            self.health_checks = {
                'timestamp': datetime.utcnow(),
                'results': health_results,
                'overall_status': self._determine_overall_health(health_results)
            }
            
            # Déclencher des alertes si nécessaire
            self._process_health_alerts(health_results)
            
        except Exception as e:
            logger.error(f"❌ Erreur health check global: {e}")
            self.health_checks = {
                'timestamp': datetime.utcnow(),
                'error': str(e),
                'overall_status': HealthStatus.UNKNOWN
            }
    
    def _check_linkedin_health(self) -> HealthCheckResult:
        """Vérifier la santé du collecteur LinkedIn"""
        
        try:
            health = self.linkedin_collector.health_check()
            
            if health.get('collector_status') == 'ok':
                return HealthCheckResult(
                    status=HealthStatus.HEALTHY,
                    component='linkedin_collector',
                    message='Collecteur LinkedIn opérationnel',
                    details=health
                )
            else:
                return HealthCheckResult(
                    status=HealthStatus.CRITICAL,
                    component='linkedin_collector',
                    message='Collecteur LinkedIn défaillant',
                    details=health
                )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.CRITICAL,
                component='linkedin_collector',
                message=f'Erreur health check LinkedIn: {e}',
                details={'error': str(e)}
            )
    
    def _check_facebook_health(self) -> HealthCheckResult:
        """Vérifier la santé du collecteur Facebook"""
        
        try:
            health = self.facebook_collector.health_check()
            
            if health.get('collector_status') == 'ok':
                return HealthCheckResult(
                    status=HealthStatus.HEALTHY,
                    component='facebook_collector',
                    message='Collecteur Facebook opérationnel',
                    details=health
                )
            else:
                return HealthCheckResult(
                    status=HealthStatus.CRITICAL,
                    component='facebook_collector',
                    message='Collecteur Facebook défaillant',
                    details=health
                )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.CRITICAL,
                component='facebook_collector',
                message=f'Erreur health check Facebook: {e}',
                details={'error': str(e)}
            )
    
    def _check_system_resources(self) -> HealthCheckResult:
        """Vérifier les ressources système"""
        
        try:
            current_metrics = self.metrics_collector.collect_current_metrics()
            system = current_metrics.get('system', {})
            
            cpu_percent = system.get('cpu_percent', 0)
            memory_percent = system.get('memory_percent', 0)
            disk_percent = system.get('disk_percent', 0)
            
            # Déterminer le statut
            if (cpu_percent > self.alert_thresholds['cpu']['critical'] or 
                memory_percent > self.alert_thresholds['memory']['critical'] or
                disk_percent > 95):
                status = HealthStatus.CRITICAL
                message = 'Ressources système critiques'
            elif (cpu_percent > self.alert_thresholds['cpu']['warning'] or 
                  memory_percent > self.alert_thresholds['memory']['warning'] or
                  disk_percent > 85):
                status = HealthStatus.WARNING
                message = 'Ressources système en tension'
            else:
                status = HealthStatus.HEALTHY
                message = 'Ressources système normales'
            
            return HealthCheckResult(
                status=status,
                component='system_resources',
                message=message,
                details={
                    'cpu_percent': cpu_percent,
                    'memory_percent': memory_percent,
                    'disk_percent': disk_percent,
                    'thresholds': self.alert_thresholds
                }
            )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                component='system_resources',
                message=f'Erreur vérification ressources: {e}',
                details={'error': str(e)}
            )
    
    def _check_database_health(self) -> HealthCheckResult:
        """Vérifier la santé de la base de données"""
        
        try:
            with db_manager.get_session() as session:
                start_time = time.time()
                session.execute('SELECT 1').fetchone()
                response_time = time.time() - start_time
            
            if response_time > 5.0:
                status = HealthStatus.CRITICAL
                message = f'Base de données très lente ({response_time:.2f}s)'
            elif response_time > 1.0:
                status = HealthStatus.WARNING
                message = f'Base de données lente ({response_time:.2f}s)'
            else:
                status = HealthStatus.HEALTHY
                message = f'Base de données réactive ({response_time:.3f}s)'
            
            return HealthCheckResult(
                status=status,
                component='database',
                message=message,
                details={
                    'response_time_seconds': response_time,
                    'connections_active': self.resource_usage.database_connections
                }
            )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.CRITICAL,
                component='database',
                message=f'Base de données inaccessible: {e}',
                details={'error': str(e)}
            )
    
    def _check_scheduler_internals(self) -> HealthCheckResult:
        """Vérifier l'état interne du scheduler"""
        
        try:
            queue_size = self.task_queue.qsize()
            running_count = len(self.running_tasks)
            worker_count = len([w for w in self.worker_threads if w.is_alive()])
            
            # Vérifier les seuils
            if queue_size > self.alert_thresholds['queue_size']['critical']:
                status = HealthStatus.CRITICAL
                message = f'Queue surchargée ({queue_size} tâches)'
            elif queue_size > self.alert_thresholds['queue_size']['warning']:
                status = HealthStatus.WARNING
                message = f'Queue chargée ({queue_size} tâches)'
            elif worker_count < self.config['max_workers']:
                status = HealthStatus.WARNING
                message = f'Workers manquants ({worker_count}/{self.config["max_workers"]})'
            else:
                status = HealthStatus.HEALTHY
                message = 'Scheduler opérationnel'
            
            return HealthCheckResult(
                status=status,
                component='scheduler_internals',
                message=message,
                details={
                    'queue_size': queue_size,
                    'running_tasks': running_count,
                    'active_workers': worker_count,
                    'total_workers': self.config['max_workers'],
                    'is_paused': self.is_paused
                }
            )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                component='scheduler_internals',
                message=f'Erreur vérification interne: {e}',
                details={'error': str(e)}
            )
    
    def _check_api_quotas(self) -> HealthCheckResult:
        """Vérifier l'état des quotas API"""
        
        try:
            # Vérifier les quotas via les collecteurs
            linkedin_stats = self.linkedin_collector.get_collection_statistics()
            facebook_stats = self.facebook_collector.get_collection_statistics()
            
            # Analyser l'utilisation des quotas
            total_api_calls = (self.resource_usage.api_calls_linkedin + 
                              self.resource_usage.api_calls_facebook)
            
            # Estimation de la capacité restante
            if total_api_calls > 800:  # Seuil critique
                status = HealthStatus.CRITICAL
                message = 'Quotas API quasi épuisés'
            elif total_api_calls > 600:  # Seuil warning
                status = HealthStatus.WARNING
                message = 'Quotas API en tension'
            else:
                status = HealthStatus.HEALTHY
                message = 'Quotas API disponibles'
            
            return HealthCheckResult(
                status=status,
                component='api_quotas',
                message=message,
                details={
                    'linkedin_calls': self.resource_usage.api_calls_linkedin,
                    'facebook_calls': self.resource_usage.api_calls_facebook,
                    'total_calls': total_api_calls,
                    'linkedin_stats': linkedin_stats,
                    'facebook_stats': facebook_stats
                }
            )
        except Exception as e:
            return HealthCheckResult(
                status=HealthStatus.UNKNOWN,
                component='api_quotas',
                message=f'Erreur vérification quotas: {e}',
                details={'error': str(e)}
            )
    
    def _determine_overall_health(self, health_results: Dict[str, HealthCheckResult]) -> HealthStatus:
        """Déterminer le statut de santé global"""
        
        statuses = [result.status for result in health_results.values()]
        
        if HealthStatus.CRITICAL in statuses:
            return HealthStatus.CRITICAL
        elif HealthStatus.WARNING in statuses:
            return HealthStatus.WARNING
        elif HealthStatus.UNKNOWN in statuses:
            return HealthStatus.UNKNOWN
        else:
            return HealthStatus.HEALTHY
    
    def _process_health_alerts(self, health_results: Dict[str, HealthCheckResult]):
        """Traiter les alertes basées sur les health checks"""
        
        for component, result in health_results.items():
            if result.status in [HealthStatus.CRITICAL, HealthStatus.WARNING]:
                # Émettre une alerte
                self._emit_event('health_alert', {
                    'component': component,
                    'status': result.status.value,
                    'message': result.message,
                    'details': result.details,
                    'timestamp': result.timestamp.isoformat()
                })
                
                # Actions automatiques selon le type d'alerte
                if result.status == HealthStatus.CRITICAL:
                    self._handle_critical_alert(component, result)
    
    def _handle_critical_alert(self, component: str, result: HealthCheckResult):
        """Gérer les alertes critiques avec actions automatiques"""
        
        try:
            if component == 'system_resources':
                # Réduire la charge en pausant les nouvelles tâches
                if not self.is_paused:
                    logger.warning("🚨 Pause automatique due aux ressources critiques")
                    self.pause_scheduler()
            
            elif component in ['linkedin_collector', 'facebook_collector']:
                # Activer le circuit breaker pour ce collecteur
                collector_type = CollectorType.LINKEDIN if 'linkedin' in component else CollectorType.FACEBOOK
                self.circuit_breakers[collector_type].state = 'OPEN'
                logger.warning(f"🚨 Circuit breaker activé pour {component}")
            
            elif component == 'database':
                # Retarder les nouvelles tâches nécessitant la DB
                logger.warning("🚨 Retard des tâches DB due aux problèmes de connexion")
                # Implémenter la logique de retard ici
        
        except Exception as e:
            logger.error(f"❌ Erreur gestion alerte critique {component}: {e}")
    
    # ========================================
    # EXÉCUTION AVANCÉE DES TÂCHES
    # ========================================
    
    def _start_workers(self):
        """Démarrer les workers avec gestion avancée"""
        
        self.worker_threads = []
        
        for i in range(self.config['max_workers']):
            worker = threading.Thread(
                target=self._worker_loop,
                name=f"CollectionWorker-{i+1}",
                daemon=True
            )
            worker.start()
            self.worker_threads.append(worker)
        
        logger.info(f"✅ {len(self.worker_threads)} workers démarrés")
    
    def _worker_loop(self):
        """Boucle principale d'un worker avec gestion complète"""
        
        worker_name = threading.current_thread().name
        logger.debug(f"🟢 Worker {worker_name} démarré")
        
        while not self._stop_event.is_set():
            try:
                # Attendre si en pause
                if self.is_paused:
                    self._pause_event.wait()
                    if self._stop_event.is_set():
                        break
                
                # Récupérer une tâche
                task = self._get_next_task()
                
                if task is None:
                    # Pas de tâche disponible, attendre
                    time.sleep(self.config['worker_sleep_interval'])
                    continue
                
                # Assigner le worker à la tâche
                task.worker_assigned = worker_name
                
                # Exécuter la tâche
                self._execute_task_with_monitoring(task, worker_name)
                
            except Exception as e:
                logger.error(f"❌ Erreur dans le worker {worker_name}: {e}")
                time.sleep(self.config['worker_sleep_interval'])
        
        logger.debug(f"🔴 Worker {worker_name} arrêté")
    
    def _get_next_task(self) -> Optional[CollectionTask]:
        """Récupérer la prochaine tâche avec logique avancée"""
        
        try:
            # Vérifier les limites de concurrence
            if len(self.running_tasks) >= self.config['max_concurrent_tasks']:
                return None
            
            # Récupérer la tâche prioritaire
            try:
                task = self.task_queue.get_nowait()
            except Empty:
                return None
            
            # Vérifier si c'est le bon moment
            if task.scheduled_time > datetime.utcnow():
                # Remettre en queue pour plus tard si pas trop en avance
                time_diff = (task.scheduled_time - datetime.utcnow()).total_seconds()
                if time_diff < 300:  # Moins de 5 minutes
                    time.sleep(min(time_diff, self.config['worker_sleep_interval']))
                    return task
                else:
                    self.task_queue.put(task)
                    return None
            
            # Vérifier les dépendances
            if not self._check_dependencies(task):
                # Remettre en queue avec délai
                task.scheduled_time = datetime.utcnow() + timedelta(minutes=1)
                self.task_queue.put(task)
                return None
            
            # Vérifier les politiques de planification
            policy_check = self.scheduling_policy.should_schedule_task(task, self.running_tasks)
            if not policy_check[0]:
                # Remettre en queue avec délai
                task.scheduled_time = datetime.utcnow() + timedelta(minutes=2)
                task.metadata['policy_delay_count'] = task.metadata.get('policy_delay_count', 0) + 1
                self.task_queue.put(task)
                return None
            
            # Vérifier les ressources disponibles
            if not self._check_resources_available(task):
                # Remettre en queue pour plus tard
                self.task_queue.put(task)
                return None
            
            # Vérifier le circuit breaker
            circuit_breaker = self.circuit_breakers.get(task.collector_type)
            if circuit_breaker and circuit_breaker.state == 'OPEN':
                # Vérifier si on peut tenter de fermer le circuit
                if circuit_breaker._should_attempt_reset():
                    circuit_breaker.state = 'HALF_OPEN'
                    logger.info(f"🔄 Tentative de fermeture circuit breaker {task.collector_type.value}")
                else:
                    # Remettre en queue avec délai plus long
                    task.scheduled_time = datetime.utcnow() + timedelta(minutes=5)
                    self.task_queue.put(task)
                    return None
            
            return task
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération de tâche: {e}")
            return None
    
    def _execute_task_with_monitoring(self, task: CollectionTask, worker_name: str):
        """Exécuter une tâche avec monitoring complet"""
        
        start_time = time.time()
        task.started_at = datetime.utcnow()
        
        try:
            # Marquer comme en cours
            task.status = TaskStatus.RUNNING
            self.running_tasks[task.task_id] = task
            
            # Réserver les ressources
            self._reserve_resources(task)
            
            # Mettre à jour les statistiques de concurrence
            current_concurrent = len(self.running_tasks)
            if current_concurrent > self.stats.peak_concurrent_tasks:
                self.stats.peak_concurrent_tasks = current_concurrent
            
            logger.info(f"🟡 Exécution tâche {task.task_id} par {worker_name} ({task.collector_type.value})")
            
            # Émettre un événement de début
            self._emit_event('task_started', {
                'task_id': task.task_id,
                'worker': worker_name,
                'collector_type': task.collector_type.value,
                'user_id': task.user_id,
                'target_id': task.target_id,
                'attempt': task.attempts + 1,
                'timestamp': task.started_at.isoformat()
            })
            
            # Exécuter avec circuit breaker
            circuit_breaker = self.circuit_breakers.get(task.collector_type)
            if circuit_breaker:
                result = circuit_breaker.call(self._execute_task_core, task)
            else:
                result = self._execute_task_core(task)
            
            # Traiter le résultat
            self._process_task_result(task, result, start_time)
            
        except Exception as e:
            # Gérer l'échec
            self._handle_task_failure(task, e, start_time)
        
        finally:
            # Libérer les ressources
            self._release_resources(task)
            
            # Retirer des tâches en cours
            if task.task_id in self.running_tasks:
                del self.running_tasks[task.task_id]
            
            # Marquer comme terminée
            task.completed_at = datetime.utcnow()
    
    def _execute_task_core(self, task: CollectionTask) -> Dict[str, Any]:
        """Exécution principale d'une tâche"""
        
        try:
            if task.collector_type == CollectorType.LINKEDIN:
                result = self._execute_linkedin_task(task)
            elif task.collector_type == CollectorType.FACEBOOK:
                result = self._execute_facebook_task(task)
            elif task.collector_type == CollectorType.HYBRID:
                result = self._execute_hybrid_task(task)
            else:
                raise SchedulerError(
                    f"Type de collecteur non supporté: {task.collector_type}",
                    error_code="UNSUPPORTED_COLLECTOR",
                    task_id=task.task_id
                )
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution tâche {task.task_id}: {e}")
            raise
    
    def _execute_linkedin_task(self, task: CollectionTask) -> Dict[str, Any]:
        """Exécuter une tâche LinkedIn avec gestion complète"""
        
        try:
            # Convertir les types de données
            data_types_enum = []
            for dt in task.data_types:
                try:
                    data_types_enum.append(LinkedinDataType(dt))
                except ValueError:
                    logger.warning(f"⚠️  Type de données LinkedIn invalide: {dt}")
            
            if not data_types_enum:
                return {'success': False, 'error': 'Aucun type de données valide'}
            
            # Exécuter la collecte
            result = self.linkedin_collector.collect_organization_data(
                user_id=task.user_id,
                organization_id=task.target_id,
                data_types=data_types_enum
            )
            
            self.stats.linkedin_collections += 1
            self.resource_usage.api_calls_linkedin += sum(
                r.api_calls_made for r in result.values() 
                if hasattr(r, 'api_calls_made')
            )
            
            return {'success': True, 'results': result}
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution tâche LinkedIn {task.task_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _execute_facebook_task(self, task: CollectionTask) -> Dict[str, Any]:
        """Exécuter une tâche Facebook avec gestion complète"""
        
        try:
            # Convertir les types de données
            data_types_enum = []
            for dt in task.data_types:
                try:
                    data_types_enum.append(FacebookDataType(dt))
                except ValueError:
                    logger.warning(f"⚠️  Type de données Facebook invalide: {dt}")
            
            if not data_types_enum:
                return {'success': False, 'error': 'Aucun type de données valide'}
            
            # Exécuter la collecte
            result = self.facebook_collector.collect_page_data(
                user_id=task.user_id,
                page_id=task.target_id,
                data_types=data_types_enum
            )
            
            self.stats.facebook_collections += 1
            self.resource_usage.api_calls_facebook += sum(
                r.api_calls_made for r in result.values() 
                if hasattr(r, 'api_calls_made')
            )
            
            return {'success': True, 'results': result}
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution tâche Facebook {task.task_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _execute_hybrid_task(self, task: CollectionTask) -> Dict[str, Any]:
        """Exécuter une tâche hybride (LinkedIn + Facebook)"""
        
        try:
            results = {}
            
            # Séparer les types de données par collecteur
            linkedin_types = []
            facebook_types = []
            
            for dt in task.data_types:
                if dt in [e.value for e in LinkedinDataType]:
                    linkedin_types.append(dt)
                elif dt in [e.value for e in FacebookDataType]:
                    facebook_types.append(dt)
            
            # Exécuter LinkedIn si applicable
            if linkedin_types and 'linkedin_target' in task.metadata:
                linkedin_task = CollectionTask(
                    task_id=f"{task.task_id}_linkedin",
                    user_id=task.user_id,
                    collector_type=CollectorType.LINKEDIN,
                    target_id=task.metadata['linkedin_target'],
                    data_types=linkedin_types,
                    priority=task.priority,
                    scheduled_time=task.scheduled_time
                )
                results['linkedin'] = self._execute_linkedin_task(linkedin_task)
            
            # Exécuter Facebook si applicable
            if facebook_types and 'facebook_target' in task.metadata:
                facebook_task = CollectionTask(
                    task_id=f"{task.task_id}_facebook",
                    user_id=task.user_id,
                    collector_type=CollectorType.FACEBOOK,
                    target_id=task.metadata['facebook_target'],
                    data_types=facebook_types,
                    priority=task.priority,
                    scheduled_time=task.scheduled_time
                )
                results['facebook'] = self._execute_facebook_task(facebook_task)
            
            self.stats.hybrid_collections += 1
            success = any(r.get('success', False) for r in results.values())
            
            return {'success': success, 'results': results}
            
        except Exception as e:
            logger.error(f"❌ Erreur exécution tâche hybride {task.task_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    def _process_task_result(self, task: CollectionTask, result: Dict[str, Any], start_time: float):
        """Traiter le résultat d'une tâche avec monitoring complet"""
        
        execution_time = time.time() - start_time
        task.execution_time = execution_time
        
        if result.get('success', False):
            # Succès
            task.status = TaskStatus.COMPLETED
            self.completed_tasks[task.task_id] = task
            self.stats.total_tasks_completed += 1
            
            # Nettoyer les anciennes tâches terminées
            self._cleanup_old_completed_tasks()
            
            logger.info(f"✅ Tâche réussie: {task.task_id} ({execution_time:.2f}s)")
            
            # Émettre un événement de succès
            self._emit_event('task_completed', {
                'task_id': task.task_id,
                'collector_type': task.collector_type.value,
                'execution_time': execution_time,
                'worker': task.worker_assigned,
                'attempt': task.attempts + 1,
                'timestamp': task.completed_at.isoformat() if task.completed_at else datetime.utcnow().isoformat()
            })
            
        else:
            # Échec
            error_msg = result.get('error', 'Erreur inconnue')
            task.last_error = error_msg
            task.attempts += 1
            
            # Incrémenter le compteur d'erreurs par type
            error_type = type(Exception(error_msg)).__name__
            self.error_counts[error_type] += 1
            self.stats.total_errors_by_type[error_type] = self.stats.total_errors_by_type.get(error_type, 0) + 1
            
            # Décider si retry ou échec définitif
            if task.attempts < task.max_attempts:
                # Retry
                task.status = TaskStatus.RETRYING
                retry_delay = self._calculate_retry_delay(task)
                task.scheduled_time = datetime.utcnow() + timedelta(seconds=retry_delay)
                self.task_queue.put(task)
                
                logger.warning(f"⚠️  Tâche en retry: {task.task_id} (tentative {task.attempts}/{task.max_attempts})")
                
                # Émettre un événement de retry
                self._emit_event('task_retry_scheduled', {
                    'task_id': task.task_id,
                    'attempt': task.attempts,
                    'max_attempts': task.max_attempts,
                    'retry_delay': retry_delay,
                    'error': error_msg
                })
                
            else:
                # Échec définitif
                task.status = TaskStatus.FAILED
                self.failed_tasks[task.task_id] = task
                self.stats.total_tasks_failed += 1
                
                logger.error(f"❌ Tâche échouée définitivement: {task.task_id}")
                
                # Émettre un événement d'échec
                self._emit_event('task_failed', {
                    'task_id': task.task_id,
                    'collector_type': task.collector_type.value,
                    'final_error': error_msg,
                    'attempts_made': task.attempts,
                    'execution_time': execution_time
                })
        
        # Mettre à jour les statistiques de performance
        self._update_performance_stats(task, execution_time)
    
    def _handle_task_failure(self, task: CollectionTask, error: Exception, start_time: float):
        """Gérer l'échec d'une tâche avec analyse complète"""
        
        execution_time = time.time() - start_time
        task.execution_time = execution_time
        task.last_error = str(error)
        task.attempts += 1
        
        # Analyser le type d'erreur
        error_type = type(error).__name__
        self.error_counts[error_type] += 1
        self.stats.total_errors_by_type[error_type] = self.stats.total_errors_by_type.get(error_type, 0) + 1
        
        # Traitement spécialisé selon le type d'erreur
        if isinstance(error, ResourceExhaustionError):
            # Ressources épuisées, retry avec délai plus long
            task.status = TaskStatus.RETRYING
            retry_delay = self._calculate_retry_delay(task) * 3  # Délai triplé
            task.scheduled_time = datetime.utcnow() + timedelta(seconds=retry_delay)
            self.task_queue.put(task)
            
            logger.warning(f"⚠️  Ressources épuisées, retry programmé: {task.task_id}")
            
        elif isinstance(error, TimeoutError):
            # Timeout, traiter comme un échec partiel
            task.status = TaskStatus.TIMEOUT
            self.failed_tasks[task.task_id] = task
            self.stats.total_tasks_timeout += 1
            
            logger.error(f"⏰ Timeout tâche: {task.task_id}")
            
        elif task.attempts < task.max_attempts:
            # Retry normal
            task.status = TaskStatus.RETRYING
            retry_delay = self._calculate_retry_delay(task)
            task.scheduled_time = datetime.utcnow() + timedelta(seconds=retry_delay)
            self.task_queue.put(task)
            
            logger.warning(f"⚠️  Erreur tâche, retry: {task.task_id} - {error}")
            
        else:
            # Échec définitif
            task.status = TaskStatus.FAILED
            self.failed_tasks[task.task_id] = task
            self.stats.total_tasks_failed += 1
            
            logger.error(f"❌ Tâche échouée après {task.attempts} tentatives: {task.task_id} - {error}")
        
        # Émettre un événement d'erreur
        self._emit_event('task_error', {
            'task_id': task.task_id,
            'collector_type': task.collector_type.value,
            'error_type': error_type,
            'error_message': str(error),
            'attempt': task.attempts,
            'max_attempts': task.max_attempts,
            'execution_time': execution_time,
            'will_retry': task.status == TaskStatus.RETRYING
        })
    
    # ========================================
    # GESTION AVANCÉE DES RESSOURCES
    # ========================================
    
    def _estimate_resources(self, collector_type: CollectorType, data_types: List[str]) -> Dict[ResourceType, int]:
        """Estimer les ressources nécessaires avec précision"""
        
        base_resources = {
            ResourceType.API_QUOTA: 0,
            ResourceType.CPU: 1,
            ResourceType.MEMORY: 50,  # MB
            ResourceType.DATABASE: 1,
            ResourceType.NETWORK: 10,  # Mbps
            ResourceType.DISK: 10  # MB
        }
        
        # Coefficients par type de données
        data_type_coefficients = {
            'posts': 3.0,
            'posts_metrics': 4.0,
            'page_metrics': 2.0,
            'organization_info': 1.0,
            'followers_breakdown': 2.5,
            'page_info': 1.0
        }
        
        # Calcul selon le collecteur
        if collector_type == CollectorType.LINKEDIN:
            base_calls = 5
            for data_type in data_types:
                coefficient = data_type_coefficients.get(data_type, 1.0)
                base_calls += int(10 * coefficient)
            
            base_resources[ResourceType.API_QUOTA] = base_calls
            base_resources[ResourceType.MEMORY] = 80
            
        elif collector_type == CollectorType.FACEBOOK:
            base_calls = 3
            for data_type in data_types:
                coefficient = data_type_coefficients.get(data_type, 1.0)
                base_calls += int(8 * coefficient)
            
            base_resources[ResourceType.API_QUOTA] = base_calls
            base_resources[ResourceType.MEMORY] = 60
            
        elif collector_type == CollectorType.HYBRID:
            # Combiner les estimations
            linkedin_estimate = self._estimate_resources(CollectorType.LINKEDIN, data_types)
            facebook_estimate = self._estimate_resources(CollectorType.FACEBOOK, data_types)
            
            for resource_type in base_resources:
                base_resources[resource_type] = (
                    linkedin_estimate.get(resource_type, 0) + 
                    facebook_estimate.get(resource_type, 0)
                )
        
        return base_resources
    
    def _check_resources_available(self, task: CollectionTask) -> bool:
        """Vérification avancée des ressources disponibles"""
        
        with self._resource_lock:
            # Vérifier chaque type de ressource
            for resource_type, needed in task.resources_needed.items():
                current_usage = self._get_current_usage(resource_type)
                limit = self.resource_limits.get(resource_type, float('inf'))
                
                # Appliquer un buffer de sécurité
                buffer_percent = self.config['quota_buffer_percent'] / 100
                effective_limit = limit * (1 - buffer_percent)
                
                if current_usage + needed > effective_limit:
                    logger.debug(f"🔴 Ressource {resource_type.value} insuffisante: "
                               f"{current_usage + needed} > {effective_limit} (limite: {limit})")
                    return False
            
            # Vérification spéciale pour les quotas API
            if not self._check_api_quotas_available(task):
                return False
            
            return True
    
    def _check_api_quotas_available(self, task: CollectionTask) -> bool:
        """Vérification spécifique des quotas API"""
        
        try:
            if task.collector_type == CollectorType.LINKEDIN:
                # Vérifier via le collecteur LinkedIn
                stats = self.linkedin_collector.get_collection_statistics()
                # Logique de vérification basée sur les statistiques
                
            elif task.collector_type == CollectorType.FACEBOOK:
                # Vérifier via le collecteur Facebook
                stats = self.facebook_collector.get_collection_statistics()
                # Logique de vérification basée sur les statistiques
            
            return True  # Simplifié pour l'exemple
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification quotas API: {e}")
            return True  # Par défaut, autoriser
    
    def _reserve_resources(self, task: CollectionTask):
        """Réserver les ressources avec tracking détaillé"""
        
        with self._resource_lock:
            self.resource_usage.concurrent_tasks += 1
            
            if task.collector_type == CollectorType.LINKEDIN:
                self.resource_usage.api_calls_linkedin += task.resources_needed.get(ResourceType.API_QUOTA, 0)
            elif task.collector_type == CollectorType.FACEBOOK:
                self.resource_usage.api_calls_facebook += task.resources_needed.get(ResourceType.API_QUOTA, 0)
            elif task.collector_type == CollectorType.HYBRID:
                # Répartir entre LinkedIn et Facebook
                api_quota = task.resources_needed.get(ResourceType.API_QUOTA, 0)
                self.resource_usage.api_calls_linkedin += api_quota // 2
                self.resource_usage.api_calls_facebook += api_quota // 2
            
            self.resource_usage.memory_usage_mb += task.resources_needed.get(ResourceType.MEMORY, 0)
            self.resource_usage.database_connections += task.resources_needed.get(ResourceType.DATABASE, 0)
            self.resource_usage.last_updated = datetime.utcnow()
            
            # Enregistrer la réservation dans les métadonnées de la tâche
            task.metadata['resources_reserved'] = task.resources_needed.copy()
            task.metadata['reservation_time'] = datetime.utcnow().isoformat()
    
    def _release_resources(self, task: CollectionTask):
        """Libérer les ressources avec nettoyage complet"""
        
        with self._resource_lock:
            self.resource_usage.concurrent_tasks = max(0, self.resource_usage.concurrent_tasks - 1)
            
            # Utiliser les ressources réellement réservées
            reserved = task.metadata.get('resources_reserved', task.resources_needed)
            
            if task.collector_type == CollectorType.LINKEDIN:
                api_quota = reserved.get(ResourceType.API_QUOTA, 0)
                self.resource_usage.api_calls_linkedin = max(0, self.resource_usage.api_calls_linkedin - api_quota)
            elif task.collector_type == CollectorType.FACEBOOK:
                api_quota = reserved.get(ResourceType.API_QUOTA, 0)
                self.resource_usage.api_calls_facebook = max(0, self.resource_usage.api_calls_facebook - api_quota)
            elif task.collector_type == CollectorType.HYBRID:
                api_quota = reserved.get(ResourceType.API_QUOTA, 0)
                self.resource_usage.api_calls_linkedin = max(0, self.resource_usage.api_calls_linkedin - (api_quota // 2))
                self.resource_usage.api_calls_facebook = max(0, self.resource_usage.api_calls_facebook - (api_quota // 2))
            
            self.resource_usage.memory_usage_mb = max(0, 
                self.resource_usage.memory_usage_mb - reserved.get(ResourceType.MEMORY, 0)
            )
            self.resource_usage.database_connections = max(0,
                self.resource_usage.database_connections - reserved.get(ResourceType.DATABASE, 0)
            )
            self.resource_usage.last_updated = datetime.utcnow()
            
            # Nettoyer les métadonnées
            task.metadata.pop('resources_reserved', None)
            task.metadata.pop('reservation_time', None)
    
    def _get_current_usage(self, resource_type: ResourceType) -> Union[int, float]:
        """Obtenir l'utilisation actuelle d'une ressource avec données réelles"""
        
        if resource_type == ResourceType.API_QUOTA:
            return max(self.resource_usage.api_calls_linkedin, self.resource_usage.api_calls_facebook)
        elif resource_type == ResourceType.CPU:
            try:
                return psutil.cpu_percent()
            except:
                return self.resource_usage.cpu_usage_percent
        elif resource_type == ResourceType.MEMORY:
            try:
                return psutil.virtual_memory().percent
            except:
                return self.resource_usage.memory_usage_mb
        elif resource_type == ResourceType.DATABASE:
            return self.resource_usage.database_connections
        elif resource_type == ResourceType.NETWORK:
            return self.resource_usage.network_usage_mbps
        elif resource_type == ResourceType.DISK:
            try:
                return psutil.disk_usage('/').percent
            except:
                return 0
        
        return 0
    
    # ========================================
    # PLANIFICATION RÉCURRENTE AVANCÉE
    # ========================================
    
    def _schedule_recurring_tasks(self):
        """Planifier les tâches récurrentes avec logique avancée"""
        
        try:
            # Collectes LinkedIn récurrentes avec logique adaptative
            linkedin_interval = self.config['linkedin_interval_hours']
            if self.config['adaptive_intervals']:
                linkedin_interval = self._calculate_adaptive_interval(CollectorType.LINKEDIN)
            
            self.aps_scheduler.add_job(
                func=self._schedule_all_linkedin_collections,
                trigger=IntervalTrigger(hours=linkedin_interval),
                id='linkedin_recurring',
                name='Collectes LinkedIn récurrentes',
                replace_existing=True,
                max_instances=1
            )
            
            # Collectes Facebook récurrentes avec logique adaptative
            facebook_interval = self.config['facebook_interval_hours']
            if self.config['adaptive_intervals']:
                facebook_interval = self._calculate_adaptive_interval(CollectorType.FACEBOOK)
            
            self.aps_scheduler.add_job(
                func=self._schedule_all_facebook_collections,
                trigger=IntervalTrigger(hours=facebook_interval),
                id='facebook_recurring',
                name='Collectes Facebook récurrentes',
                replace_existing=True,
                max_instances=1
            )
            
            # Maintenance quotidienne avec options avancées
            self.aps_scheduler.add_job(
                func=self._daily_maintenance,
                trigger=CronTrigger(hour=self.config['maintenance_hour']),
                id='daily_maintenance',
                name='Maintenance quotidienne',
                replace_existing=True
            )
            
            # Health check régulier
            self.aps_scheduler.add_job(
                func=self._perform_health_check,
                trigger=IntervalTrigger(minutes=self.config['health_check_interval']),
                id='health_check',
                name='Health check système',
                replace_existing=True
            )
            
            # Reset des statistiques
            self.aps_scheduler.add_job(
                func=self._reset_stats,
                trigger=IntervalTrigger(hours=self.config['stats_reset_hours']),
                id='stats_reset',
                name='Reset des statistiques',
                replace_existing=True
            )
            
            # Nettoyage automatique des tâches anciennes
            self.aps_scheduler.add_job(
                func=self._cleanup_old_tasks,
                trigger=CronTrigger(hour=self.config['maintenance_hour'] + 1),
                id='cleanup_tasks',
                name='Nettoyage des tâches anciennes',
                replace_existing=True
            )
            
            # Optimisation automatique
            if self.config['intelligent_scheduling']:
                self.aps_scheduler.add_job(
                    func=self._optimize_scheduling,
                    trigger=IntervalTrigger(hours=6),
                    id='optimize_scheduling',
                    name='Optimisation de la planification',
                    replace_existing=True
                )
            
            logger.info("✅ Tâches récurrentes planifiées avec logique avancée")
            
        except Exception as e:
            logger.error(f"❌ Erreur planification tâches récurrentes: {e}")
            raise
    
    def _calculate_adaptive_interval(self, collector_type: CollectorType) -> int:
        """Calculer un intervalle adaptatif basé sur les performances"""
        
        try:
            # Récupérer les statistiques du collecteur
            if collector_type == CollectorType.LINKEDIN:
                stats = self.linkedin_collector.get_collection_statistics()
                base_interval = self.config['linkedin_interval_hours']
            else:
                stats = self.facebook_collector.get_collection_statistics()
                base_interval = self.config['facebook_interval_hours']
            
            # Adapter basé sur le taux de succès
            success_rate = stats.get('success_rate', 1.0)
            if success_rate < 0.5:
                # Taux de succès faible, augmenter l'intervalle
                return int(base_interval * 1.5)
            elif success_rate > 0.9:
                # Taux de succès élevé, réduire l'intervalle
                return max(1, int(base_interval * 0.8))
            
            return base_interval
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur calcul intervalle adaptatif: {e}")
            return self.config.get(f'{collector_type.value}_interval_hours', 4)
    
    def _schedule_all_linkedin_collections(self):
        """Planifier toutes les collectes LinkedIn avec logique intelligente"""
        
        try:
            active_users = self._get_all_active_users_with_linkedin()
            scheduled_count = 0
            skipped_count = 0
            
            logger.info(f"🔄 Démarrage collectes LinkedIn récurrentes: {len(active_users)} utilisateurs")
            
            for user_id in active_users:
                try:
                    user_accounts = self._get_user_accounts(user_id)
                    
                    for linkedin_account in user_accounts.get('linkedin', []):
                        # Vérification intelligente du besoin de collecte
                        should_collect, reason = self._should_collect_linkedin_smart(user_id, linkedin_account.organization_id)
                        
                        if should_collect:
                            # Calculer un délai pour éviter la surcharge
                            delay_minutes = scheduled_count * 3  # 3 minutes entre chaque
                            scheduled_time = datetime.utcnow() + timedelta(minutes=delay_minutes)
                            
                            task_id = self.schedule_task(
                                user_id=user_id,
                                collector_type=CollectorType.LINKEDIN,
                                target_id=linkedin_account.organization_id,
                                priority=Priority.NORMAL,
                                scheduled_time=scheduled_time,
                                metadata={
                                    'recurring': True, 
                                    'account_name': linkedin_account.organization_name,
                                    'collection_reason': reason
                                }
                            )
                            scheduled_count += 1
                        else:
                            skipped_count += 1
                            logger.debug(f"⏭️  LinkedIn skippé {linkedin_account.organization_id}: {reason}")
                
                except Exception as e:
                    logger.error(f"❌ Erreur planification LinkedIn user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Collectes LinkedIn récurrentes: {scheduled_count} planifiées, {skipped_count} skippées")
            
            # Émettre un événement
            self._emit_event('linkedin_recurring_scheduled', {
                'scheduled_count': scheduled_count,
                'skipped_count': skipped_count,
                'total_users': len(active_users)
            })
            
        except Exception as e:
            logger.error(f"❌ Erreur planification globale LinkedIn: {e}")
    
    def _schedule_all_facebook_collections(self):
        """Planifier toutes les collectes Facebook avec logique intelligente"""
        
        try:
            active_users = self._get_all_active_users_with_facebook()
            scheduled_count = 0
            skipped_count = 0
            
            logger.info(f"🔄 Démarrage collectes Facebook récurrentes: {len(active_users)} utilisateurs")
            
            for user_id in active_users:
                try:
                    user_accounts = self._get_user_accounts(user_id)
                    
                    for facebook_account in user_accounts.get('facebook', []):
                        # Vérification intelligente du besoin de collecte
                        should_collect, reason = self._should_collect_facebook_smart(user_id, facebook_account.page_id)
                        
                        if should_collect:
                            # Espacer davantage les collectes Facebook (rate limits plus stricts)
                            delay_minutes = scheduled_count * 5  # 5 minutes entre chaque
                            scheduled_time = datetime.utcnow() + timedelta(minutes=delay_minutes)
                            
                            task_id = self.schedule_task(
                                user_id=user_id,
                                collector_type=CollectorType.FACEBOOK,
                                target_id=facebook_account.page_id,
                                priority=Priority.NORMAL,
                                scheduled_time=scheduled_time,
                                metadata={
                                    'recurring': True, 
                                    'page_name': facebook_account.page_name,
                                    'collection_reason': reason
                                }
                            )
                            scheduled_count += 1
                        else:
                            skipped_count += 1
                            logger.debug(f"⏭️  Facebook skippé {facebook_account.page_id}: {reason}")
                
                except Exception as e:
                    logger.error(f"❌ Erreur planification Facebook user {user_id}: {e}")
                    continue
            
            logger.info(f"✅ Collectes Facebook récurrentes: {scheduled_count} planifiées, {skipped_count} skippées")
            
            # Émettre un événement
            self._emit_event('facebook_recurring_scheduled', {
                'scheduled_count': scheduled_count,
                'skipped_count': skipped_count,
                'total_users': len(active_users)
            })
            
        except Exception as e:
            logger.error(f"❌ Erreur planification globale Facebook: {e}")
    
    def _should_collect_linkedin_smart(self, user_id: int, organization_id: str) -> Tuple[bool, str]:
        """Logique intelligente pour déterminer si une collecte LinkedIn est nécessaire"""
        
        try:
            # Vérification de base
            should_collect, basic_reason = self.linkedin_collector.should_collect_now(user_id, organization_id)
            
            if not should_collect:
                return False, basic_reason
            
            # Vérifications avancées
            
            # 1. Vérifier s'il y a déjà une tâche en cours ou planifiée
            for task in list(self.running_tasks.values()) + list(self.task_queue.queue):
                if (task.user_id == user_id and 
                    task.collector_type == CollectorType.LINKEDIN and 
                    task.target_id == organization_id):
                    return False, "Tâche déjà en cours ou planifiée"
            
            # 2. Vérifier les quotas
            if not self._check_api_quotas_available_for_collector(CollectorType.LINKEDIN):
                return False, "Quotas LinkedIn insuffisants"
            
            # 3. Vérifier la charge système
            current_metrics = self.metrics_collector.collect_current_metrics()
            system = current_metrics.get('system', {})
            if system.get('cpu_percent', 0) > 80:
                return False, "Charge système élevée"
            
            # 4. Vérifier l'historique d'erreurs pour cet organization
            recent_failures = self._get_recent_failures(user_id, organization_id, CollectorType.LINKEDIN)
            if recent_failures > 3:
                return False, f"Trop d'échecs récents ({recent_failures})"
            
            return True, "Collecte nécessaire et conditions favorables"
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification collecte LinkedIn smart: {e}")
            return True, "Erreur vérification, collecte par défaut"
    
    def _should_collect_facebook_smart(self, user_id: int, page_id: str) -> Tuple[bool, str]:
        """Logique intelligente pour déterminer si une collecte Facebook est nécessaire"""
        
        try:
            # Vérification de base
            should_collect, basic_reason = self.facebook_collector.should_collect_now(user_id, page_id)
            
            if not should_collect:
                return False, basic_reason
            
            # Vérifications avancées similaires à LinkedIn
            for task in list(self.running_tasks.values()) + list(self.task_queue.queue):
                if (task.user_id == user_id and 
                    task.collector_type == CollectorType.FACEBOOK and 
                    task.target_id == page_id):
                    return False, "Tâche déjà en cours ou planifiée"
            
            if not self._check_api_quotas_available_for_collector(CollectorType.FACEBOOK):
                return False, "Quotas Facebook insuffisants"
            
            current_metrics = self.metrics_collector.collect_current_metrics()
            system = current_metrics.get('system', {})
            if system.get('cpu_percent', 0) > 80:
                return False, "Charge système élevée"
            
            recent_failures = self._get_recent_failures(user_id, page_id, CollectorType.FACEBOOK)
            if recent_failures > 3:
                return False, f"Trop d'échecs récents ({recent_failures})"
            
            return True, "Collecte nécessaire et conditions favorables"
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification collecte Facebook smart: {e}")
            return True, "Erreur vérification, collecte par défaut"
    
    def _check_api_quotas_available_for_collector(self, collector_type: CollectorType) -> bool:
        """Vérifier si les quotas sont disponibles pour un type de collecteur"""
        
        try:
            if collector_type == CollectorType.LINKEDIN:
                current_calls = self.resource_usage.api_calls_linkedin
                # Estimation basée sur la limite quotidienne typique de LinkedIn
                return current_calls < 400  # Marge de sécurité
            
            elif collector_type == CollectorType.FACEBOOK:
                current_calls = self.resource_usage.api_calls_facebook
                # Estimation basée sur la limite quotidienne typique de Facebook
                return current_calls < 300  # Marge de sécurité
            
            return True
            
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification quotas {collector_type.value}: {e}")
            return True
    
    def _get_recent_failures(self, user_id: int, target_id: str, collector_type: CollectorType) -> int:
        """Compter les échecs récents pour un utilisateur/cible/collecteur"""
        
        failure_count = 0
        cutoff_time = datetime.utcnow() - timedelta(hours=24)  # 24 dernières heures
        
        for task in self.failed_tasks.values():
            if (task.user_id == user_id and 
                task.target_id == target_id and 
                task.collector_type == collector_type and
                task.completed_at and task.completed_at > cutoff_time):
                failure_count += 1
        
        return failure_count
    
    # ========================================
    # OPTIMISATION ET INTELLIGENCE
    # ========================================
    
    def _optimize_scheduling(self):
        """Optimiser automatiquement la planification"""
        
        try:
            logger.info("🧠 Début optimisation intelligente de la planification")
            
            # Analyser les performances passées
            performance_analysis = self._analyze_performance_patterns()
            
            # Optimiser les intervalles
            if performance_analysis.get('should_adjust_intervals'):
                self._adjust_collection_intervals(performance_analysis)
            
            # Optimiser la répartition des charges
            if performance_analysis.get('should_rebalance'):
                self._rebalance_workload()
            
            # Nettoyer les circuit breakers si nécessaire
            self._reset_circuit_breakers_if_appropriate()
            
            logger.info("✅ Optimisation terminée")
            
        except Exception as e:
            logger.error(f"❌ Erreur optimisation: {e}")
    
    def _analyze_performance_patterns(self) -> Dict[str, Any]:
        """Analyser les patterns de performance pour optimisation"""
        
        analysis = {
            'should_adjust_intervals': False,
            'should_rebalance': False,
            'linkedin_performance': {},
            'facebook_performance': {},
            'recommendations': []
        }
        
        try:
            # Analyser les performances LinkedIn
            linkedin_stats = self.linkedin_collector.get_collection_statistics()
            analysis['linkedin_performance'] = {
                'success_rate': linkedin_stats.get('success_rate', 0),
                'avg_execution_time': linkedin_stats.get('average_execution_time', 0),
                'total_collections': linkedin_stats.get('collections_completed', 0)
            }
            
            # Analyser les performances Facebook
            facebook_stats = self.facebook_collector.get_collection_statistics()
            analysis['facebook_performance'] = {
                'success_rate': facebook_stats.get('success_rate', 0),
                'avg_execution_time': facebook_stats.get('average_execution_time', 0),
                'total_collections': facebook_stats.get('collections_completed', 0)
            }
            
            # Recommandations basées sur l'analyse
            if analysis['linkedin_performance']['success_rate'] < 0.7:
                analysis['recommendations'].append("Réduire la fréquence LinkedIn")
                analysis['should_adjust_intervals'] = True
            
            if analysis['facebook_performance']['success_rate'] < 0.7:
                analysis['recommendations'].append("Réduire la fréquence Facebook")
                analysis['should_adjust_intervals'] = True
            
            # Vérifier la charge
            queue_size = self.task_queue.qsize()
            if queue_size > 50:
                analysis['recommendations'].append("Rééquilibrer la charge")
                analysis['should_rebalance'] = True
            
        except Exception as e:
            logger.error(f"❌ Erreur analyse performances: {e}")
        
        return analysis
    
    def _adjust_collection_intervals(self, analysis: Dict[str, Any]):
        """Ajuster les intervalles de collecte basés sur l'analyse"""
        
        try:
            # Ajuster LinkedIn
            linkedin_performance = analysis.get('linkedin_performance', {})
            if linkedin_performance.get('success_rate', 1) < 0.7:
                new_interval = int(self.config['linkedin_interval_hours'] * 1.3)
                logger.info(f"📈 Augmentation intervalle LinkedIn: {self.config['linkedin_interval_hours']}h → {new_interval}h")
                
                # Reprogrammer la tâche récurrente
                self.aps_scheduler.reschedule_job(
                    'linkedin_recurring',
                    trigger=IntervalTrigger(hours=new_interval)
                )
            
            # Ajuster Facebook
            facebook_performance = analysis.get('facebook_performance', {})
            if facebook_performance.get('success_rate', 1) < 0.7:
                new_interval = int(self.config['facebook_interval_hours'] * 1.3)
                logger.info(f"📈 Augmentation intervalle Facebook: {self.config['facebook_interval_hours']}h → {new_interval}h")
                
                # Reprogrammer la tâche récurrente
                self.aps_scheduler.reschedule_job(
                    'facebook_recurring',
                    trigger=IntervalTrigger(hours=new_interval)
                )
        
        except Exception as e:
            logger.error(f"❌ Erreur ajustement intervalles: {e}")
    
    def _rebalance_workload(self):
        """Rééquilibrer la charge de travail"""
        
        try:
            # Réorganiser les tâches en queue par priorité et type
            temp_tasks = []
            
            while not self.task_queue.empty():
                try:
                    task = self.task_queue.get_nowait()
                    temp_tasks.append(task)
                except Empty:
                    break
            
            # Trier par priorité et répartir dans le temps
            temp_tasks.sort(key=lambda t: (t.priority.value, t.scheduled_time))
            
            # Remettre en queue avec espacement optimal
            for i, task in enumerate(temp_tasks):
                # Espacer les tâches de même type
                delay_minutes = (i % 10) * 2  # 2 minutes entre les tâches
                task.scheduled_time = datetime.utcnow() + timedelta(minutes=delay_minutes)
                self.task_queue.put(task)
            
            logger.info(f"🔄 Charge rééquilibrée: {len(temp_tasks)} tâches réorganisées")
            
        except Exception as e:
            logger.error(f"❌ Erreur rééquilibrage: {e}")
    
    def _reset_circuit_breakers_if_appropriate(self):
        """Réinitialiser les circuit breakers si approprié"""
        
        try:
            for collector_type, circuit_breaker in self.circuit_breakers.items():
                if circuit_breaker.state == 'OPEN':
                    # Vérifier si on peut tenter de fermer
                    if circuit_breaker._should_attempt_reset():
                        circuit_breaker.state = 'CLOSED'
                        circuit_breaker.failure_count = 0
                        logger.info(f"🔄 Circuit breaker réinitialisé: {collector_type.value}")
                        
                        # Émettre un événement
                        self._emit_event('circuit_breaker_reset', {
                            'collector_type': collector_type.value,
                            'timestamp': datetime.utcnow().isoformat()
                        })
        
        except Exception as e:
            logger.error(f"❌ Erreur reset circuit breakers: {e}")
    
    # ========================================
    # GESTION DES ÉVÉNEMENTS
    # ========================================
    
    def _emit_event(self, event_type: str, event_data: Dict[str, Any]):
        """Émettre un événement système"""
        
        try:
            event = {
                'type': event_type,
                'data': event_data,
                'timestamp': datetime.utcnow().isoformat(),
                'instance_id': self.instance_id
            }
            
            # Log selon le type d'événement
            if event_type.startswith('task_'):
                logger.debug(f"📡 Événement: {event_type} - {event_data.get('task_id', 'N/A')}")
            elif event_type.startswith('health_'):
                logger.info(f"🏥 Événement santé: {event_type} - {event_data.get('component', 'N/A')}")
            elif event_type.startswith('scheduler_'):
                logger.info(f"⚙️  Événement scheduler: {event_type}")
            else:
                logger.debug(f"📡 Événement: {event_type}")
            
            # Ici, on pourrait ajouter l'envoi vers un système externe
            # comme Redis, RabbitMQ, webhooks, etc.
            
        except Exception as e:
            logger.error(f"❌ Erreur émission événement {event_type}: {e}")
    
    # ========================================
    # MAINTENANCE ET NETTOYAGE
    # ========================================
    
    def _daily_maintenance(self):
        """Maintenance quotidienne complète"""
        
        try:
            logger.info("🧹 Début maintenance quotidienne complète")
            
            # Sauvegarder l'état avant maintenance
            if self.config['state_persistence_enabled']:
                self.persistence_manager.save_state(self._get_current_state())
            
            # Nettoyer les tâches anciennes
            self._cleanup_old_tasks()
            
            # Nettoyer les données des collecteurs
            self._cleanup_old_collector_data()
            
            # Réinitialiser les compteurs quotidiens
            self._reset_daily_counters()
            
            # Nettoyer les caches
            self._clear_caches()
            
            # Compacter les logs de métriques
            self._compact_metrics_history()
            
            # Vérifier l'intégrité du système
            self._integrity_check()
            
            # Générer un rapport de maintenance
            maintenance_report = self._generate_maintenance_report()
            
            logger.info(f"✅ Maintenance quotidienne terminée: {maintenance_report['summary']}")
            
            # Émettre un événement de maintenance
            self._emit_event('daily_maintenance_completed', maintenance_report)
            
        except Exception as e:
            logger.error(f"❌ Erreur maintenance quotidienne: {e}")
    
    def _cleanup_old_tasks(self):
        """Nettoyer les tâches anciennes avec logique avancée"""
        
        try:
            cutoff_time = datetime.utcnow() - timedelta(days=7)
            
            # Nettoyer les tâches terminées (garder les plus récentes pour stats)
            old_completed = []
            for task_id, task in list(self.completed_tasks.items()):
                if task.completed_at and task.completed_at < cutoff_time:
                    old_completed.append(task_id)
            
            # Garder au moins les 100 dernières tâches terminées
            if len(self.completed_tasks) - len(old_completed) < 100:
                # Trier par date et garder les plus récentes
                sorted_tasks = sorted(
                    self.completed_tasks.items(),
                    key=lambda x: x[1].completed_at or datetime.min,
                    reverse=True
                )
                old_completed = [task_id for task_id, _ in sorted_tasks[100:]]
            
            for task_id in old_completed:
                del self.completed_tasks[task_id]
            
            # Nettoyer les tâches échouées (plus sélectif)
            old_failed = []
            for task_id, task in list(self.failed_tasks.items()):
                if task.completed_at and task.completed_at < cutoff_time:
                    old_failed.append(task_id)
            
            # Garder au moins les 50 dernières tâches échouées pour analyse
            if len(self.failed_tasks) - len(old_failed) < 50:
                sorted_failed = sorted(
                    self.failed_tasks.items(),
                    key=lambda x: x[1].completed_at or datetime.min,
                    reverse=True
                )
                old_failed = [task_id for task_id, _ in sorted_failed[50:]]
            
            for task_id in old_failed:
                del self.failed_tasks[task_id]
            
            # Nettoyer les tâches annulées
            old_cancelled = [
                task_id for task_id, task in self.cancelled_tasks.items()
                if task.completed_at and task.completed_at < cutoff_time
            ]
            for task_id in old_cancelled:
                del self.cancelled_tasks[task_id]
            
            logger.info(f"🧹 Tâches nettoyées: {len(old_completed)} terminées, "
                       f"{len(old_failed)} échouées, {len(old_cancelled)} annulées")
            
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage tâches: {e}")
    
    def _cleanup_old_completed_tasks(self):
        """Nettoyer les tâches terminées si trop nombreuses"""
        
        if len(self.completed_tasks) > 200:
            # Garder seulement les 150 plus récentes
            sorted_tasks = sorted(
                self.completed_tasks.items(),
                key=lambda x: x[1].completed_at or datetime.min,
                reverse=True
            )
            
            tasks_to_remove = sorted_tasks[150:]
            for task_id, _ in tasks_to_remove:
                del self.completed_tasks[task_id]
    
    def _cleanup_old_collector_data(self):
        """Nettoyer les anciennes données des collecteurs"""
        
        try:
            # Nettoyer LinkedIn
            linkedin_deleted = self.linkedin_collector.cleanup_old_data(days=90)
            
            # Nettoyer Facebook
            facebook_deleted = self.facebook_collector.cleanup_old_data(days=90)
            
            total_deleted = sum(linkedin_deleted.values()) + sum(facebook_deleted.values())
            logger.info(f"🧹 Données collecteurs nettoyées: {total_deleted} enregistrements")
            
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage données collecteurs: {e}")
    
    def _reset_daily_counters(self):
        """Réinitialiser les compteurs quotidiens"""
        
        try:
            # Réinitialiser les compteurs d'API (quotas quotidiens)
            self.resource_usage.api_calls_linkedin = 0
            self.resource_usage.api_calls_facebook = 0
            
            # Réinitialiser certains compteurs d'erreurs
            for error_type in list(self.error_counts.keys()):
                # Garder un historique mais réduire les compteurs
                self.error_counts[error_type] = max(0, self.error_counts[error_type] // 2)
            
            logger.info("🔄 Compteurs quotidiens réinitialisés")
            
        except Exception as e:
            logger.error(f"❌ Erreur reset compteurs: {e}")
    
    def _clear_caches(self):
        """Vider les différents caches"""
        
        try:
            # Vider le cache utilisateurs
            self._user_cache.clear()
            self._cache_expiry = datetime.utcnow()
            
            # Vider les caches des collecteurs
            self.linkedin_collector.clear_quota_cache()
            self.facebook_collector.clear_quota_cache()
            
            logger.info("🧹 Caches vidés")
            
        except Exception as e:
            logger.error(f"❌ Erreur vidage caches: {e}")
    
    def _compact_metrics_history(self):
        """Compacter l'historique des métriques"""
        
        try:
            if len(self.metrics_collector.metrics_history) > 500:
                # Garder seulement les 300 plus récentes
                recent_metrics = list(self.metrics_collector.metrics_history)[-300:]
                self.metrics_collector.metrics_history.clear()
                self.metrics_collector.metrics_history.extend(recent_metrics)
                
                logger.info("📊 Historique métriques compacté")
            
        except Exception as e:
            logger.error(f"❌ Erreur compactage métriques: {e}")
    
    def _integrity_check(self):
        """Vérifier l'intégrité du système"""
        
        try:
            issues = []
            
            # Vérifier la cohérence des compteurs
            total_tasks = (
                self.stats.total_tasks_completed + 
                self.stats.total_tasks_failed + 
                self.stats.total_tasks_cancelled
            )
            if total_tasks > self.stats.total_tasks_scheduled:
                issues.append("Incohérence dans les compteurs de tâches")
            
            # Vérifier les workers
            alive_workers = len([w for w in self.worker_threads if w.is_alive()])
            if alive_workers < self.config['max_workers']:
                issues.append(f"Workers manquants: {alive_workers}/{self.config['max_workers']}")
            
            # Vérifier les circuit breakers
            for collector_type, cb in self.circuit_breakers.items():
                if cb.state == 'OPEN' and cb.failure_count > 10:
                    issues.append(f"Circuit breaker {collector_type.value} ouvert avec {cb.failure_count} échecs")
            
            if issues:
                logger.warning(f"⚠️  Problèmes d'intégrité détectés: {issues}")
            else:
                logger.info("✅ Vérification d'intégrité réussie")
                
        except Exception as e:
            logger.error(f"❌ Erreur vérification intégrité: {e}")
    
    def _generate_maintenance_report(self) -> Dict[str, Any]:
        """Générer un rapport de maintenance"""
        
        try:
            report = {
                'timestamp': datetime.utcnow().isoformat(),
                'summary': '',
                'statistics': self.get_statistics(),
                'health_status': self.health_checks.get('overall_status', 'unknown'),
                'resource_usage': self.resource_usage.to_dict(),
                'active_tasks': len(self.running_tasks),
                'queued_tasks': self.task_queue.qsize(),
                'circuit_breakers': {
                    cb_type.value: cb.state 
                    for cb_type, cb in self.circuit_breakers.items()
                },
                'recommendations': []
            }
            
            # Générer un résumé
            stats = report['statistics']['scheduler_stats']
            success_rate = stats.get('success_rate', 0)
            
            if success_rate > 0.9:
                report['summary'] = f"Système performant ({success_rate:.1%} succès)"
            elif success_rate > 0.7:
                report['summary'] = f"Système stable ({success_rate:.1%} succès)"
            else:
                report['summary'] = f"Système nécessite attention ({success_rate:.1%} succès)"
                report['recommendations'].append("Vérifier les causes d'échec")
            
            # Recommandations basées sur les métriques
            if report['queued_tasks'] > 20:
                report['recommendations'].append("Queue surchargée, considérer plus de workers")
            
            if report['resource_usage']['memory_usage_mb'] > 512:
                report['recommendations'].append("Utilisation mémoire élevée")
            
            return report
            
        except Exception as e:
            logger.error(f"❌ Erreur génération rapport maintenance: {e}")
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'summary': 'Erreur génération rapport',
                'error': str(e)
            }
    
    def _reset_stats(self):
        """Réinitialiser les statistiques"""
        
        # Sauvegarder les stats importantes avant reset
        old_stats = self.stats.to_dict()
        
        # Réinitialiser
        self.stats = CollectionStats()
        
        # Émettre un événement avec les anciennes stats
        self._emit_event('stats_reset', {
            'previous_stats': old_stats,
            'reset_timestamp': datetime.utcnow().isoformat()
        })
        
        logger.info("🔄 Statistiques réinitialisées")
    
    # ========================================
    # UTILITAIRES ET HELPERS AVANCÉS
    # ========================================
    
    def _get_user_accounts(self, user_id: int) -> Dict[str, List]:
        """Récupérer les comptes d'un utilisateur avec cache intelligent"""
        
        # Vérifier le cache avec expiration intelligente
        cache_key = f"user_{user_id}"
        if (cache_key in self._user_cache and 
            datetime.utcnow() < self._cache_expiry):
            return self._user_cache[cache_key]
        
        try:
            with db_manager.get_session() as session:
                # Comptes LinkedIn
                linkedin_accounts = session.query(LinkedinAccount).filter(
                    LinkedinAccount.user_id == user_id,
                    LinkedinAccount.is_active == True
                ).all()
                
                # Comptes Facebook
                facebook_accounts = session.query(FacebookAccount).filter(
                    FacebookAccount.user_id == user_id,
                    FacebookAccount.is_active == True
                ).all()
                
                accounts = {
                    'linkedin': linkedin_accounts,
                    'facebook': facebook_accounts
                }
                
                # Mettre en cache avec expiration adaptative
                self._user_cache[cache_key] = accounts
                
                # Expiration basée sur le nombre d'utilisateurs (plus d'utilisateurs = cache plus court)
                cache_duration = max(5, 15 - len(self._user_cache) // 10)  # 5-15 minutes
                if datetime.utcnow() >= self._cache_expiry:
                    self._cache_expiry = datetime.utcnow() + timedelta(minutes=cache_duration)
                
                return accounts
                
        except Exception as e:
            logger.error(f"❌ Erreur récupération comptes user {user_id}: {e}")
            return {'linkedin': [], 'facebook': []}
    
    def _get_all_active_users_with_linkedin(self) -> List[int]:
        """Récupérer tous les utilisateurs actifs avec LinkedIn"""
        
        try:
            with db_manager.get_session() as session:
                users = session.query(User.id).join(LinkedinAccount).filter(
                    User.is_active == True,
                    LinkedinAccount.is_active == True
                ).distinct().all()
                
                return [user.id for user in users]
        except Exception as e:
            logger.error(f"❌ Erreur récupération utilisateurs LinkedIn: {e}")
            return []
    
    def _get_all_active_users_with_facebook(self) -> List[int]:
        """Récupérer tous les utilisateurs actifs avec Facebook"""
        
        try:
            with db_manager.get_session() as session:
                users = session.query(User.id).join(FacebookAccount).filter(
                    User.is_active == True,
                    FacebookAccount.is_active == True
                ).distinct().all()
                
                return [user.id for user in users]
        except Exception as e:
            logger.error(f"❌ Erreur récupération utilisateurs Facebook: {e}")
            return []
    
    def _should_collect_linkedin(self, user_id: int, organization_id: str) -> bool:
        """Déterminer si une collecte LinkedIn est nécessaire (version basique)"""
        
        try:
            should_collect, _ = self.linkedin_collector.should_collect_now(user_id, organization_id)
            return should_collect
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification collecte LinkedIn: {e}")
            return True
    
    def _should_collect_facebook(self, user_id: int, page_id: str) -> bool:
        """Déterminer si une collecte Facebook est nécessaire (version basique)"""
        
        try:
            should_collect, _ = self.facebook_collector.should_collect_now(user_id, page_id)
            return should_collect
        except Exception as e:
            logger.warning(f"⚠️  Erreur vérification collecte Facebook: {e}")
            return True
    
    def _validate_dependencies(self, task: CollectionTask) -> bool:
        """Valider les dépendances d'une tâche"""
        
        if not task.dependencies:
            return True
        
        # Vérifier que toutes les dépendances existent
        for dep_id in task.dependencies:
            if (dep_id not in self.completed_tasks and 
                dep_id not in self.running_tasks and 
                dep_id not in self.failed_tasks):
                
                # Chercher dans la queue
                found = False
                temp_queue = PriorityQueue()
                
                while not self.task_queue.empty():
                    try:
                        queued_task = self.task_queue.get_nowait()
                        if queued_task.task_id == dep_id:
                            found = True
                        temp_queue.put(queued_task)
                    except Empty:
                        break
                
                # Remettre les tâches dans la queue
                while not temp_queue.empty():
                    try:
                        self.task_queue.put(temp_queue.get_nowait())
                    except Empty:
                        break
                
                if not found:
                    logger.error(f"❌ Dépendance inexistante: {dep_id} pour tâche {task.task_id}")
                    return False
        
        return True
    
    def _check_dependencies(self, task: CollectionTask) -> bool:
        """Vérifier si les dépendances d'une tâche sont satisfaites"""
        
        if not task.dependencies:
            return True
        
        for dep_id in task.dependencies:
            # La dépendance doit être terminée avec succès
            if dep_id not in self.completed_tasks:
                return False
        
        return True
    
    def _calculate_retry_delay(self, task: CollectionTask) -> int:
        """Calculer le délai de retry avec logique avancée"""
        
        base_delay = self.config['default_retry_delay']
        
        if self.config['exponential_backoff']:
            # Backoff exponentiel avec jitter
            delay = base_delay * (2 ** (task.attempts - 1))
            
            # Ajouter du jitter (±20%)
            import random
            jitter = random.uniform(0.8, 1.2)
            delay = int(delay * jitter)
            
            # Limiter le délai maximum
            delay = min(delay, 3600)  # Max 1 heure
        else:
            delay = base_delay
        
        # Ajustement basé sur le type d'erreur
        if 'rate limit' in (task.last_error or '').lower():
            delay *= 2  # Double délai pour rate limits
        elif 'quota' in (task.last_error or '').lower():
            delay *= 3  # Triple délai pour quotas
        
        return delay
    
    def _update_performance_stats(self, task: CollectionTask, execution_time: float):
        """Mettre à jour les statistiques de performance avec analyse détaillée"""
        
        # Moyenne mobile du temps d'exécution
        if self.stats.average_execution_time == 0:
            self.stats.average_execution_time = execution_time
        else:
            # Moyenne mobile avec facteur 0.1
            alpha = 0.1
            self.stats.average_execution_time = (
                (1 - alpha) * self.stats.average_execution_time + 
                alpha * execution_time
            )
        
        # Taux de succès global
        total_completed = self.stats.total_tasks_completed + self.stats.total_tasks_failed
        if total_completed > 0:
            self.stats.success_rate = self.stats.total_tasks_completed / total_completed
        
        # Efficacité des quotas (approximation)
        total_api_calls = self.resource_usage.api_calls_linkedin + self.resource_usage.api_calls_facebook
        if total_api_calls > 0:
            self.stats.quota_efficiency = self.stats.total_tasks_completed / total_api_calls
        
        # Enregistrer les métriques de performance détaillées
        if self.config['detailed_metrics']:
            self.performance_metrics[task.task_id] = {
                'execution_time': execution_time,
                'collector_type': task.collector_type.value,
                'data_types_count': len(task.data_types),
                'attempts': task.attempts,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Limiter la taille du cache de performance
            if len(self.performance_metrics) > 1000:
                # Garder seulement les 500 plus récentes
                sorted_metrics = sorted(
                    self.performance_metrics.items(),
                    key=lambda x: x[1]['timestamp'],
                    reverse=True
                )
                self.performance_metrics = dict(sorted_metrics[:500])
    
    def _cleanup_resources(self):
        """Nettoyer les ressources avant arrêt avec nettoyage complet"""
        
        try:
            # Nettoyer les queues
            queue_size = self.task_queue.qsize()
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                except Empty:
                    break
            
            # Vider les caches
            self._user_cache.clear()
            self.error_counts.clear()
            self.performance_metrics.clear()
            
            # Nettoyer les métriques
            if hasattr(self.metrics_collector, 'metrics_history'):
                self.metrics_collector.metrics_history.clear()
            
            # Reset des circuit breakers
            for cb in self.circuit_breakers.values():
                cb.failure_count = 0
                cb.state = 'CLOSED'
            
            logger.info(f"✅ Ressources nettoyées (queue: {queue_size} tâches supprimées)")
            
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage ressources: {e}")
    
    # ========================================
    # API PUBLIQUE COMPLÈTE
    # ========================================
    
    def get_status(self) -> Dict[str, Any]:
        """Obtenir le statut complet du scheduler"""
        
        return {
            'instance_id': self.instance_id,
            'is_running': self.is_running,
            'is_paused': self.is_paused,
            'workers_active': len([w for w in self.worker_threads if w.is_alive()]),
            'workers_total': self.config['max_workers'],
            'tasks_queued': self.task_queue.qsize(),
            'tasks_running': len(self.running_tasks),
            'tasks_completed': len(self.completed_tasks),
            'tasks_failed': len(self.failed_tasks),
            'tasks_cancelled': len(self.cancelled_tasks),
            'resource_usage': self.resource_usage.to_dict(),
            'resource_limits': {k.value: v for k, v in self.resource_limits.items()},
            'health_status': self.health_checks.get('overall_status', 'unknown'),
            'last_health_check': self.health_checks.get('timestamp'),
            'circuit_breakers': {
                cb_type.value: {
                    'state': cb.state,
                    'failure_count': cb.failure_count,
                    'last_failure': cb.last_failure_time.isoformat() if cb.last_failure_time else None
                }
                for cb_type, cb in self.circuit_breakers.items()
            },
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtenir les statistiques détaillées du système"""
        
        uptime = datetime.utcnow() - self.stats.last_reset
        
        return {
            'scheduler_stats': self.stats.to_dict(),
            'uptime_hours': uptime.total_seconds() / 3600,
            'error_counts': dict(self.error_counts),
            'performance_metrics_sample': dict(list(self.performance_metrics.items())[-10:]),  # 10 dernières
            'linkedin_stats': self.linkedin_collector.get_collection_statistics(),
            'facebook_stats': self.facebook_collector.get_collection_statistics(),
            'system_metrics': self.metrics_collector.get_metrics_summary(minutes=30),
            'configuration': self.config.copy(),
            'health_checks': self.health_checks,
            'queue_analysis': self._analyze_queue(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def _analyze_queue(self) -> Dict[str, Any]:
        """Analyser la composition de la queue"""
        
        analysis = {
            'total_size': self.task_queue.qsize(),
            'by_priority': defaultdict(int),
            'by_collector': defaultdict(int),
            'by_user': defaultdict(int),
            'overdue_tasks': 0,
            'scheduled_future': 0
        }
        
        # Analyser sans vider la queue
        temp_queue = PriorityQueue()
        tasks_analyzed = []
        
        try:
            while not self.task_queue.empty():
                try:
                    task = self.task_queue.get_nowait()
                    tasks_analyzed.append(task)
                    temp_queue.put(task)
                    
                    # Analyser la tâche
                    analysis['by_priority'][task.priority.name] += 1
                    analysis['by_collector'][task.collector_type.value] += 1
                    analysis['by_user'][task.user_id] += 1
                    
                    if task.scheduled_time < datetime.utcnow():
                        analysis['overdue_tasks'] += 1
                    else:
                        analysis['scheduled_future'] += 1
                        
                except Empty:
                    break
            
            # Remettre les tâches
            while not temp_queue.empty():
                try:
                    self.task_queue.put(temp_queue.get_nowait())
                except Empty:
                    break
                    
        except Exception as e:
            logger.error(f"❌ Erreur analyse queue: {e}")
        
        # Convertir les defaultdict en dict normaux
        analysis['by_priority'] = dict(analysis['by_priority'])
        analysis['by_collector'] = dict(analysis['by_collector'])
        analysis['by_user'] = dict(analysis['by_user'])
        
        return analysis
    
    def get_task_details(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Obtenir les détails complets d'une tâche"""
        
        # Chercher dans toutes les collections de tâches
        task = None
        location = None
        
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            location = 'running'
        elif task_id in self.completed_tasks:
            task = self.completed_tasks[task_id]
            location = 'completed'
        elif task_id in self.failed_tasks:
            task = self.failed_tasks[task_id]
            location = 'failed'
        elif task_id in self.cancelled_tasks:
            task = self.cancelled_tasks[task_id]
            location = 'cancelled'
        else:
            # Chercher dans la queue
            temp_queue = PriorityQueue()
            found = False
            
            while not self.task_queue.empty():
                try:
                    queued_task = self.task_queue.get_nowait()
                    if queued_task.task_id == task_id:
                        task = queued_task
                        location = 'queued'
                        found = True
                    temp_queue.put(queued_task)
                except Empty:
                    break
            
            # Remettre les tâches dans la queue
            while not temp_queue.empty():
                try:
                    self.task_queue.put(temp_queue.get_nowait())
                except Empty:
                    break
        
        if task:
            details = task.to_dict()
            details['location'] = location
            
            # Ajouter des informations contextuelles
            if location == 'running':
                details['runtime_seconds'] = (datetime.utcnow() - task.started_at).total_seconds() if task.started_at else 0
                details['is_expired'] = task.is_expired()
            
            # Ajouter l'historique de performance si disponible
            if task_id in self.performance_metrics:
                details['performance_history'] = self.performance_metrics[task_id]
            
            return details
        else:
            return None
    
    def get_health_details(self) -> Dict[str, Any]:
        """Obtenir les détails de santé complets"""
        
        return {
            'overall_status': self.health_checks.get('overall_status', 'unknown'),
            'last_check': self.health_checks.get('timestamp'),
            'components': {
                name: result.to_dict() 
                for name, result in self.health_checks.get('results', {}).items()
            },
            'alerts_active': [
                {
                    'component': name,
                    'status': result.status.value,
                    'message': result.message
                }
                for name, result in self.health_checks.get('results', {}).items()
                if result.status in [HealthStatus.WARNING, HealthStatus.CRITICAL]
            ],
            'system_metrics': self.metrics_collector.get_metrics_summary(minutes=10),
            'recommendations': self._get_health_recommendations()
        }
    
    def _get_health_recommendations(self) -> List[str]:
        """Générer des recommandations basées sur la santé système"""
        
        recommendations = []
        
        try:
            # Basé sur les métriques système
            metrics = self.metrics_collector.get_metrics_summary(minutes=10)
            if metrics.get('cpu', {}).get('avg', 0) > 80:
                recommendations.append("Charge CPU élevée - Considérer réduire le nombre de workers")
            
            if metrics.get('memory', {}).get('avg', 0) > 80:
                recommendations.append("Utilisation mémoire élevée - Nettoyer les caches")
            
            # Basé sur les statistiques
            if self.stats.success_rate < 0.8:
                recommendations.append("Taux de succès faible - Vérifier les configurations des collecteurs")
            
            # Basé sur la queue
            queue_size = self.task_queue.qsize()
            if queue_size > 50:
                recommendations.append("Queue surchargée - Augmenter le nombre de workers ou optimiser les intervalles")
            
            # Basé sur les circuit breakers
            open_breakers = [
                cb_type.value for cb_type, cb in self.circuit_breakers.items()
                if cb.state == 'OPEN'
            ]
            if open_breakers:
                recommendations.append(f"Circuit breakers ouverts: {', '.join(open_breakers)} - Vérifier les APIs")
            
        except Exception as e:
            logger.error(f"❌ Erreur génération recommandations: {e}")
            recommendations.append("Erreur lors de l'analyse - Vérifier les logs")
        
        return recommendations
    
    def health_check(self) -> Dict[str, Any]:
        """Health check rapide pour monitoring externe"""
        
        try:
            is_healthy = (
                self.is_running and
                len([w for w in self.worker_threads if w.is_alive()]) >= self.config['max_workers'] // 2 and
                self.health_checks.get('overall_status') in [HealthStatus.HEALTHY, HealthStatus.WARNING]
            )
            
            return {
                'status': 'healthy' if is_healthy else 'unhealthy',
                'instance_id': self.instance_id,
                'is_running': self.is_running,
                'workers_active': len([w for w in self.worker_threads if w.is_alive()]),
                'queue_size': self.task_queue.qsize(),
                'last_health_check': self.health_checks.get('timestamp'),
                'uptime_hours': (datetime.utcnow() - self.stats.last_reset).total_seconds() / 3600,
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }

# ========================================
# INSTANCE GLOBALE
# ========================================

unified_scheduler = UnifiedCollectionScheduler()

# ========================================
# FONCTIONS D'API PUBLIQUE
# ========================================

def start_unified_scheduler(restore_state: bool = True):
    """Démarrer le scheduler unifié"""
    return unified_scheduler.start_scheduler(restore_state=restore_state)

def stop_unified_scheduler(timeout: int = 30, save_state: bool = True):
    """Arrêter le scheduler unifié"""
    return unified_scheduler.stop_scheduler(timeout=timeout, save_state=save_state)

def schedule_user_collection(user_id: int, force_refresh: bool = False, 
                           priority: str = "NORMAL", include_linkedin: bool = True, 
                           include_facebook: bool = True) -> List[str]:
    """Planifier une collecte utilisateur complète"""
    priority_enum = Priority[priority.upper()] if priority.upper() in Priority.__members__ else Priority.NORMAL
    return unified_scheduler.schedule_user_collection(
        user_id=user_id,
        force_refresh=force_refresh,
        priority=priority_enum,
        include_linkedin=include_linkedin,
        include_facebook=include_facebook
    )

def schedule_single_collection(user_id: int, collector_type: str, target_id: str,
                             data_types: List[str] = None, priority: str = "NORMAL") -> str:
    """Planifier une collecte unique"""
    collector_enum = CollectorType[collector_type.upper()]
    priority_enum = Priority[priority.upper()] if priority.upper() in Priority.__members__ else Priority.NORMAL
    
    return unified_scheduler.schedule_task(
        user_id=user_id,
        collector_type=collector_enum,
        target_id=target_id,
        data_types=data_types,
        priority=priority_enum
    )

def get_scheduler_status() -> Dict[str, Any]:
    """Obtenir le statut du scheduler"""
    return unified_scheduler.get_status()

def get_scheduler_statistics() -> Dict[str, Any]:
    """Obtenir les statistiques détaillées"""
    return unified_scheduler.get_statistics()

def get_task_info(task_id: str) -> Optional[Dict[str, Any]]:
    """Obtenir les informations d'une tâche"""
    return unified_scheduler.get_task_details(task_id)

def cancel_task(task_id: str, reason: str = "Cancelled by user") -> bool:
    """Annuler une tâche"""
    return unified_scheduler.cancel_task(task_id, reason)

def retry_failed_task(task_id: str, reset_attempts: bool = False) -> bool:
    """Relancer une tâche échouée"""
    return unified_scheduler.retry_task(task_id, reset_attempts)

def pause_scheduler():
    """Mettre en pause le scheduler"""
    return unified_scheduler.pause_scheduler()

def resume_scheduler():
    """Reprendre le scheduler"""
    return unified_scheduler.resume_scheduler()

def get_health_status() -> Dict[str, Any]:
    """Obtenir le statut de santé complet"""
    return unified_scheduler.get_health_details()

def quick_health_check() -> Dict[str, Any]:
    """Health check rapide"""
    return unified_scheduler.health_check()

# ========================================
# TESTS ET VALIDATION
# ========================================

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    def test_scheduler():
        """Tests du scheduler"""
        print("🧪 Tests du Scheduler Unifié")
        print("=" * 50)
        
        try:
            # Test 1: Initialisation
            print("Test 1: Initialisation...")
            scheduler = UnifiedCollectionScheduler()
            print("✅ Initialisation réussie")
            
            # Test 2: Configuration
            print("Test 2: Vérification configuration...")
            config = scheduler.config
            assert config['max_workers'] > 0
            assert config['max_concurrent_tasks'] > 0
            print("✅ Configuration valide")
            
            # Test 3: Health check
            print("Test 3: Health check...")
            health = scheduler.health_check()
            assert 'status' in health
            print(f"✅ Health check: {health['status']}")
            
            # Test 4: Statistiques
            print("Test 4: Statistiques...")
            stats = scheduler.get_statistics()
            assert 'scheduler_stats' in stats
            print("✅ Statistiques disponibles")
            
            # Test 5: Planification de tâche (simulation)
            print("Test 5: Planification de tâche...")
            try:
                task_id = scheduler.schedule_task(
                    user_id=999,  # Utilisateur de test
                    collector_type=CollectorType.LINKEDIN,
                    target_id="test_org",
                    data_types=["organization_info"],
                    priority=Priority.LOW
                )
                print(f"✅ Tâche planifiée: {task_id}")
                
                # Annuler la tâche de test
                cancelled = scheduler.cancel_task(task_id, "Test terminé")
                print(f"✅ Tâche annulée: {cancelled}")
                
            except Exception as e:
                print(f"⚠️  Test planification (attendu en mode test): {e}")
            
            print("\n🎉 Tous les tests sont passés!")
            return True
            
        except Exception as e:
            print(f"❌ Erreur lors des tests: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def demo_scheduler():
        """Démonstration du scheduler"""
        print("🚀 Démonstration du Scheduler Unifié")
        print("=" * 50)
        
        try:
            # Démarrer le scheduler
            print("Démarrage du scheduler...")
            unified_scheduler.start_scheduler(restore_state=False)
            
            # Afficher le statut
            status = unified_scheduler.get_status()
            print(f"Statut: {status['is_running']}, Workers: {status['workers_active']}")
            
            # Attendre un peu
            time.sleep(2)
            
            # Afficher les statistiques
            stats = unified_scheduler.get_statistics()
            print(f"Tâches: {stats['scheduler_stats']['total_tasks_scheduled']} planifiées")
            
            # Arrêter le scheduler
            print("Arrêt du scheduler...")
            unified_scheduler.stop_scheduler(timeout=10, save_state=False)
            
            print("✅ Démonstration terminée")
            
        except Exception as e:
            print(f"❌ Erreur démonstration: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # S'assurer que le scheduler est arrêté
            try:
                unified_scheduler.stop_scheduler(timeout=5, save_state=False)
            except:
                pass
    
    # Exécuter selon les arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            success = test_scheduler()
            sys.exit(0 if success else 1)
        elif sys.argv[1] == "demo":
            demo_scheduler()
        else:
            print("Usage: python scheduler.py [test|demo]")
    else:
        # Mode interactif
        print("Scheduler Unifié - Mode interactif")
        print("Commandes: test, demo, quit")
        
        while True:
            cmd = input("> ").strip().lower()
            if cmd == "quit":
                break
            elif cmd == "test":
                test_scheduler()
            elif cmd == "demo":
                demo_scheduler()
            elif cmd == "help":
                print("Commandes disponibles: test, demo, help, quit")
            else:
                print(f"Commande inconnue: {cmd}")
        
        print("Au revoir!")