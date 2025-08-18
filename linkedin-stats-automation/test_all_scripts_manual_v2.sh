#!/bin/bash
# test_all_scripts_manual_v2.sh

# Couleurs pour l'affichage
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

LOCATION="europe-west1"
REGION="europe-west1"

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}     TEST COMPLET DU SYSTÈME LINKEDIN ANALYTICS${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Fonction pour afficher un séparateur
separator() {
    echo -e "\n${YELLOW}────────────────────────────────────────────────${NC}\n"
}

# Fonction pour exécuter un job et afficher le statut
run_job() {
    local job_name=$1
    local description=$2
    local wait_time=${3:-30}
    local script_name=${4:-${job_name#linkedin-}}
    
    # Convertir le nom du script (remplacer - par _)
    script_name=$(echo $script_name | tr '-' '_')
    
    separator
    echo -e "${YELLOW}▶ LANCEMENT: $description${NC}"
    echo -e "Job: ${BLUE}$job_name${NC}"
    echo "Heure: $(date '+%H:%M:%S')"
    echo ""
    
    # Lancer le job
    if gcloud scheduler jobs run $job_name --location=$LOCATION 2>&1; then
        echo -e "${GREEN}✓ Job lancé avec succès${NC}"
        
        # Attendre un peu
        echo -e "\n⏳ Attente de ${wait_time} secondes pour laisser le job s'exécuter..."
        for i in $(seq $wait_time -1 1); do
            printf "\r   %02d secondes restantes..." $i
            sleep 1
        done
        echo -e "\n"
        
        # Afficher les derniers logs pour ce script
        echo -e "${BLUE}📊 Derniers logs du job:${NC}"
        gcloud logging read "resource.type=\"cloud_function\" AND jsonPayload.script=\"${script_name}\" AND timestamp>=\"$(date -u -d '2 minutes ago' +%Y-%m-%dT%H:%M:%S)Z\"" \
            --project=$(gcloud config get-value project) \
            --limit=15 \
            --format="table(timestamp.date('%H:%M:%S'),jsonPayload.message)" \
            2>/dev/null || echo "Pas encore de logs disponibles"
            
    else
        echo -e "${RED}❌ Erreur lors du lancement${NC}"
    fi
}

# 1. VÉRIFICATION PRÉLIMINAIRE
separator
echo -e "${BLUE}1. VÉRIFICATION PRÉLIMINAIRE${NC}"
echo ""

# Vérifier l'état de la fonction
echo "📦 État de la fonction Cloud:"
STATE=$(gcloud functions describe run-linkedin-analytics --region=$REGION --format="value(state)" 2>/dev/null)
UPDATE_TIME=$(gcloud functions describe run-linkedin-analytics --region=$REGION --format="value(updateTime)" 2>/dev/null)
if [ "$STATE" == "ACTIVE" ]; then
    echo -e "   ${GREEN}✓ Fonction active${NC}"
    echo -e "   Dernière mise à jour: $UPDATE_TIME"
else
    echo -e "   ${RED}✗ Fonction non active: $STATE${NC}"
    echo "   Relancez le déploiement avant de continuer"
    exit 1
fi

# Afficher les jobs disponibles (avec filtre corrigé)
echo -e "\n⏰ Jobs Cloud Scheduler disponibles:"
gcloud scheduler jobs list --location=$LOCATION --format="table(name,state,schedule,lastAttemptTime.date('%Y-%m-%d %H:%M'))" | grep -E "(NAME|linkedin-)" || echo "Aucun job LinkedIn trouvé"

# Demander confirmation
echo ""
read -p "Voulez-vous lancer tous les tests ? (o/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Oo]$ ]]; then
    echo "Test annulé."
    exit 0
fi

# 2. DIAGNOSTIC SYSTÈME
run_job "linkedin-diagnostic" "🔍 DIAGNOSTIC SYSTÈME" 20 "diagnostic"

# 3. STATISTIQUES QUOTIDIENNES
run_job "linkedin-daily-statistics" "📊 STATISTIQUES QUOTIDIENNES (collecte principale)" 45 "daily_statistics"

# 4. STATISTIQUES DES FOLLOWERS (avec le code corrigé!)
echo -e "\n${GREEN}★ Test du code corrigé des followers ★${NC}"
run_job "linkedin-follower-statistics" "👥 STATISTIQUES DES FOLLOWERS" 45 "follower_statistics"

# 5. STATISTIQUES DE PARTAGE
run_job "linkedin-share-statistics" "🔄 STATISTIQUES DE PARTAGE" 30 "share_statistics"

# 6. STATISTIQUES DES PAGES
run_job "linkedin-page-statistics" "📄 STATISTIQUES DES PAGES" 30 "page_statistics"

# 7. MÉTRIQUES DES POSTS
run_job "linkedin-post-metrics" "📈 MÉTRIQUES DÉTAILLÉES DES POSTS" 60 "post_metrics"

# RÉSUMÉ FINAL
separator
echo -e "${BLUE}📋 RÉSUMÉ FINAL${NC}"
echo ""

# Compter les erreurs récentes
echo "🔍 Analyse des erreurs..."
ERROR_COUNT=$(gcloud logging read "resource.type=\"cloud_function\" AND severity=\"ERROR\" AND timestamp>=\"$(date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%S)Z\"" \
    --project=$(gcloud config get-value project) \
    --limit=100 --format="value(jsonPayload.message)" 2>/dev/null | wc -l)

if [ "$ERROR_COUNT" -gt 0 ]; then
    echo -e "${RED}⚠️  $ERROR_COUNT erreurs détectées dans les 15 dernières minutes${NC}"
    echo -e "\nDernières erreurs:"
    gcloud logging read "severity=\"ERROR\" AND timestamp>=\"$(date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%S)Z\"" \
        --project=$(gcloud config get-value project) \
        --limit=5 --format="table(timestamp.date('%H:%M'),jsonPayload.script,FIRST(jsonPayload.message,50))"
else
    echo -e "${GREEN}✅ Aucune erreur détectée${NC}"
fi

# Afficher les Google Sheets créés/mis à jour
echo -e "\n${BLUE}📊 Google Sheets récemment mis à jour:${NC}"
SHEETS=$(gcloud logging read "jsonPayload.message=~\"URL du tableau:\" AND timestamp>=\"$(date -u -d '15 minutes ago' +%Y-%m-%dT%H:%M:%S)Z\"" \
    --project=$(gcloud config get-value project) \
    --limit=20 --format="value(jsonPayload.message)" 2>/dev/null | grep -o "https://[^ ]*" | sort -u)

if [ -n "$SHEETS" ]; then
    echo "$SHEETS" | while read url; do
        echo "  📄 $url"
    done
else
    echo "  Aucun sheet mis à jour dans les 15 dernières minutes"
fi

echo -e "\n${GREEN}✅ TEST TERMINÉ!${NC}"
echo ""
echo "📊 Commandes utiles pour approfondir:"
echo "───────────────────────────────────"
echo "• Voir tous les logs: gcloud functions logs read run-linkedin-analytics --region=$REGION --limit=50"
echo "• Logs des followers: gcloud logging read 'jsonPayload.script=\"follower_statistics\"' --limit=20"
echo "• Voir les erreurs: gcloud logging read 'severity=\"ERROR\"' --limit=20"
echo "• État des jobs: gcloud scheduler jobs list --location=$LOCATION"
