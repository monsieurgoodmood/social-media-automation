#!/bin/bash
# Configuration des Cloud Scheduler pour LinkedIn Analytics

PROJECT_ID="authentic-ether-457013-t5"
REGION="europe-west1"
FUNCTION_URL="https://europe-west1-authentic-ether-457013-t5.cloudfunctions.net/linkedin-analytics"
SERVICE_ACCOUNT="linkedin-analytics-sa@${PROJECT_ID}.iam.gserviceaccount.com"

echo "🕐 Configuration des tâches planifiées LinkedIn"
echo "=============================================="

# 1. Statistiques des followers - Quotidien à 2h du matin
echo ""
echo "📊 Création de la tâche: Statistiques des followers (quotidien 2h)"
gcloud scheduler jobs create http linkedin-follower-statistics \
    --location=$REGION \
    --schedule="0 2 * * *" \
    --time-zone="Europe/Paris" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script":"follower_statistics"}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --attempt-deadline=540s \
    --description="Collecte quotidienne des statistiques de followers LinkedIn"

# 2. Statistiques de partage - Quotidien à 3h du matin
echo ""
echo "📊 Création de la tâche: Statistiques de partage (quotidien 3h)"
gcloud scheduler jobs create http linkedin-share-statistics \
    --location=$REGION \
    --schedule="0 3 * * *" \
    --time-zone="Europe/Paris" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script":"share_statistics"}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --attempt-deadline=540s \
    --description="Collecte quotidienne des statistiques de partage LinkedIn"

# 3. Métriques des posts - Quotidien à 4h du matin
echo ""
echo "📊 Création de la tâche: Métriques des posts (quotidien 4h)"
gcloud scheduler jobs create http linkedin-post-metrics \
    --location=$REGION \
    --schedule="0 4 * * *" \
    --time-zone="Europe/Paris" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script":"post_metrics"}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --attempt-deadline=540s \
    --description="Collecte quotidienne des métriques de posts LinkedIn"

# 4. Statistiques quotidiennes - Quotidien à 5h du matin
echo ""
echo "📊 Création de la tâche: Statistiques quotidiennes (quotidien 5h)"
gcloud scheduler jobs create http linkedin-daily-statistics \
    --location=$REGION \
    --schedule="0 5 * * *" \
    --time-zone="Europe/Paris" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script":"daily_statistics"}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --attempt-deadline=540s \
    --description="Collecte quotidienne des statistiques générales LinkedIn"

# 5. Statistiques des pages - Hebdomadaire le lundi à 6h du matin
echo ""
echo "📊 Création de la tâche: Statistiques des pages (hebdomadaire lundi 6h)"
gcloud scheduler jobs create http linkedin-page-statistics \
    --location=$REGION \
    --schedule="0 6 * * 1" \
    --time-zone="Europe/Paris" \
    --uri=$FUNCTION_URL \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script":"page_statistics"}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --attempt-deadline=540s \
    --description="Collecte hebdomadaire des statistiques de pages LinkedIn"

echo ""
echo "✅ Configuration terminée!"
echo ""
echo "📋 Résumé des tâches créées:"
echo "- linkedin-follower-statistics : Tous les jours à 2h00"
echo "- linkedin-share-statistics    : Tous les jours à 3h00"
echo "- linkedin-post-metrics        : Tous les jours à 4h00"
echo "- linkedin-daily-statistics    : Tous les jours à 5h00"
echo "- linkedin-page-statistics     : Tous les lundis à 6h00"
echo ""
echo "🔍 Pour vérifier les tâches:"
echo "gcloud scheduler jobs list --location=$REGION | grep linkedin"
echo ""
echo "▶️  Pour tester une tâche maintenant:"
echo "gcloud scheduler jobs run linkedin-follower-statistics --location=$REGION"