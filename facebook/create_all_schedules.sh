#!/bin/bash

# Configuration
PROJECT_ID="linkedin-analytics-auto"
REGION="europe-west1"
FUNCTION_NAME="facebook-automation"
SERVICE_ACCOUNT="facebook-automation@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Création des Cloud Scheduler pour Facebook automation..."

# Créer le job pour la découverte des pages (hebdomadaire - lundi 8h)
gcloud scheduler jobs create http fb-discover-pages \
    --location=${REGION} \
    --schedule="0 8 * * 1" \
    --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}?action=discover_pages" \
    --http-method=GET \
    --oidc-service-account-email=${SERVICE_ACCOUNT} \
    --time-zone="Europe/Paris" \
    --description="Découverte hebdomadaire des pages Facebook"

# Créer le job pour les métriques de page (quotidien - 9h)
gcloud scheduler jobs create http fb-page-metrics \
    --location=${REGION} \
    --schedule="0 9 * * *" \
    --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}?action=page_metrics" \
    --http-method=GET \
    --oidc-service-account-email=${SERVICE_ACCOUNT} \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des métriques de page Facebook"

# Créer le job pour les métriques lifetime des posts (quotidien - 10h)
gcloud scheduler jobs create http fb-posts-lifetime \
    --location=${REGION} \
    --schedule="0 10 * * *" \
    --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}?action=posts_lifetime" \
    --http-method=GET \
    --oidc-service-account-email=${SERVICE_ACCOUNT} \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des métriques lifetime des posts Facebook"

# Créer le job pour les métadonnées des posts (quotidien - 11h)
gcloud scheduler jobs create http fb-posts-metadata \
    --location=${REGION} \
    --schedule="0 11 * * *" \
    --uri="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}?action=posts_metadata" \
    --http-method=GET \
    --oidc-service-account-email=${SERVICE_ACCOUNT} \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des métadonnées des posts Facebook"

echo "✓ Tous les Cloud Scheduler ont été créés avec succès!"
echo ""
echo "Pour déployer la fonction:"
echo "gcloud functions deploy ${FUNCTION_NAME} \\"
echo "    --runtime python310 \\"
echo "    --trigger-http \\"
echo "    --entry-point facebook_automation \\"
echo "    --region ${REGION} \\"
echo "    --memory 512MB \\"
echo "    --timeout 540s \\"
echo "    --service-account ${SERVICE_ACCOUNT} \\"
echo "    --set-env-vars FB_CLIENT_ID=\$FB_CLIENT_ID,FB_CLIENT_SECRET=\$FB_CLIENT_SECRET,GCP_PROJECT_ID=${PROJECT_ID},GOOGLE_ADMIN_EMAIL=byteberry.analytics@gmail.com,CREDENTIALS_FILE=/workspace/credentials/service_account_credentials.json"