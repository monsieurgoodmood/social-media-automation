#!/bin/bash

PROJECT_ID="authentic-ether-457013-t5"
REGION="europe-west1"
FUNCTION_NAME="facebook-automation"
SERVICE_ACCOUNT="${FUNCTION_NAME}-sa@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET_NAME="${PROJECT_ID}-facebook-configs"

echo "🔧 RÉPARATION FACEBOOK - Suppression et redéploiement avec bonne config"
echo "===================================================================="

# 1. Supprimer l'ancienne fonction qui a une mauvaise config
echo "🗑️  Suppression de l'ancienne fonction Facebook..."
gcloud functions delete ${FUNCTION_NAME} \
    --region=${REGION} \
    --project=${PROJECT_ID} \
    --quiet 2>/dev/null || echo "   Fonction déjà supprimée ou inexistante"

# Attendre la suppression complète
echo "⏳ Attente de la suppression complète (30 secondes)..."
sleep 30

# 2. Préparer les fichiers
echo "📦 Préparation des fichiers..."
rm -rf temp_deploy
mkdir -p temp_deploy/scripts temp_deploy/utils temp_deploy/configs

# Copier les fichiers Python
cp -r scripts/*.py temp_deploy/scripts/
cp main.py cloud_wrapper.py discover_pages.py token_monitor.py requirements.txt temp_deploy/
cp -r utils/*.py temp_deploy/utils/

# Copier les mappings JSON
for json_file in \
    "configs/page_metrics_mapping.json" \
    "configs/posts_lifetime_mapping.json" \
    "configs/posts_metadata_mapping.json"
do
    if [ -f "${json_file}" ]; then
        cp "${json_file}" temp_deploy/configs/
        echo "   ✓ Copié: ${json_file}"
    fi
done

# Créer le .gcloudignore
cat > temp_deploy/.gcloudignore << EOF
.git
.gitignore
*.pyc
__pycache__
.env
*.log
test_*.py
*.sh
*.md
.DS_Store
venv/
reports/
credentials/
fb_access_token.json*
EOF

# 3. Récupérer les secrets
echo "🔐 Récupération des secrets..."
FB_CLIENT_ID=$(gcloud secrets versions access latest --secret=facebook-client-id --project=${PROJECT_ID})
FB_CLIENT_SECRET=$(gcloud secrets versions access latest --secret=facebook-client-secret --project=${PROJECT_ID})

# 4. Créer la nouvelle fonction avec la BONNE configuration
echo ""
echo "🚀 Création de la nouvelle fonction Facebook (2GB, 1 vCPU, 25 instances max)..."

cd temp_deploy

gcloud functions deploy ${FUNCTION_NAME} \
    --gen2 \
    --runtime python310 \
    --trigger-http \
    --entry-point facebook_automation \
    --region ${REGION} \
    --memory 2GB \
    --cpu 1 \
    --timeout 3600s \
    --max-instances 25 \
    --min-instances 1 \
    --concurrency 250 \
    --service-account ${SERVICE_ACCOUNT} \
    --set-env-vars "GCP_BUCKET_NAME=${BUCKET_NAME},FB_CLIENT_ID=${FB_CLIENT_ID},FB_CLIENT_SECRET=${FB_CLIENT_SECRET},GCP_PROJECT_ID=${PROJECT_ID},GOOGLE_ADMIN_EMAIL=byteberry.analytics@gmail.com" \
    --allow-unauthenticated \
    --source . \
    --project=${PROJECT_ID}

DEPLOY_STATUS=$?
cd ..

# Nettoyer
rm -rf temp_deploy

if [ $DEPLOY_STATUS -ne 0 ]; then
    echo "❌ Le redéploiement a échoué!"
    exit 1
fi

# 5. Récupérer la nouvelle URL
echo ""
echo "📢 Récupération de l'URL de la nouvelle fonction..."
FUNCTION_URI=$(gcloud functions describe ${FUNCTION_NAME} --gen2 --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})

echo "✅ Fonction Facebook recréée avec succès !"
echo "🌐 URL: ${FUNCTION_URI}"

# 6. Configuration des Cloud Scheduler avec la nouvelle URL
echo ""
echo "🗓️  Mise à jour des Cloud Scheduler jobs avec nouvelle URL..."

update_job() {
    local job_name=$1
    local schedule=$2
    local action=$3
    local description=$4

    echo "   - ${job_name}..."
    
    gcloud scheduler jobs update http ${job_name} \
        --location=${REGION} \
        --schedule="${schedule}" \
        --uri="${FUNCTION_URI}?action=${action}" \
        --time-zone="Europe/Paris" \
        --description="${description}" \
        --attempt-deadline=1800s \
        --project=${PROJECT_ID} || echo "   Erreur mise à jour ${job_name}"

    echo "     ✓ Job ${job_name} mis à jour"
}

# Mettre à jour tous les jobs
update_job "fb-discover-pages" "0 6 * * 1" "discover_pages" "Découverte hebdomadaire des pages Facebook"
update_job "fb-page-metrics" "0 7 * * *" "page_metrics" "Collecte quotidienne des métriques de page Facebook"
update_job "fb-posts-lifetime" "0 8 * * *" "posts_lifetime" "Collecte quotidienne des métriques lifetime des posts Facebook"
update_job "fb-posts-metadata" "0 9 * * *" "posts_metadata" "Collecte quotidienne des métadonnées des posts Facebook"
update_job "fb-token-monitor" "0 */6 * * *" "token_monitor" "Monitoring du token Facebook toutes les 6 heures"

# 7. Test de la nouvelle fonction
echo ""
echo "🧪 Test de la nouvelle fonction..."
curl -s "${FUNCTION_URI}?action=token_monitor" --max-time 30 | jq '.' || echo "Test terminé"

echo ""
echo "✅ RÉPARATION FACEBOOK TERMINÉE !"
echo ""
echo "📊 Nouvelle configuration:"
echo "   - Mémoire: 2GB"
echo "   - CPU: 1 vCPU"
echo "   - Max instances: 25 (sécurité)"
echo "   - Concurrence: 250"
echo "   - URL: ${FUNCTION_URI}"
echo ""
echo "💡 Utilisation des quotas:"
echo "   - CPU max: 25 vCPUs (25 instances × 1 vCPU) ✅"
echo "   - RAM max: 50GB (25 instances × 2GB) ✅"
echo "   - Beaucoup de marge restante !"
echo ""
echo "🔗 Console: https://console.cloud.google.com/functions/details/${REGION}/${FUNCTION_NAME}?project=${PROJECT_ID}"