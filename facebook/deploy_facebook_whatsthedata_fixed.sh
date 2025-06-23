#!/bin/bash

# Configuration
PROJECT_ID="authentic-ether-457013-t5"
REGION="europe-west1"
FUNCTION_NAME="facebook-automation"
SERVICE_ACCOUNT="${FUNCTION_NAME}-sa@${PROJECT_ID}.iam.gserviceaccount.com"
BUCKET_NAME="${PROJECT_ID}-facebook-configs"

echo "🚀 Déploiement de l'automatisation Facebook sur ${PROJECT_ID}"
echo "============================================"
echo "Compte actif: $(gcloud config get-value account)"
echo "Région: ${REGION}"
echo "SERVICE_ACCOUNT=${SERVICE_ACCOUNT}"
echo "BUCKET: gs://${BUCKET_NAME}"
echo ""

# Vérifier que nous sommes sur le bon projet
CURRENT_PROJECT=$(gcloud config get-value project)
if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
    echo "⚠️  Changement de projet de $CURRENT_PROJECT vers $PROJECT_ID"
    gcloud config set project ${PROJECT_ID}
fi

# Vérifier que le service account existe
echo "🔍 Vérification du service account..."
if ! gcloud iam service-accounts describe ${SERVICE_ACCOUNT} --project=${PROJECT_ID} &>/dev/null; then
    echo "❌ Le service account ${SERVICE_ACCOUNT} n'existe pas."
    echo "   Création du service account..."
    gcloud iam service-accounts create ${FUNCTION_NAME}-sa \
        --display-name="Facebook Automation Service Account" \
        --project=${PROJECT_ID}
    
    # Attendre que le SA soit créé
    sleep 5
    
    # Ajouter les rôles nécessaires
    echo "   Ajout des rôles IAM..."
    for role in \
        "roles/secretmanager.secretAccessor" \
        "roles/storage.objectAdmin" \
        "roles/cloudfunctions.invoker" \
        "roles/logging.logWriter"
    do
        gcloud projects add-iam-policy-binding ${PROJECT_ID} \
            --member="serviceAccount:${SERVICE_ACCOUNT}" \
            --role="${role}"
    done
else
    echo "✅ Le service account existe: ${SERVICE_ACCOUNT}"
fi

# Vérifier que le bucket existe
echo ""
echo "🔍 Vérification du bucket..."
if ! gsutil ls -p ${PROJECT_ID} gs://${BUCKET_NAME} &>/dev/null; then
    echo "⚠️  Le bucket n'existe pas. Exécutez d'abord:"
    echo "   python3 init_gcs_configs.py"
    exit 1
else
    echo "✅ Bucket trouvé: gs://${BUCKET_NAME}"
fi

# Vérifier les secrets
echo ""
echo "🔍 Vérification des secrets..."
for secret in "facebook-access-token" "facebook-client-id" "facebook-client-secret"; do
    if ! gcloud secrets describe ${secret} --project=${PROJECT_ID} &>/dev/null; then
        echo "❌ Secret manquant: ${secret}"
        exit 1
    else
        echo "✅ Secret trouvé: ${secret}"
    fi
done

# Préparer les fichiers pour le déploiement
echo ""
echo "📦 Préparation des fichiers..."
rm -rf temp_deploy
mkdir -p temp_deploy/scripts temp_deploy/utils temp_deploy/configs

# Copier les fichiers Python
cp -r scripts/*.py temp_deploy/scripts/
cp main.py cloud_wrapper.py discover_pages.py token_monitor.py requirements.txt temp_deploy/
cp -r utils/*.py temp_deploy/utils/

# Copier UNIQUEMENT les mappings JSON (pas les configs qui sont dans GCS)
echo "📦 Copie des mappings JSON..."
for json_file in \
    "configs/page_metrics_mapping.json" \
    "configs/posts_lifetime_mapping.json" \
    "configs/posts_metadata_mapping.json"
do
    if [ -f "${json_file}" ]; then
        cp "${json_file}" temp_deploy/configs/
        echo "   ✓ Copié: ${json_file}"
    else
        echo "   ❌ Manquant: ${json_file}"
    fi
done

# Récupérer les valeurs des secrets
echo ""
echo "🔐 Récupération des secrets..."
FB_CLIENT_ID=$(gcloud secrets versions access latest --secret=facebook-client-id --project=${PROJECT_ID})
FB_CLIENT_SECRET=$(gcloud secrets versions access latest --secret=facebook-client-secret --project=${PROJECT_ID})

# Déploiement de la fonction
echo ""
echo "🚀 Déploiement de la Cloud Function..."

cd temp_deploy

gcloud functions deploy ${FUNCTION_NAME} \
    --gen2 \
    --runtime python310 \
    --trigger-http \
    --entry-point facebook_automation \
    --region ${REGION} \
    --memory 2GB \
    --timeout 3600s \
    --service-account ${SERVICE_ACCOUNT} \
    --set-env-vars "GCP_BUCKET_NAME=${BUCKET_NAME},FB_CLIENT_ID=${FB_CLIENT_ID},FB_CLIENT_SECRET=${FB_CLIENT_SECRET},GCP_PROJECT_ID=${PROJECT_ID},GOOGLE_ADMIN_EMAIL=byteberry.analytics@gmail.com" \
    --allow-unauthenticated \
    --source . \
    --project=${PROJECT_ID}

DEPLOY_STATUS=$?
cd ..

# Nettoyer
echo ""
echo "🧹 Nettoyage..."
rm -rf temp_deploy

if [ $DEPLOY_STATUS -ne 0 ]; then
    echo "❌ Le déploiement a échoué!"
    exit 1
fi

# Récupérer l'URL
echo ""
echo "📢 Récupération de l'URL de la fonction..."
FUNCTION_URI=$(gcloud functions describe ${FUNCTION_NAME} --gen2 --region=${REGION} --format='value(serviceConfig.uri)' --project=${PROJECT_ID})

if [ -z "$FUNCTION_URI" ]; then
    FUNCTION_URI="https://${REGION}-${PROJECT_ID}.cloudfunctions.net/${FUNCTION_NAME}"
fi

echo "✅ Fonction déployée : ${FUNCTION_NAME}"
echo "🌐 URL: ${FUNCTION_URI}"

# Tester la fonction
echo ""
echo "🧪 Test de la fonction..."
echo "   Test token_monitor..."
curl -s "${FUNCTION_URI}?action=token_monitor" | jq '.'

# Créer les Cloud Scheduler
echo ""
echo "🗓️  Configuration des Cloud Scheduler jobs..."

create_or_update_job() {
    local job_name=$1
    local schedule=$2
    local action=$3
    local description=$4

    echo "   - ${job_name}..."

    if gcloud scheduler jobs describe ${job_name} --location=${REGION} --project=${PROJECT_ID} &>/dev/null; then
        echo "     Mise à jour..."
        gcloud scheduler jobs update http ${job_name} \
            --location=${REGION} \
            --schedule="${schedule}" \
            --uri="${FUNCTION_URI}?action=${action}" \
            --time-zone="Europe/Paris" \
            --description="${description}" \
            --project=${PROJECT_ID}
    else
        echo "     Création..."
        gcloud scheduler jobs create http ${job_name} \
            --location=${REGION} \
            --schedule="${schedule}" \
            --uri="${FUNCTION_URI}?action=${action}" \
            --http-method=GET \
            --oidc-service-account-email=${SERVICE_ACCOUNT} \
            --time-zone="Europe/Paris" \
            --description="${description}" \
            --project=${PROJECT_ID}
    fi

    echo "     ✓ Job ${job_name} configuré"
}

# Créer les jobs
create_or_update_job "fb-discover-pages" "0 6 * * 1" "discover_pages" "Découverte hebdomadaire des pages Facebook"
create_or_update_job "fb-page-metrics" "0 7 * * *" "page_metrics" "Collecte quotidienne des métriques de page Facebook"
create_or_update_job "fb-posts-lifetime" "0 8 * * *" "posts_lifetime" "Collecte quotidienne des métriques lifetime des posts Facebook"
create_or_update_job "fb-posts-metadata" "0 9 * * *" "posts_metadata" "Collecte quotidienne des métadonnées des posts Facebook"
create_or_update_job "fb-token-monitor" "0 */6 * * *" "token_monitor" "Monitoring du token Facebook toutes les 6 heures"

# Résumé
echo ""
echo "✅ Déploiement Facebook terminé!"
echo ""
echo "📊 Configuration:"
echo "   - Projet: ${PROJECT_ID}"
echo "   - Fonction: ${FUNCTION_NAME}"
echo "   - URL: ${FUNCTION_URI}"
echo "   - Bucket: gs://${BUCKET_NAME}"
echo "   - Service account: ${SERVICE_ACCOUNT}"
echo ""
echo "🔍 Commandes utiles:"
echo "   - Logs: gcloud functions logs read ${FUNCTION_NAME} --region=${REGION} --project=${PROJECT_ID} --limit=50"
echo "   - Jobs: gcloud scheduler jobs list --location=${REGION} --project=${PROJECT_ID} --filter='name:fb-'"
echo "   - Test: curl '${FUNCTION_URI}?action=token_monitor'"
echo ""
echo "📝 Prochaines étapes:"
echo "   1. Vérifier les logs après quelques minutes"
echo "   2. Lancer manuellement un job pour tester:"
echo "      gcloud scheduler jobs run fb-discover-pages --location=${REGION} --project=${PROJECT_ID}"
echo "   3. Vérifier le bucket GCS:"
echo "      gsutil ls -r gs://${BUCKET_NAME}/"