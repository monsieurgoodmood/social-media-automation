#!/bin/bash
# Script de déploiement LinkedIn optimisé pour gros volumes

# Configuration
PROJECT_ID="authentic-ether-457013-t5"
REGION="europe-west1"
FUNCTION_NAME="linkedin-analytics"

echo "🚀 Déploiement LinkedIn avec configuration optimisée"
echo "=========================================================="

# 1. Nettoyer les fichiers __pycache__ locaux
echo "🧹 Nettoyage des caches Python..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -name "*.pyc" -delete 2>/dev/null || true

# 2. Vérifier que les fichiers critiques existent
echo "📋 Vérification des fichiers critiques..."
REQUIRED_FILES=(
    "main.py"
    "batch_processor.py"
    "requirements.txt"
    "discover_organizations_auto.py"
    "linkedin_multi_follower_stats.py"
    "linkedin_multi_org_share_tracker.py"
    "linkedin_multi_page_stats.py"
    "linkedin_multi_post_metrics.py"
    "linkedin_multi_org_tracker.py"
    "follower_stats_mapping.json"
    "sheet_mapping.json"
    "organizations_config.json"
    "page_stats_mapping.json"
    "post_metrics_mapping.json"
    "share_stats_mapping.json"
)

ALL_GOOD=true
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo "❌ Fichier manquant: $file"
        ALL_GOOD=false
    else
        echo "✅ $file"
    fi
done

if [ "$ALL_GOOD" = false ]; then
    echo "❌ Des fichiers sont manquants. Déploiement annulé."
    exit 1
fi

# 3. Créer/Mettre à jour le fichier .gcloudignore
echo "📝 Mise à jour de .gcloudignore..."
cat > .gcloudignore << EOF
# Ignorer les fichiers non nécessaires
.git
.gitignore
*.pyc
__pycache__
.env
.env.local
*.log
test_*.py
*_test.py
*.sh
*.md
.DS_Store
venv/
env/
node_modules/
*.zip
configs/function-source.zip
cloud_config_check/
discovery_summary.json
organizations_report.json
dashboard.html
logs.txt
EOF

# 4. Configurer les permissions étendues pour le service account
echo "🔐 Configuration des permissions étendues..."
SERVICE_ACCOUNT="$FUNCTION_NAME-sa@$PROJECT_ID.iam.gserviceaccount.com"

# Créer le service account s'il n'existe pas
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT_ID &>/dev/null; then
    echo "Création du service account..."
    gcloud iam service-accounts create ${FUNCTION_NAME}-sa \
        --display-name="LinkedIn Analytics Service Account" \
        --project=$PROJECT_ID
fi

# Ajouter tous les rôles nécessaires (uniquement ceux supportés)
for role in \
    "roles/storage.admin" \
    "roles/bigquery.admin" \
    "roles/secretmanager.secretAccessor" \
    "roles/cloudfunctions.invoker" \
    "roles/logging.logWriter" \
    "roles/compute.instanceAdmin.v1"
do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="$role" \
        --quiet
done

# 5. Déployer la fonction avec configuration haute performance
echo ""
echo "☁️  Déploiement de la Cloud Function (configuration optimisée)..."
gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=run_linkedin_analytics \
    --trigger-http \
    --allow-unauthenticated \
    --service-account="$SERVICE_ACCOUNT" \
    --memory=2GB \
    --cpu=1 \
    --timeout=3600s \
    --max-instances=50 \
    --min-instances=1 \
    --concurrency=250 \
    --set-env-vars="GCP_PROJECT=$PROJECT_ID,AUTOMATED_MODE=true,LINKEDIN_SORT_SHEET_DATA=False" \
    --project=$PROJECT_ID

# 6. Upload des fichiers de configuration vers Cloud Storage
echo ""
echo "📤 Upload des fichiers de configuration vers Cloud Storage..."
gsutil cp follower_stats_mapping.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ follower_stats_mapping.json non trouvé"
gsutil cp sheet_mapping.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ sheet_mapping.json non trouvé"
gsutil cp organizations_config.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ organizations_config.json non trouvé"
gsutil cp page_stats_mapping.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ page_stats_mapping.json non trouvé"
gsutil cp post_metrics_mapping.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ post_metrics_mapping.json non trouvé"
gsutil cp share_stats_mapping.json gs://$PROJECT_ID-config/ 2>/dev/null || echo "⚠ share_stats_mapping.json non trouvé"
echo "✅ Fichiers de configuration uploadés"

# 7. Récupérer l'URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME \
    --region=$REGION \
    --project=$PROJECT_ID \
    --format="value(url)")

echo ""
echo "✅ Déploiement optimisé terminé!"
echo "📍 URL: $FUNCTION_URL"
echo ""
echo "🚀 Configuration appliquée:"
echo "   - Mémoire: 2GB"
echo "   - CPU: 1 vCPU" 
echo "   - Timeout: 1 heure"
echo "   - Max instances: 50"
echo "   - Concurrence: 250"
echo "   - Fichiers JSON uploadés automatiquement"

# 8. Test de diagnostic
echo ""
echo "🧪 Test de diagnostic..."
echo "Attendez 10 secondes pour que la fonction se déploie..."
sleep 10

curl -X POST $FUNCTION_URL \
    -H "Content-Type: application/json" \
    -d '{"script":"diagnostic"}' \
    --max-time 60

echo ""
echo ""
echo "📝 Tests supplémentaires disponibles:"
echo ""
echo "1️⃣ Découverte des organisations:"
echo "curl -X POST $FUNCTION_URL -H \"Content-Type: application/json\" -d '{\"script\":\"discover_organizations\"}'"
echo ""
echo "2️⃣ Statistiques des followers (TOUTES les organisations):"
echo "curl -X POST $FUNCTION_URL -H \"Content-Type: application/json\" -d '{\"script\":\"follower_statistics\"}'"
echo ""
echo "3️⃣ Vérifier l'état du bucket de configuration:"
echo "gsutil ls gs://$PROJECT_ID-config/"