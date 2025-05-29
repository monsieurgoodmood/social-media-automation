#!/bin/bash
PROJECT_ID="authentic-ether-457013-t5"
FUNCTION_URL="https://europe-west1-authentic-ether-457013-t5.cloudfunctions.net/run-linkedin-analytics"

echo "Création des tâches planifiées..."

# 1. Découverte hebdomadaire
gcloud scheduler jobs create http discover-organizations \
    --location=europe-west1 \
    --schedule="0 3 * * 1" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "discover_organizations"}' \
    --time-zone="Europe/Paris" \
    --description="Découverte hebdomadaire des organisations LinkedIn" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ discover-organizations créé" || echo "⚠️  discover-organizations existe déjà"

# 2. Statistiques quotidiennes
gcloud scheduler jobs create http daily-statistics \
    --location=europe-west1 \
    --schedule="0 2 * * *" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "daily_statistics"}' \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des statistiques LinkedIn" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ daily-statistics créé" || echo "⚠️  daily-statistics existe déjà"

# 3. Followers
gcloud scheduler jobs create http follower-statistics \
    --location=europe-west1 \
    --schedule="0 4 * * *" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "follower_statistics"}' \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des statistiques de followers" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ follower-statistics créé" || echo "⚠️  follower-statistics existe déjà"

# 4. Posts
gcloud scheduler jobs create http post-metrics \
    --location=europe-west1 \
    --schedule="0 5 * * 2,5" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "post_metrics"}' \
    --time-zone="Europe/Paris" \
    --description="Collecte bi-hebdomadaire des métriques de posts" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ post-metrics créé" || echo "⚠️  post-metrics existe déjà"

# 5. Partages
gcloud scheduler jobs create http share-statistics \
    --location=europe-west1 \
    --schedule="30 3 * * *" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "share_statistics"}' \
    --time-zone="Europe/Paris" \
    --description="Collecte quotidienne des statistiques de partage" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ share-statistics créé" || echo "⚠️  share-statistics existe déjà"

# 6. Pages/Pays
gcloud scheduler jobs create http page-statistics \
    --location=europe-west1 \
    --schedule="0 6 * * 3" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"script": "page_statistics"}' \
    --time-zone="Europe/Paris" \
    --description="Collecte hebdomadaire des statistiques par pays" \
    --project=${PROJECT_ID} 2>/dev/null && echo "✅ page-statistics créé" || echo "⚠️  page-statistics existe déjà"

echo -e "\n✅ Configuration terminée !"
