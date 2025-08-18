#!/bin/bash

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

FUNCTION_NAME="run-linkedin-analytics"
REGION="europe-west1"
PROJECT=$(gcloud config get-value project)

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}     RÉSOLUTION DES CONFLITS SECRETS/VARIABLES${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"

# Variables qui sont en conflit (définies comme secrets)
CONFLICTING_VARS=(
    "GOOGLE_ADMIN_EMAIL"
    "GOOGLE_SERVICE_ACCOUNT_JSON"
    "LINKEDIN_ACCESS_TOKEN"
    "LINKEDIN_CLIENT_ID"
    "LINKEDIN_CLIENT_SECRET"
    "LINKEDIN_REFRESH_TOKEN"
    "PORTABILITY_ACCESS_TOKEN"
    "PORTABILITY_CLIENT_ID"
    "PORTABILITY_CLIENT_SECRET"
)

echo -e "\n${YELLOW}Variables en conflit détectées:${NC}"
for var in "${CONFLICTING_VARS[@]}"; do
    echo "  - $var"
done

echo -e "\n${YELLOW}Choix de résolution:${NC}"
echo "1. Garder comme variables d'environnement normales (recommandé pour votre cas)"
echo "2. Garder comme secrets (plus sécurisé mais plus complexe)"
echo "3. Annuler"

read -p "Votre choix (1-3): " choice

case $choice in
    1)
        echo -e "\n${GREEN}Option 1: Migration vers variables d'environnement normales${NC}"
        
        # Étape 1: Supprimer tous les secrets de la fonction
        echo -e "\n${YELLOW}1. Suppression des secrets de la fonction...${NC}"
        gcloud functions deploy $FUNCTION_NAME \
            --region=$REGION \
            --clear-secrets \
            --quiet
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✓ Secrets supprimés de la fonction${NC}"
        fi
        
        # Étape 2: Attendre
        echo -e "\n⏳ Attente de 10 secondes..."
        sleep 10
        
        # Étape 3: Déployer avec env_vars.yaml
        echo -e "\n${YELLOW}2. Déploiement avec env_vars.yaml...${NC}"
        
        gcloud functions deploy $FUNCTION_NAME \
            --region=$REGION \
            --runtime=python311 \
            --trigger-http \
            --allow-unauthenticated \
            --entry-point=run_linkedin_analytics \
            --memory=512MB \
            --timeout=540s \
            --max-instances=10 \
            --env-vars-file=env_vars.yaml \
            --project=$PROJECT
        
        if [ $? -eq 0 ]; then
            echo -e "\n${GREEN}✅ Déploiement réussi!${NC}"
            
            # Optionnel: Proposer de supprimer les secrets inutilisés
            echo -e "\n${YELLOW}Les secrets Google suivants ne sont plus utilisés:${NC}"
            echo "  - google-admin-email"
            echo -e "\n${RED}⚠️  Note: Les autres secrets peuvent être utilisés par d'autres ressources${NC}"
            
            read -p "Voulez-vous supprimer le secret 'google-admin-email' ? (o/N) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Oo]$ ]]; then
                gcloud secrets delete google-admin-email --quiet
                echo -e "${GREEN}✓ Secret supprimé${NC}"
            fi
        else
            echo -e "${RED}❌ Échec du déploiement${NC}"
        fi
        ;;
        
    2)
        echo -e "\n${GREEN}Option 2: Migration vers secrets${NC}"
        
        # Créer un fichier temporaire sans les variables en conflit
        echo -e "\n${YELLOW}1. Création d'un fichier env_vars temporaire sans conflits...${NC}"
        
        cp env_vars.yaml env_vars_temp.yaml
        
        # Retirer les variables en conflit du fichier temporaire
        for var in "${CONFLICTING_VARS[@]}"; do
            sed -i "/^$var:/d" env_vars_temp.yaml
        done
        
        echo -e "${GREEN}✓ Fichier temporaire créé${NC}"
        
        # Déployer avec le fichier temporaire et les secrets
        echo -e "\n${YELLOW}2. Déploiement avec secrets...${NC}"
        
        # Construire la chaîne des secrets
        SECRET_STRING=""
        for var in "${CONFLICTING_VARS[@]}"; do
            # Convertir le nom en minuscules avec tirets
            secret_name=$(echo "$var" | tr '[:upper:]' '[:lower:]' | tr '_' '-')
            SECRET_STRING="${SECRET_STRING}${var}=${secret_name}:latest,"
        done
        SECRET_STRING=${SECRET_STRING%,}  # Retirer la dernière virgule
        
        gcloud functions deploy $FUNCTION_NAME \
            --region=$REGION \
            --runtime=python311 \
            --trigger-http \
            --allow-unauthenticated \
            --entry-point=run_linkedin_analytics \
            --memory=512MB \
            --timeout=540s \
            --max-instances=10 \
            --env-vars-file=env_vars_temp.yaml \
            --set-secrets="$SECRET_STRING" \
            --project=$PROJECT
        
        # Nettoyer
        rm -f env_vars_temp.yaml
        ;;
        
    3)
        echo "Annulé."
        exit 0
        ;;
        
    *)
        echo "Choix invalide"
        exit 1
        ;;
esac

# Vérification finale
echo -e "\n${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                    VÉRIFICATION FINALE${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"

STATE=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(state)" 2>/dev/null)
UPDATE_TIME=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(updateTime)" 2>/dev/null)

echo -e "\n📊 État de la fonction: ${GREEN}$STATE${NC}"
echo -e "🕐 Dernière mise à jour: $UPDATE_TIME"

# Test rapide
echo -e "\n${YELLOW}Test rapide avec le diagnostic ?${NC}"
read -p "Lancer le test ? (o/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Oo]$ ]]; then
    gcloud scheduler jobs run linkedin-diagnostic --location=europe-west1
    echo -e "\n⏳ Attente de 20 secondes..."
    sleep 20
    
    echo -e "\n📊 Derniers logs:"
    gcloud logging read "resource.type=\"cloud_function\" AND jsonPayload.script=\"diagnostic\" AND timestamp>=\"$(date -u -d '1 minute ago' +%Y-%m-%dT%H:%M:%S)Z\"" \
        --project=$PROJECT \
        --limit=10 \
        --format="table(timestamp.date('%H:%M:%S'),jsonPayload.message)"
fi

echo -e "\n${GREEN}✅ Terminé!${NC}"