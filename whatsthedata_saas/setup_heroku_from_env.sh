#!/bin/bash

# setup_heroku_from_env.sh
# Script pour configurer automatiquement Heroku depuis votre fichier .env

set -e  # Arrêter en cas d'erreur
set -u  # Arrêter si variable non définie

APP_NAME="whats-the-data"

echo "🚀 Configuration automatique Heroku depuis .env"
echo "App: $APP_NAME"
echo "=================================================="

# Vérifier que .env existe
if [[ ! -f ".env" ]]; then
    echo "❌ Fichier .env non trouvé"
    exit 1
fi

# Variables critiques pour que l'app démarre
CRITICAL_VARS=(
    "STRIPE_PUBLISHABLE_KEY"
    "STRIPE_SECRET_KEY" 
    "JWT_SECRET_KEY"
    "LINKEDIN_CLIENT_ID"
    "LINKEDIN_CLIENT_SECRET"
    "FB_CLIENT_ID"
    "FB_CLIENT_SECRET"
)

# Variables importantes pour le fonctionnement
IMPORTANT_VARS=(
    "APP_NAME"
    "APP_VERSION"
    "ENVIRONMENT"
    "DEBUG"
    "LOG_LEVEL"
    "CORS_ORIGINS"
    "LINKEDIN_ACCESS_TOKEN"
    "LINKEDIN_REFRESH_TOKEN"
    "COMMUNITY_CLIENT_ID"
    "COMMUNITY_CLIENT_SECRET"
    "PORTABILITY_CLIENT_ID"
    "PORTABILITY_CLIENT_SECRET"
    "FB_PERMISSIONS"
    "LINKEDIN_BASE_URL"
    "LINKEDIN_API_BASE_URL"
    "FACEBOOK_BASE_URL"
    "FACEBOOK_GRAPH_URL"
)

# Fonction pour extraire une variable du .env
get_env_var() {
    local var_name="$1"
    local value
    value=$(grep "^${var_name}=" .env 2>/dev/null | cut -d'=' -f2- | sed 's/^["'\'']//' | sed 's/["'\'']$//' || echo "")
    echo "$value"
}

# Fonction pour configurer une variable
set_heroku_var() {
    local var_name="$1"
    local var_value="$2"
    
    if [[ -n "$var_value" ]]; then
        echo "⚙️  Configurant $var_name..."
        
        # Échapper les caractères spéciaux pour heroku
        local var_value_escaped
        var_value_escaped=$(printf '%s\n' "$var_value" | sed 's/"/\\"/g')
        
        # Essayer avec heroku CLI
        if command -v heroku >/dev/null 2>&1; then
            heroku config:set "${var_name}=${var_value_escaped}" -a "$APP_NAME"
        else
            # Si pas de CLI, générer la commande
            echo "heroku config:set \"${var_name}=${var_value_escaped}\" -a $APP_NAME"
        fi
    else
        echo "⚠️  Variable $var_name vide ou non trouvée"
    fi
}

# Adapter les valeurs pour la production
adapt_for_production() {
    local var_name="$1"
    local var_value="$2"
    
    case "$var_name" in
        "ENVIRONMENT")
            echo "production"
            ;;
        "DEBUG")
            echo "false"
            ;;
        "DEVELOPMENT_MODE")
            echo "false"
            ;;
        "BASE_URL")
            echo "https://${APP_NAME}.herokuapp.com"
            ;;
        "CORS_ORIGINS")
            echo "https://datastudio.google.com,https://lookerstudio.google.com,https://script.google.com"
            ;;
        "LINKEDIN_REDIRECT_URI")
            echo "https://${APP_NAME}.herokuapp.com/auth/linkedin/callback"
            ;;
        "FACEBOOK_REDIRECT_URI")
            echo "https://${APP_NAME}.herokuapp.com/auth/facebook/callback"
            ;;
        *)
            echo "$var_value"
            ;;
    esac
}

# Vérifier si heroku CLI est disponible
CLI_MODE=false
if command -v heroku >/dev/null 2>&1; then
    echo "✅ Heroku CLI détecté - configuration directe"
    CLI_MODE=true
else
    echo "⚠️  Heroku CLI non trouvé - génération des commandes"
    echo "📝 Copiez-collez les commandes générées ci-dessous:"
    echo ""
fi

# Configurer les variables critiques d'abord
echo "🔥 Configuration des variables CRITIQUES..."
for var in "${CRITICAL_VARS[@]}"; do
    value=$(get_env_var "$var")
    adapted_value=$(adapt_for_production "$var" "$value")
    set_heroku_var "$var" "$adapted_value"
done

echo ""
echo "📊 Configuration des variables IMPORTANTES..."
for var in "${IMPORTANT_VARS[@]}"; do
    value=$(get_env_var "$var")
    adapted_value=$(adapt_for_production "$var" "$value")
    set_heroku_var "$var" "$adapted_value"
done

# Ajouter des variables spécifiques pour Heroku
echo ""
echo "🌐 Configuration des variables spécifiques PRODUCTION..."

if [[ "$CLI_MODE" == "true" ]]; then
    heroku config:set "BASE_URL=https://${APP_NAME}.herokuapp.com" -a "$APP_NAME"
    heroku config:set "ENVIRONMENT=production" -a "$APP_NAME"
    heroku config:set "DEBUG=false" -a "$APP_NAME"
    heroku config:set "CORS_ORIGINS=https://datastudio.google.com,https://lookerstudio.google.com,https://script.google.com" -a "$APP_NAME"
else
    echo "heroku config:set \"BASE_URL=https://${APP_NAME}.herokuapp.com\" -a $APP_NAME"
    echo "heroku config:set \"ENVIRONMENT=production\" -a $APP_NAME"
    echo "heroku config:set \"DEBUG=false\" -a $APP_NAME"
    echo "heroku config:set \"CORS_ORIGINS=https://datastudio.google.com,https://lookerstudio.google.com,https://script.google.com\" -a $APP_NAME"
fi

echo ""
if [[ "$CLI_MODE" == "true" ]]; then
    echo "✅ Configuration terminée!"
    echo "🔍 Vérifiez avec: heroku config -a $APP_NAME"
    echo "🚀 Testez avec: curl https://${APP_NAME}.herokuapp.com/"
else
    echo "📋 COMMANDES GÉNÉRÉES CI-DESSUS"
    echo "👆 Copiez-collez les commandes dans votre terminal"
    echo ""
    echo "OU installez Heroku CLI pour automatiser:"
    echo "curl https://cli-assets.heroku.com/install.sh | sh"
fi

echo ""
echo "🌐 Une fois configuré, mettez à jour votre connecteur avec:"
echo "   URL API: https://${APP_NAME}.herokuapp.com/api/v1"

echo "✅ Script terminé avec succès"
exit 0