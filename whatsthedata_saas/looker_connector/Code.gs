/**
 * WhatsTheData - Connecteur Looker Studio
 * Connecte vos métriques LinkedIn & Facebook depuis votre base PostgreSQL
 * 
 * 🎯 Ce fichier doit être placé dans Google Apps Script
 * 📊 Exploite votre base PostgreSQL existante
 */

// ================================
// 1. CONFIGURATION & AUTHENTIFICATION
// ================================

/**
 * Retourne le type d'authentification requis
 */
function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_TOKEN)
    .setHelpUrl('https://whatsthedata.com/help/api-key')
    .build();
}

/**
 * Vérifie si l'authentification est valide
 */
function isAuthValid() {
  var userToken = PropertiesService.getUserProperties().getProperty('dscc.token');
  if (!userToken) return false;
  
  try {
    // Test de l'API key avec votre backend
    var response = validateApiKey(userToken);
    return response && response.valid === true;
  } catch (e) {
    console.error('Erreur validation auth:', e);
    return false;
  }
}

/**
 * Reset l'authentification
 */
function resetAuth() {
  PropertiesService.getUserProperties().deleteProperty('dscc.token');
}

/**
 * Configure les credentials utilisateur
 */
function setCredentials(request) {
  var token = request.userToken.token;
  
  // Validation du token avec votre API
  try {
    var validationResponse = validateApiKey(token);
    if (!validationResponse || !validationResponse.valid) {
      return {
        errorCode: 'INVALID_CREDENTIALS'
      };
    }
    
    // Stocker le token validé
    PropertiesService.getUserProperties().setProperty('dscc.token', token);
    return {
      errorCode: 'NONE'
    };
  } catch (e) {
    console.error('Erreur setCredentials:', e);
    return {
      errorCode: 'INVALID_CREDENTIALS'
    };
  }
}

// ================================
// 2. CONFIGURATION DU CONNECTEUR
// ================================

/**
 * Configuration du connecteur - paramètres utilisateur
 */
function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  // Instructions
  config
    .newInfo()
    .setId('instructions')
    .setText('🚀 Connectez vos pages LinkedIn et Facebook pour visualiser toutes vos métriques dans Looker Studio. Récupérez votre clé API depuis votre tableau de bord WhatsTheData.');
  
  // Sélection des plateformes
  config
    .newSelectMultiple()
    .setId('platforms')
    .setName('Plateformes à inclure')
    .setHelpText('Sélectionnez les plateformes selon votre abonnement')
    .addOption(config.newOptionBuilder().setLabel('LinkedIn').setValue('linkedin'))
    .addOption(config.newOptionBuilder().setLabel('Facebook').setValue('facebook'))
    .setAllowOverride(true);
  
  // Période de données
  config
    .newSelectSingle()
    .setId('date_range')
    .setName('Période de données')
    .setHelpText('Derniers jours à inclure dans les données')
    .addOption(config.newOptionBuilder().setLabel('7 derniers jours').setValue('7'))
    .addOption(config.newOptionBuilder().setLabel('30 derniers jours').setValue('30'))
    .addOption(config.newOptionBuilder().setLabel('90 derniers jours').setValue('90'))
    .setAllowOverride(true);
  
  // Type de métriques
  config
    .newSelectSingle()
    .setId('metrics_type')
    .setName('Type de métriques')
    .addOption(config.newOptionBuilder().setLabel('Vue d\'ensemble (pages + posts)').setValue('overview'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de pages uniquement').setValue('pages'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de posts uniquement').setValue('posts'))
    .setAllowOverride(true);
  
  config.setDateRangeRequired(true);
  
  return config.build();
}

// ================================
// 3. SCHÉMA DES DONNÉES
// ================================

/**
 * Définit le schéma unifié LinkedIn + Facebook
 */
function getSchema(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // ================================
  // DIMENSIONS COMMUNES
  // ================================
  
  fields.newDimension()
    .setId('platform')
    .setName('Plateforme')
    .setDescription('LinkedIn ou Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('date')
    .setName('Date')
    .setDescription('Date de la métrique')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('page_id')
    .setName('ID Page')
    .setDescription('Identifiant de la page')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('page_name')
    .setName('Nom de la Page')
    .setDescription('Nom de la page/organisation')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('content_type')
    .setName('Type de Contenu')
    .setDescription('page_metrics ou post_metrics')
    .setType(types.TEXT);
  
  // ================================
  // MÉTRIQUES FOLLOWERS/FANS
  // ================================
  
  fields.newMetric()
    .setId('followers_total')
    .setName('Followers/Fans Total')
    .setDescription('Nombre total de followers (LinkedIn) ou fans (Facebook)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('followers_gained')
    .setName('Nouveaux Followers')
    .setDescription('Nouveaux followers/fans obtenus')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_lost')
    .setName('Followers Perdus')
    .setDescription('Followers/fans perdus')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ================================
  // MÉTRIQUES IMPRESSIONS & PORTÉE
  // ================================
  
  fields.newMetric()
    .setId('impressions')
    .setName('Impressions')
    .setDescription('Nombre total d\'impressions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('unique_impressions')
    .setName('Impressions Uniques')
    .setDescription('Nombre d\'impressions uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_views')
    .setName('Vues de Page')
    .setDescription('Nombre de vues de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ================================
  // MÉTRIQUES ENGAGEMENT
  // ================================
  
  fields.newMetric()
    .setId('total_engagement')
    .setName('Engagement Total')
    .setDescription('Somme de tous les engagements')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('likes')
    .setName('J\'aime')
    .setDescription('Nombre de j\'aime/likes')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('comments')
    .setName('Commentaires')
    .setDescription('Nombre de commentaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('shares')
    .setName('Partages')
    .setDescription('Nombre de partages')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('clicks')
    .setName('Clics')
    .setDescription('Nombre de clics')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ================================
  // MÉTRIQUES RÉACTIONS
  // ================================
  
  fields.newMetric()
    .setId('reactions_positive')
    .setName('Réactions Positives')
    .setDescription('Like + Love + Celebrate + Wow')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_negative')
    .setName('Réactions Négatives')
    .setDescription('Sorry + Anger')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ================================
  // MÉTRIQUES CALCULÉES
  // ================================
  
  fields.newMetric()
    .setId('engagement_rate')
    .setName('Taux d\'Engagement (%)')
    .setDescription('Pourcentage d\'engagement par rapport aux impressions')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('click_through_rate')
    .setName('Taux de Clic (%)')
    .setDescription('Pourcentage de clics par rapport aux impressions')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ================================
  // MÉTRIQUES VIDÉO
  // ================================
  
  fields.newMetric()
    .setId('video_views')
    .setName('Vues Vidéo')
    .setDescription('Nombre de vues vidéo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('video_complete_views')
    .setName('Vues Vidéo Complètes')
    .setDescription('Vues vidéo de 30s+')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  return {schema: fields.build()};
}

// ================================
// 4. RÉCUPÉRATION DES DONNÉES
// ================================

/**
 * Récupère les données depuis votre API PostgreSQL
 */
function getData(request) {
  var userToken = PropertiesService.getUserProperties().getProperty('dscc.token');
  
  if (!userToken) {
    throw new Error('Token d\'authentification manquant');
  }
  
  try {
    // Construction des paramètres de requête
    var queryParams = {
      platforms: request.configParams.platforms || ['linkedin', 'facebook'],
      dateRange: request.configParams.date_range || '30',
      metricsType: request.configParams.metrics_type || 'overview',
      startDate: request.dateRange.startDate,
      endDate: request.dateRange.endDate,
      fields: request.fields.map(function(field) { return field.name; })
    };
    
    // Appel à votre API
    var apiResponse = callWhatsTheDataAPI(userToken, queryParams);
    
    if (!apiResponse || !apiResponse.data) {
      throw new Error('Aucune donnée reçue de l\'API');
    }
    
    // Transformation des données pour Looker Studio
    var transformedData = transformDataForLookerStudio(apiResponse.data, request.fields);
    
    return {
      schema: getSchema(request).schema,
      rows: transformedData
    };
    
  } catch (e) {
    console.error('Erreur getData:', e);
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Erreur technique: ' + e.toString())
      .setText('Impossible de récupérer les données. Vérifiez votre clé API et votre abonnement.')
      .throwException();
  }
}

// ================================
// 5. FONCTIONS UTILITAIRES
// ================================

/**
 * Valide la clé API avec votre backend
 */
function validateApiKey(token) {
  var url = 'https://api.whatsthedata.com/v1/validate-token';  // Votre URL API
  
  var options = {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify({
      source: 'looker_studio_connector'
    })
  };
  
  var response = UrlFetchApp.fetch(url, options);
  
  if (response.getResponseCode() === 200) {
    return JSON.parse(response.getContentText());
  }
  
  return null;
}

/**
 * Appelle votre API pour récupérer les données
 */
function callWhatsTheDataAPI(token, params) {
  var url = 'https://api.whatsthedata.com/v1/looker-data';  // Votre endpoint
  
  var options = {
    method: 'POST',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Content-Type': 'application/json'
    },
    payload: JSON.stringify(params)
  };
  
  var response = UrlFetchApp.fetch(url, options);
  
  if (response.getResponseCode() !== 200) {
    throw new Error('Erreur API: ' + response.getResponseCode() + ' - ' + response.getContentText());
  }
  
  return JSON.parse(response.getContentText());
}

/**
 * Transforme les données PostgreSQL pour Looker Studio
 */
function transformDataForLookerStudio(rawData, requestedFields) {
  var fieldNames = requestedFields.map(function(field) { return field.name; });
  var rows = [];
  
  rawData.forEach(function(record) {
    var row = [];
    
    fieldNames.forEach(function(fieldName) {
      var value = getFieldValue(record, fieldName);
      row.push(value);
    });
    
    rows.push({values: row});
  });
  
  return rows;
}

/**
 * Extrait la valeur d'un champ depuis un enregistrement
 */
function getFieldValue(record, fieldName) {
  switch (fieldName) {
    case 'platform':
      return record.platform || 'unknown';
    
    case 'date':
      return record.date || new Date().toISOString().split('T')[0];
    
    case 'page_id':
      return record.page_id || record.organization_id || '';
    
    case 'page_name':
      return record.page_name || record.organization_name || '';
    
    case 'content_type':
      return record.content_type || 'page_metrics';
    
    // Métriques followers
    case 'followers_total':
      return record.followers_total || record.page_fans || record.followers_count || 0;
    
    case 'followers_gained':
      return record.followers_gained || record.page_fan_adds || record.new_followers || 0;
    
    case 'followers_lost':
      return record.followers_lost || record.page_fan_removes || 0;
    
    // Métriques impressions
    case 'impressions':
      return record.impressions || record.page_impressions || record.impression_count || 0;
    
    case 'unique_impressions':
      return record.unique_impressions || record.page_impressions_unique || record.unique_impression_count || 0;
    
    case 'page_views':
      return record.page_views || record.page_views_total || record.all_page_views || 0;
    
    // Métriques engagement
    case 'total_engagement':
      return record.total_engagement || record.page_post_engagements || record.engagement_total || 0;
    
    case 'likes':
      return record.likes || record.like_count || record.likes_count || 0;
    
    case 'comments':
      return record.comments || record.comment_count || record.comments_count || 0;
    
    case 'shares':
      return record.shares || record.share_count || record.shares_count || 0;
    
    case 'clicks':
      return record.clicks || record.click_count || record.post_clicks || 0;
    
    // Métriques réactions
    case 'reactions_positive':
      var positive = (record.reactions_like || 0) + 
                    (record.reactions_love || 0) + 
                    (record.reactions_celebrate || 0) + 
                    (record.reactions_wow || 0);
      return positive;
    
    case 'reactions_negative':
      var negative = (record.reactions_sorry || 0) + (record.reactions_anger || 0);
      return negative;
    
    // Métriques calculées
    case 'engagement_rate':
      return record.engagement_rate || 0;
    
    case 'click_through_rate':
      var impressions = record.impressions || record.page_impressions || 1;
      var clicks = record.clicks || record.click_count || 0;
      return impressions > 0 ? (clicks / impressions) * 100 : 0;
    
    // Métriques vidéo
    case 'video_views':
      return record.video_views || record.page_video_views || record.post_video_views || 0;
    
    case 'video_complete_views':
      return record.video_complete_views || record.page_video_complete_views_30s || record.post_video_complete_views || 0;
    
    default:
      return record[fieldName] || 0;
  }
}

// ================================
// 6. GESTION DES ERREURS
// ================================

/**
 * Fonction pour déboguer les administrateurs
 */
function isAdminUser() {
  var email = Session.getActiveUser().getEmail();
  var adminEmails = ['votre-email@gmail.com']; // Remplacez par votre email
  return adminEmails.indexOf(email) !== -1;
}

// ================================
// 🎯 CONFIGURATION FINALE
// ================================

/**
 * Configuration pour la soumission à Google
 * Ne pas modifier ces fonctions
 */
function getConnectorConfig() {
  return {
    name: 'WhatsTheData - Social Media Analytics',
    description: 'Connectez vos pages LinkedIn et Facebook pour visualiser toutes vos métriques dans Looker Studio.',
    logoUrl: 'https://votre-site.com/logo-connecteur.png',
    company: 'WhatsTheData',
    companyUrl: 'https://whatsthedata.com',
    supportUrl: 'https://whatsthedata.com/support',
    privacyPolicyUrl: 'https://whatsthedata.com/privacy',
    termsOfServiceUrl: 'https://whatsthedata.com/terms'
  };
}