/**
 * WhatsTheData - Connecteur Looker Studio LINKEDIN ONLY
 * Toutes les métriques LinkedIn avec abonnement Stripe intégré
 * ID Connecteur: AKfycbwPeyPwxul5cTIM4x5kc1zF3a1MuQIuCR_hqN0OGEqfQHO1xgeZqO3HLbqKYhtVrr17
 * Offre Stripe: price_1Ryho8JoIj8R31C3EXMDQ9tY (LinkedIn Only - 29€/mois)
 */

// ================================
// 1. CONFIGURATION DE L'API
// ================================

var API_BASE_URL = 'https://whats-the-data-d954d4d4cb5f.herokuapp.com';
var CONNECTOR_ID = 'AKfycbwPeyPwxul5cTIM4x5kc1zF3a1MuQIuCR_hqN0OGEqfQHO1xgeZqO3HLbqKYhtVrr17';

// ================================
// 2. AUTHENTIFICATION OAUTH2 GOOGLE
// ================================

function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.OAUTH2)
    .build();
}

function isAuthValid() {
  console.log('=== isAuthValid - LinkedIn Only ===');
  var userEmail = Session.getActiveUser().getEmail();
  console.log('Email utilisateur:', userEmail);
  
  if (!userEmail) {
    console.log('Pas d\'email utilisateur');
    return false;
  }
  
  return true;
}

function resetAuth() {
  console.log('resetAuth appelée - OAuth2 Google');
  return;
}

function get3PAuthorizationUrls() {
  return null;
}

function authCallback(request) {
  return { errorCode: 'NONE' };
}

/**
 * Vérifie l'abonnement LinkedIn Only de l'utilisateur
 */
function checkUserSubscription(userEmail) {
  try {
    var response = UrlFetchApp.fetch(API_BASE_URL + '/api/v1/check-user-looker', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      payload: JSON.stringify({
        email: userEmail,
        connector_id: CONNECTOR_ID,
        platform: 'linkedin'
      }),
      muteHttpExceptions: true
    });
    
    var responseCode = response.getResponseCode();
    var data = JSON.parse(response.getContentText());
    
    console.log('Réponse checkUserSubscription:', responseCode, data);
    
    if (responseCode === 200 && data.valid) {
      return { valid: true, user: data.user };
    } else if (responseCode === 404) {
      // Redirection vers inscription LinkedIn Only
      var redirectUrl = API_BASE_URL + '/connect?source=looker&email=' + 
                       encodeURIComponent(userEmail) + '&connector=' + CONNECTOR_ID;
      throw new Error('REDIRECT_TO_SIGNUP:' + redirectUrl);
    } else if (responseCode === 403) {
      // Redirection vers mise à niveau LinkedIn Only
      var redirectUrl = API_BASE_URL + '/connect/upgrade?source=looker&email=' + 
                       encodeURIComponent(userEmail) + '&connector=' + CONNECTOR_ID;
      throw new Error('REDIRECT_TO_UPGRADE:' + redirectUrl);
    } else {
      return { valid: false, error: data.message || 'Erreur inconnue' };
    }
    
  } catch (e) {
    if (e.message.startsWith('REDIRECT_TO_')) {
      throw e;
    }
    console.error('Erreur checkUserSubscription:', e);
    return { valid: false, error: e.toString() };
  }
}

// ================================
// 3. CONFIGURATION DU CONNECTEUR
// ================================

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  config
    .newInfo()
    .setId('instructions')
    .setText('📊 WhatsTheData LINKEDIN - Toutes les métriques LinkedIn depuis votre compte');
  
  config
    .newSelectSingle()
    .setId('date_range')
    .setName('Période de données')
    .addOption(config.newOptionBuilder().setLabel('7 derniers jours').setValue('7'))
    .addOption(config.newOptionBuilder().setLabel('30 derniers jours').setValue('30'))
    .addOption(config.newOptionBuilder().setLabel('90 derniers jours').setValue('90'))
    .setAllowOverride(true);
  
  config
    .newSelectSingle()
    .setId('metrics_type')
    .setName('Type de métriques LinkedIn')
    .addOption(config.newOptionBuilder().setLabel('Vue d\'ensemble (pages + posts)').setValue('overview'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de pages uniquement').setValue('pages'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de posts uniquement').setValue('posts'))
    .addOption(config.newOptionBuilder().setLabel('Breakdown followers détaillé').setValue('followers_breakdown'))
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_reactions')
    .setName('Inclure détail des réactions LinkedIn')
    .setHelpText('Like, Celebrate, Love, Insightful, Support, Funny')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_breakdown')
    .setName('Inclure breakdown démographique')
    .setHelpText('Segmentation par pays, industrie, séniorité, taille entreprise')
    .setAllowOverride(true);
  
  return config.build();
}

// ================================
// 4. SCHÉMA COMPLET LINKEDIN
// ================================

function getSchema(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // DIMENSIONS COMMUNES
  fields.newDimension()
    .setId('platform')
    .setName('Plateforme')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('date')
    .setName('Date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('account_name')
    .setName('Nom Organisation LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('account_id')
    .setName('ID Organisation LinkedIn')
    .setType(types.TEXT);
  
  // DIMENSIONS POSTS LINKEDIN
  fields.newDimension()
    .setId('post_id')
    .setName('ID Post LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_type')
    .setName('Type Publication LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_creation_date')
    .setName('Date Publication LinkedIn')
    .setType(types.YEAR_MONTH_DAY_HOUR);
  
  fields.newDimension()
    .setId('post_text')
    .setName('Texte Publication LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_type')
    .setName('Type Média LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('is_reshare')
    .setName('Est un Repost LinkedIn')
    .setType(types.BOOLEAN);
  
  // DIMENSIONS BREAKDOWN
  fields.newDimension()
    .setId('breakdown_type')
    .setName('Type Breakdown LinkedIn')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('breakdown_value')
    .setName('Valeur Breakdown LinkedIn')
    .setType(types.TEXT);
  
  // ============================
  // MÉTRIQUES PAGE LINKEDIN
  // ============================
  
  fields.newMetric()
    .setId('total_page_views')
    .setName('LinkedIn - Vues Page Totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('unique_page_views')
    .setName('LinkedIn - Vues Page Uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('desktop_page_views')
    .setName('LinkedIn - Vues Page Desktop')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('mobile_page_views')
    .setName('LinkedIn - Vues Page Mobile')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('overview_page_views')
    .setName('LinkedIn - Vues Page Accueil')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('about_page_views')
    .setName('LinkedIn - Vues Page À Propos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('people_page_views')
    .setName('LinkedIn - Vues Page Employés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('jobs_page_views')
    .setName('LinkedIn - Vues Page Emplois')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('careers_page_views')
    .setName('LinkedIn - Vues Page Carrières')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('life_at_page_views')
    .setName('LinkedIn - Vues Page Vie Entreprise')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('desktop_button_clicks')
    .setName('LinkedIn - Clics Boutons Desktop')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('mobile_button_clicks')
    .setName('LinkedIn - Clics Boutons Mobile')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_button_clicks')
    .setName('LinkedIn - Clics Boutons Total')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES FOLLOWERS LINKEDIN
  // ============================
  
  fields.newMetric()
    .setId('total_followers')
    .setName('LinkedIn - Total Abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('organic_follower_gain')
    .setName('LinkedIn - Nouveaux Abonnés Organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('paid_follower_gain')
    .setName('LinkedIn - Nouveaux Abonnés Payants')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_follower_gain')
    .setName('LinkedIn - Nouveaux Abonnés Total')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES POSTS LINKEDIN
  // ============================
  
  fields.newMetric()
    .setId('post_impressions')
    .setName('LinkedIn - Affichages Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_unique_impressions')
    .setName('LinkedIn - Affichages Uniques Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_clicks')
    .setName('LinkedIn - Clics Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_shares')
    .setName('LinkedIn - Partages Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_comments')
    .setName('LinkedIn - Commentaires Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_engagement_rate')
    .setName('LinkedIn - Taux Engagement Post')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('post_click_through_rate')
    .setName('LinkedIn - Taux de Clic Post')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES RÉACTIONS LINKEDIN
  // ============================
  
  fields.newMetric()
    .setId('reactions_like')
    .setName('LinkedIn - Réactions J\'aime')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_celebrate')
    .setName('LinkedIn - Réactions Bravo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_love')
    .setName('LinkedIn - Réactions J\'adore')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_insightful')
    .setName('LinkedIn - Réactions Instructif')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_support')
    .setName('LinkedIn - Réactions Soutien')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_funny')
    .setName('LinkedIn - Réactions Amusant')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_reactions')
    .setName('LinkedIn - Total Réactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // POURCENTAGES RÉACTIONS
  fields.newMetric()
    .setId('like_percentage')
    .setName('LinkedIn - % J\'aime')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('celebrate_percentage')
    .setName('LinkedIn - % Bravo')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('love_percentage')
    .setName('LinkedIn - % J\'adore')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('insightful_percentage')
    .setName('LinkedIn - % Instructif')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('support_percentage')
    .setName('LinkedIn - % Soutien')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('funny_percentage')
    .setName('LinkedIn - % Amusant')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES CALCULÉES
  // ============================
  
  fields.newMetric()
    .setId('total_interactions')
    .setName('LinkedIn - Total Interactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('interaction_rate')
    .setName('LinkedIn - Taux Interaction')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('avg_reactions_per_post')
    .setName('LinkedIn - Réactions Moyennes par Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('reach_rate')
    .setName('LinkedIn - Taux de Portée')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES BREAKDOWN FOLLOWERS
  // ============================
  
  fields.newMetric()
    .setId('followers_by_country')
    .setName('LinkedIn - Abonnés par Pays')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_by_industry')
    .setName('LinkedIn - Abonnés par Industrie')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_by_function')
    .setName('LinkedIn - Abonnés par Fonction')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_by_seniority')
    .setName('LinkedIn - Abonnés par Ancienneté')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_by_company_size')
    .setName('LinkedIn - Abonnés par Taille Entreprise')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  return fields;
}

// ================================
// 5. RÉCUPÉRATION DES DONNÉES
// ================================

function getData(request) {
  console.log('=== getData LinkedIn Only - Début ===');
  
  try {
    var userEmail = Session.getActiveUser().getEmail();
    console.log('Email utilisateur:', userEmail);
    
    // Vérifier l'abonnement LinkedIn Only
    var subscriptionCheck = checkUserSubscription(userEmail);
    
    if (!subscriptionCheck.valid) {
      console.error('Abonnement invalide:', subscriptionCheck.error);
      return {
        schema: [],
        rows: [],
        error: 'Abonnement LinkedIn non valide: ' + subscriptionCheck.error
      };
    }
    
    console.log('Abonnement LinkedIn valide, récupération des données...');
    
    // Récupérer les données LinkedIn depuis l'API
    var apiData = fetchLinkedInData(request, userEmail);
    
    if (!apiData || !apiData.success) {
      console.error('Erreur récupération données:', apiData ? apiData.error : 'Pas de données');
      return {
        schema: [],
        rows: [],
        error: 'Erreur lors de la récupération des données LinkedIn'
      };
    }
    
    // Transformer les données pour Looker Studio
    var transformedData = transformLinkedInData(apiData.data, request);
    
    console.log('Données transformées:', transformedData.rows.length, 'lignes');
    
    return {
      schema: transformedData.schema,
      rows: transformedData.rows
    };
    
  } catch (e) {
    console.error('Erreur getData:', e);
    
    // Gestion des redirections
    if (e.message.startsWith('REDIRECT_TO_SIGNUP:')) {
      var redirectUrl = e.message.split(':')[1];
      var cc = DataStudioApp.createCommunityConnector();
      cc.newUserError()
        .setDebugText('Redirection vers inscription')
        .setText('Veuillez vous inscrire pour accéder aux données LinkedIn: ' + redirectUrl)
        .throwException();
    } else if (e.message.startsWith('REDIRECT_TO_UPGRADE:')) {
      var redirectUrl = e.message.split(':')[1];
      var cc = DataStudioApp.createCommunityConnector();
      cc.newUserError()
        .setDebugText('Redirection vers mise à niveau')
        .setText('Veuillez mettre à niveau votre abonnement LinkedIn: ' + redirectUrl)
        .throwException();
    }
    
    var cc = DataStudioApp.createCommunityConnector();
    cc.newUserError()
      .setDebugText('Erreur générale: ' + e.toString())
      .setText('Erreur lors de la récupération des données LinkedIn')
      .throwException();
  }
}

/**
 * Récupère les données LinkedIn depuis l'API backend
 */
function fetchLinkedInData(request, userEmail) {
  try {
    var params = {
      platforms: ['linkedin'],
      date_range: request.configParams.date_range || '30',
      metrics_type: request.configParams.metrics_type || 'overview',
      include_reactions: request.configParams.include_reactions || false,
      include_breakdown: request.configParams.include_breakdown || false
    };
    
    var response = UrlFetchApp.fetch(API_BASE_URL + '/api/v1/linkedin/metrics', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + userEmail // Utiliser l'email comme token temporaire
      },
      payload: JSON.stringify(params),
      muteHttpExceptions: true
    });
    
    var responseCode = response.getResponseCode();
    var data = JSON.parse(response.getContentText());
    
    console.log('Réponse API LinkedIn:', responseCode);
    
    if (responseCode === 200) {
      return { success: true, data: data };
    } else {
      return { success: false, error: data.message || 'Erreur API' };
    }
    
  } catch (e) {
    console.error('Erreur fetchLinkedInData:', e);
    return { success: false, error: e.toString() };
  }
}

/**
 * Transforme les données LinkedIn pour Looker Studio
 */
function transformLinkedInData(apiData, request) {
  console.log('Transformation des données LinkedIn...');
  
  var requestedFields = request.fields || [];
  var rows = [];
  
  // Si pas de données, retourner structure vide
  if (!apiData || !apiData.linkedin_data) {
    console.log('Pas de données LinkedIn à transformer');
    return {
      schema: getFieldsFromRequest(requestedFields),
      rows: []
    };
  }
  
  var linkedinData = apiData.linkedin_data;
  
  // Traiter les différents types de données LinkedIn
  if (linkedinData.page_metrics) {
    rows = rows.concat(transformPageMetrics(linkedinData.page_metrics, requestedFields));
  }
  
  if (linkedinData.post_metrics) {
    rows = rows.concat(transformPostMetrics(linkedinData.post_metrics, requestedFields));
  }
  
  if (linkedinData.follower_metrics) {
    rows = rows.concat(transformFollowerMetrics(linkedinData.follower_metrics, requestedFields));
  }
  
  if (linkedinData.breakdown_data) {
    rows = rows.concat(transformBreakdownData(linkedinData.breakdown_data, requestedFields));
  }
  
  console.log('Transformation terminée:', rows.length, 'lignes générées');
  
  return {
    schema: getFieldsFromRequest(requestedFields),
    rows: rows
  };
}

/**
 * Transforme les métriques de page LinkedIn
 */
function transformPageMetrics(pageMetrics, requestedFields) {
  var rows = [];
  
  if (!pageMetrics || !Array.isArray(pageMetrics)) {
    return rows;
  }
  
  pageMetrics.forEach(function(metric) {
    var row = {};
    
    // Ajouter les valeurs selon les champs demandés
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'linkedin';
          break;
        case 'date':
          row[field.getId()] = metric.date || new Date().toISOString().split('T')[0];
          break;
        case 'account_name':
          row[field.getId()] = metric.account_name || 'LinkedIn Page';
          break;
        case 'account_id':
          row[field.getId()] = metric.account_id || '';
          break;
        case 'total_page_views':
          row[field.getId()] = metric.total_page_views || 0;
          break;
        case 'unique_page_views':
          row[field.getId()] = metric.unique_page_views || 0;
          break;
        case 'desktop_page_views':
          row[field.getId()] = metric.desktop_page_views || 0;
          break;
        case 'mobile_page_views':
          row[field.getId()] = metric.mobile_page_views || 0;
          break;
        case 'total_button_clicks':
          row[field.getId()] = (metric.desktop_button_clicks || 0) + (metric.mobile_button_clicks || 0);
          break;
        default:
          row[field.getId()] = metric[field.getId()] || 0;
      }
    });
    
    rows.push({ values: Object.values(row) });
  });
  
  return rows;
}

/**
 * Transforme les métriques de posts LinkedIn
 */
function transformPostMetrics(postMetrics, requestedFields) {
  var rows = [];
  
  if (!postMetrics || !Array.isArray(postMetrics)) {
    return rows;
  }
  
  postMetrics.forEach(function(post) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'linkedin';
          break;
        case 'date':
          row[field.getId()] = post.date || new Date().toISOString().split('T')[0];
          break;
        case 'post_id':
          row[field.getId()] = post.post_id || '';
          break;
        case 'post_type':
          row[field.getId()] = post.post_type || 'ugcPost';
          break;
        case 'post_text':
          row[field.getId()] = (post.post_text || '').substring(0, 100);
          break;
        case 'total_reactions':
          var totalReactions = (post.reactions_like || 0) + (post.reactions_celebrate || 0) + 
                              (post.reactions_love || 0) + (post.reactions_insightful || 0) + 
                              (post.reactions_support || 0) + (post.reactions_funny || 0);
          row[field.getId()] = totalReactions;
          break;
        case 'total_interactions':
          var totalInteractions = (post.post_clicks || 0) + (post.post_shares || 0) + 
                                 (post.post_comments || 0) + (post.total_reactions || 0);
          row[field.getId()] = totalInteractions;
          break;
        case 'interaction_rate':
          var impressions = post.post_impressions || 1;
          var interactions = (post.post_clicks || 0) + (post.post_shares || 0) + 
                           (post.post_comments || 0) + (post.total_reactions || 0);
          row[field.getId()] = impressions > 0 ? (interactions / impressions) * 100 : 0;
          break;
        case 'like_percentage':
          var totalReacts = post.total_reactions || 1;
          row[field.getId()] = totalReacts > 0 ? ((post.reactions_like || 0) / totalReacts) * 100 : 0;
          break;
        default:
          row[field.getId()] = post[field.getId()] || 0;
      }
    });
    
    rows.push({ values: Object.values(row) });
  });
  
  return rows;
}

/**
 * Transforme les métriques de followers LinkedIn
 */
function transformFollowerMetrics(followerMetrics, requestedFields) {
  var rows = [];
  
  if (!followerMetrics || !Array.isArray(followerMetrics)) {
    return rows;
  }
  
  followerMetrics.forEach(function(metric) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'linkedin';
          break;
        case 'date':
          row[field.getId()] = metric.date || new Date().toISOString().split('T')[0];
          break;
        case 'total_follower_gain':
          row[field.getId()] = (metric.organic_follower_gain || 0) + (metric.paid_follower_gain || 0);
          break;
        default:
          row[field.getId()] = metric[field.getId()] || 0;
      }
    });
    
    rows.push({ values: Object.values(row) });
  });
  
  return rows;
}

/**
 * Transforme les données de breakdown LinkedIn
 */
function transformBreakdownData(breakdownData, requestedFields) {
  var rows = [];
  
  if (!breakdownData || !Array.isArray(breakdownData)) {
    return rows;
  }
  
  breakdownData.forEach(function(breakdown) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'linkedin';
          break;
        case 'breakdown_type':
          row[field.getId()] = breakdown.breakdown_type || '';
          break;
        case 'breakdown_value':
          row[field.getId()] = breakdown.breakdown_value || '';
          break;
        default:
          row[field.getId()] = breakdown[field.getId()] || 0;
      }
    });
    
    rows.push({ values: Object.values(row) });
  });
  
  return rows;
}

/**
 * Récupère les champs demandés dans la requête
 */
function getFieldsFromRequest(requestedFields) {
  return requestedFields.map(function(field) {
    return {
      name: field.getId(),
      dataType: field.getType()
    };
  });
}