/**
 * WhatsTheData - Connecteur Looker Studio FACEBOOK ONLY
 * Toutes les métriques Facebook avec abonnement Stripe intégré
 * ID Connecteur: AKfycbx4B4ncfzfu8ntW9qEz2i_ValbjxiqU4yWISeETRmdCv--oq69xaVrlskRjt1Pj7Wqncw
 * Offre Stripe: price_1RyhoWJoIj8R31C3uiSRLcw8 (Facebook Only - 29€/mois)
 */

// ================================
// 1. CONFIGURATION DE L'API
// ================================

var API_BASE_URL = 'https://whats-the-data-d954d4d4cb5f.herokuapp.com';
var CONNECTOR_ID = 'AKfycbx4B4ncfzfu8ntW9qEz2i_ValbjxiqU4yWISeETRmdCv--oq69xaVrlskRjt1Pj7Wqncw';

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
  console.log('=== isAuthValid - Facebook Only ===');
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
 * Vérifie l'abonnement Facebook Only de l'utilisateur
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
        platform: 'facebook'
      }),
      muteHttpExceptions: true
    });
    
    var responseCode = response.getResponseCode();
    var data = JSON.parse(response.getContentText());
    
    console.log('Réponse checkUserSubscription:', responseCode, data);
    
    if (responseCode === 200 && data.valid) {
      return { valid: true, user: data.user };
    } else if (responseCode === 404) {
      // Redirection vers inscription Facebook Only
      var redirectUrl = API_BASE_URL + '/connect?source=looker&email=' + 
                       encodeURIComponent(userEmail) + '&connector=' + CONNECTOR_ID;
      throw new Error('REDIRECT_TO_SIGNUP:' + redirectUrl);
    } else if (responseCode === 403) {
      // Redirection vers mise à niveau Facebook Only
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
    .setText('📘 WhatsTheData FACEBOOK - Toutes les métriques Facebook depuis votre page');
  
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
    .setName('Type de métriques Facebook')
    .addOption(config.newOptionBuilder().setLabel('Vue d\'ensemble (pages + posts)').setValue('overview'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de pages uniquement').setValue('pages'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de posts uniquement').setValue('posts'))
    .addOption(config.newOptionBuilder().setLabel('Métriques vidéo détaillées').setValue('video_detailed'))
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_reactions')
    .setName('Inclure détail des réactions Facebook')
    .setHelpText('Like, Love, Wow, Haha, Sorry, Anger')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_video_metrics')
    .setName('Inclure métriques vidéo avancées')
    .setHelpText('Vues complètes, temps de visionnage, retention')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_breakdown')
    .setName('Inclure breakdown démographique')
    .setHelpText('Segmentation par source de trafic et données démographiques')
    .setAllowOverride(true);
  
  return config.build();
}

// ================================
// 4. SCHÉMA COMPLET FACEBOOK
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
    .setName('Nom Page Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('account_id')
    .setName('ID Page Facebook')
    .setType(types.TEXT);
  
  // DIMENSIONS POSTS FACEBOOK
  fields.newDimension()
    .setId('post_id')
    .setName('ID Post Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('status_type')
    .setName('Type Publication Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('created_time')
    .setName('Date Publication Facebook')
    .setType(types.YEAR_MONTH_DAY_HOUR);
  
  fields.newDimension()
    .setId('message')
    .setName('Message Publication Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('permalink_url')
    .setName('Lien Permanent Facebook')
    .setType(types.URL);
  
  fields.newDimension()
    .setId('full_picture')
    .setName('Image Publication Facebook')
    .setType(types.URL);
  
  // ============================
  // MÉTRIQUES PAGE FACEBOOK
  // ============================
  
  // Impressions Page
  fields.newMetric()
    .setId('page_impressions')
    .setName('Facebook - Affichages de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_unique')
    .setName('Facebook - Visiteurs de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_nonviral')
    .setName('Facebook - Affichages non viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_viral')
    .setName('Facebook - Affichages viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Impressions Posts de la Page
  fields.newMetric()
    .setId('page_posts_impressions')
    .setName('Facebook - Affichages des publications')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_unique')
    .setName('Facebook - Visiteurs de la publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_paid')
    .setName('Facebook - Affichages publicitaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_organic')
    .setName('Facebook - Affichages organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_organic_unique')
    .setName('Facebook - Visiteurs uniques organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_views_total')
    .setName('Facebook - Vues totales de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES FANS FACEBOOK
  // ============================
  
  fields.newMetric()
    .setId('page_fans')
    .setName('Facebook - Nombre de fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('page_fan_adds')
    .setName('Facebook - Nouveaux fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_fan_removes')
    .setName('Facebook - Fans perdus')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_follows')
    .setName('Facebook - Nombre d\'abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('page_daily_follows')
    .setName('Facebook - Nouveaux abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_daily_unfollows')
    .setName('Facebook - Désabonnements')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES VIDÉO PAGE FACEBOOK
  // ============================
  
  fields.newMetric()
    .setId('page_video_views')
    .setName('Facebook - Vues de vidéos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_views_unique')
    .setName('Facebook - Vues uniques de vidéos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_views_paid')
    .setName('Facebook - Vues vidéos via pub')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_views_organic')
    .setName('Facebook - Vues vidéos organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_view_time')
    .setName('Facebook - Temps de visionnage (sec)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_complete_views_30s')
    .setName('Facebook - Vues complètes (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_video_complete_views_30s_unique')
    .setName('Facebook - Vues complètes uniques (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES ENGAGEMENT PAGE FACEBOOK
  // ============================
  
  fields.newMetric()
    .setId('page_post_engagements')
    .setName('Facebook - Interactions sur publications')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_total_actions')
    .setName('Facebook - Actions totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Réactions Page Facebook
  fields.newMetric()
    .setId('page_actions_post_reactions_like_total')
    .setName('Facebook Page - Nombre de "J\'aime"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_actions_post_reactions_love_total')
    .setName('Facebook Page - Nombre de "J\'adore"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_actions_post_reactions_wow_total')
    .setName('Facebook Page - Nombre de "Wow"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_actions_post_reactions_haha_total')
    .setName('Facebook Page - Nombre de "Haha"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_actions_post_reactions_sorry_total')
    .setName('Facebook Page - Nombre de "Triste"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_actions_post_reactions_anger_total')
    .setName('Facebook Page - Nombre de "En colère"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES POSTS FACEBOOK
  // ============================
  
  // Impressions Posts
  fields.newMetric()
    .setId('post_impressions')
    .setName('Facebook - Affichages publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_impressions_organic')
    .setName('Facebook - Affichages organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_impressions_paid')
    .setName('Facebook - Affichages sponsorisés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_impressions_viral')
    .setName('Facebook - Affichages viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_impressions_unique')
    .setName('Facebook - Visiteurs de la publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Réactions Posts Facebook
  fields.newMetric()
    .setId('post_reactions_like_total')
    .setName('Facebook Post - Nombre de "J\'aime"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reactions_love_total')
    .setName('Facebook Post - Nombre de "J\'adore"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reactions_wow_total')
    .setName('Facebook Post - Nombre de "Wow"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reactions_haha_total')
    .setName('Facebook Post - Nombre de "Haha"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reactions_sorry_total')
    .setName('Facebook Post - Nombre de "Triste"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reactions_anger_total')
    .setName('Facebook Post - Nombre de "En colère"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Clics et Engagement Posts
  fields.newMetric()
    .setId('post_clicks')
    .setName('Facebook - Nombre de clics')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_consumptions')
    .setName('Facebook - Interactions totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_fan_reach')
    .setName('Facebook - Portée fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Activités Posts détaillées
  fields.newMetric()
    .setId('post_activity_by_action_type_share')
    .setName('Facebook - Partages')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_activity_by_action_type_comment')
    .setName('Facebook - Nombre de commentaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES VIDÉO POSTS FACEBOOK
  // ============================
  
  fields.newMetric()
    .setId('post_video_views')
    .setName('Facebook - Vues vidéo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_video_views_organic')
    .setName('Facebook - Vues vidéo organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_video_views_paid')
    .setName('Facebook - Vues vidéo sponsorisées')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_video_views_unique')
    .setName('Facebook - Visiteurs vidéo uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_video_complete_views_30s')
    .setName('Facebook - Vues complètes (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_video_avg_time_watched')
    .setName('Facebook - Temps moyen visionné')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('post_video_view_time')
    .setName('Facebook - Durée totale visionnage')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES CALCULÉES FACEBOOK
  // ============================
  
  fields.newMetric()
    .setId('taux_engagement_page')
    .setName('Facebook - Taux d\'engagement page (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('reactions_positives')
    .setName('Facebook - Réactions positives')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_negatives')
    .setName('Facebook - Réactions négatives')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_reactions')
    .setName('Facebook - Total réactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('taux_de_clic')
    .setName('Facebook - Taux de clic (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('vtr_percentage')
    .setName('Facebook - VTR (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES MÉTADONNÉES
  // ============================
  
  fields.newMetric()
    .setId('comments_count')
    .setName('Facebook - Nombre de commentaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('likes_count')
    .setName('Facebook - Nombre de J\'aime')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('shares_count')
    .setName('Facebook - Nombre de partages')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  return fields;
}

// ================================
// 5. RÉCUPÉRATION DES DONNÉES
// ================================

function getData(request) {
  console.log('=== getData Facebook Only - Début ===');
  
  try {
    var userEmail = Session.getActiveUser().getEmail();
    console.log('Email utilisateur:', userEmail);
    
    // Vérifier l'abonnement Facebook Only
    var subscriptionCheck = checkUserSubscription(userEmail);
    
    if (!subscriptionCheck.valid) {
      console.error('Abonnement invalide:', subscriptionCheck.error);
      return {
        schema: [],
        rows: [],
        error: 'Abonnement Facebook non valide: ' + subscriptionCheck.error
      };
    }
    
    console.log('Abonnement Facebook valide, récupération des données...');
    
    // Récupérer les données Facebook depuis l'API
    var apiData = fetchFacebookData(request, userEmail);
    
    if (!apiData || !apiData.success) {
      console.error('Erreur récupération données:', apiData ? apiData.error : 'Pas de données');
      return {
        schema: [],
        rows: [],
        error: 'Erreur lors de la récupération des données Facebook'
      };
    }
    
    // Transformer les données pour Looker Studio
    var transformedData = transformFacebookData(apiData.data, request);
    
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
        .setText('Veuillez vous inscrire pour accéder aux données Facebook: ' + redirectUrl)
        .throwException();
    } else if (e.message.startsWith('REDIRECT_TO_UPGRADE:')) {
      var redirectUrl = e.message.split(':')[1];
      var cc = DataStudioApp.createCommunityConnector();
      cc.newUserError()
        .setDebugText('Redirection vers mise à niveau')
        .setText('Veuillez mettre à niveau votre abonnement Facebook: ' + redirectUrl)
        .throwException();
    }
    
    var cc = DataStudioApp.createCommunityConnector();
    cc.newUserError()
      .setDebugText('Erreur générale: ' + e.toString())
      .setText('Erreur lors de la récupération des données Facebook')
      .throwException();
  }
}

/**
 * Récupère les données Facebook depuis l'API backend
 */
function fetchFacebookData(request, userEmail) {
  try {
    var params = {
      platforms: ['facebook'],
      date_range: request.configParams.date_range || '30',
      metrics_type: request.configParams.metrics_type || 'overview',
      include_reactions: request.configParams.include_reactions || false,
      include_video_metrics: request.configParams.include_video_metrics || false,
      include_breakdown: request.configParams.include_breakdown || false
    };
    
    var response = UrlFetchApp.fetch(API_BASE_URL + '/api/v1/facebook/metrics', {
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
    
    console.log('Réponse API Facebook:', responseCode);
    
    if (responseCode === 200) {
      return { success: true, data: data };
    } else {
      return { success: false, error: data.message || 'Erreur API' };
    }
    
  } catch (e) {
    console.error('Erreur fetchFacebookData:', e);
    return { success: false, error: e.toString() };
  }
}

/**
 * Transforme les données Facebook pour Looker Studio
 */
function transformFacebookData(apiData, request) {
  console.log('Transformation des données Facebook...');
  
  var requestedFields = request.fields || [];
  var rows = [];
  
  // Si pas de données, retourner structure vide
  if (!apiData || !apiData.facebook_data) {
    console.log('Pas de données Facebook à transformer');
    return {
      schema: getFieldsFromRequest(requestedFields),
      rows: []
    };
  }
  
  var facebookData = apiData.facebook_data;
  
  // Traiter les différents types de données Facebook
  if (facebookData.page_metrics) {
    rows = rows.concat(transformPageMetrics(facebookData.page_metrics, requestedFields));
  }
  
  if (facebookData.post_metrics) {
    rows = rows.concat(transformPostMetrics(facebookData.post_metrics, requestedFields));
  }
  
  if (facebookData.fan_metrics) {
    rows = rows.concat(transformFanMetrics(facebookData.fan_metrics, requestedFields));
  }
  
  if (facebookData.video_metrics) {
    rows = rows.concat(transformVideoMetrics(facebookData.video_metrics, requestedFields));
  }
  
  console.log('Transformation terminée:', rows.length, 'lignes générées');
  
  return {
    schema: getFieldsFromRequest(requestedFields),
    rows: rows
  };
}

/**
 * Transforme les métriques de page Facebook
 */
function transformPageMetrics(pageMetrics, requestedFields) {
  var rows = [];
  
  if (!pageMetrics || !Array.isArray(pageMetrics)) {
    return rows;
  }
  
  pageMetrics.forEach(function(metric) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'facebook';
          break;
        case 'date':
          row[field.getId()] = metric.date || new Date().toISOString().split('T')[0];
          break;
        case 'account_name':
          row[field.getId()] = metric.account_name || 'Facebook Page';
          break;
        case 'account_id':
          row[field.getId()] = metric.account_id || '';
          break;
        case 'taux_engagement_page':
          var impressions = metric.page_impressions || 1;
          var engagements = metric.page_post_engagements || 0;
          row[field.getId()] = impressions > 0 ? (engagements / impressions) * 100 : 0;
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
 * Transforme les métriques de posts Facebook
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
          row[field.getId()] = 'facebook';
          break;
        case 'date':
          row[field.getId()] = post.date || new Date().toISOString().split('T')[0];
          break;
        case 'post_id':
          row[field.getId()] = post.post_id || '';
          break;
        case 'status_type':
          row[field.getId()] = post.status_type || 'status_update';
          break;
        case 'message':
          row[field.getId()] = (post.message || '').substring(0, 100);
          break;
        case 'total_reactions':
          var totalReactions = (post.post_reactions_like_total || 0) + 
                              (post.post_reactions_love_total || 0) + 
                              (post.post_reactions_wow_total || 0) + 
                              (post.post_reactions_haha_total || 0) + 
                              (post.post_reactions_sorry_total || 0) + 
                              (post.post_reactions_anger_total || 0);
          row[field.getId()] = totalReactions;
          break;
        case 'reactions_positives':
          var positive = (post.post_reactions_like_total || 0) + 
                        (post.post_reactions_love_total || 0) + 
                        (post.post_reactions_wow_total || 0) + 
                        (post.post_reactions_haha_total || 0);
          row[field.getId()] = positive;
          break;
        case 'reactions_negatives':
          var negative = (post.post_reactions_sorry_total || 0) + 
                        (post.post_reactions_anger_total || 0);
          row[field.getId()] = negative;
          break;
        case 'taux_de_clic':
          var impressions = post.post_impressions || 1;
          var clicks = post.post_clicks || 0;
          row[field.getId()] = impressions > 0 ? (clicks / impressions) * 100 : 0;
          break;
        case 'vtr_percentage':
          var videoViews = post.post_video_views || 0;
          var videoCompleteViews = post.post_video_complete_views_30s || 0;
          row[field.getId()] = videoViews > 0 ? (videoCompleteViews / videoViews) * 100 : 0;
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
 * Transforme les métriques de fans Facebook
 */
function transformFanMetrics(fanMetrics, requestedFields) {
  var rows = [];
  
  if (!fanMetrics || !Array.isArray(fanMetrics)) {
    return rows;
  }
  
  fanMetrics.forEach(function(metric) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'facebook';
          break;
        case 'date':
          row[field.getId()] = metric.date || new Date().toISOString().split('T')[0];
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
 * Transforme les métriques vidéo Facebook
 */
function transformVideoMetrics(videoMetrics, requestedFields) {
  var rows = [];
  
  if (!videoMetrics || !Array.isArray(videoMetrics)) {
    return rows;
  }
  
  videoMetrics.forEach(function(metric) {
    var row = {};
    
    requestedFields.forEach(function(field) {
      switch (field.getId()) {
        case 'platform':
          row[field.getId()] = 'facebook';
          break;
        case 'date':
          row[field.getId()] = metric.date || new Date().toISOString().split('T')[0];
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