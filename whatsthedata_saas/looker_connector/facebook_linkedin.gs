/**
 * WhatsTheData - Connecteur Looker Studio LINKEDIN + FACEBOOK
 * Toutes les métriques LinkedIn ET Facebook avec abonnement Stripe intégré
 * ID Connecteur: AKfycbyNlF25yTJzlO3j63xMX5ccUVnOaF2J6H4VX_bN4uJeZVYDiCv4zy1ojDrshmTR5nL-
 * Offre Stripe: price_1RyhpiJoIj8R31C3EmVclb8P (LinkedIn + Facebook - 49€/mois)
 */

// ================================
// 1. CONFIGURATION DE L'API
// ================================

var API_BASE_URL = 'https://whats-the-data-d954d4d4cb5f.herokuapp.com';
var CONNECTOR_ID = 'AKfycbyNlF25yTJzlO3j63xMX5ccUVnOaF2J6H4VX_bN4uJeZVYDiCv4zy1ojDrshmTR5nL-';

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
  console.log('=== isAuthValid - LinkedIn + Facebook ===');
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
 * Vérifie l'abonnement LinkedIn + Facebook de l'utilisateur
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
        platforms: ['linkedin', 'facebook']
      }),
      muteHttpExceptions: true
    });
    
    var responseCode = response.getResponseCode();
    var data = JSON.parse(response.getContentText());
    
    console.log('Réponse checkUserSubscription:', responseCode, data);
    
    if (responseCode === 200 && data.valid) {
      return { valid: true, user: data.user };
    } else if (responseCode === 404) {
      // Redirection vers inscription LinkedIn + Facebook
      var redirectUrl = API_BASE_URL + '/connect?source=looker&email=' + 
                       encodeURIComponent(userEmail) + '&connector=' + CONNECTOR_ID;
      throw new Error('REDIRECT_TO_SIGNUP:' + redirectUrl);
    } else if (responseCode === 403) {
      // Redirection vers mise à niveau LinkedIn + Facebook
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
    .setText('📊 WhatsTheData COMPLET - Toutes les métriques LinkedIn & Facebook combinées');
  
  config
    .newSelectMultiple()
    .setId('platforms')
    .setName('Plateformes à inclure')
    .setHelpText('Sélectionnez LinkedIn et/ou Facebook (Premium inclut les deux)')
    .addOption(config.newOptionBuilder().setLabel('LinkedIn').setValue('linkedin'))
    .addOption(config.newOptionBuilder().setLabel('Facebook').setValue('facebook'))
    .setAllowOverride(true);
  
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
    .setName('Type de métriques')
    .addOption(config.newOptionBuilder().setLabel('Vue d\'ensemble (pages + posts)').setValue('overview'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de pages uniquement').setValue('pages'))
    .addOption(config.newOptionBuilder().setLabel('Métriques de posts uniquement').setValue('posts'))
    .addOption(config.newOptionBuilder().setLabel('Breakdown followers détaillé').setValue('followers_breakdown'))
    .addOption(config.newOptionBuilder().setLabel('Métriques vidéo avancées').setValue('video_detailed'))
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_linkedin_reactions')
    .setName('Inclure réactions LinkedIn détaillées')
    .setHelpText('Like, Celebrate, Love, Insightful, Support, Funny')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_facebook_reactions')
    .setName('Inclure réactions Facebook détaillées')
    .setHelpText('Like, Love, Wow, Haha, Sorry, Anger')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_video_metrics')
    .setName('Inclure métriques vidéo avancées')
    .setHelpText('Vues complètes, temps de visionnage, VTR')
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_breakdown')
    .setName('Inclure breakdown démographique')
    .setHelpText('Segmentation par pays, industrie, séniorité, etc.')
    .setAllowOverride(true);
  
  return config.build();
}

// ================================
// 4. SCHÉMA COMPLET LINKEDIN + FACEBOOK
// ================================

function getSchema(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // ============================
  // DIMENSIONS COMMUNES
  // ============================
  
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
    .setName('Nom du Compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('account_id')
    .setName('ID du Compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('content_type')
    .setName('Type de Contenu')
    .setType(types.TEXT);
  
  // ============================
  // DIMENSIONS POSTS
  // ============================
  
  fields.newDimension()
    .setId('post_id')
    .setName('ID Post')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_type')
    .setName('Type de Post')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_creation_date')
    .setName('Date de Publication')
    .setType(types.YEAR_MONTH_DAY_HOUR);
  
  fields.newDimension()
    .setId('post_text')
    .setName('Texte du Post')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_type')
    .setName('Type de Média')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_url')
    .setName('URL Média')
    .setType(types.URL);
  
  fields.newDimension()
    .setId('permalink_url')
    .setName('Lien Permanent')
    .setType(types.URL);
  
  // Dimensions spécifiques Facebook
  fields.newDimension()
    .setId('status_type')
    .setName('Type Statut Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('message')
    .setName('Message Facebook')
    .setType(types.TEXT);
  
  // Dimensions spécifiques LinkedIn
  fields.newDimension()
    .setId('is_reshare')
    .setName('Est un Repost LinkedIn')
    .setType(types.BOOLEAN);
  
  // ============================
  // DIMENSIONS BREAKDOWN
  // ============================
  
  fields.newDimension()
    .setId('breakdown_type')
    .setName('Type de Breakdown')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('breakdown_value')
    .setName('Valeur Breakdown')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('breakdown_label')
    .setName('Label Breakdown')
    .setType(types.TEXT);
  
  // ============================
  // MÉTRIQUES FOLLOWERS/FANS GLOBALES
  // ============================
  
  fields.newMetric()
    .setId('total_followers')
    .setName('Total Followers/Fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('new_followers')
    .setName('Nouveaux Followers/Fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_lost')
    .setName('Followers/Fans Perdus')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES LINKEDIN SPÉCIFIQUES
  // ============================
  
  // Pages LinkedIn
  fields.newMetric()
    .setId('linkedin_total_page_views')
    .setName('LinkedIn - Vues Page Totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_unique_page_views')
    .setName('LinkedIn - Vues Page Uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_desktop_page_views')
    .setName('LinkedIn - Vues Page Desktop')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_mobile_page_views')
    .setName('LinkedIn - Vues Page Mobile')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_overview_page_views')
    .setName('LinkedIn - Vues Page Accueil')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_about_page_views')
    .setName('LinkedIn - Vues Page À Propos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_people_page_views')
    .setName('LinkedIn - Vues Page Employés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_jobs_page_views')
    .setName('LinkedIn - Vues Page Emplois')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_careers_page_views')
    .setName('LinkedIn - Vues Page Carrières')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_life_at_page_views')
    .setName('LinkedIn - Vues Page Vie Entreprise')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_total_button_clicks')
    .setName('LinkedIn - Clics Boutons Total')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Followers LinkedIn
  fields.newMetric()
    .setId('linkedin_total_followers')
    .setName('LinkedIn - Total Abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('linkedin_organic_follower_gain')
    .setName('LinkedIn - Nouveaux Abonnés Organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_paid_follower_gain')
    .setName('LinkedIn - Nouveaux Abonnés Payants')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Posts LinkedIn
  fields.newMetric()
    .setId('linkedin_post_impressions')
    .setName('LinkedIn - Affichages Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_unique_impressions')
    .setName('LinkedIn - Affichages Uniques Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_clicks')
    .setName('LinkedIn - Clics Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_shares')
    .setName('LinkedIn - Partages Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_comments')
    .setName('LinkedIn - Commentaires Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_engagement_rate')
    .setName('LinkedIn - Taux Engagement Post')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // Réactions LinkedIn
  fields.newMetric()
    .setId('linkedin_reactions_like')
    .setName('LinkedIn - Réactions J\'aime')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_celebrate')
    .setName('LinkedIn - Réactions Bravo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_love')
    .setName('LinkedIn - Réactions J\'adore')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_insightful')
    .setName('LinkedIn - Réactions Instructif')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_support')
    .setName('LinkedIn - Réactions Soutien')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_funny')
    .setName('LinkedIn - Réactions Amusant')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_total_reactions')
    .setName('LinkedIn - Total Réactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_like_percentage')
    .setName('LinkedIn - % J\'aime')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_celebrate_percentage')
    .setName('LinkedIn - % Bravo')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_total_interactions')
    .setName('LinkedIn - Total Interactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES FACEBOOK SPÉCIFIQUES
  // ============================
  
  // Pages Facebook
  fields.newMetric()
    .setId('facebook_page_impressions')
    .setName('Facebook - Affichages de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_impressions_unique')
    .setName('Facebook - Visiteurs de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_impressions_viral')
    .setName('Facebook - Affichages viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_impressions_nonviral')
    .setName('Facebook - Affichages non viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_posts_impressions')
    .setName('Facebook - Affichages des publications')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_posts_impressions_unique')
    .setName('Facebook - Visiteurs de la publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_posts_impressions_paid')
    .setName('Facebook - Affichages publicitaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_posts_impressions_organic')
    .setName('Facebook - Affichages organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_views_total')
    .setName('Facebook - Vues totales de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Fans Facebook
  fields.newMetric()
    .setId('facebook_page_fans')
    .setName('Facebook - Nombre de fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('facebook_page_fan_adds')
    .setName('Facebook - Nouveaux fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_fan_removes')
    .setName('Facebook - Fans perdus')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_follows')
    .setName('Facebook - Nombre d\'abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('facebook_page_daily_follows')
    .setName('Facebook - Nouveaux abonnés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_daily_unfollows')
    .setName('Facebook - Désabonnements')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Engagement Facebook
  fields.newMetric()
    .setId('facebook_page_post_engagements')
    .setName('Facebook - Interactions sur publications')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_total_actions')
    .setName('Facebook - Actions totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Posts Facebook
  fields.newMetric()
    .setId('facebook_post_impressions')
    .setName('Facebook - Affichages publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_impressions_organic')
    .setName('Facebook - Affichages organiques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_impressions_paid')
    .setName('Facebook - Affichages sponsorisés')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_impressions_viral')
    .setName('Facebook - Affichages viraux')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_impressions_unique')
    .setName('Facebook - Visiteurs de la publication')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_clicks')
    .setName('Facebook - Nombre de clics')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_consumptions')
    .setName('Facebook - Interactions totales')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Réactions Facebook
  fields.newMetric()
    .setId('facebook_post_reactions_like_total')
    .setName('Facebook - Nombre de "J\'aime"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_love_total')
    .setName('Facebook - Nombre de "J\'adore"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_wow_total')
    .setName('Facebook - Nombre de "Wow"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_haha_total')
    .setName('Facebook - Nombre de "Haha"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_sorry_total')
    .setName('Facebook - Nombre de "Triste"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_anger_total')
    .setName('Facebook - Nombre de "En colère"')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Activités posts Facebook
  fields.newMetric()
    .setId('facebook_post_activity_by_action_type_share')
    .setName('Facebook - Partages')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_activity_by_action_type_comment')
    .setName('Facebook - Nombre de commentaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ============================
  // MÉTRIQUES VIDÉO COMBINÉES
  // ============================
  
  fields.newMetric()
    .setId('video_views')
    .setName('Vues Vidéo (Toutes plateformes)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_video_views')
    .setName('LinkedIn - Vues Vidéo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_video_views')
    .setName('Facebook - Vues de vidéos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_video_views_unique')
    .setName('Facebook - Vues uniques de vidéos')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_video_view_time')
    .setName('Facebook - Temps de visionnage (sec)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_video_complete_views_30s')
    .setName('Facebook - Vues complètes (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_video_views')
    .setName('Facebook Post - Vues vidéo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_video_complete_views_30s')
    .setName('Facebook Post - Vues complètes (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_video_avg_time_watched')
    .setName('Facebook Post - Temps moyen visionné')
    .setType(types.NUMBER)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES CALCULÉES COMBINÉES
  // ============================
  
  fields.newMetric()
    .setId('total_engagement')
    .setName('Engagement Total (Toutes plateformes)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('engagement_rate')
    .setName('Taux d\'Engagement Global (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('total_reactions')
    .setName('Total Réactions (Toutes plateformes)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_positive')
    .setName('Facebook - Réactions positives')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_negative')
    .setName('Facebook - Réactions négatives')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_taux_engagement_page')
    .setName('Facebook - Taux d\'engagement page (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('facebook_taux_de_clic')
    .setName('Facebook - Taux de clic (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('facebook_vtr_percentage')
    .setName('Facebook - VTR (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ============================
  // MÉTRIQUES BREAKDOWN
  // ============================
  
  fields.newMetric()
    .setId('breakdown_count')
    .setName('Compteur Breakdown')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('breakdown_percentage')
    .setName('% Breakdown')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_followers_by_country')
    .setName('LinkedIn - Abonnés par Pays')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_followers_by_industry')
    .setName('LinkedIn - Abonnés par Industrie')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_followers_by_function')
    .setName('LinkedIn - Abonnés par Fonction')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_followers_by_seniority')
    .setName('LinkedIn - Abonnés par Ancienneté')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_followers_by_company_size')
    .setName('LinkedIn - Abonnés par Taille Entreprise')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  return fields;
}

// ================================
// 5. RÉCUPÉRATION DES DONNÉES
// ================================

function getData(request) {
  console.log('=== getData LinkedIn + Facebook - Début ===');
  
  try {
    var userEmail = Session.getActiveUser().getEmail();
    console.log('Email utilisateur:', userEmail);
    
    // Vérifier l'abonnement LinkedIn + Facebook
    var subscriptionCheck = checkUserSubscription(userEmail);
    
    if (!subscriptionCheck.valid) {
      console.error('Abonnement invalide:', subscriptionCheck.error);
      return {
        schema: [],
        rows: [],
        error: 'Abonnement LinkedIn + Facebook non valide: ' + subscriptionCheck.error
      };
    }
    
    console.log('Abonnement LinkedIn + Facebook valide, récupération des données...');
    
    // Récupérer les données depuis l'API
    var apiData = fetchCombinedData(request, userEmail);
    
    if (!apiData || !apiData.success) {
      console.error('Erreur récupération données:', apiData ? apiData.error : 'Pas de données');
      return {
        schema: [],
        rows: [],
        error: 'Erreur lors de la récupération des données'
      };
    }
    
    // Transformer les données pour Looker Studio
    var transformedData = transformCombinedData(apiData.data, request);
    
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
        .setText('Veuillez vous inscrire pour accéder aux données LinkedIn + Facebook: ' + redirectUrl)
        .throwException();
    } else if (e.message.startsWith('REDIRECT_TO_UPGRADE:')) {
      var redirectUrl = e.message.split(':')[1];
      var cc = DataStudioApp.createCommunityConnector();
      cc.newUserError()
        .setDebugText('Redirection vers mise à niveau')
        .setText('Veuillez mettre à niveau votre abonnement LinkedIn + Facebook: ' + redirectUrl)
        .throwException();
    }
    
    var cc = DataStudioApp.createCommunityConnector();
    cc.newUserError()
      .setDebugText('Erreur générale: ' + e.toString())
      .setText('Erreur lors de la récupération des données LinkedIn + Facebook')
      .throwException();
  }
}

/**
 * Récupère les données combinées LinkedIn + Facebook depuis l'API backend
 */
function fetchCombinedData(request, userEmail) {
  try {
    var platforms = request.configParams.platforms || ['linkedin', 'facebook'];
    
    var params = {
      platforms: platforms,
      date_range: request.configParams.date_range || '30',
      metrics_type: request.configParams.metrics_type || 'overview',
      include_linkedin_reactions: request.configParams.include_linkedin_reactions || false,
      include_facebook_reactions: request.configParams.include_facebook_reactions || false,
      include_video_metrics: request.configParams.include_video_metrics || false,
      include_breakdown: request.configParams.include_breakdown || false
    };
    
    var response = UrlFetchApp.fetch(API_BASE_URL + '/api/v1/combined/metrics', {
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
    
    console.log('Réponse API Combined:', responseCode);
    
    if (responseCode === 200) {
      return { success: true, data: data };
    } else {
      return { success: false, error: data.message || 'Erreur API' };
    }
    
  } catch (e) {
    console.error('Erreur fetchCombinedData:', e);
    return { success: false, error: e.toString() };
  }
}

/**
 * Transforme les données combinées pour Looker Studio
 */
function transformCombinedData(apiData, request) {
  console.log('Transformation des données combinées...');
  
  var requestedFields = request.fields || [];
  var rows = [];
  
  if (!apiData) {
    console.log('Pas de données à transformer');
    return {
      schema: getFieldsFromRequest(requestedFields),
      rows: []
    };
  }
  
  // Traiter les données LinkedIn
  if (apiData.linkedin_data) {
    rows = rows.concat(transformLinkedInDataForCombo(apiData.linkedin_data, requestedFields));
  }
  
  // Traiter les données Facebook
  if (apiData.facebook_data) {
    rows = rows.concat(transformFacebookDataForCombo(apiData.facebook_data, requestedFields));
  }
  
  console.log('Transformation terminée:', rows.length, 'lignes générées');
  
  return {
    schema: getFieldsFromRequest(requestedFields),
    rows: rows
  };
}

/**
 * Transforme les données LinkedIn pour le connecteur combiné
 */
function transformLinkedInDataForCombo(linkedinData, requestedFields) {
  var rows = [];
  
  // Traiter tous les types de données LinkedIn
  if (linkedinData.page_metrics) {
    linkedinData.page_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'linkedin', 'page', requestedFields));
    });
  }
  
  if (linkedinData.post_metrics) {
    linkedinData.post_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'linkedin', 'post', requestedFields));
    });
  }
  
  if (linkedinData.follower_metrics) {
    linkedinData.follower_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'linkedin', 'follower', requestedFields));
    });
  }
  
  if (linkedinData.breakdown_data) {
    linkedinData.breakdown_data.forEach(function(metric) {
      rows.push(createComboRow(metric, 'linkedin', 'breakdown', requestedFields));
    });
  }
  
  return rows;
}

/**
 * Transforme les données Facebook pour le connecteur combiné
 */
function transformFacebookDataForCombo(facebookData, requestedFields) {
  var rows = [];
  
  // Traiter tous les types de données Facebook
  if (facebookData.page_metrics) {
    facebookData.page_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'facebook', 'page', requestedFields));
    });
  }
  
  if (facebookData.post_metrics) {
    facebookData.post_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'facebook', 'post', requestedFields));
    });
  }
  
  if (facebookData.fan_metrics) {
    facebookData.fan_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'facebook', 'fan', requestedFields));
    });
  }
  
  if (facebookData.video_metrics) {
    facebookData.video_metrics.forEach(function(metric) {
      rows.push(createComboRow(metric, 'facebook', 'video', requestedFields));
    });
  }
  
  return rows;
}

/**
 * Crée une ligne de données combinée
 */
function createComboRow(metric, platform, type, requestedFields) {
  var row = {};
  
  requestedFields.forEach(function(field) {
    var fieldId = field.getId();
    
    // Valeurs par défaut
    switch (fieldId) {
      case 'platform':
        row[fieldId] = platform;
        break;
      case 'content_type':
        row[fieldId] = type;
        break;
      case 'date':
        row[fieldId] = metric.date || new Date().toISOString().split('T')[0];
        break;
      case 'account_name':
        row[fieldId] = metric.account_name || (platform === 'linkedin' ? 'LinkedIn Page' : 'Facebook Page');
        break;
      case 'account_id':
        row[fieldId] = metric.account_id || '';
        break;
        
      // Métriques calculées combinées
      case 'total_engagement':
        if (platform === 'linkedin') {
          row[fieldId] = (metric.linkedin_post_clicks || 0) + (metric.linkedin_post_shares || 0) + 
                        (metric.linkedin_post_comments || 0) + (metric.linkedin_total_reactions || 0);
        } else if (platform === 'facebook') {
          row[fieldId] = (metric.facebook_post_clicks || 0) + (metric.facebook_post_activity_by_action_type_share || 0) + 
                        (metric.facebook_post_activity_by_action_type_comment || 0) + 
                        ((metric.facebook_post_reactions_like_total || 0) + (metric.facebook_post_reactions_love_total || 0) + 
                         (metric.facebook_post_reactions_wow_total || 0) + (metric.facebook_post_reactions_haha_total || 0) + 
                         (metric.facebook_post_reactions_sorry_total || 0) + (metric.facebook_post_reactions_anger_total || 0));
        }
        break;
        
      case 'total_reactions':
        if (platform === 'linkedin') {
          row[fieldId] = (metric.linkedin_reactions_like || 0) + (metric.linkedin_reactions_celebrate || 0) + 
                        (metric.linkedin_reactions_love || 0) + (metric.linkedin_reactions_insightful || 0) + 
                        (metric.linkedin_reactions_support || 0) + (metric.linkedin_reactions_funny || 0);
        } else if (platform === 'facebook') {
          row[fieldId] = (metric.facebook_post_reactions_like_total || 0) + (metric.facebook_post_reactions_love_total || 0) + 
                        (metric.facebook_post_reactions_wow_total || 0) + (metric.facebook_post_reactions_haha_total || 0) + 
                        (metric.facebook_post_reactions_sorry_total || 0) + (metric.facebook_post_reactions_anger_total || 0);
        }
        break;
        
      case 'total_followers':
        if (platform === 'linkedin') {
          row[fieldId] = metric.linkedin_total_followers || 0;
        } else if (platform === 'facebook') {
          row[fieldId] = metric.facebook_page_fans || 0;
        }
        break;
        
      case 'new_followers':
        if (platform === 'linkedin') {
          row[fieldId] = (metric.linkedin_organic_follower_gain || 0) + (metric.linkedin_paid_follower_gain || 0);
        } else if (platform === 'facebook') {
          row[fieldId] = metric.facebook_page_fan_adds || 0;
        }
        break;
        
      case 'followers_lost':
        if (platform === 'facebook') {
          row[fieldId] = metric.facebook_page_fan_removes || 0;
        } else {
          row[fieldId] = 0; // LinkedIn n'a pas cette métrique
        }
        break;
        
      case 'video_views':
        if (platform === 'linkedin') {
          row[fieldId] = metric.linkedin_video_views || 0;
        } else if (platform === 'facebook') {
          row[fieldId] = (metric.facebook_page_video_views || 0) + (metric.facebook_post_video_views || 0);
        }
        break;
        
      case 'facebook_reactions_positive':
        if (platform === 'facebook') {
          row[fieldId] = (metric.facebook_post_reactions_like_total || 0) + (metric.facebook_post_reactions_love_total || 0) + 
                        (metric.facebook_post_reactions_wow_total || 0) + (metric.facebook_post_reactions_haha_total || 0);
        } else {
          row[fieldId] = 0;
        }
        break;
        
      case 'facebook_reactions_negative':
        if (platform === 'facebook') {
          row[fieldId] = (metric.facebook_post_reactions_sorry_total || 0) + (metric.facebook_post_reactions_anger_total || 0);
        } else {
          row[fieldId] = 0;
        }
        break;
        
      case 'facebook_taux_engagement_page':
        if (platform === 'facebook') {
          var impressions = metric.facebook_page_impressions || 1;
          var engagements = metric.facebook_page_post_engagements || 0;
          row[fieldId] = impressions > 0 ? (engagements / impressions) * 100 : 0;
        } else {
          row[fieldId] = 0;
        }
        break;
        
      case 'facebook_taux_de_clic':
        if (platform === 'facebook') {
          var postImpressions = metric.facebook_post_impressions || 1;
          var postClicks = metric.facebook_post_clicks || 0;
          row[fieldId] = postImpressions > 0 ? (postClicks / postImpressions) * 100 : 0;
        } else {
          row[fieldId] = 0;
        }
        break;
        
      case 'facebook_vtr_percentage':
        if (platform === 'facebook') {
          var videoViews = metric.facebook_post_video_views || 0;
          var videoCompleteViews = metric.facebook_post_video_complete_views_30s || 0;
          row[fieldId] = videoViews > 0 ? (videoCompleteViews / videoViews) * 100 : 0;
        } else {
          row[fieldId] = 0;
        }
        break;
        
      // Métriques avec préfixe de plateforme
      default:
        if (fieldId.startsWith('linkedin_') && platform === 'linkedin') {
          var metricKey = fieldId.replace('linkedin_', '');
          row[fieldId] = metric[metricKey] || metric[fieldId] || 0;
        } else if (fieldId.startsWith('facebook_') && platform === 'facebook') {
          var metricKey = fieldId.replace('facebook_', '');
          row[fieldId] = metric[metricKey] || metric[fieldId] || 0;
        } else {
          row[fieldId] = metric[fieldId] || 0;
        }
    }
  });
  
  return { values: Object.values(row) };
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