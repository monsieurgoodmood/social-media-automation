/**
 * WhatsTheData - Connecteur Looker Studio COMPLET
 * TOUTES les métriques LinkedIn & Facebook intégrées
 * Développé par Arthur Choisnet (arthur.choisnet@isatis-conseil.fr)
 */

// ================================
// 1. CONFIGURATION & AUTHENTIFICATION
// ================================

function getAuthType() {
  var cc = DataStudioApp.createCommunityConnector();
  return cc
    .newAuthTypeResponse()
    .setAuthType(cc.AuthType.USER_TOKEN)
    .setHelpUrl('mailto:arthur.choisnet@isatis-conseil.fr')
    .build();
}

function isAuthValid() {
  var userToken = PropertiesService.getUserProperties().getProperty('dscc.token');
  return userToken && userToken.length > 3;
}

function resetAuth() {
  PropertiesService.getUserProperties().deleteProperty('dscc.token');
}

function setCredentials(request) {
  var token = request.userToken.token;
  
  if (!token || token.trim() === '') {
    return { errorCode: 'INVALID_CREDENTIALS' };
  }
  
  PropertiesService.getUserProperties().setProperty('dscc.token', token);
  return { errorCode: 'NONE' };
}

// ================================
// 2. CONFIGURATION DU CONNECTEUR
// ================================

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  config
    .newInfo()
    .setId('instructions')
    .setText('📊 WhatsTheData COMPLET - Toutes les métriques LinkedIn & Facebook. Token test: "test2024"');
  
  config
    .newSelectMultiple()
    .setId('platforms')
    .setName('Plateformes à inclure')
    .setHelpText('Sélectionnez LinkedIn et/ou Facebook')
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
    .setAllowOverride(true);
  
  config
    .newCheckbox()
    .setId('include_breakdown')
    .setName('Inclure breakdown démographique')
    .setHelpText('Ajouter la segmentation par pays, industrie, séniorité, etc.')
    .setAllowOverride(true);
  
  config.setDateRangeRequired(true);
  
  return config.build();
}

// ================================
// 3. SCHÉMA COMPLET DES DONNÉES
// ================================

function getSchema(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // DIMENSIONS PRINCIPALES
  fields.newDimension()
    .setId('platform')
    .setName('Plateforme')
    .setDescription('linkedin ou facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('date')
    .setName('Date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('account_id')
    .setName('ID Compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('account_name')
    .setName('Nom du Compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('content_type')
    .setName('Type de Contenu')
    .setDescription('page_metrics, post_metrics, followers_breakdown')
    .setType(types.TEXT);
  
  // DIMENSIONS POSTS
  fields.newDimension()
    .setId('post_id')
    .setName('ID Post')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_type')
    .setName('Type de Post')
    .setDescription('ugcPost, share, photo, video, status, link')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_date')
    .setName('Date de Publication')
    .setType(types.YEAR_MONTH_DAY_HOUR);
  
  fields.newDimension()
    .setId('post_text')
    .setName('Texte du Post')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_type')
    .setName('Type de Média')
    .setDescription('image, video, article, none')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_url')
    .setName('URL Média')
    .setType(types.URL);
  
  // DIMENSIONS BREAKDOWN
  fields.newDimension()
    .setId('breakdown_type')
    .setName('Type de Breakdown')
    .setDescription('country, industry, seniority, company_size, function')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('breakdown_value')
    .setName('Valeur Breakdown')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('breakdown_label')
    .setName('Label Breakdown')
    .setType(types.TEXT);
  
  // MÉTRIQUES FOLLOWERS/FANS
  fields.newMetric()
    .setId('total_followers')
    .setName('Total Followers')
    .setDescription('LinkedIn: followers, Facebook: page_fans')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('new_followers')
    .setName('Nouveaux Followers')
    .setDescription('LinkedIn: new_followers, Facebook: page_fan_adds')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_lost')
    .setName('Followers Perdus')
    .setDescription('LinkedIn: followers_lost, Facebook: page_fan_removes')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES IMPRESSIONS & PORTÉE
  fields.newMetric()
    .setId('page_impressions')
    .setName('Impressions Page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_unique')
    .setName('Impressions Page Uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_viral')
    .setName('Impressions Virales')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_impressions_nonviral')
    .setName('Impressions Non-Virales')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions')
    .setName('Impressions Posts')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_unique')
    .setName('Impressions Posts Uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_paid')
    .setName('Impressions Posts Payées')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_posts_impressions_organic')
    .setName('Impressions Posts Organiques')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_views')
    .setName('Vues de Page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES POSTS INDIVIDUELS
  fields.newMetric()
    .setId('post_impressions')
    .setName('Impressions Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_unique_impressions')
    .setName('Impressions Post Uniques')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('post_reach')
    .setName('Portée Post')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES ENGAGEMENT GLOBAL
  fields.newMetric()
    .setId('total_engagement')
    .setName('Engagement Total')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_post_engagements')
    .setName('Engagements Posts de Page')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('page_total_actions')
    .setName('Actions Totales Page')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('engagement_rate')
    .setName('Taux d\'Engagement (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('reach_rate')
    .setName('Taux de Portée (%)')
    .setDescription('Facebook uniquement')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // MÉTRIQUES INTERACTION DE BASE
  fields.newMetric()
    .setId('likes')
    .setName('Likes')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('comments')
    .setName('Commentaires')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('shares')
    .setName('Partages')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('clicks')
    .setName('Clics')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('click_through_rate')
    .setName('Taux de Clic (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // MÉTRIQUES RÉACTIONS LINKEDIN
  fields.newMetric()
    .setId('linkedin_reactions_like')
    .setName('LinkedIn - Reactions Like')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_celebrate')
    .setName('LinkedIn - Reactions Celebrate')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_love')
    .setName('LinkedIn - Reactions Love')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_insightful')
    .setName('LinkedIn - Reactions Insightful')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_support')
    .setName('LinkedIn - Reactions Support')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_reactions_funny')
    .setName('LinkedIn - Reactions Funny')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_like_percentage')
    .setName('LinkedIn - % Like')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_celebrate_percentage')
    .setName('LinkedIn - % Celebrate')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_love_percentage')
    .setName('LinkedIn - % Love')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_insight_percentage')
    .setName('LinkedIn - % Insightful')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_support_percentage')
    .setName('LinkedIn - % Support')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_funny_percentage')
    .setName('LinkedIn - % Funny')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_total_reactions')
    .setName('LinkedIn - Total Réactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_total_interactions')
    .setName('LinkedIn - Total Interactions')
    .setDescription('Clics + Partages + Commentaires + Réactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES RÉACTIONS FACEBOOK
  fields.newMetric()
    .setId('facebook_reactions_like')
    .setName('Facebook - Reactions Like')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_love')
    .setName('Facebook - Reactions Love')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_wow')
    .setName('Facebook - Reactions Wow')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_haha')
    .setName('Facebook - Reactions Haha')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_sorry')
    .setName('Facebook - Reactions Sorry')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_anger')
    .setName('Facebook - Reactions Anger')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_positive')
    .setName('Facebook - Réactions Positives')
    .setDescription('Like + Love + Wow + Haha')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_reactions_negative')
    .setName('Facebook - Réactions Négatives')
    .setDescription('Sorry + Anger')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES VIDÉO
  fields.newMetric()
    .setId('video_views')
    .setName('Vues Vidéo')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('video_views_unique')
    .setName('Vues Vidéo Uniques')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('video_complete_views_30s')
    .setName('Vues Vidéo Complètes (30s)')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('video_view_time')
    .setName('Temps de Vue Vidéo (secondes)')
    .setDescription('Facebook uniquement')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // MÉTRIQUES BREAKDOWN DÉMOGRAPHIQUE
  fields.newMetric()
    .setId('breakdown_count')
    .setName('Compteur Breakdown')
    .setDescription('Nombre de followers/vues pour cette segmentation')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('breakdown_percentage')
    .setName('% Breakdown')
    .setDescription('Pourcentage de cette segmentation')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // MÉTRIQUES CALCULÉES AVANCÉES
  fields.newMetric()
    .setId('taux_engagement_complet')
    .setName('Taux d\'Engagement Complet (%)')
    .setDescription('(Réactions + Clics) / Impressions')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('ratio_reactions_positives')
    .setName('Ratio Réactions Positives (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('ratio_reactions_negatives')
    .setName('Ratio Réactions Négatives (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  return { schema: fields.build() };
}

// ================================
// 4. RÉCUPÉRATION DES DONNÉES
// ================================

function getData(request) {
  var userToken = PropertiesService.getUserProperties().getProperty('dscc.token');
  
  if (!userToken) {
    throw new Error('Token d\'authentification manquant');
  }
  
  try {
    var platforms = request.configParams.platforms || ['linkedin', 'facebook'];
    var dateRange = parseInt(request.configParams.date_range) || 30;
    var metricsType = request.configParams.metrics_type || 'overview';
    var includeBreakdown = request.configParams.include_breakdown || false;
    var startDate = request.dateRange.startDate;
    var endDate = request.dateRange.endDate;
    
    var allData = generateCompleteTestData(platforms, startDate, endDate, metricsType, includeBreakdown);
    var fieldNames = request.fields.map(function(field) { return field.name; });
    var rows = transformDataForLookerStudio(allData, fieldNames);
    
    return {
      schema: getSchema(request).schema,
      rows: rows
    };
    
  } catch (e) {
    console.error('Erreur getData:', e);
    DataStudioApp.createCommunityConnector()
      .newUserError()
      .setDebugText('Erreur: ' + e.toString())
      .setText('Impossible de récupérer les données. Vérifiez votre configuration.')
      .throwException();
  }
}

function generateCompleteTestData(platforms, startDate, endDate, metricsType, includeBreakdown) {
  var data = [];
  var start = new Date(startDate);
  var end = new Date(endDate);
  
  var accounts = {
    linkedin: {
      id: 'linkedin_org_123456',
      name: 'WhatsTheData LinkedIn',
      base_followers: 1500
    },
    facebook: {
      id: 'facebook_page_789012', 
      name: 'WhatsTheData Facebook',
      base_followers: 3200
    }
  };
  
  var currentDate = new Date(start);
  var dayIndex = 0;
  
  while (currentDate <= end) {
    var dateStr = currentDate.toISOString().split('T')[0];
    
    platforms.forEach(function(platform) {
      if (!accounts[platform]) return;
      
      var account = accounts[platform];
      
      if (metricsType === 'overview' || metricsType === 'pages') {
        var pageMetrics = generatePageMetrics(platform, account, dateStr, dayIndex);
        data.push(pageMetrics);
      }
      
      if (metricsType === 'overview' || metricsType === 'posts') {
        var postsPerDay = platform === 'facebook' ? 3 : 2;
        
        for (var i = 0; i < postsPerDay; i++) {
          var postMetrics = generatePostMetrics(platform, account, dateStr, i, dayIndex);
          data.push(postMetrics);
        }
      }
      
      if (includeBreakdown || metricsType === 'followers_breakdown') {
        var breakdownData = generateBreakdownMetrics(platform, account, dateStr, dayIndex);
        data = data.concat(breakdownData);
      }
    });
    
    currentDate.setDate(currentDate.getDate() + 1);
    dayIndex++;
  }
  
  return data;
}

function generatePageMetrics(platform, account, dateStr, dayIndex) {
  var baseFollowers = account.base_followers + dayIndex * (platform === 'facebook' ? 15 : 10);
  var newFollowers = Math.floor(Math.random() * 20) + 5;
  var followersLost = Math.floor(Math.random() * 8) + 1;
  
  var record = {
    platform: platform,
    date: dateStr,
    account_id: account.id,
    account_name: account.name,
    content_type: 'page_metrics',
    
    total_followers: baseFollowers,
    new_followers: newFollowers,
    followers_lost: followersLost,
    
    page_impressions: Math.floor(Math.random() * 3000) + (platform === 'facebook' ? 4000 : 2000),
    page_impressions_unique: Math.floor(Math.random() * 2500) + (platform === 'facebook' ? 3200 : 1500),
    page_views: Math.floor(Math.random() * 400) + (platform === 'facebook' ? 600 : 200),
    
    total_engagement: Math.floor(Math.random() * 300) + (platform === 'facebook' ? 400 : 150),
    page_post_engagements: platform === 'facebook' ? Math.floor(Math.random() * 250) + 200 : 0,
    page_total_actions: platform === 'facebook' ? Math.floor(Math.random() * 150) + 100 : 0,
    
    likes: Math.floor(Math.random() * 120) + (platform === 'facebook' ? 180 : 60),
    comments: Math.floor(Math.random() * 40) + (platform === 'facebook' ? 50 : 20),
    shares: Math.floor(Math.random() * 60) + (platform === 'facebook' ? 90 : 25),
    clicks: Math.floor(Math.random() * 80) + (platform === 'facebook' ? 120 : 40),
    
    video_views: Math.floor(Math.random() * 200) + 50,
    video_views_unique: platform === 'facebook' ? Math.floor(Math.random() * 150) + 40 : 0,
    video_complete_views_30s: Math.floor(Math.random() * 80) + 20,
    video_view_time: platform === 'facebook' ? Math.floor(Math.random() * 3600) + 1200 : 0
  };
  
  if (platform === 'facebook') {
    record.page_impressions_viral = Math.floor(record.page_impressions * 0.3);
    record.page_impressions_nonviral = record.page_impressions - record.page_impressions_viral;
    record.page_posts_impressions = Math.floor(Math.random() * 2500) + 1500;
    record.page_posts_impressions_unique = Math.floor(record.page_posts_impressions * 0.8);
    record.page_posts_impressions_paid = Math.floor(record.page_posts_impressions * 0.2);
    record.page_posts_impressions_organic = record.page_posts_impressions - record.page_posts_impressions_paid;
    
    record.facebook_reactions_like = Math.floor(Math.random() * 100) + 80;
    record.facebook_reactions_love = Math.floor(Math.random() * 30) + 15;
    record.facebook_reactions_wow = Math.floor(Math.random() * 20) + 8;
    record.facebook_reactions_haha = Math.floor(Math.random() * 25) + 10;
    record.facebook_reactions_sorry = Math.floor(Math.random() * 5) + 1;
    record.facebook_reactions_anger = Math.floor(Math.random() * 3) + 1;
    
    record.facebook_reactions_positive = record.facebook_reactions_like + record.facebook_reactions_love + record.facebook_reactions_wow + record.facebook_reactions_haha;
    record.facebook_reactions_negative = record.facebook_reactions_sorry + record.facebook_reactions_anger;
    
    record.linkedin_reactions_like = 0;
    record.linkedin_reactions_celebrate = 0;
    record.linkedin_reactions_love = 0;
    record.linkedin_reactions_insightful = 0;
    record.linkedin_reactions_support = 0;
    record.linkedin_reactions_funny = 0;
    record.linkedin_total_reactions = 0;
    record.linkedin_total_interactions = 0;
    record.linkedin_like_percentage = 0;
    record.linkedin_celebrate_percentage = 0;
    record.linkedin_love_percentage = 0;
    record.linkedin_insight_percentage = 0;
    record.linkedin_support_percentage = 0;
    record.linkedin_funny_percentage = 0;
  } else {
    record.page_impressions_viral = 0;
    record.page_impressions_nonviral = 0;
    record.page_posts_impressions = 0;
    record.page_posts_impressions_unique = 0;
    record.page_posts_impressions_paid = 0;
    record.page_posts_impressions_organic = 0;
    record.facebook_reactions_like = 0;
    record.facebook_reactions_love = 0;
    record.facebook_reactions_wow = 0;
    record.facebook_reactions_haha = 0;
    record.facebook_reactions_sorry = 0;
    record.facebook_reactions_anger = 0;
    record.facebook_reactions_positive = 0;
    record.facebook_reactions_negative = 0;
    
    record.linkedin_reactions_like = Math.floor(Math.random() * 80) + 50;
    record.linkedin_reactions_celebrate = Math.floor(Math.random() * 30) + 15;
    record.linkedin_reactions_love = Math.floor(Math.random() * 20) + 8;
    record.linkedin_reactions_insightful = Math.floor(Math.random() * 15) + 5;
    record.linkedin_reactions_support = Math.floor(Math.random() * 12) + 3;
    record.linkedin_reactions_funny = Math.floor(Math.random() * 8) + 1;
    
    record.linkedin_total_reactions = record.linkedin_reactions_like + record.linkedin_reactions_celebrate + 
                                     record.linkedin_reactions_love + record.linkedin_reactions_insightful + 
                                     record.linkedin_reactions_support + record.linkedin_reactions_funny;
    
    record.linkedin_total_interactions = record.linkedin_total_reactions + record.clicks + record.shares + record.comments;
    
    if (record.linkedin_total_reactions > 0) {
      record.linkedin_like_percentage = (record.linkedin_reactions_like / record.linkedin_total_reactions) * 100;
      record.linkedin_celebrate_percentage = (record.linkedin_reactions_celebrate / record.linkedin_total_reactions) * 100;
      record.linkedin_love_percentage = (record.linkedin_reactions_love / record.linkedin_total_reactions) * 100;
      record.linkedin_insight_percentage = (record.linkedin_reactions_insightful / record.linkedin_total_reactions) * 100;
      record.linkedin_support_percentage = (record.linkedin_reactions_support / record.linkedin_total_reactions) * 100;
      record.linkedin_funny_percentage = (record.linkedin_reactions_funny / record.linkedin_total_reactions) * 100;
    } else {
      record.linkedin_like_percentage = 0;
      record.linkedin_celebrate_percentage = 0;
      record.linkedin_love_percentage = 0;
      record.linkedin_insight_percentage = 0;
      record.linkedin_support_percentage = 0;
      record.linkedin_funny_percentage = 0;
    }
  }
  
  if (record.page_impressions > 0) {
    record.engagement_rate = (record.total_engagement / record.page_impressions) * 100;
    record.click_through_rate = (record.clicks / record.page_impressions) * 100;
    record.reach_rate = platform === 'facebook' ? (record.page_impressions_unique / record.page_impressions) * 100 : 0;
    record.taux_engagement_complet = ((record.total_engagement + record.clicks) / record.page_impressions) * 100;
  } else {
    record.engagement_rate = 0;
    record.click_through_rate = 0;
    record.reach_rate = 0;
    record.taux_engagement_complet = 0;
  }
  
  var totalReactions = platform === 'facebook' ? 
    (record.facebook_reactions_positive + record.facebook_reactions_negative) :
    record.linkedin_total_reactions;
  
  if (totalReactions > 0) {
    if (platform === 'facebook') {
      record.ratio_reactions_positives = (record.facebook_reactions_positive / totalReactions) * 100;
      record.ratio_reactions_negatives = (record.facebook_reactions_negative / totalReactions) * 100;
    } else {
      record.ratio_reactions_positives = 95 + Math.random() * 5;
      record.ratio_reactions_negatives = 5 - Math.random() * 5;
    }
  } else {
    record.ratio_reactions_positives = 0;
    record.ratio_reactions_negatives = 0;
  }
  
  return record;
}

function generatePostMetrics(platform, account, dateStr, postIndex, dayIndex) {
  var postTypes = platform === 'facebook' ? 
    ['photo', 'video', 'status', 'link'] : 
    ['ugcPost', 'share', 'article'];
  
  var postType = postTypes[postIndex % postTypes.length];
  var mediaTypes = ['image', 'video', 'none', 'article'];
  var mediaType = mediaTypes[postIndex % mediaTypes.length];
  
  var postHour = 9 + (postIndex * 4);
  var postDateTime = dateStr + 'T' + (postHour < 10 ? '0' : '') + postHour + ':00:00';
  
  var baseImpressions = Math.floor(Math.random() * 1500) + (platform === 'facebook' ? 2000 : 800);
  var reach = Math.floor(baseImpressions * (0.6 + Math.random() * 0.3));
  
  var record = {
    platform: platform,
    date: dateStr,
    account_id: account.id,
    account_name: account.name,
    content_type: 'post_metrics',
    
    post_id: account.id + '_post_' + dateStr.replace(/-/g, '') + '_' + postIndex,
    post_type: postType,
    post_date: postDateTime,
    post_text: 'Post ' + platform + ' du ' + dateStr + ' #' + (postIndex + 1) + ' - Contenu de test',
    media_type: mediaType,
    media_url: mediaType !== 'none' ? 'https://example.com/media/' + postIndex + '.jpg' : '',
    
    post_impressions: baseImpressions,
    post_unique_impressions: Math.floor(baseImpressions * 0.8),
    post_reach: reach,
    
    total_engagement: Math.floor(Math.random() * 150) + (platform === 'facebook' ? 200 : 80),
    likes: Math.floor(Math.random() * 80) + (platform === 'facebook' ? 120 : 40),
    comments: Math.floor(Math.random() * 25) + (platform === 'facebook' ? 30 : 10),
    shares: Math.floor(Math.random() * 40) + (platform === 'facebook' ? 60 : 15),
    clicks: Math.floor(Math.random() * 50) + (platform === 'facebook' ? 80 : 25)
  };
  
  if (platform === 'facebook') {
    record.facebook_reactions_like = Math.floor(Math.random() * 60) + 40;
    record.facebook_reactions_love = Math.floor(Math.random() * 20) + 8;
    record.facebook_reactions_wow = Math.floor(Math.random() * 15) + 5;
    record.facebook_reactions_haha = Math.floor(Math.random() * 18) + 7;
    record.facebook_reactions_sorry = Math.floor(Math.random() * 3) + 1;
    record.facebook_reactions_anger = Math.floor(Math.random() * 2);
    
    record.facebook_reactions_positive = record.facebook_reactions_like + record.facebook_reactions_love + 
                                        record.facebook_reactions_wow + record.facebook_reactions_haha;
    record.facebook_reactions_negative = record.facebook_reactions_sorry + record.facebook_reactions_anger;
    
    record.linkedin_reactions_like = 0;
    record.linkedin_reactions_celebrate = 0;
    record.linkedin_reactions_love = 0;
    record.linkedin_reactions_insightful = 0;
    record.linkedin_reactions_support = 0;
    record.linkedin_reactions_funny = 0;
    record.linkedin_total_reactions = 0;
    record.linkedin_total_interactions = 0;
  } else {
    record.linkedin_reactions_like = Math.floor(Math.random() * 50) + 30;
    record.linkedin_reactions_celebrate = Math.floor(Math.random() * 20) + 8;
    record.linkedin_reactions_love = Math.floor(Math.random() * 15) + 5;
    record.linkedin_reactions_insightful = Math.floor(Math.random() * 12) + 3;
    record.linkedin_reactions_support = Math.floor(Math.random() * 8) + 2;
    record.linkedin_reactions_funny = Math.floor(Math.random() * 5) + 1;
    
    record.linkedin_total_reactions = record.linkedin_reactions_like + record.linkedin_reactions_celebrate + 
                                     record.linkedin_reactions_love + record.linkedin_reactions_insightful + 
                                     record.linkedin_reactions_support + record.linkedin_reactions_funny;
    
    record.linkedin_total_interactions = record.linkedin_total_reactions + record.clicks + record.shares + record.comments;
    
    record.facebook_reactions_like = 0;
    record.facebook_reactions_love = 0;
    record.facebook_reactions_wow = 0;
    record.facebook_reactions_haha = 0;
    record.facebook_reactions_sorry = 0;
    record.facebook_reactions_anger = 0;
    record.facebook_reactions_positive = 0;
    record.facebook_reactions_negative = 0;
  }
  
  if (mediaType === 'video') {
    record.video_views = Math.floor(Math.random() * 300) + 100;
    record.video_views_unique = platform === 'facebook' ? Math.floor(record.video_views * 0.7) : 0;
    record.video_complete_views_30s = Math.floor(record.video_views * 0.4);
    record.video_view_time = platform === 'facebook' ? Math.floor(Math.random() * 1800) + 600 : 0;
  } else {
    record.video_views = 0;
    record.video_views_unique = 0;
    record.video_complete_views_30s = 0;
    record.video_view_time = 0;
  }
  
  if (record.post_impressions > 0) {
    record.engagement_rate = (record.total_engagement / record.post_impressions) * 100;
    record.click_through_rate = (record.clicks / record.post_impressions) * 100;
    record.taux_engagement_complet = ((record.total_engagement + record.clicks) / record.post_impressions) * 100;
  } else {
    record.engagement_rate = 0;
    record.click_through_rate = 0;
    record.taux_engagement_complet = 0;
  }
  
  if (record.post_reach > 0) {
    record.reach_rate = (record.post_reach / record.post_impressions) * 100;
  } else {
    record.reach_rate = 0;
  }
  
  return record;
}

function generateBreakdownMetrics(platform, account, dateStr, dayIndex) {
  var breakdownData = [];
  
  if (platform === 'linkedin') {
    var breakdownTypes = ['country', 'industry', 'seniority', 'company_size', 'function'];
    
    var sampleData = {
      country: [
        {value: 'FR', label: 'France', count: 450, percentage: 45},
        {value: 'US', label: 'États-Unis', count: 200, percentage: 20},
        {value: 'CA', label: 'Canada', count: 150, percentage: 15},
        {value: 'UK', label: 'Royaume-Uni', count: 100, percentage: 10},
        {value: 'DE', label: 'Allemagne', count: 100, percentage: 10}
      ],
      industry: [
        {value: 'tech', label: 'Technologie', count: 300, percentage: 30},
        {value: 'consulting', label: 'Conseil', count: 250, percentage: 25},
        {value: 'finance', label: 'Finance', count: 200, percentage: 20},
        {value: 'marketing', label: 'Marketing', count: 150, percentage: 15},
        {value: 'other', label: 'Autres', count: 100, percentage: 10}
      ],
      seniority: [
        {value: 'mid', label: 'Niveau intermédiaire', count: 400, percentage: 40},
        {value: 'senior', label: 'Senior', count: 300, percentage: 30},
        {value: 'junior', label: 'Junior', count: 200, percentage: 20},
        {value: 'executive', label: 'Cadre dirigeant', count: 100, percentage: 10}
      ],
      company_size: [
        {value: '1-10', label: '1-10 employés', count: 200, percentage: 20},
        {value: '11-50', label: '11-50 employés', count: 250, percentage: 25},
        {value: '51-200', label: '51-200 employés', count: 300, percentage: 30},
        {value: '201-1000', label: '201-1000 employés', count: 150, percentage: 15},
        {value: '1000+', label: '1000+ employés', count: 100, percentage: 10}
      ],
      function: [
        {value: 'engineering', label: 'Ingénierie', count: 300, percentage: 30},
        {value: 'sales', label: 'Ventes', count: 200, percentage: 20},
        {value: 'marketing', label: 'Marketing', count: 200, percentage: 20},
        {value: 'operations', label: 'Opérations', count: 150, percentage: 15},
        {value: 'hr', label: 'Ressources Humaines', count: 150, percentage: 15}
      ]
    };
    
    breakdownTypes.forEach(function(breakdownType) {
      sampleData[breakdownType].forEach(function(item) {
        var record = {
          platform: platform,
          date: dateStr,
          account_id: account.id,
          account_name: account.name,
          content_type: 'followers_breakdown',
          
          breakdown_type: breakdownType,
          breakdown_value: item.value,
          breakdown_label: item.label,
          breakdown_count: item.count + Math.floor(Math.random() * 20) - 10,
          breakdown_percentage: item.percentage + (Math.random() * 4) - 2,
          
          total_followers: 0,
          new_followers: 0,
          followers_lost: 0,
          page_impressions: 0,
          total_engagement: 0,
          likes: 0,
          comments: 0,
          shares: 0,
          clicks: 0,
          engagement_rate: 0,
          linkedin_reactions_like: 0,
          linkedin_reactions_celebrate: 0,
          linkedin_reactions_love: 0,
          linkedin_reactions_insightful: 0,
          linkedin_reactions_support: 0,
          linkedin_reactions_funny: 0,
          facebook_reactions_like: 0,
          facebook_reactions_love: 0,
          facebook_reactions_wow: 0,
          facebook_reactions_haha: 0,
          facebook_reactions_sorry: 0,
          facebook_reactions_anger: 0
        };
        
        breakdownData.push(record);
      });
    });
  }
  
  return breakdownData;
}

function transformDataForLookerStudio(rawData, fieldNames) {
  var rows = [];
  
  rawData.forEach(function(record) {
    var row = [];
    
    fieldNames.forEach(function(fieldName) {
      var value = record[fieldName];
      
      if (fieldName === 'date') {
        value = record.date || new Date().toISOString().split('T')[0];
      } else if (fieldName === 'post_date') {
        value = record.post_date || new Date().toISOString();
      } else if (fieldName.includes('percentage') || fieldName.includes('rate')) {
        value = (value || 0) / 100;
      } else {
        value = value !== undefined ? value : (typeof value === 'number' ? 0 : '');
      }
      
      row.push(value);
    });
    
    rows.push({ values: row });
  });
  
  return rows;
}

function isAdminUser() {
  var email = Session.getActiveUser().getEmail();
  var adminEmails = ['arthur.choisnet@isatis-conseil.fr'];
  return adminEmails.indexOf(email) !== -1;
}

function debugInfo() {
  if (!isAdminUser()) {
    throw new Error('Accès non autorisé');
  }
  
  return {
    user_email: Session.getActiveUser().getEmail(),
    stored_token: PropertiesService.getUserProperties().getProperty('dscc.token'),
    timezone: Session.getScriptTimeZone(),
    version: '2.0.0-complete',
    total_metrics: 50
  };
}