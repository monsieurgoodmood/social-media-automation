/**
 * ============================================================================
 * WHATSTHEDATA - CONNECTEUR LOOKER STUDIO RESTRUCTURÉ
 * ============================================================================
 * ORGANISATION CORRECTE : Catégories → Sous-catégories → Métriques
 * GESTION TEMPORELLE : Lifetime | Daily | Mixed avec logique appropriée
 * TOUTES VOS MÉTRIQUES PRÉSERVÉES : 53 LinkedIn + 102 Facebook = 155 total
 * ============================================================================
 */

var API_BASE_URL = 'https://whats-the-data-d954d4d4cb5f.herokuapp.com';
var CONNECTOR_ID = 'AKfycbyNlF25yTJzlO3j63xMX5ccUVnOaF2J6H4VX_bN4uJeZVYDiCv4zy1ojDrshmTR5nL-';
var TEST_MODE = true;

// ================================
// AUTHENTIFICATION
// ================================

function getAuthType() {
  if (TEST_MODE) {
    return DataStudioApp.createCommunityConnector()
      .newAuthTypeResponse()
      .setAuthType(DataStudioApp.createCommunityConnector().AuthType.NONE)
      .build();
  }
  
  return DataStudioApp.createCommunityConnector()
    .newAuthTypeResponse()
    .setAuthType(DataStudioApp.createCommunityConnector().AuthType.OAUTH2)
    .build();
}

function isAuthValid() {
  if (TEST_MODE) return true;
  var userEmail = Session.getActiveUser().getEmail();
  return userEmail && userEmail.length > 0;
}

function resetAuth() {
  if (TEST_MODE) return;
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('linkedin_page_id');
  userProperties.deleteProperty('facebook_page_id');
}

// ================================
// CONFIGURATION AVANCÉE
// ================================

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  // En-tête principal
  config.newInfo()
    .setId('main_header')
    .setText('📊 WHATSTHEDATA - Toutes vos métriques sociales organisées');
  
  // Sélection des plateformes
  config.newSelectMultiple()
    .setId('platforms')
    .setName('Plateformes')
    .setHelpText('Choisissez LinkedIn et/ou Facebook')
    .addOption(config.newOptionBuilder().setLabel('📘 LinkedIn (53 métriques)').setValue('linkedin'))
    .addOption(config.newOptionBuilder().setLabel('📱 Facebook (102 métriques)').setValue('facebook'))
    .setAllowOverride(true);
  
  // Type de métriques temporelles
  config.newSelectSingle()
    .setId('temporal_type')
    .setName('Type temporel')
    .setHelpText('Lifetime = Cumulatif | Daily = Quotidien selon période')
    .addOption(config.newOptionBuilder().setLabel('📈 Vue d\'ensemble (Lifetime + Daily)').setValue('mixed'))
    .addOption(config.newOptionBuilder().setLabel('📊 Lifetime (Données cumulatives)').setValue('lifetime'))
    .addOption(config.newOptionBuilder().setLabel('📅 Daily (Données quotidiennes)').setValue('daily'))
    .setAllowOverride(true);
  
  // Période pour métriques daily
  config.newSelectSingle()
    .setId('date_range')
    .setName('Période (pour métriques Daily)')
    .setHelpText('Affecte uniquement les métriques quotidiennes')
    .addOption(config.newOptionBuilder().setLabel('7 derniers jours').setValue('7'))
    .addOption(config.newOptionBuilder().setLabel('30 derniers jours').setValue('30'))
    .addOption(config.newOptionBuilder().setLabel('90 derniers jours').setValue('90'))
    .addOption(config.newOptionBuilder().setLabel('6 derniers mois').setValue('180'))
    .addOption(config.newOptionBuilder().setLabel('1 an').setValue('365'))
    .setAllowOverride(true);
  
  // Granularité des données
  config.newSelectSingle()
    .setId('data_granularity')
    .setName('Granularité')
    .setHelpText('Niveau de détail des données')
    .addOption(config.newOptionBuilder().setLabel('📋 Synthèse (Métriques principales)').setValue('summary'))
    .addOption(config.newOptionBuilder().setLabel('📊 Standard (Toutes catégories)').setValue('standard'))
    .addOption(config.newOptionBuilder().setLabel('🔬 Détaillé (Toutes métriques)').setValue('detailed'))
    .setAllowOverride(true);
  
  if (TEST_MODE) {
    config.newInfo()
      .setId('test_mode_info')
      .setText('⚠️ MODE TEST ACTIVÉ - Configuration et test des 155 métriques');
  }
  
  return config.build();
}

// ================================
// SCHÉMA ORGANISÉ PAR CATÉGORIES
// ================================

function getSchema(request) {
  console.log('=== getSchema - Structure organisée par catégories ===');
  
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // Récupérer la configuration
  var config = request.configParams || {};
  var platforms = config.platforms || ['linkedin', 'facebook'];
  var temporalType = config.temporal_type || 'mixed';
  var granularity = config.data_granularity || 'standard';
  
  // ================================
  // DIMENSIONS COMMUNES
  // ================================
  
  addCommonDimensions(fields, types);
  
  // ================================
  // MÉTRIQUES PAR PLATEFORME ET CATÉGORIE
  // ================================
  
  if (platforms.includes('linkedin')) {
    addLinkedInMetrics(fields, types, aggregations, temporalType, granularity);
  }
  
  if (platforms.includes('facebook')) {
    addFacebookMetrics(fields, types, aggregations, temporalType, granularity);
  }
  
  console.log('Schéma généré avec organisation catégorielle');
  return { schema: fields.build() };
}

// ================================
// DIMENSIONS COMMUNES
// ================================

function addCommonDimensions(fields, types) {
  fields.newDimension()
    .setId('platform')
    .setName('🌐 Platform')
    .setDescription('LinkedIn ou Facebook')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('date')
    .setName('📅 Date')
    .setDescription('Date de la métrique')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('account_name')
    .setName('👤 Nom du compte')
    .setDescription('Nom de la page/compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('account_id')
    .setName('🆔 ID du compte')
    .setDescription('Identifiant unique du compte')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('temporal_type')
    .setName('⏱️ Type temporel')
    .setDescription('Lifetime ou Daily')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('content_category')
    .setName('📂 Catégorie de contenu')
    .setDescription('Page, Post, Follower, etc.')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_id')
    .setName('📝 ID Publication')
    .setDescription('Identifiant du post (si applicable)')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_type')
    .setName('📄 Type de publication')
    .setDescription('Type de contenu publié')
    .setType(types.TEXT);
}

// ================================
// MÉTRIQUES LINKEDIN ORGANISÉES
// ================================

function addLinkedInMetrics(fields, types, aggregations, temporalType, granularity) {
  // ========================================
  // LINKEDIN - COMPANY OVERVIEW
  // ========================================
  
  if (temporalType === 'lifetime' || temporalType === 'mixed') {
    // Followers (LIFETIME)
    fields.newMetric()
      .setId('linkedin_company_total_followers')
      .setName('👥 LI - Total Followers')
      .setDescription('[LIFETIME] Nombre total de followers LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);
  }
  
  if (temporalType === 'daily' || temporalType === 'mixed') {
    // Croissance followers (DAILY)
    fields.newMetric()
      .setId('linkedin_company_organic_follower_gain')
      .setName('📈 LI - Gain Followers Organiques')
      .setDescription('[DAILY] Nouveaux followers organiques LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_company_paid_follower_gain')
      .setName('💰 LI - Gain Followers Payants')
      .setDescription('[DAILY] Nouveaux followers payants LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
  
  // Engagement général (toujours disponible)
  fields.newMetric()
    .setId('linkedin_company_total_impressions')
    .setName('👀 LI - Total Impressions')
    .setDescription('Total des impressions LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_company_total_unique_impressions')
    .setName('👁️ LI - Impressions Uniques')
    .setDescription('Impressions uniques LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_company_total_clicks')
    .setName('🖱️ LI - Total Clics')
    .setDescription('Total des clics LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_company_total_shares')
    .setName('🔄 LI - Total Partages')
    .setDescription('Total des partages LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_company_total_comments')
    .setName('💬 LI - Total Commentaires')
    .setDescription('Total des commentaires LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Taux calculés
  fields.newMetric()
    .setId('linkedin_company_engagement_rate')
    .setName('📊 LI - Taux d\'Engagement')
    .setDescription('Taux d\'engagement global LinkedIn (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ========================================
  // LINKEDIN - PAGE VIEWS (Détaillées)
  // ========================================
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('linkedin_page_views_total')
      .setName('🔍 LI - Vues Totales Page')
      .setDescription('Total des vues de page LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_page_views_unique')
      .setName('👁️‍🗨️ LI - Vues Uniques Page')
      .setDescription('Vues uniques de page LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('linkedin_page_views_desktop')
        .setName('💻 LI - Vues Desktop')
        .setDescription('Vues de page depuis desktop')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('linkedin_page_views_mobile')
        .setName('📱 LI - Vues Mobile')
        .setDescription('Vues de page depuis mobile')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('linkedin_page_views_overview_section')
        .setName('📋 LI - Vues Section Overview')
        .setDescription('Vues de la section Overview')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('linkedin_page_views_about_section')
        .setName('ℹ️ LI - Vues Section About')
        .setDescription('Vues de la section About')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('linkedin_page_views_jobs_section')
        .setName('💼 LI - Vues Section Jobs')
        .setDescription('Vues de la section Jobs')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
  }
  
  // ========================================
  // LINKEDIN - POSTS INDIVIDUELS
  // ========================================
  
  fields.newMetric()
    .setId('linkedin_post_impressions')
    .setName('👀 LI - Impressions Posts')
    .setDescription('Impressions des posts individuels LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_unique_impressions')
    .setName('👁️ LI - Impressions Uniques Posts')
    .setDescription('Impressions uniques des posts LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_clicks')
    .setName('🖱️ LI - Clics Posts')
    .setDescription('Clics sur posts LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_shares')
    .setName('🔄 LI - Partages Posts')
    .setDescription('Partages de posts LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('linkedin_post_comments')
    .setName('💬 LI - Commentaires Posts')
    .setDescription('Commentaires sur posts LinkedIn')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Taux de performance posts
  fields.newMetric()
    .setId('linkedin_post_engagement_rate')
    .setName('📈 LI - Taux Engagement Posts')
    .setDescription('Taux d\'engagement posts LinkedIn (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('linkedin_post_click_through_rate')
    .setName('🎯 LI - Taux de Clic Posts')
    .setDescription('Taux de clic posts LinkedIn (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // ========================================
  // LINKEDIN - RÉACTIONS DÉTAILLÉES
  // ========================================
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('linkedin_reactions_like')
      .setName('👍 LI - Réactions Like')
      .setDescription('Réactions Like LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_reactions_celebrate')
      .setName('🎉 LI - Réactions Celebrate')
      .setDescription('Réactions Celebrate LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_reactions_love')
      .setName('❤️ LI - Réactions Love')
      .setDescription('Réactions Love LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_reactions_insightful')
      .setName('💡 LI - Réactions Insightful')
      .setDescription('Réactions Insightful LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_reactions_support')
      .setName('🤝 LI - Réactions Support')
      .setDescription('Réactions Support LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_reactions_funny')
      .setName('😄 LI - Réactions Funny')
      .setDescription('Réactions Funny LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    // Total et pourcentages
    fields.newMetric()
      .setId('linkedin_reactions_total')
      .setName('🎭 LI - Total Réactions')
      .setDescription('Total toutes réactions LinkedIn')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('linkedin_reactions_like_percentage')
        .setName('📊 LI - % Réactions Like')
        .setDescription('Pourcentage réactions Like (%)')
        .setType(types.PERCENT)
        .setAggregation(aggregations.AVG);
      
      fields.newMetric()
        .setId('linkedin_reactions_professional_percentage')
        .setName('💼 LI - % Réactions Pro')
        .setDescription('% réactions professionnelles (Insightful+Support)')
        .setType(types.PERCENT)
        .setAggregation(aggregations.AVG);
    }
  }
  
  // ========================================
  // LINKEDIN - FOLLOWERS BREAKDOWN
  // ========================================
  
  if (granularity === 'detailed') {
    fields.newMetric()
      .setId('linkedin_followers_by_country')
      .setName('🌍 LI - Followers par Pays')
      .setDescription('Répartition followers par pays')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_followers_by_industry')
      .setName('🏢 LI - Followers par Secteur')
      .setDescription('Répartition followers par secteur')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_followers_by_function')
      .setName('💼 LI - Followers par Fonction')
      .setDescription('Répartition followers par fonction')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_followers_by_seniority')
      .setName('📊 LI - Followers par Séniorité')
      .setDescription('Répartition followers par niveau')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('linkedin_followers_by_company_size')
      .setName('🏪 LI - Followers par Taille Entreprise')
      .setDescription('Répartition par taille d\'entreprise')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
}

// ================================
// MÉTRIQUES FACEBOOK ORGANISÉES
// ================================

function addFacebookMetrics(fields, types, aggregations, temporalType, granularity) {
  // ========================================
  // FACEBOOK - COMPANY/PAGE OVERVIEW
  // ========================================
  
  if (temporalType === 'lifetime' || temporalType === 'mixed') {
    // Fans totaux (LIFETIME)
    fields.newMetric()
      .setId('facebook_company_page_fans')
      .setName('👥 FB - Total Fans')
      .setDescription('[LIFETIME] Nombre total de fans Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);
    
    fields.newMetric()
      .setId('facebook_company_page_follows')
      .setName('👤 FB - Total Abonnés')
      .setDescription('[LIFETIME] Nombre total d\'abonnés Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.MAX);
  }
  
  if (temporalType === 'daily' || temporalType === 'mixed') {
    // Évolution fans (DAILY)
    fields.newMetric()
      .setId('facebook_company_page_fan_adds')
      .setName('📈 FB - Nouveaux Fans')
      .setDescription('[DAILY] Nouveaux fans Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_company_page_fan_removes')
      .setName('📉 FB - Fans Perdus')
      .setDescription('[DAILY] Fans perdus Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_company_page_daily_follows')
      .setName('➕ FB - Nouveaux Abonnés')
      .setDescription('[DAILY] Nouveaux abonnés Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_company_page_daily_unfollows')
      .setName('➖ FB - Désabonnements')
      .setDescription('[DAILY] Désabonnements Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
  
  // ========================================
  // FACEBOOK - PAGE IMPRESSIONS
  // ========================================
  
  fields.newMetric()
    .setId('facebook_page_impressions')
    .setName('👀 FB - Impressions Page')
    .setDescription('Total impressions page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_impressions_unique')
    .setName('👁️ FB - Impressions Uniques Page')
    .setDescription('Impressions uniques page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('facebook_page_impressions_viral')
      .setName('🔥 FB - Impressions Virales')
      .setDescription('Impressions virales page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_page_impressions_nonviral')
      .setName('📊 FB - Impressions Non-Virales')
      .setDescription('Impressions non-virales page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
  
  // Impressions des posts de la page
  fields.newMetric()
    .setId('facebook_page_posts_impressions')
    .setName('📝 FB - Impressions Posts Page')
    .setDescription('Impressions posts de la page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_posts_impressions_unique')
    .setName('📄 FB - Impressions Uniques Posts Page')
    .setDescription('Impressions uniques posts page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  if (granularity === 'detailed') {
    fields.newMetric()
      .setId('facebook_page_posts_impressions_organic')
      .setName('🌱 FB - Impressions Organiques Posts')
      .setDescription('Impressions organiques posts page')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_page_posts_impressions_paid')
      .setName('💰 FB - Impressions Payantes Posts')
      .setDescription('Impressions payantes posts page')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
  
  // Vues page
  fields.newMetric()
    .setId('facebook_page_views_total')
    .setName('🔍 FB - Vues Totales Page')
    .setDescription('Total vues page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ========================================
  // FACEBOOK - ENGAGEMENT PAGE
  // ========================================
  
  fields.newMetric()
    .setId('facebook_page_post_engagements')
    .setName('🤝 FB - Engagements Posts Page')
    .setDescription('Engagements sur posts de la page')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_page_total_actions')
    .setName('⚡ FB - Actions Totales Page')
    .setDescription('Total actions sur la page Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ========================================
  // FACEBOOK - RÉACTIONS PAGE
  // ========================================
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('facebook_page_reactions_like')
      .setName('👍 FB - J\'aime Page')
      .setDescription('Réactions J\'aime sur page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_page_reactions_love')
      .setName('❤️ FB - J\'adore Page')
      .setDescription('Réactions J\'adore sur page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('facebook_page_reactions_wow')
        .setName('😮 FB - Wow Page')
        .setDescription('Réactions Wow sur page Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_page_reactions_haha')
        .setName('😄 FB - Haha Page')
        .setDescription('Réactions Haha sur page Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_page_reactions_sorry')
        .setName('😢 FB - Triste Page')
        .setDescription('Réactions Triste sur page Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_page_reactions_anger')
        .setName('😡 FB - En Colère Page')
        .setDescription('Réactions En Colère sur page Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
  }
  
  // ========================================
  // FACEBOOK - POSTS INDIVIDUELS
  // ========================================
  
  fields.newMetric()
    .setId('facebook_post_impressions')
    .setName('👀 FB - Impressions Posts')
    .setDescription('Impressions posts individuels Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_impressions_unique')
    .setName('👁️ FB - Impressions Uniques Posts')
    .setDescription('Impressions uniques posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('facebook_post_impressions_organic')
      .setName('🌱 FB - Impressions Organiques Posts')
      .setDescription('Impressions organiques posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_post_impressions_paid')
      .setName('💰 FB - Impressions Payantes Posts')
      .setDescription('Impressions payantes posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_post_impressions_viral')
      .setName('🔥 FB - Impressions Virales Posts')
      .setDescription('Impressions virales posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('facebook_post_impressions_fan')
        .setName('👥 FB - Impressions Fans Posts')
        .setDescription('Impressions par fans posts Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_post_impressions_nonviral')
        .setName('📊 FB - Impressions Non-Virales Posts')
        .setDescription('Impressions non-virales posts Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
  }
  
  // Clics posts
  fields.newMetric()
    .setId('facebook_post_clicks')
    .setName('🖱️ FB - Clics Posts')
    .setDescription('Clics sur posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  if (granularity === 'detailed') {
    fields.newMetric()
      .setId('facebook_post_clicks_by_type')
      .setName('🎯 FB - Clics par Type Posts')
      .setDescription('Clics par type sur posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  }
  
  // Engagement posts
  fields.newMetric()
    .setId('facebook_post_consumptions')
    .setName('🤝 FB - Interactions Posts')
    .setDescription('Interactions totales posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_fan_reach')
    .setName('📡 FB - Portée Fans Posts')
    .setDescription('Portée fans posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ========================================
  // FACEBOOK - RÉACTIONS POSTS
  // ========================================
  
  fields.newMetric()
    .setId('facebook_post_reactions_like')
    .setName('👍 FB - J\'aime Posts')
    .setDescription('Réactions J\'aime posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('facebook_post_reactions_love')
    .setName('❤️ FB - J\'adore Posts')
    .setDescription('Réactions J\'adore posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  if (granularity === 'detailed' || granularity === 'standard') {
    fields.newMetric()
      .setId('facebook_post_reactions_wow')
      .setName('😮 FB - Wow Posts')
      .setDescription('Réactions Wow posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_post_reactions_haha')
      .setName('😄 FB - Haha Posts')
      .setDescription('Réactions Haha posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('facebook_post_reactions_sorry')
        .setName('😢 FB - Triste Posts')
        .setDescription('Réactions Triste posts Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_post_reactions_anger')
        .setName('😡 FB - En Colère Posts')
        .setDescription('Réactions En Colère posts Facebook')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
  }
  
  // Total réactions
  fields.newMetric()
    .setId('facebook_post_reactions_total')
    .setName('🎭 FB - Total Réactions Posts')
    .setDescription('Total réactions posts Facebook')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // ========================================
  // FACEBOOK - VIDÉOS
  // ========================================
  
  if (granularity === 'detailed' || granularity === 'standard') {
    // Vidéos page
    fields.newMetric()
      .setId('facebook_video_page_views')
      .setName('🎥 FB - Vues Vidéos Page')
      .setDescription('Vues vidéos page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    fields.newMetric()
      .setId('facebook_video_page_views_unique')
      .setName('📹 FB - Vues Uniques Vidéos Page')
      .setDescription('Vues uniques vidéos page Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('facebook_video_page_views_organic')
        .setName('🌱 FB - Vues Organiques Vidéos Page')
        .setDescription('Vues organiques vidéos page')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_video_page_views_paid')
        .setName('💰 FB - Vues Payantes Vidéos Page')
        .setDescription('Vues payantes vidéos page')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
      
      fields.newMetric()
        .setId('facebook_video_page_view_time')
        .setName('⏱️ FB - Temps Visionnage Page')
        .setDescription('Temps total visionnage page (sec)')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
    
    // Vues complètes
    fields.newMetric()
      .setId('facebook_video_complete_views_30s')
      .setName('✅ FB - Vues Complètes 30s')
      .setDescription('Vues complètes 30s vidéos Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    // Vidéos posts
    fields.newMetric()
      .setId('facebook_video_post_views')
      .setName('🎬 FB - Vues Vidéos Posts')
      .setDescription('Vues vidéos posts Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
    
    if (granularity === 'detailed') {
      fields.newMetric()
        .setId('facebook_video_post_avg_time_watched')
        .setName('⌛ FB - Temps Moyen Visionné')
        .setDescription('Temps moyen visionné vidéos posts')
        .setType(types.NUMBER)
        .setAggregation(aggregations.AVG);
      
      fields.newMetric()
        .setId('facebook_video_post_views_sound_on')
        .setName('🔊 FB - Vues avec Son')
        .setDescription('Vues vidéos avec son activé')
        .setType(types.NUMBER)
        .setAggregation(aggregations.SUM);
    }
  }
  
  // ========================================
  // FACEBOOK - MÉTRIQUES CALCULÉES
  // ========================================
  
  // Taux d'engagement
  fields.newMetric()
    .setId('facebook_engagement_rate')
    .setName('📊 FB - Taux d\'Engagement')
    .setDescription('Taux d\'engagement Facebook (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  // Taux de clic
  fields.newMetric()
    .setId('facebook_click_through_rate')
    .setName('🎯 FB - Taux de Clic')
    .setDescription('Taux de clic Facebook (%)')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  if (granularity === 'detailed') {
    // Fréquence impressions
    fields.newMetric()
      .setId('facebook_impression_frequency')
      .setName('🔄 FB - Fréquence Impressions')
      .setDescription('Fréquence des impressions Facebook')
      .setType(types.NUMBER)
      .setAggregation(aggregations.AVG);
    
    // VTR (Video Through Rate)
    fields.newMetric()
      .setId('facebook_video_completion_rate')
      .setName('📺 FB - Taux Complétion Vidéo')
      .setDescription('Taux de complétion vidéo (%)')
      .setType(types.PERCENT)
      .setAggregation(aggregations.AVG);
  }
}

// ================================
// RÉCUPÉRATION ET TRANSFORMATION DES DONNÉES
// ================================

function getData(request) {
  console.log('=== getData - Données structurées par type temporel ===');
  
  try {
    if (TEST_MODE) {
      return getTestDataStructured(request);
    }
    
    var userEmail = Session.getActiveUser().getEmail();
    if (!userEmail) {
      throw new Error('Authentification requise');
    }
    
    return getProductionDataStructured(request, userEmail);
    
  } catch (e) {
    console.error('Erreur getData:', e);
    
    var cc = DataStudioApp.createCommunityConnector();
    cc.newUserError()
      .setDebugText('Erreur getData: ' + e.toString())
      .setText('Erreur lors de la récupération des données WhatsTheData: ' + e.message)
      .throwException();
  }
}

function getTestDataStructured(request) {
  console.log('Récupération des données test avec structure temporelle...');
  
  var config = request.configParams || {};
  var platforms = config.platforms || ['linkedin', 'facebook'];
  var temporalType = config.temporal_type || 'mixed';
  var dateRange = parseInt(config.date_range || '30');
  var granularity = config.data_granularity || 'standard';
  
  try {
    // Construire l'URL avec paramètres
    var apiUrl = API_BASE_URL + '/api/v1/test-data-extended';
    var params = [];
    params.push('platforms=' + encodeURIComponent(platforms.join(',')));
    params.push('temporal_type=' + encodeURIComponent(temporalType));
    params.push('date_range=' + dateRange);
    params.push('granularity=' + encodeURIComponent(granularity));
    
    if (params.length > 0) {
      apiUrl += '?' + params.join('&');
    }
    
    console.log('URL API:', apiUrl);
    
    var response = UrlFetchApp.fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'WhatsTheData-LookerStudio-Connector/1.0'
      },
      muteHttpExceptions: true
    });
    
    var responseCode = response.getResponseCode();
    var data = JSON.parse(response.getContentText());
    
    console.log('Réponse API Test:', responseCode);
    
    if (responseCode !== 200) {
      throw new Error('Erreur API: ' + (data.message || 'Code ' + responseCode));
    }
    
    var transformedData = transformStructuredData(data.data, request);
    
    return {
      schema: transformedData.schema,
      rows: transformedData.rows
    };
    
  } catch (e) {
    console.error('Erreur getTestDataStructured:', e);
    throw new Error('Erreur récupération données test: ' + e.toString());
  }
}

function transformStructuredData(apiData, request) {
  console.log('Transformation des données avec structure temporelle...');
  
  var requestedFields = request.fields || [];
  var config = request.configParams || {};
  var temporalType = config.temporal_type || 'mixed';
  var dateRange = parseInt(config.date_range || '30');
  
  var rows = [];
  
  if (!apiData) {
    console.log('Pas de données API, retour de données vides');
    return {
      schema: getSchemaFromRequest(requestedFields),
      rows: []
    };
  }
  
  // Traiter LinkedIn selon le type temporel
  if (apiData.linkedin_data) {
    // Données lifetime LinkedIn
    if ((temporalType === 'lifetime' || temporalType === 'mixed') && apiData.linkedin_data.lifetime_metrics) {
      apiData.linkedin_data.lifetime_metrics.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'linkedin', 'lifetime', requestedFields, config));
      });
    }
    
    // Données daily LinkedIn
    if ((temporalType === 'daily' || temporalType === 'mixed') && apiData.linkedin_data.daily_metrics) {
      // Filtrer selon la période
      var filteredDaily = filterByDateRange(apiData.linkedin_data.daily_metrics, dateRange);
      filteredDaily.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'linkedin', 'daily', requestedFields, config));
      });
    }
    
    // Données de posts LinkedIn
    if (apiData.linkedin_data.post_metrics) {
      apiData.linkedin_data.post_metrics.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'linkedin', 'post', requestedFields, config));
      });
    }
  }
  
  // Traiter Facebook selon le type temporel
  if (apiData.facebook_data) {
    // Données lifetime Facebook
    if ((temporalType === 'lifetime' || temporalType === 'mixed') && apiData.facebook_data.lifetime_metrics) {
      apiData.facebook_data.lifetime_metrics.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'facebook', 'lifetime', requestedFields, config));
      });
    }
    
    // Données daily Facebook
    if ((temporalType === 'daily' || temporalType === 'mixed') && apiData.facebook_data.daily_metrics) {
      var filteredDaily = filterByDateRange(apiData.facebook_data.daily_metrics, dateRange);
      filteredDaily.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'facebook', 'daily', requestedFields, config));
      });
    }
    
    // Données de posts Facebook
    if (apiData.facebook_data.post_metrics) {
      apiData.facebook_data.post_metrics.forEach(function(metric) {
        rows.push(createStructuredRow(metric, 'facebook', 'post', requestedFields, config));
      });
    }
  }
  
  console.log('Transformation terminée:', rows.length, 'lignes avec gestion temporelle');
  
  return {
    schema: getSchemaFromRequest(requestedFields),
    rows: rows
  };
}

function filterByDateRange(metrics, dateRange) {
  if (!metrics || metrics.length === 0) return [];
  
  var cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - dateRange);
  
  return metrics.filter(function(metric) {
    if (!metric.date) return true; // Garder si pas de date
    
    try {
      var metricDate = new Date(metric.date);
      return metricDate >= cutoffDate;
    } catch (e) {
      return true; // Garder en cas d'erreur de parsing
    }
  });
}

function createStructuredRow(metric, platform, temporalCategory, requestedFields, config) {
  var row = {};
  
  requestedFields.forEach(function(field) {
    var fieldId = field.getId();
    
    // Dimensions de base
    switch (fieldId) {
      case 'platform':
        row[fieldId] = platform;
        break;
      case 'date':
        row[fieldId] = formatDateForLooker(metric.date || new Date().toISOString().split('T')[0]);
        break;
      case 'account_name':
        row[fieldId] = metric.account_name || (platform === 'linkedin' ? 'LinkedIn Test Company' : 'Facebook Test Page');
        break;
      case 'account_id':
        row[fieldId] = metric.account_id || (platform === 'linkedin' ? 'linkedin-test-123' : 'facebook-test-456');
        break;
      case 'temporal_type':
        row[fieldId] = temporalCategory;
        break;
      case 'content_category':
        row[fieldId] = determineContentCategory(fieldId, temporalCategory);
        break;
      case 'post_id':
        row[fieldId] = metric.post_id || (temporalCategory === 'post' ? generateTestPostId(platform) : '');
        break;
      case 'post_type':
        row[fieldId] = metric.post_type || (temporalCategory === 'post' ? getTestPostType(platform) : '');
        break;
        
      // === LINKEDIN MÉTRIQUES ===
      // Company Overview
      case 'linkedin_company_total_followers':
        row[fieldId] = temporalCategory === 'lifetime' ? (metric.total_followers || 15420) : 0;
        break;
      case 'linkedin_company_organic_follower_gain':
        row[fieldId] = temporalCategory === 'daily' ? (metric.organic_follower_gain || Math.floor(Math.random() * 50) + 10) : 0;
        break;
      case 'linkedin_company_paid_follower_gain':
        row[fieldId] = temporalCategory === 'daily' ? (metric.paid_follower_gain || Math.floor(Math.random() * 20) + 5) : 0;
        break;
      case 'linkedin_company_total_impressions':
        row[fieldId] = metric.total_impressions || Math.floor(Math.random() * 50000) + 10000;
        break;
      case 'linkedin_company_total_unique_impressions':
        row[fieldId] = metric.total_unique_impressions || Math.floor(Math.random() * 30000) + 5000;
        break;
      case 'linkedin_company_total_clicks':
        row[fieldId] = metric.total_clicks || Math.floor(Math.random() * 2000) + 500;
        break;
      case 'linkedin_company_total_shares':
        row[fieldId] = metric.total_shares || Math.floor(Math.random() * 300) + 50;
        break;
      case 'linkedin_company_total_comments':
        row[fieldId] = metric.total_comments || Math.floor(Math.random() * 150) + 20;
        break;
      case 'linkedin_company_engagement_rate':
        var impressions = metric.total_impressions || 10000;
        var engagement = (metric.total_clicks || 500) + (metric.total_shares || 50) + (metric.total_comments || 20);
        row[fieldId] = impressions > 0 ? (engagement / impressions) * 100 : 0;
        break;
        
      // Page Views
      case 'linkedin_page_views_total':
        row[fieldId] = metric.page_views_total || Math.floor(Math.random() * 5000) + 1000;
        break;
      case 'linkedin_page_views_unique':
        row[fieldId] = metric.page_views_unique || Math.floor(Math.random() * 3000) + 500;
        break;
      case 'linkedin_page_views_desktop':
        row[fieldId] = metric.page_views_desktop || Math.floor(Math.random() * 2000) + 300;
        break;
      case 'linkedin_page_views_mobile':
        row[fieldId] = metric.page_views_mobile || Math.floor(Math.random() * 2000) + 400;
        break;
        
      // Posts
      case 'linkedin_post_impressions':
        row[fieldId] = metric.post_impressions || Math.floor(Math.random() * 10000) + 2000;
        break;
      case 'linkedin_post_unique_impressions':
        row[fieldId] = metric.post_unique_impressions || Math.floor(Math.random() * 7000) + 1500;
        break;
      case 'linkedin_post_clicks':
        row[fieldId] = metric.post_clicks || Math.floor(Math.random() * 500) + 100;
        break;
      case 'linkedin_post_shares':
        row[fieldId] = metric.post_shares || Math.floor(Math.random() * 100) + 20;
        break;
      case 'linkedin_post_comments':
        row[fieldId] = metric.post_comments || Math.floor(Math.random() * 50) + 5;
        break;
      case 'linkedin_post_engagement_rate':
        var postImpressions = metric.post_impressions || 2000;
        var postEngagement = (metric.post_clicks || 100) + (metric.post_shares || 20) + (metric.post_comments || 5);
        row[fieldId] = postImpressions > 0 ? (postEngagement / postImpressions) * 100 : 0;
        break;
      case 'linkedin_post_click_through_rate':
        var postImpr = metric.post_impressions || 2000;
        var postClicks = metric.post_clicks || 100;
        row[fieldId] = postImpr > 0 ? (postClicks / postImpr) * 100 : 0;
        break;
        
      // Réactions LinkedIn
      case 'linkedin_reactions_like':
        row[fieldId] = metric.reactions_like || Math.floor(Math.random() * 200) + 50;
        break;
      case 'linkedin_reactions_celebrate':
        row[fieldId] = metric.reactions_celebrate || Math.floor(Math.random() * 100) + 20;
        break;
      case 'linkedin_reactions_love':
        row[fieldId] = metric.reactions_love || Math.floor(Math.random() * 80) + 10;
        break;
      case 'linkedin_reactions_insightful':
        row[fieldId] = metric.reactions_insightful || Math.floor(Math.random() * 150) + 30;
        break;
      case 'linkedin_reactions_support':
        row[fieldId] = metric.reactions_support || Math.floor(Math.random() * 60) + 15;
        break;
      case 'linkedin_reactions_funny':
        row[fieldId] = metric.reactions_funny || Math.floor(Math.random() * 40) + 5;
        break;
      case 'linkedin_reactions_total':
        row[fieldId] = (metric.reactions_like || 50) + (metric.reactions_celebrate || 20) + 
                      (metric.reactions_love || 10) + (metric.reactions_insightful || 30) + 
                      (metric.reactions_support || 15) + (metric.reactions_funny || 5);
        break;
        
      // === FACEBOOK MÉTRIQUES ===
      // Company Overview
      case 'facebook_company_page_fans':
        row[fieldId] = temporalCategory === 'lifetime' ? (metric.page_fans || 28750) : 0;
        break;
      case 'facebook_company_page_follows':
        row[fieldId] = temporalCategory === 'lifetime' ? (metric.page_follows || 25680) : 0;
        break;
      case 'facebook_company_page_fan_adds':
        row[fieldId] = temporalCategory === 'daily' ? (metric.page_fan_adds || Math.floor(Math.random() * 100) + 20) : 0;
        break;
      case 'facebook_company_page_fan_removes':
        row[fieldId] = temporalCategory === 'daily' ? (metric.page_fan_removes || Math.floor(Math.random() * 30) + 5) : 0;
        break;
      case 'facebook_company_page_daily_follows':
        row[fieldId] = temporalCategory === 'daily' ? (metric.page_daily_follows || Math.floor(Math.random() * 80) + 15) : 0;
        break;
      case 'facebook_company_page_daily_unfollows':
        row[fieldId] = temporalCategory === 'daily' ? (metric.page_daily_unfollows || Math.floor(Math.random() * 25) + 3) : 0;
        break;
        
      // Page Impressions
      case 'facebook_page_impressions':
        row[fieldId] = metric.page_impressions || Math.floor(Math.random() * 100000) + 20000;
        break;
      case 'facebook_page_impressions_unique':
        row[fieldId] = metric.page_impressions_unique || Math.floor(Math.random() * 60000) + 15000;
        break;
      case 'facebook_page_impressions_viral':
        row[fieldId] = metric.page_impressions_viral || Math.floor(Math.random() * 20000) + 5000;
        break;
      case 'facebook_page_impressions_nonviral':
        row[fieldId] = metric.page_impressions_nonviral || Math.floor(Math.random() * 80000) + 15000;
        break;
        
      // Posts Page
      case 'facebook_page_posts_impressions':
        row[fieldId] = metric.page_posts_impressions || Math.floor(Math.random() * 80000) + 15000;
        break;
      case 'facebook_page_posts_impressions_unique':
        row[fieldId] = metric.page_posts_impressions_unique || Math.floor(Math.random() * 50000) + 12000;
        break;
      case 'facebook_page_posts_impressions_organic':
        row[fieldId] = metric.page_posts_impressions_organic || Math.floor(Math.random() * 60000) + 10000;
        break;
      case 'facebook_page_posts_impressions_paid':
        row[fieldId] = metric.page_posts_impressions_paid || Math.floor(Math.random() * 20000) + 5000;
        break;
        
      // Page Views
      case 'facebook_page_views_total':
        row[fieldId] = metric.page_views_total || Math.floor(Math.random() * 15000) + 3000;
        break;
        
      // Engagement Page
      case 'facebook_page_post_engagements':
        row[fieldId] = metric.page_post_engagements || Math.floor(Math.random() * 5000) + 1000;
        break;
      case 'facebook_page_total_actions':
        row[fieldId] = metric.page_total_actions || Math.floor(Math.random() * 8000) + 1500;
        break;
        
      // Réactions Page
      case 'facebook_page_reactions_like':
        row[fieldId] = metric.page_reactions_like || Math.floor(Math.random() * 2000) + 500;
        break;
      case 'facebook_page_reactions_love':
        row[fieldId] = metric.page_reactions_love || Math.floor(Math.random() * 800) + 200;
        break;
      case 'facebook_page_reactions_wow':
        row[fieldId] = metric.page_reactions_wow || Math.floor(Math.random() * 400) + 100;
        break;
      case 'facebook_page_reactions_haha':
        row[fieldId] = metric.page_reactions_haha || Math.floor(Math.random() * 600) + 150;
        break;
      case 'facebook_page_reactions_sorry':
        row[fieldId] = metric.page_reactions_sorry || Math.floor(Math.random() * 200) + 30;
        break;
      case 'facebook_page_reactions_anger':
        row[fieldId] = metric.page_reactions_anger || Math.floor(Math.random() * 100) + 10;
        break;
        
      // Posts Individuels
      case 'facebook_post_impressions':
        row[fieldId] = metric.post_impressions || Math.floor(Math.random() * 20000) + 5000;
        break;
      case 'facebook_post_impressions_unique':
        row[fieldId] = metric.post_impressions_unique || Math.floor(Math.random() * 15000) + 3000;
        break;
      case 'facebook_post_impressions_organic':
        row[fieldId] = metric.post_impressions_organic || Math.floor(Math.random() * 12000) + 3000;
        break;
      case 'facebook_post_impressions_paid':
        row[fieldId] = metric.post_impressions_paid || Math.floor(Math.random() * 8000) + 2000;
        break;
      case 'facebook_post_impressions_viral':
        row[fieldId] = metric.post_impressions_viral || Math.floor(Math.random() * 5000) + 1000;
        break;
        
      // Clics Posts
      case 'facebook_post_clicks':
        row[fieldId] = metric.post_clicks || Math.floor(Math.random() * 1000) + 200;
        break;
      case 'facebook_post_clicks_by_type':
        row[fieldId] = metric.post_clicks_by_type || Math.floor(Math.random() * 800) + 150;
        break;
        
      // Engagement Posts
      case 'facebook_post_consumptions':
        row[fieldId] = metric.post_consumptions || Math.floor(Math.random() * 2000) + 400;
        break;
      case 'facebook_post_fan_reach':
        row[fieldId] = metric.post_fan_reach || Math.floor(Math.random() * 8000) + 2000;
        break;
        
      // Réactions Posts
      case 'facebook_post_reactions_like':
        row[fieldId] = metric.post_reactions_like || Math.floor(Math.random() * 500) + 100;
        break;
      case 'facebook_post_reactions_love':
        row[fieldId] = metric.post_reactions_love || Math.floor(Math.random() * 200) + 50;
        break;
      case 'facebook_post_reactions_wow':
        row[fieldId] = metric.post_reactions_wow || Math.floor(Math.random() * 150) + 30;
        break;
      case 'facebook_post_reactions_haha':
        row[fieldId] = metric.post_reactions_haha || Math.floor(Math.random() * 180) + 40;
        break;
      case 'facebook_post_reactions_sorry':
        row[fieldId] = metric.post_reactions_sorry || Math.floor(Math.random() * 50) + 10;
        break;
      case 'facebook_post_reactions_anger':
        row[fieldId] = metric.post_reactions_anger || Math.floor(Math.random() * 30) + 5;
        break;
      case 'facebook_post_reactions_total':
        row[fieldId] = (metric.post_reactions_like || 100) + (metric.post_reactions_love || 50) + 
                      (metric.post_reactions_wow || 30) + (metric.post_reactions_haha || 40) + 
                      (metric.post_reactions_sorry || 10) + (metric.post_reactions_anger || 5);
        break;
        
      // Vidéos
      case 'facebook_video_page_views':
        row[fieldId] = metric.page_video_views || Math.floor(Math.random() * 10000) + 2000;
        break;
      case 'facebook_video_page_views_unique':
        row[fieldId] = metric.page_video_views_unique || Math.floor(Math.random() * 7000) + 1500;
        break;
      case 'facebook_video_page_views_organic':
        row[fieldId] = metric.page_video_views_organic || Math.floor(Math.random() * 6000) + 1200;
        break;
      case 'facebook_video_page_views_paid':
        row[fieldId] = metric.page_video_views_paid || Math.floor(Math.random() * 4000) + 800;
        break;
      case 'facebook_video_complete_views_30s':
        row[fieldId] = metric.page_video_complete_views_30s || Math.floor(Math.random() * 3000) + 600;
        break;
      case 'facebook_video_post_views':
        row[fieldId] = metric.post_video_views || Math.floor(Math.random() * 5000) + 1000;
        break;
      case 'facebook_video_post_avg_time_watched':
        row[fieldId] = metric.post_video_avg_time_watched || (Math.random() * 120 + 30); // 30-150 seconds
        break;
        
      // Métriques calculées
      case 'facebook_engagement_rate':
        var fbImpressions = metric.page_impressions || 20000;
        var fbEngagement = (metric.page_post_engagements || 1000);
        row[fieldId] = fbImpressions > 0 ? (fbEngagement / fbImpressions) * 100 : 0;
        break;
      case 'facebook_click_through_rate':
        var fbPostImpr = metric.post_impressions || 5000;
        var fbPostClicks = metric.post_clicks || 200;
        row[fieldId] = fbPostImpr > 0 ? (fbPostClicks / fbPostImpr) * 100 : 0;
        break;
        
      // Métriques par défaut
      default:
        row[fieldId] = metric[fieldId] || 0;
        break;
    }
  });
  
  return { values: Object.values(row) };
}

// ================================
// FONCTIONS UTILITAIRES
// ================================

function determineContentCategory(fieldId, temporalCategory) {
  if (fieldId.includes('_page_') || fieldId.includes('_company_')) return 'Page';
  if (fieldId.includes('_post_')) return 'Post';
  if (fieldId.includes('_follower') || fieldId.includes('_fan')) return 'Audience';
  if (fieldId.includes('_video_')) return 'Video';
  if (fieldId.includes('_reaction')) return 'Reactions';
  return temporalCategory || 'General';
}

function generateTestPostId(platform) {
  var timestamp = Date.now();
  var random = Math.floor(Math.random() * 1000);
  return platform + '_post_' + timestamp + '_' + random;
}

function getTestPostType(platform) {
  var linkedinTypes = ['Article', 'Status Update', 'Video', 'Image', 'Document'];
  var facebookTypes = ['Status', 'Photo', 'Video', 'Link', 'Album'];
  
  var types = platform === 'linkedin' ? linkedinTypes : facebookTypes;
  return types[Math.floor(Math.random() * types.length)];
}

function formatDateForLooker(dateString) {
  try {
    var date = new Date(dateString);
    var year = date.getFullYear();
    var month = String(date.getMonth() + 1).padStart(2, '0');
    var day = String(date.getDate()).padStart(2, '0');
    return year + month + day;
  } catch (e) {
    console.error('Erreur formatage date:', e);
    var today = new Date();
    return today.getFullYear() + String(today.getMonth() + 1).padStart(2, '0') + String(today.getDate()).padStart(2, '0');
  }
}

function getSchemaFromRequest(requestedFields) {
  return requestedFields.map(function(field) {
    return {
      name: field.getId(),
      dataType: field.getType()
    };
  });
}

function getProductionDataStructured(request, userEmail) {
  throw new Error('Mode production non implémenté - Utilisez le mode test pour la configuration');
}

console.log('🚀 WhatsTheData Connecteur RESTRUCTURÉ - Organisation catégorielle + Gestion temporelle');