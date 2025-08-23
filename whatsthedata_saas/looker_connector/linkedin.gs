/**
 * ============================================================================
 * WHATSTHEDATA - CONNECTEUR LOOKER STUDIO LINKEDIN COMPLET
 * ============================================================================
 * Version Apps Script intégrant toutes les métriques LinkedIn
 * Migré depuis les scripts Python existants
 * ============================================================================
 */

// Configuration globale
var CONFIG = {
  API_BASE_URL: 'https://api.linkedin.com/rest',
  API_VERSION: '202505',
  MAX_EXECUTION_TIME: 300000, // 5 minutes en millisecondes
  BATCH_SIZE: 10, // Traitement par lots pour éviter les timeouts
  CACHE_DURATION: 3600, // 1 heure en secondes
};

// ================================
// AUTHENTIFICATION ET CONFIGURATION
// ================================

function getAuthType() {
  return DataStudioApp.createCommunityConnector()
    .newAuthTypeResponse()
    .setAuthType(DataStudioApp.createCommunityConnector().AuthType.USER_TOKEN)
    .setAuthUrl('https://www.linkedin.com/oauth/v2/authorization')
    .build();
}

function isAuthValid() {
  try {
    var token = getOAuthToken();
    if (!token) return false;
    
    // Vérifier le token avec l'API LinkedIn
    var response = makeLinkedInRequest('/me', {}, token);
    return response && response.id;
  } catch (e) {
    console.error('Erreur validation auth:', e);
    return false;
  }
}

function resetAuth() {
  var userProperties = PropertiesService.getUserProperties();
  userProperties.deleteProperty('oauth_token');
  userProperties.deleteProperty('selected_organizations');
}

function getOAuthToken() {
  // En production, récupérer le token OAuth
  var userProperties = PropertiesService.getUserProperties();
  var token = userProperties.getProperty('oauth_token');
  
  // Pour le développement, utiliser un token fixe depuis les properties du script
  if (!token) {
    token = PropertiesService.getScriptProperties().getProperty('LINKEDIN_ACCESS_TOKEN');
  }
  
  return token;
}

// ================================
// CONFIGURATION DU CONNECTEUR
// ================================

function getConfig(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var config = cc.getConfig();
  
  config
    .newInfo()
    .setId('header')
    .setText('📊 LinkedIn Analytics - Données Complètes');
  
  config
    .newSelectMultiple()
    .setId('organizations')
    .setName('Organisations LinkedIn')
    .setHelpText('Sélectionnez les organisations à analyser')
    .setAllowOverride(true);
  
  // Récupérer les organisations disponibles pour cet utilisateur
  try {
    var organizations = getUserOrganizations();
    organizations.forEach(function(org) {
      config.getOptionBuilder().setLabel(org.name).setValue(org.id);
    });
  } catch (e) {
    console.warn('Impossible de récupérer les organisations:', e);
  }
  
  config
    .newSelectSingle()
    .setId('data_type')
    .setName('Type de données')
    .addOption(config.newOptionBuilder().setLabel('Vue d\'ensemble').setValue('overview'))
    .addOption(config.newOptionBuilder().setLabel('Métriques des posts').setValue('posts'))
    .addOption(config.newOptionBuilder().setLabel('Statistiques des followers').setValue('followers'))
    .addOption(config.newOptionBuilder().setLabel('Statistiques de partage').setValue('shares'))
    .addOption(config.newOptionBuilder().setLabel('Statistiques quotidiennes').setValue('daily'))
    .setAllowOverride(true);
  
  config
    .newSelectSingle()
    .setId('date_range')
    .setName('Période')
    .addOption(config.newOptionBuilder().setLabel('7 derniers jours').setValue('7'))
    .addOption(config.newOptionBuilder().setLabel('30 derniers jours').setValue('30'))
    .addOption(config.newOptionBuilder().setLabel('90 derniers jours').setValue('90'))
    .addOption(config.newOptionBuilder().setLabel('Données lifetime').setValue('lifetime'))
    .setAllowOverride(true);
  
  return config.build();
}

// ================================
// GESTION DES ORGANISATIONS
// ================================

function getUserOrganizations() {
  var token = getOAuthToken();
  if (!token) return [];
  
  try {
    var response = makeLinkedInRequest('/organizationAcls?q=roleAssignee', {}, token);
    if (!response || !response.elements) return [];
    
    var organizations = [];
    response.elements.forEach(function(element) {
      if (element.organization) {
        var orgId = extractIdFromUrn(element.organization);
        var orgDetails = getOrganizationDetails(orgId, token);
        if (orgDetails) {
          organizations.push({
            id: orgId,
            name: orgDetails.localizedName || 'Organisation ' + orgId
          });
        }
      }
    });
    
    return organizations;
  } catch (e) {
    console.error('Erreur récupération organisations:', e);
    return [];
  }
}

function getOrganizationDetails(orgId, token) {
  try {
    var response = makeLinkedInRequest('/organizations/' + orgId, {}, token);
    return response;
  } catch (e) {
    console.warn('Impossible de récupérer les détails de l\'organisation:', orgId);
    return null;
  }
}

// ================================
// UTILITAIRES API LINKEDIN
// ================================

function makeLinkedInRequest(endpoint, params, token) {
  var url = CONFIG.API_BASE_URL + endpoint;
  
  // Ajouter les paramètres de query si présents
  if (params && Object.keys(params).length > 0) {
    var queryString = Object.keys(params)
      .map(function(key) { return key + '=' + encodeURIComponent(params[key]); })
      .join('&');
    url += (url.indexOf('?') === -1 ? '?' : '&') + queryString;
  }
  
  var options = {
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + token,
      'X-Restli-Protocol-Version': '2.0.0',
      'LinkedIn-Version': CONFIG.API_VERSION,
      'Content-Type': 'application/json'
    }
  };
  
  var response = UrlFetchApp.fetch(url, options);
  var responseCode = response.getResponseCode();
  
  if (responseCode === 200) {
    return JSON.parse(response.getContentText());
  } else if (responseCode === 429) {
    // Rate limit - attendre et réessayer
    Utilities.sleep(2000);
    return makeLinkedInRequest(endpoint, params, token);
  } else {
    throw new Error('Erreur API LinkedIn: ' + responseCode + ' - ' + response.getContentText());
  }
}

function extractIdFromUrn(urn) {
  if (!urn) return null;
  var parts = urn.split(':');
  return parts[parts.length - 1];
}

// ================================
// SCHÉMA COMPLET OPTIMISÉ
// ================================

function getSchema(request) {
  var cc = DataStudioApp.createCommunityConnector();
  var fields = cc.getFields();
  var types = cc.FieldType;
  var aggregations = cc.AggregationType;
  
  // Configuration basée sur le type de données demandé
  var dataType = request.configParams && request.configParams.data_type || 'overview';
  
  // Dimensions communes
  addCommonDimensions(fields, types);
  
  // Métriques spécifiques selon le type
  switch (dataType) {
    case 'posts':
      addPostMetrics(fields, types, aggregations);
      break;
    case 'followers':
      addFollowerMetrics(fields, types, aggregations);
      break;
    case 'shares':
      addShareMetrics(fields, types, aggregations);
      break;
    case 'daily':
      addDailyMetrics(fields, types, aggregations);
      break;
    default:
      addOverviewMetrics(fields, types, aggregations);
  }
  
  return { schema: fields.build() };
}

function addCommonDimensions(fields, types) {
  fields.newDimension()
    .setId('date')
    .setName('Date')
    .setType(types.YEAR_MONTH_DAY);
  
  fields.newDimension()
    .setId('organization_id')
    .setName('Organization ID')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('organization_name')
    .setName('Organization Name')
    .setType(types.TEXT);
}

function addPostMetrics(fields, types, aggregations) {
  // Métriques des posts individuels
  fields.newDimension()
    .setId('post_id')
    .setName('Post ID')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('post_type')
    .setName('Post Type')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('media_type')
    .setName('Media Type')
    .setType(types.TEXT);
  
  // Métriques de performance
  fields.newMetric()
    .setId('impressions')
    .setName('Impressions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('unique_impressions')
    .setName('Unique Impressions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('clicks')
    .setName('Clicks')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('shares')
    .setName('Shares')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('comments')
    .setName('Comments')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('reactions_total')
    .setName('Total Reactions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  // Réactions détaillées
  ['like', 'praise', 'empathy', 'interest', 'appreciation', 'entertainment'].forEach(function(reaction) {
    fields.newMetric()
      .setId('reactions_' + reaction)
      .setName('Reactions ' + reaction.charAt(0).toUpperCase() + reaction.slice(1))
      .setType(types.NUMBER)
      .setAggregation(aggregations.SUM);
  });
  
  fields.newMetric()
    .setId('engagement_rate')
    .setName('Engagement Rate')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
}

function addFollowerMetrics(fields, types, aggregations) {
  // Dimensions pour les followers
  fields.newDimension()
    .setId('category_type')
    .setName('Category Type')
    .setType(types.TEXT);
  
  fields.newDimension()
    .setId('category_value')
    .setName('Category Value')
    .setType(types.TEXT);
  
  // Métriques des followers
  fields.newMetric()
    .setId('followers_total')
    .setName('Total Followers')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('followers_organic')
    .setName('Organic Followers')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_paid')
    .setName('Paid Followers')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('followers_percentage')
    .setName('Followers Percentage')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
}

function addShareMetrics(fields, types, aggregations) {
  // Métriques de partage (lifetime)
  fields.newMetric()
    .setId('impression_count')
    .setName('Impression Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('unique_impressions_count')
    .setName('Unique Impressions Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('click_count')
    .setName('Click Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('like_count')
    .setName('Like Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('comment_count')
    .setName('Comment Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('share_count')
    .setName('Share Count')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('engagement')
    .setName('Engagement Rate')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
}

function addDailyMetrics(fields, types, aggregations) {
  // Statistiques quotidiennes
  fields.newMetric()
    .setId('daily_page_views')
    .setName('Daily Page Views')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('daily_unique_page_views')
    .setName('Daily Unique Page Views')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('daily_follower_gain')
    .setName('Daily Follower Gain')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('daily_organic_follower_gain')
    .setName('Daily Organic Follower Gain')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('daily_paid_follower_gain')
    .setName('Daily Paid Follower Gain')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
}

function addOverviewMetrics(fields, types, aggregations) {
  // Vue d'ensemble combinée
  fields.newMetric()
    .setId('total_followers')
    .setName('Total Followers')
    .setType(types.NUMBER)
    .setAggregation(aggregations.MAX);
  
  fields.newMetric()
    .setId('total_impressions')
    .setName('Total Impressions')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_clicks')
    .setName('Total Clicks')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
  
  fields.newMetric()
    .setId('total_engagement_rate')
    .setName('Total Engagement Rate')
    .setType(types.PERCENT)
    .setAggregation(aggregations.AVG);
  
  fields.newMetric()
    .setId('total_page_views')
    .setName('Total Page Views')
    .setType(types.NUMBER)
    .setAggregation(aggregations.SUM);
}

// ================================
// RÉCUPÉRATION DES DONNÉES
// ================================

function getData(request) {
  try {
    var token = getOAuthToken();
    if (!token) {
      throw new Error('Token d\'authentification manquant');
    }
    
    var dataType = request.configParams && request.configParams.data_type || 'overview';
    var organizations = request.configParams && request.configParams.organizations || [];
    var dateRange = request.configParams && request.configParams.date_range || '30';
    
    if (!organizations || organizations.length === 0) {
      throw new Error('Aucune organisation sélectionnée');
    }
    
    var data = [];
    var startTime = Date.now();
    
    // Traitement par lots pour éviter les timeouts
    for (var i = 0; i < organizations.length; i++) {
      // Vérifier le temps d'exécution
      if (Date.now() - startTime > CONFIG.MAX_EXECUTION_TIME) {
        console.warn('Timeout approché, arrêt du traitement');
        break;
      }
      
      var orgId = organizations[i];
      var orgData = getOrganizationData(orgId, dataType, dateRange, token);
      
      if (orgData && orgData.length > 0) {
        data = data.concat(orgData);
      }
      
      // Pause entre les organisations
      if (i < organizations.length - 1) {
        Utilities.sleep(1000);
      }
    }
    
    return {
      schema: getSchemaFromRequest(request.fields),
      rows: data
    };
    
  } catch (e) {
    console.error('Erreur getData:', e);
    var cc = DataStudioApp.createCommunityConnector();
    cc.newUserError()
      .setDebugText('Erreur getData: ' + e.toString())
      .setText('Erreur lors de la récupération des données LinkedIn: ' + e.message)
      .throwException();
  }
}

function getOrganizationData(orgId, dataType, dateRange, token) {
  try {
    switch (dataType) {
      case 'posts':
        return getPostMetrics(orgId, dateRange, token);
      case 'followers':
        return getFollowerStatistics(orgId, token);
      case 'shares':
        return getShareStatistics(orgId, token);
      case 'daily':
        return getDailyStatistics(orgId, dateRange, token);
      default:
        return getOverviewData(orgId, dateRange, token);
    }
  } catch (e) {
    console.error('Erreur récupération données org', orgId, ':', e);
    return [];
  }
}

// ================================
// MÉTRIQUES DES POSTS
// ================================

function getPostMetrics(orgId, dateRange, token) {
  var data = [];
  var organizationUrn = 'urn:li:organization:' + orgId;
  var orgDetails = getOrganizationDetails(orgId, token);
  var orgName = orgDetails ? orgDetails.localizedName : 'Organisation ' + orgId;
  
  // Récupérer les posts
  var postsResponse = makeLinkedInRequest('/posts', {
    q: 'author',
    author: organizationUrn,
    count: 50,
    sortBy: 'CREATED'
  }, token);
  
  if (!postsResponse || !postsResponse.elements) {
    return data;
  }
  
  // Traiter chaque post
  for (var i = 0; i < Math.min(postsResponse.elements.length, 20); i++) {
    var post = postsResponse.elements[i];
    var postData = processPost(post, orgId, orgName, token);
    
    if (postData) {
      data.push(postData);
    }
    
    // Limite de temps
    if (i > 0 && i % 5 === 0) {
      Utilities.sleep(500);
    }
  }
  
  return data;
}

function processPost(post, orgId, orgName, token) {
  try {
    var postId = post.id;
    var createdAt = post.publishedAt ? new Date(post.publishedAt) : new Date();
    
    // Extraire le contenu
    var content = extractPostContent(post);
    
    // Récupérer les métriques
    var metrics = getPostMetricsData(postId, token);
    
    return {
      values: [
        formatDate(createdAt), // date
        orgId, // organization_id
        orgName, // organization_name
        postId, // post_id
        content.postType, // post_type
        content.mediaType, // media_type
        metrics.impressions || 0, // impressions
        metrics.uniqueImpressions || 0, // unique_impressions
        metrics.clicks || 0, // clicks
        metrics.shares || 0, // shares
        metrics.comments || 0, // comments
        metrics.reactionsTotal || 0, // reactions_total
        metrics.reactionsLike || 0, // reactions_like
        metrics.reactionsPraise || 0, // reactions_praise
        metrics.reactionsEmpathy || 0, // reactions_empathy
        metrics.reactionsInterest || 0, // reactions_interest
        metrics.reactionsAppreciation || 0, // reactions_appreciation
        metrics.reactionsEntertainment || 0, // reactions_entertainment
        metrics.engagementRate || 0 // engagement_rate
      ]
    };
  } catch (e) {
    console.error('Erreur traitement post:', e);
    return null;
  }
}

function extractPostContent(post) {
  var content = {
    postType: 'ugcPost',
    mediaType: 'NONE'
  };
  
  // Détecter le type de post
  if (post.resharedShare || post.resharedPost) {
    content.postType = 'reshare';
  }
  
  // Détecter le type de média
  if (post.content) {
    if (post.content.article) {
      content.mediaType = 'ARTICLE';
    } else if (post.content.media && post.content.media.length > 0) {
      content.mediaType = 'IMAGE'; // Simplification
    }
  }
  
  return content;
}

function getPostMetricsData(postUrn, token) {
  var metrics = {
    impressions: 0,
    uniqueImpressions: 0,
    clicks: 0,
    shares: 0,
    comments: 0,
    reactionsTotal: 0,
    engagementRate: 0
  };
  
  try {
    // Actions sociales
    var socialActions = makeLinkedInRequest('/socialActions/' + encodeURIComponent(postUrn), {}, token);
    if (socialActions) {
      if (socialActions.likesSummary) {
        // Extraire le nombre de likes (approximation)
        metrics.reactionsTotal += socialActions.likesSummary.totalLikes || 0;
        metrics.reactionsLike = socialActions.likesSummary.totalLikes || 0;
      }
      
      if (socialActions.commentsSummary) {
        metrics.comments = socialActions.commentsSummary.aggregatedTotalComments || 0;
      }
    }
  } catch (e) {
    console.warn('Impossible de récupérer les actions sociales:', e);
  }
  
  try {
    // Statistiques de partage
    var shareStats = getPostShareStatistics(postUrn, token);
    if (shareStats) {
      metrics.impressions = shareStats.impressionCount || 0;
      metrics.uniqueImpressions = shareStats.uniqueImpressionsCount || 0;
      metrics.clicks = shareStats.clickCount || 0;
      metrics.shares = shareStats.shareCount || 0;
      metrics.engagementRate = shareStats.engagement || 0;
    }
  } catch (e) {
    console.warn('Impossible de récupérer les statistiques de partage:', e);
  }
  
  return metrics;
}

function getPostShareStatistics(postUrn, token) {
  // Cette fonction nécessite des droits spéciaux et peut ne pas fonctionner
  // pour tous les types de posts
  try {
    var orgId = extractIdFromUrn(postUrn.split(':')[2]); // Extraction approximative
    var response = makeLinkedInRequest('/organizationalEntityShareStatistics', {
      q: 'organizationalEntity',
      organizationalEntity: 'urn:li:organization:' + orgId,
      posts: 'List(' + postUrn + ')'
    }, token);
    
    if (response && response.elements && response.elements.length > 0) {
      return response.elements[0].totalShareStatistics;
    }
  } catch (e) {
    // Ignorer silencieusement les erreurs
  }
  
  return null;
}

// ================================
// STATISTIQUES DES FOLLOWERS
// ================================

function getFollowerStatistics(orgId, token) {
  var data = [];
  var organizationUrn = 'urn:li:organization:' + orgId;
  var orgDetails = getOrganizationDetails(orgId, token);
  var orgName = orgDetails ? orgDetails.localizedName : 'Organisation ' + orgId;
  
  try {
    var response = makeLinkedInRequest('/organizationalEntityFollowerStatistics', {
      q: 'organizationalEntity',
      organizationalEntity: organizationUrn
    }, token);
    
    if (!response || !response.elements || response.elements.length === 0) {
      return data;
    }
    
    var element = response.elements[0];
    var today = formatDate(new Date());
    
    // Traitement par catégorie
    var categories = [
      { key: 'followerCountsByStaffCountRange', type: 'company_size' },
      { key: 'followerCountsByFunction', type: 'function' },
      { key: 'followerCountsBySeniority', type: 'seniority' },
      { key: 'followerCountsByIndustry', type: 'industry' }
    ];
    
    categories.forEach(function(category) {
      if (element[category.key]) {
        element[category.key].forEach(function(item) {
          var categoryValue = getCategoryLabel(item, category.type);
          var followerCounts = item.followerCounts || {};
          var organic = followerCounts.organicFollowerCount || 0;
          var paid = followerCounts.paidFollowerCount || 0;
          var total = organic + paid;
          
          if (total > 0) {
            data.push({
              values: [
                today, // date
                orgId, // organization_id
                orgName, // organization_name
                category.type, // category_type
                categoryValue, // category_value
                total, // followers_total
                organic, // followers_organic
                paid, // followers_paid
                0 // followers_percentage (calculé côté Looker)
              ]
            });
          }
        });
      }
    });
    
  } catch (e) {
    console.error('Erreur récupération followers:', e);
  }
  
  return data;
}

function getCategoryLabel(item, categoryType) {
  switch (categoryType) {
    case 'company_size':
      return formatCompanySize(item.staffCountRange);
    case 'function':
      return formatFunction(extractIdFromUrn(item.function));
    case 'seniority':
      return formatSeniority(extractIdFromUrn(item.seniority));
    case 'industry':
      return formatIndustry(extractIdFromUrn(item.industry));
    default:
      return 'Non spécifié';
  }
}

function formatCompanySize(sizeCode) {
  var sizeMap = {
    'SIZE_1': '1 employé',
    'SIZE_2_TO_10': '2-10 employés',
    'SIZE_11_TO_50': '11-50 employés',
    'SIZE_51_TO_200': '51-200 employés',
    'SIZE_201_TO_500': '201-500 employés',
    'SIZE_501_TO_1000': '501-1000 employés',
    'SIZE_1001_TO_5000': '1001-5000 employés',
    'SIZE_5001_TO_10000': '5001-10000 employés',
    'SIZE_10001_OR_MORE': '10001+ employés'
  };
  return sizeMap[sizeCode] || sizeCode;
}

function formatFunction(functionId) {
  var functionMap = {
    '1': 'Comptabilité',
    '2': 'Administration',
    '3': 'Arts et design',
    '4': 'Commercial',
    '13': 'Marketing',
    '27': 'Technologies'
    // Ajoutez d'autres selon vos besoins
  };
  return functionMap[functionId] || ('Fonction ' + functionId);
}

function formatSeniority(seniorityId) {
  var seniorityMap = {
    '1': 'Stagiaire',
    '2': 'Débutant',
    '3': 'Junior',
    '4': 'Intermédiaire',
    '5': 'Senior',
    '6': 'Chef d\'équipe',
    '7': 'Directeur',
    '8': 'Vice-président',
    '9': 'Direction générale',
    '10': 'Cadre dirigeant'
  };
  return seniorityMap[seniorityId] || ('Niveau ' + seniorityId);
}

function formatIndustry(industryId) {
  var industryMap = {
    '4': 'Banque',
    '14': 'Finance',
    '27': 'Technologies de l\'information',
    '96': 'Logiciels informatiques'
    // Ajoutez d'autres selon vos besoins
  };
  return industryMap[industryId] || ('Industrie ' + industryId);
}

// ================================
// UTILITAIRES
// ================================

function formatDate(date) {
  if (!date) return '';
  var d = new Date(date);
  return d.getFullYear() + 
         String(d.getMonth() + 1).padStart(2, '0') + 
         String(d.getDate()).padStart(2, '0');
}

function getSchemaFromRequest(requestedFields) {
  return requestedFields.map(function(field) {
    return {
      name: field.getId(),
      dataType: field.getType()
    };
  });
}

// ================================
// CACHE ET OPTIMISATION
// ================================

function getCachedData(cacheKey) {
  try {
    var cache = CacheService.getScriptCache();
    var cached = cache.get(cacheKey);
    return cached ? JSON.parse(cached) : null;
  } catch (e) {
    return null;
  }
}

function setCachedData(cacheKey, data) {
  try {
    var cache = CacheService.getScriptCache();
    cache.put(cacheKey, JSON.stringify(data), CONFIG.CACHE_DURATION);
  } catch (e) {
    console.warn('Impossible de mettre en cache:', e);
  }
}

console.log('🚀 LinkedIn Looker Studio Connector - Version complète chargée');