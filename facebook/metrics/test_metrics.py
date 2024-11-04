import sys
import os
import requests

# Ajoutez le répertoire 'facebook' au chemin de recherche des modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from facebook.config import ACCESS_TOKEN, API_VERSION, FACEBOOK_PAGE_ID

# Fonction pour tester une seule métrique
def test_single_metric(metric):
    """Test une seule métrique pour voir si elle fonctionne."""
    insights_url = f"https://graph.facebook.com/{API_VERSION}/{FACEBOOK_PAGE_ID}/insights"
    
    params = {
        'metric': metric,
        'access_token': ACCESS_TOKEN
    }
    
    response = requests.get(insights_url, params=params)
    
    if response.status_code == 200:
        print(f"Metric '{metric}' works: {response.json()}")
    else:
        print(f"Error for metric '{metric}': {response.status_code} - {response.text}")

# Liste des métriques à tester pour les posts
metrics_post_list = [
    'post_impressions', 'post_impressions_organic', 'post_impressions_paid', 'post_impressions_viral',
    'post_reach', 'post_engaged_users', 'post_clicks', 'post_reactions_by_type_total', 
    'post_reactions_like_total', 'post_reactions_love_total', 'post_reactions_wow_total', 
    'post_reactions_haha_total', 'post_reactions_sad_total', 'post_reactions_angry_total',
    'post_video_avg_time_watched', 'post_video_views', 'post_video_complete_views_30s', 
    'post_video_views_organic', 'post_video_views_paid', 'post_video_views_sound_on',
    'post_video_views_time', 'post_video_avg_time_watched', 'post_video_completion_rate',
    'post_engagement_rate', 'post_clicks_by_type', 'post_comments_count', 'post_shares',
    'post_full_picture', 'post_link', 'post_name', 'post_type', 'post_story', 'post_description',
    'post_date', 'post_comments', 'post_reactions', 'post_reactions_total', 
    'post_average_time_video_viewed', 'post_video_ad_break_impressions', 'post_paid_impressions',
    'post_viral_impressions', 'post_viral_reach', 'post_paid_reach', 'post_organic_impressions',
    'post_organic_reach', 'post_negative_feedback', 'post_video_ad_break_earnings', 'post_profile_views',
    'post_negative_feedback_by_type', 'post_fans_online', 'post_likes_count', 'post_follower_count',
    'post_actions', 'post_actions_by_type', 'post_engagement_by_type', 'post_scheduled_publish_time',
    'post_photo_views', 'post_video_view_duration', 'post_video_sound_on', 'post_video_organic_views',
    'post_video_paid_views', 'post_video_unique_views', 'post_video_completion_rate', 'post_video_views_time',
    'post_page_views_logged_in_unique', 'post_page_views_total', 'post_page_engagements', 'post_page_likes',
    'post_page_unlikes', 'post_page_messages', 'post_video_views_sound_on', 'post_reach_by_type',
    'post_reach_nonviral', 'post_paid_reach_total', 'post_video_clicks', 'post_video_unique_complete_views',
    'post_video_complete_views', 'post_video_autoplay_views', 'post_video_manual_views',
    'post_video_sound_off_views', 'post_fans_online_unique', 'post_page_places_checkin_total', 
    'post_video_views_3s', 'post_video_view_rate', 'post_actions_like', 'post_actions_love', 
    'post_actions_wow', 'post_actions_haha', 'post_actions_sad', 'post_actions_angry', 
    'post_profile_picture_views', 'post_profile_picture_url', 'post_profile_clicks', 'post_paid_video_views',
    'post_paid_video_views_total', 'post_paid_video_impressions', 'post_paid_video_views_complete',
    'post_paid_video_completion_rate', 'post_reactions_angry', 'post_reactions_haha', 
    'post_reactions_like', 'post_reactions_love', 'post_reactions_sad', 'post_reactions_wow',
    'post_viral_video_views', 'post_viral_video_impressions', 'post_viral_video_views_total',
    'post_viral_video_views_complete', 'post_video_viral_completion_rate', 'post_comments_unique_count',
    'post_comment_likes', 'post_fans_count', 'post_fans_comments', 'post_fans_reactions', 'post_fans_views',
    'post_fans_impressions', 'post_viral_reach_by_type', 'post_fans_unique_engaged', 
    'post_fans_video_views', 'post_fans_video_impressions', 'post_fans_video_views_unique', 
    'post_video_complete_views_30sec_rate', 'post_video_unique_30sec_views', 
    'post_video_avg_30sec_watched', 'post_video_views_3sec', 'post_video_views_10sec',
    'post_video_avg_10sec_watched', 'post_video_complete_views_10sec_rate', 'post_organic_video_views_10sec',
    'post_paid_video_views_10sec', 'post_video_views_auto', 'post_video_views_click', 
    'post_video_views_sound_off', 'post_video_click_to_play', 'post_page_engagement_by_type', 
    'post_profile_page_engagement', 'post_profile_page_views', 'post_profile_page_likes', 
    'post_profile_page_followers', 'post_profile_video_views_organic', 'post_profile_video_views_paid', 
    'post_video_autoplay_3sec_views', 'post_video_autoplay_10sec_views', 'post_video_autoplay_30sec_views', 
    'post_page_checkins_unique', 'post_page_places_checkin_mobile', 'post_page_places_checkin', 
    'post_link_clicks', 'post_posted_link', 'post_full_image', 'post_image', 'post_image_url', 
    'post_message', 'post_click_through_rate', 'post_likes_to_unlikes_ratio', 'post_likes_by_country', 
    'post_comments_by_country', 'post_shares_by_country', 'post_page_unlike_to_likes_ratio',
    'post_comments_by_country_unique', 'post_reactions_total_unique', 'post_video_views_total_unique', 
    'post_profile_views_logged_in', 'post_video_ads_count', 'post_video_paid_ads_count', 
    'post_video_ads_earning', 'post_organic_reach_by_type', 'post_paid_reach_by_type', 
    'post_viral_reach_by_type', 'post_click_to_play_rate', 'post_video_views_to_total_views_ratio',
    'post_video_views_replay_rate', 'post_video_play_time_total', 'post_profile_views_total', 
    'post_profile_follower_views', 'post_profile_fan_views', 'post_recommendation_views',
    'post_comment_unique_likes', 'post_comment_engagement', 'post_video_recommendations', 
    'post_engagement_percent'
]

# Liste des métriques à tester pour les pages
metrics_page_list = [
    'page_impressions', 'page_impressions_organic', 'page_impressions_paid', 'page_impressions_viral',
    'page_impressions_unique', 'page_engaged_users', 'page_total_actions', 'page_fan_adds',
    'page_fan_removes', 'page_views_total', 'page_video_views', 'page_video_avg_time_watched', 
    'page_video_total_time_watched', 'page_fans_online', 'page_organic_reach', 'page_paid_reach',
    'page_fans_reactions', 'page_fans_engagements', 'page_fans_shares', 'page_negative_feedback',
    'page_profile_views', 'page_profile_clicks', 'page_profile_likes', 'page_profile_comments',
    'page_engagement_rate', 'page_reach_nonviral', 'page_reach_viral', 'page_reach_paid', 'page_reach_total'
]

# Liste des métriques à tester pour les reels
metrics_reel_list = [
    'reel_post_impressions_unique', 'reel_post_video_avg_time_watched', 'reel_reactions_total', 
    'reel_views', 'reel_post_video_views', 'reel_post_video_complete_views_3sec', 
    'reel_post_video_complete_views_30sec', 'reel_post_video_completion_rate', 
    'reel_post_video_views_organic', 'reel_post_video_views_paid', 'reel_clicks_total', 'reel_page_engagement',
    'reel_follower_reach', 'reel_new_followers', 'reel_fans_online'
]

# Liste des métriques à tester pour les vidéos
metrics_video_list = [
    'video_views', 'video_30_sec_views', 'video_paid_views', 'video_autoplay_views', 'video_organic_views',
    'video_completion_rate', 'video_views_unique', 'video_total_time_watched', 'video_average_time_watched',
    'video_click_to_play_views', 'video_viral_impressions', 'video_reactions_total', 'video_comments_count',
    'video_ad_break_views', 'video_ad_break_earnings', 'video_fan_views', 'video_profile_views',
    'video_complete_views', 'video_complete_views_30sec', 'video_views_by_autoplay', 'video_views_by_click_to_play',
    'video_views_by_paid', 'video_views_by_organic', 'video_views_by_external_referrals', 'video_fan_engagement'
]

# Testez chaque métrique une par une pour les posts
print("\nTesting post metrics:")
for metric in metrics_post_list:
    test_single_metric(metric)

# Testez chaque métrique une par une pour les pages
print("\nTesting page metrics:")
for metric in metrics_page_list:
    test_single_metric(metric)

# Testez chaque métrique une par une pour les reels
print("\nTesting reel metrics:")
for metric in metrics_reel_list:
    test_single_metric(metric)

# Testez chaque métrique une par une pour les vidéos
print("\nTesting video metrics:")
for metric in metrics_video_list:
    test_single_metric(metric)
