"""
Modèles SQLAlchemy pour la base de données WhatTheData
Basé sur le schéma PostgreSQL existant
"""

from sqlalchemy import Column, Integer, String, Text, Boolean, Date, DateTime, Numeric, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    firstname = Column(String(255))
    lastname = Column(String(255))
    email = Column(Text, unique=True, nullable=False)
    company = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    password_hash = Column(String(255))
    plan_type = Column(String(50), default='free')
    subscription_end_date = Column(DateTime)
    last_login = Column(DateTime)
    
    # Relations
    facebook_accounts = relationship("FacebookAccount", back_populates="user")
    linkedin_accounts = relationship("LinkedinAccount", back_populates="user")
    social_tokens = relationship("SocialAccessToken", back_populates="user")

class SocialAccessToken(Base):
    __tablename__ = 'social_access_tokens'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    platform = Column(String(50), nullable=False)  # 'facebook', 'linkedin'
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text)
    expires_at = Column(DateTime)
    scope = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relations
    user = relationship("User", back_populates="social_tokens")

# ========================================
# FACEBOOK MODELS
# ========================================

class FacebookAccount(Base):
    __tablename__ = 'facebook_accounts'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    page_id = Column(Text, nullable=False)
    page_name = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relations
    user = relationship("User", back_populates="facebook_accounts")

class FacebookPageMetadata(Base):
    __tablename__ = 'facebook_page_metadata'
    
    id = Column(Integer, primary_key=True)
    page_id = Column(Text, nullable=False)
    name = Column(String(255))
    username = Column(Text)
    category = Column(Text)
    about = Column(Text)
    talking_about_count = Column(Integer)
    website = Column(Text)
    link = Column(Text)
    picture_url = Column(Text)
    cover_url = Column(Text)
    created_at = Column(DateTime, default=func.now())

class FacebookPageDaily(Base):
    __tablename__ = 'facebook_page_daily'
    
    id = Column(Integer, primary_key=True)
    page_id = Column(String(255), nullable=False)
    date = Column(Date, nullable=False)
    
    # Impressions
    page_impressions = Column(Integer)
    page_impressions_unique = Column(Integer)
    page_impressions_non_viral = Column(Integer)
    page_impressions_viral = Column(Integer)
    
    # Posts Impressions
    page_posts_impressions = Column(Integer)
    page_posts_impressions_unique = Column(Integer)
    page_posts_impressions_paid = Column(Integer)
    page_posts_impressions_organic = Column(Integer)
    page_posts_impressions_organic_unique = Column(Integer)
    
    # Views
    page_views_total = Column(Integer)
    
    # Fans & Follows
    page_fans = Column(Integer)
    page_fan_adds = Column(Integer)
    page_fan_removes = Column(Integer)
    page_fan_adds_by_paid_non_paid_unique_total = Column(Integer)
    page_fan_adds_by_paid_non_paid_unique_paid = Column(Integer)
    page_fan_adds_by_paid_non_paid_unique_unpaid = Column(Integer)
    page_follows = Column(Integer)
    page_daily_follows = Column(Integer)
    page_daily_unfollows = Column(Integer)
    page_daily_follows_unique = Column(Integer)
    
    # Video metrics
    page_video_views = Column(Integer)
    page_video_views_unique = Column(Integer)
    page_video_views_paid = Column(Integer)
    page_video_views_organic = Column(Integer)
    page_video_views_repeat = Column(Integer)
    page_video_view_time = Column(Integer)
    page_video_complete_views_30s = Column(Integer)
    page_video_complete_views_30s_unique = Column(Integer)
    page_video_complete_views_30s_paid = Column(Integer)
    page_video_complete_views_30s_organic = Column(Integer)
    page_video_complete_views_30s_autoplayed = Column(Integer)
    page_video_complete_views_30s_repeated_views = Column(Integer)
    
    # Engagement
    page_post_engagements = Column(Integer)
    page_total_actions = Column(Integer)
    page_actions_post_reactions_like_total = Column(Integer)
    page_actions_post_reactions_love_total = Column(Integer)
    page_actions_post_reactions_wow_total = Column(Integer)
    page_actions_post_reactions_haha_total = Column(Integer)
    page_actions_post_reactions_sorry_total = Column(Integer)
    page_actions_post_reactions_anger_total = Column(Integer)
    
    created_at = Column(DateTime, default=func.now())

class FacebookPostsMetadata(Base):
    __tablename__ = 'facebook_posts_metadata'
    
    id = Column(Integer, primary_key=True)
    post_id = Column(Text, nullable=False, unique=True)
    page_id = Column(Text, nullable=False)
    created_time = Column(DateTime)
    status_type = Column(String(100))
    message = Column(Text)
    permalink_url = Column(Text)
    full_picture = Column(Text)
    author_name = Column(String(255))
    author_id = Column(Text)
    comments_count = Column(Integer, default=0)
    likes_count = Column(Integer, default=0)
    shares_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())

class FacebookPostsLifetime(Base):
    __tablename__ = 'facebook_posts_lifetime'
    
    id = Column(Integer, primary_key=True)
    post_id = Column(Text, nullable=False)
    page_id = Column(Text, nullable=False)
    
    # Impressions
    post_impressions = Column(Integer)
    post_impressions_unique = Column(Integer)
    post_impressions_organic = Column(Integer)
    post_impressions_organic_unique = Column(Integer)
    post_impressions_paid = Column(Integer)
    post_impressions_paid_unique = Column(Integer)
    post_impressions_viral = Column(Integer)
    post_impressions_viral_unique = Column(Integer)
    post_impressions_fan = Column(Integer)
    post_impressions_nonviral = Column(Integer)
    post_impressions_nonviral_unique = Column(Integer)
    
    # Reactions
    post_reactions_like_total = Column(Integer)
    post_reactions_love_total = Column(Integer)
    post_reactions_wow_total = Column(Integer)
    post_reactions_haha_total = Column(Integer)
    post_reactions_sorry_total = Column(Integer)
    post_reactions_anger_total = Column(Integer)
    
    # Clics & Interactions
    post_clicks = Column(Integer)
    post_consumptions = Column(Integer)
    
    # Video metrics (posts)
    post_video_views = Column(Integer)
    post_video_views_unique = Column(Integer)
    post_video_views_organic = Column(Integer)
    post_video_views_organic_unique = Column(Integer)
    post_video_views_paid = Column(Integer)
    post_video_views_paid_unique = Column(Integer)
    post_video_views_sound_on = Column(Integer)
    post_video_complete_views_30s = Column(Integer)
    post_video_avg_time_watched = Column(Integer)
    post_video_view_time = Column(Integer)
    
    # Fan reach
    post_fan_reach = Column(Integer)
    
    created_at = Column(DateTime, default=func.now())

# ========================================
# LINKEDIN MODELS
# ========================================

class LinkedinAccount(Base):
    __tablename__ = 'linkedin_accounts'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    organization_id = Column(Text, nullable=False)
    organization_name = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relations
    user = relationship("User", back_populates="linkedin_accounts")

class LinkedinTokens(Base):
    __tablename__ = 'linkedin_tokens'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    application_type = Column(String(50), nullable=False)  # 'community', 'portability', 'signin'
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text)
    expires_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class LinkedinPagesMetadata(Base):
    __tablename__ = 'linkedin_pages_metadata'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    name = Column(String(255))
    description = Column(Text)
    website = Column(Text)
    industry = Column(String(255))
    company_size = Column(String(100))
    headquarters = Column(String(255))
    founded = Column(Integer)
    logo_url = Column(Text)
    cover_image_url = Column(Text)
    follower_count = Column(Integer)
    employee_count = Column(Integer)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class LinkedinPageDaily(Base):
    __tablename__ = 'linkedin_page_daily'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    date = Column(Date, nullable=False)
    
    # Followers metrics
    follower_count = Column(Integer)
    new_followers = Column(Integer)
    lost_followers = Column(Integer)
    
    # Page views
    page_views = Column(Integer)
    unique_page_views = Column(Integer)
    
    # Engagement
    total_engagement = Column(Integer)
    likes = Column(Integer)
    comments = Column(Integer)
    shares = Column(Integer)
    clicks = Column(Integer)
    
    # Impressions
    impressions = Column(Integer)
    unique_impressions = Column(Integer)
    
    created_at = Column(DateTime, default=func.now())

class LinkedinPageLifetime(Base):
    __tablename__ = 'linkedin_page_lifetime'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    
    # Lifetime totals
    total_posts = Column(Integer)
    total_followers = Column(Integer)
    total_engagement = Column(Integer)
    total_impressions = Column(Integer)
    total_clicks = Column(Integer)
    
    # Ratios (en décimal)
    engagement_rate = Column(Numeric(5, 4))  # ex: 0.0532 pour 5.32%
    click_through_rate = Column(Numeric(5, 4))
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class LinkedinPostsMetadata(Base):
    __tablename__ = 'linkedin_posts_metadata'
    
    id = Column(Integer, primary_key=True)
    post_urn = Column(Text, nullable=False, unique=True)
    organization_id = Column(Text, nullable=False)
    post_type = Column(String(50))  # 'post', 'ugcPost', 'share', 'instantRepost'
    post_subtype = Column(String(50))  # 'original', 'repost_with_comment', 'instant_repost'
    author_urn = Column(Text)
    created_time = Column(DateTime)
    text_content = Column(Text)  # Limité à 1000 caractères
    media_type = Column(String(50))
    media_url = Column(Text)
    created_at = Column(DateTime, default=func.now())

class LinkedinPostsDaily(Base):
    __tablename__ = 'linkedin_posts_daily'
    
    id = Column(Integer, primary_key=True)
    post_urn = Column(Text, nullable=False)
    organization_id = Column(Text, nullable=False)
    date = Column(Date, nullable=False)
    
    # Portée
    impressions = Column(Integer)
    unique_impressions = Column(Integer)
    
    # Engagement
    clicks = Column(Integer)
    shares = Column(Integer)
    comments = Column(Integer)
    engagement_rate = Column(Numeric(5, 4))  # En décimal
    
    # Réactions détaillées
    total_reactions = Column(Integer)
    likes = Column(Integer)
    celebrates = Column(Integer)
    loves = Column(Integer)
    insights = Column(Integer)
    supports = Column(Integer)
    funnies = Column(Integer)
    
    # Pourcentages de réactions (en décimal)
    like_percentage = Column(Numeric(5, 4))
    celebrate_percentage = Column(Numeric(5, 4))
    love_percentage = Column(Numeric(5, 4))
    insight_percentage = Column(Numeric(5, 4))
    support_percentage = Column(Numeric(5, 4))
    funny_percentage = Column(Numeric(5, 4))
    
    # Total interactions
    total_interactions = Column(Integer)  # clics + partages + commentaires + réactions
    
    created_at = Column(DateTime, default=func.now())

# ========================================
# FOLLOWERS BREAKDOWN TABLES (LinkedIn)
# ========================================

class LinkedinFollowerByCompanySize(Base):
    __tablename__ = 'linkedin_follower_by_company_size'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    company_size = Column(String(100), nullable=False)
    follower_count = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

class LinkedinFollowerByFunction(Base):
    __tablename__ = 'linkedin_follower_by_function'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    function_name = Column(String(255), nullable=False)
    follower_count = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

class LinkedinFollowerByIndustry(Base):
    __tablename__ = 'linkedin_follower_by_industry'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    industry_name = Column(String(255), nullable=False)
    follower_count = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

class LinkedinFollowerBySeniority(Base):
    __tablename__ = 'linkedin_follower_by_seniority'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    seniority_level = Column(String(255), nullable=False)
    follower_count = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

# ========================================
# PAGE VIEWS BREAKDOWN TABLES (LinkedIn)
# ========================================

class LinkedinPageViewsByCountry(Base):
    __tablename__ = 'linkedin_page_views_by_country'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    country_code = Column(String(10), nullable=False)
    country_name = Column(String(255))
    page_views = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

class LinkedinPageViewsByIndustry(Base):
    __tablename__ = 'linkedin_page_views_by_industry'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    industry_name = Column(String(255), nullable=False)
    page_views = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

class LinkedinPageViewsBySeniority(Base):
    __tablename__ = 'linkedin_page_views_by_seniority'
    
    id = Column(Integer, primary_key=True)
    organization_id = Column(Text, nullable=False)
    seniority_level = Column(String(255), nullable=False)
    page_views = Column(Integer)
    percentage = Column(Numeric(5, 4))
    date_collected = Column(Date, default=func.current_date())
    created_at = Column(DateTime, default=func.now())

# ========================================
# LOOKER STUDIO TEMPLATES
# ========================================

class LookerTemplate(Base):
    __tablename__ = 'looker_templates'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text)
    template_id = Column(Text, nullable=False)  # Google Looker Studio template ID
    platforms = Column(Text)  # JSON array: ["facebook", "linkedin"]
    is_active = Column(Boolean, default=True)
    plan_required = Column(String(50), default='free')  # 'free', 'basic', 'premium'
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class UserTemplateAccess(Base):
    __tablename__ = 'user_template_access'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'), nullable=False)
    template_id = Column(Integer, ForeignKey('looker_templates.id'), nullable=False)
    granted_at = Column(DateTime, default=func.now())
    expires_at = Column(DateTime)
    is_active = Column(Boolean, default=True)

# ========================================
# HELPER FUNCTIONS
# ========================================

def create_all_tables(engine):
    """Créer toutes les tables dans la base de données"""
    Base.metadata.create_all(engine)

def get_user_by_email(session, email: str):
    """Récupérer un utilisateur par email"""
    return session.query(User).filter(User.email == email).first()

def get_active_facebook_accounts(session, user_id: int):
    """Récupérer les comptes Facebook actifs d'un utilisateur"""
    return session.query(FacebookAccount).filter(
        FacebookAccount.user_id == user_id,
        FacebookAccount.is_active == True
    ).all()

def get_active_linkedin_accounts(session, user_id: int):
    """Récupérer les comptes LinkedIn actifs d'un utilisateur"""
    return session.query(LinkedinAccount).filter(
        LinkedinAccount.user_id == user_id,
        LinkedinAccount.is_active == True
    ).all()

def get_user_tokens(session, user_id: int, platform: str):
    """Récupérer les tokens d'un utilisateur pour une plateforme"""
    return session.query(SocialAccessToken).filter(
        SocialAccessToken.user_id == user_id,
        SocialAccessToken.platform == platform,
        SocialAccessToken.is_active == True
    ).first()