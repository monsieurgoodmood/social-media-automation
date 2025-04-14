#!/usr/bin/env python3
"""
Script pour générer l'URL d'authentification LinkedIn
"""

from dotenv import load_dotenv
import os

# Charger les variables depuis .env
load_dotenv()

client_id = os.getenv('LINKEDIN_CLIENT_ID', '77ni0sserlveku')
redirect_uri = os.getenv('LINKEDIN_REDIRECT_URI', 'http://localhost:8080')

# Construire l'URL d'authentification
auth_url = f"https://www.linkedin.com/oauth/v2/authorization?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}&scope=r_organization_followers%20r_organization_social%20rw_organization_admin%20r_organization_social_feed%20w_member_social%20w_organization_social%20r_basicprofile%20w_organization_social_feed%20w_member_social_feed%20r_1st_connections_size"

print("\n=== URL D'AUTHENTIFICATION LINKEDIN ===")
print(f"\n{auth_url}\n")
print("Copiez cette URL dans votre navigateur pour vous authentifier.")