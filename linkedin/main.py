# main.py

from auth import get_valid_access_token
from linkedin_data import get_organization_acls, get_organization_metrics

def main():
    print("Starting LinkedIn data extraction...")

    try:
        access_token = get_valid_access_token()
        print("Access token successfully retrieved.")
    except Exception as e:
        print(f"Failed to get access token: {e}")
        return

    # Fetch Organization ACLs
    try:
        print("Fetching organization ACLs...")
        org_acls = get_organization_acls(access_token)
        print(f"Organization ACLs fetched: {org_acls}")
    except Exception as e:
        print(f"Error fetching organization ACLs: {e}")
        return

    # Fetch metrics for a specific organization
    try:
        organization_urn = "urn:li:organization:51699835"
        start_date = "2024-11-01"
        end_date = "2024-11-20"
        
        metrics = get_organization_metrics(access_token, organization_urn, start_date, end_date, analytics_type="FOLLOWER")
        print("Metrics fetched successfully:", metrics)
    except Exception as e:
        print(f"Error fetching organization metrics: {e}")

if __name__ == "__main__":
    main()

