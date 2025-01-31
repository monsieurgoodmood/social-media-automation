import requests
import json
from pprint import pprint

# Configuration
ACCESS_TOKEN = "AQX6mYGJuuq7EfAq405zj9Pfb-gLRIB81V7gk0SsklCZ9XCP3Xal4eus0H0Tv-H2vqlLgw0xW_KUWA5Ad3zfCtg-jj9brS58Hph3k-vf7vyAA19Q6NkoztDOYCChEly5GNJaX1o3wfnOa1BJ5vRG5DpgJV0AJYrnZAFjMacR2LCuKaOsEUXOX8jdpPnTINt7j7QHjz9IyC4CNuzS2qG_WVH4s4PfbIPKS7os7FzAhYp6sGAHae6HcP0uRaheKPmYZQ5v_CM7Zqeod4-uALdRmCy9j3grBUt53gEkvom-LrdBjspK4tTZgsgogpjWFOycjC7M2TqEY-_VLz0b7LP-OKajP_Yjqw"
POST_URN = "urn:li:share:7242897710767386625"
ORGANIZATION_URN = "urn:li:organization:51699835"

# URL identique à la commande curl
url = (
    "https://api.linkedin.com/v2/organizationalEntityShareStatistics"
    f"?q=organizationalEntity&organizationalEntity={ORGANIZATION_URN.replace(':', '%3A')}"
    f"&shares=List({POST_URN.replace(':', '%3A')})"
)

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "X-Restli-Protocol-Version": "2.0.0"
}

# Fetching metrics
response = requests.get(url, headers=headers)

# Analyse de la réponse
if response.status_code == 200:
    data = response.json()
    print("\nMetrics retrieved successfully:\n")

    for element in data.get("elements", []):
        print("Post Metrics:")
        print(f"  Organizational Entity: {element.get('organizationalEntity')}")
        print(f"  Post URN: {element.get('share')}")

        stats = element.get("totalShareStatistics", {})
        print("  Total Share Statistics:")
        print(f"    Unique Impressions Count: {stats.get('uniqueImpressionsCount', 0)}")
        print(f"    Impression Count: {stats.get('impressionCount', 0)}")
        print(f"    Click Count: {stats.get('clickCount', 0)}")
        print(f"    Like Count: {stats.get('likeCount', 0)}")
        print(f"    Comment Count: {stats.get('commentCount', 0)}")
        print(f"    Share Count: {stats.get('shareCount', 0)}")
        print(f"    Engagement Rate: {stats.get('engagement', 0):.2%}")
        print("-" * 50)
else:
    print(f"Error: {response.status_code} - {response.text}")
