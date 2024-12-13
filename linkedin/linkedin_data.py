# linkedin_data

import requests
import csv
import datetime

BASE_URL = "https://api.linkedin.com/rest/"
LINKEDIN_VERSION = "202408"

def request_api(endpoint, access_token, method="GET", params=None, json_body=None):
    """
    Make an authenticated request to LinkedIn's API.
    """
    url = f"{BASE_URL}{endpoint}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": LINKEDIN_VERSION,
        "Content-Type": "application/json",
    }
    response = requests.request(method, url, headers=headers, params=params, json=json_body)
    if response.status_code == 401:
        raise requests.exceptions.HTTPError("401 Unauthorized - Access token may be invalid.", response=response)
    response.raise_for_status()
    return response.json()

def get_organization_acls(access_token):
    """
    Fetch organization ACLs.
    """
    endpoint = "dmaOrganizationAcls"
    params = {"q": "roleAssignee"}
    return request_api(endpoint, access_token, params=params).get("elements", [])


def get_organization_metrics(access_token, organization_urn, start_date, end_date, analytics_type="FOLLOWER"):
    """
    Fetch daily metrics for a specific organization within a date range.
    """
    organizational_page_urn = organization_urn.replace("organization", "organizationalPage")
    base_url = f"{BASE_URL}dmaOrganizationalPageEdgeAnalytics"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": LINKEDIN_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }

    # Convert start_date and end_date to timestamps in milliseconds
    start_timestamp = int(datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_timestamp = int(datetime.datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    # Note: Properly encode the timeIntervals parameter
    time_intervals = f"(timeRange:(start:{start_timestamp},end:{end_timestamp}))"

    params = {
        "q": "trend",
        "analyticsType": analytics_type,
        "organizationalPage": organizational_page_urn,
        "timeIntervals": time_intervals,
    }

    print(f"Fetching metrics for {organizational_page_urn} from {start_date} to {end_date}...")
    print(f"Request Parameters: {params}")

    response = requests.get(base_url, headers=headers, params=params)

    print(f"Response Status: {response.status_code}")
    if response.status_code == 200:
        print("Metrics successfully fetched.")
        return response.json()
    else:
        print(f"Failed to fetch metrics: {response.status_code} - {response.text}")
        response.raise_for_status()



def save_metrics_to_csv(metrics, output_file):
    """
    Save daily metrics to a CSV file.
    """
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Write headers
        writer.writerow(["Date", "Metric", "Value"])
        
        for date, data in metrics.items():
            if "error" in data:
                writer.writerow([date, "Error", data["error"]])
            else:
                for metric, value in data.items():
                    writer.writerow([date, metric, value])