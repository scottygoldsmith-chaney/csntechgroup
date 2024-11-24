import requests
import time
from google.cloud import bigquery

bq_client = bigquery.Client()

# Define API endpoint URLs for each type
ENDPOINTS = {
    "donations": "https://api.planningcenteronline.com/giving/v2/donations",
    "designations": "https://api.planningcenteronline.com/giving/v2/designations",
    "funds": "https://api.planningcenteronline.com/giving/v2/funds",
    "campuses": "https://api.planningcenteronline.com/giving/v2/campuses",
    "donors": "https://api.planningcenteronline.com/people/v2/people"
}

def process_all_clients():
    """Process data for all clients."""
    # Load client configuration (replace with actual configuration loading logic)
    clients = [
        {
            "name": "client1",
            "api": {
                "client_id": "your_client1_id",
                "client_secret": "your_client1_secret"
            },
            "bigquery": {
                "dataset": "csntechgroup.client1"
            }
        }
    ]

    for client in clients:
        print(f"Processing client: {client['name']}")
        process_client(client)

def process_client(client):
    """Process data for a single client."""
    api_credentials = client["api"]
    dataset = client["bigquery"]["dataset"]

    # Process endpoints
    endpoints = ["donations", "designations", "funds", "campuses", "donors"]
    for endpoint in endpoints:
        process_endpoint(api_credentials, dataset, endpoint)

def process_endpoint(api_credentials, dataset, endpoint):
    """Process a single endpoint."""
    print(f"Processing endpoint: {endpoint}")
    base_url = ENDPOINTS[endpoint]
    headers = {
        "Authorization": f"Basic {api_credentials['client_id']}:{api_credentials['client_secret']}"
    }

    # Add filtering for donations only
    params = {"per_page": 100}
    if endpoint == "donations":
        params["where[completed_at][eq]"] = "yesterday"  # Replace with dynamic date logic

    all_data = []
    next_url = base_url

    while next_url:
        try:
            response = requests.get(next_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            all_data.extend(data["data"])
            next_url = data["links"].get("next")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for endpoint {endpoint}: {e}")
            break

    load_to_bigquery(dataset, endpoint, all_data)

def load_to_bigquery(dataset, endpoint, data):
    """Load data into BigQuery."""
    table_id = f"{dataset}.{endpoint}"
    rows = [{"id": item["id"], **item["attributes"]} for item in data]
    errors = bq_client.insert_rows_json(table_id, rows)

    if errors:
        print(f"Failed to insert rows into {table_id}: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into {table_id}.")
