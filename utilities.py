import requests
import json
from google.cloud import bigquery
from datetime import datetime, timedelta

# Initialize BigQuery client
bq_client = bigquery.Client()

# Define API endpoint URLs
ENDPOINTS = {
    "pco-donations": "https://api.planningcenteronline.com/giving/v2/donations",
    "pco-designations": "https://api.planningcenteronline.com/giving/v2/designations",
    "pco-funds": "https://api.planningcenteronline.com/giving/v2/funds",
    "pco-campuses": "https://api.planningcenteronline.com/giving/v2/campuses",
    "pco-donors": "https://api.planningcenteronline.com/people/v2/people"
}

def fetch_data(api_credentials, endpoint, filters=None):
    """Fetch data from Planning Center API with optional filters."""
    base_url = ENDPOINTS[endpoint]
    headers = {
        "Authorization": f"Basic {api_credentials['client_id']}:{api_credentials['client_secret']}"
    }
    params = {"per_page": 100}
    if filters:
        params.update(filters)

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

    return all_data

def load_to_bigquery(dataset, table, data):
    """Load data into BigQuery, skipping empty data sets."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"
    rows = [{"id": item["id"], **item["attributes"]} for item in data]

    try:
        errors = bq_client.insert_rows_json(table_id, rows)
        if errors:
            print(f"Failed to insert rows into {table_id}: {errors}")
        else:
            print(f"Inserted {len(rows)} rows into {table_id}.")
    except Exception as e:
        print(f"Error loading data into BigQuery for table {table}: {e}")

def process_client(client):
    """Process all endpoints for a single client."""
    api_credentials = client["api"]
    dataset = client["bigquery"]["dataset"]

    # Process each endpoint
    endpoints = ["pco-donations", "pco-designations", "pco-funds", "pco-campuses", "pco-donors"]
    for endpoint in endpoints:
        filters = None
        if endpoint == "donations":
            # Filter for completed donations from yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            filters = {"where[completed_at][gte]": yesterday}

        print(f"Processing endpoint: {endpoint} for client: {client['name']}")
        try:
            data = fetch_data(api_credentials, endpoint, filters)
            load_to_bigquery(dataset, endpoint, data)
        except Exception as e:
            print(f"Error processing {endpoint} for {client['name']}: {e}")

def process_all_clients():
    """Process data for all clients defined in config.json."""
    try:
        # Load client configuration from config.json
        with open("config.json", "r") as f:
            config = json.load(f)

        clients = config.get("clients", [])
        if not clients:
            print("No clients defined in config.json.")
            return

        for client in clients:
            print(f"Processing client: {client['name']}")
            process_client(client)
    except FileNotFoundError:
        print("config.json file not found. Please ensure it exists in the working directory.")
    except json.JSONDecodeError as e:
        print(f"Error parsing config.json: {e}")
    except Exception as e:
        print(f"Unexpected error in process_all_clients: {e}")
