import requests
import json
from google.cloud import bigquery
from datetime import datetime, timedelta
import base64

# Initialize BigQuery client
bq_client = bigquery.Client()

# Define API endpoint URLs
ENDPOINTS = {
    "pco-donations": "https://api.planningcenteronline.com/giving/v2/donations",
    "pco-designations": "https://api.planningcenteronline.com/giving/v2/designations",
    "pco-funds": "https://api.planningcenteronline.com/giving/v2/funds",
    "pco-campuses": "https://api.planningcenteronline.com/giving/v2/campuses",
    "pco-donors": "https://api.planningcenteronline.com/people/v2/people&per_page=100"
}

def fetch_data(api_credentials, endpoint, filters):
    """Fetch data from Planning Center API with optional filters."""
    base_url = ENDPOINTS[endpoint]
    
    # Construct Basic Auth Header
    token = base64.b64encode(f"{api_credentials['client_id']}:{api_credentials['client_secret']}".encode()).decode()
    headers = {"Authorization": f"Basic {token}"}

    # Add query parameters for filtering
    params = filters if filters else {}

    all_data = []
    next_url = base_url

    while next_url:
        try:
            print(f"Making request to: {next_url}")
            response = requests.get(next_url, headers=headers, params=params)
            print(f"Response status code: {response.status_code}")
            response.raise_for_status()
            data = response.json()
            fetched_data = data["data"]

            # Filter donations for `payment_status = 'succeeded'` if endpoint is donations
            if endpoint == "pco-donations":
                fetched_data = [item for item in fetched_data if item["attributes"].get("payment_status") == "succeeded"]

            print(f"Fetched {len(fetched_data)} records from page.")
            all_data.extend(fetched_data)
            
            # Handle pagination
            next_url = data["links"].get("next")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for endpoint {endpoint}: {e}")
            break

    print(f"Total records fetched from {endpoint}: {len(all_data)}")
    return all_data

def load_to_bigquery(dataset, table, data):
    """Load data into BigQuery, skipping empty data sets."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"
    rows = [{"id": item["id"], **item["attributes"]} for item in data]

    print(f"Prepared {len(rows)} rows for BigQuery table: {table_id}")
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
    for endpoint in ENDPOINTS:
        filters = None
        if endpoint == "pco-donations":
            # Filter for completed donations from yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
            filters = {
                "where[completed_at][gte]": yesterday,
                "per_page": 100
            }


        print(f"Processing endpoint: {endpoint} for client: {client['name']}")
        try:
            data = fetch_data(api_credentials, endpoint, filters)
            load_to_bigquery(dataset, endpoint, data)
        except Exception as e:
            print(f"Error processing {endpoint} for {client['name']}: {e}")

def process_all_clients():
    """Process data for all clients defined in config.json."""
    try:
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
