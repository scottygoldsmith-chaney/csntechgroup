import requests
from google.cloud import bigquery

bq_client = bigquery.Client()

# Define API endpoint URLs
ENDPOINTS = {
    "donations": "https://api.planningcenteronline.com/giving/v2/donations",
    "designations": "https://api.planningcenteronline.com/giving/v2/designations",
    "funds": "https://api.planningcenteronline.com/giving/v2/funds",
    "campuses": "https://api.planningcenteronline.com/giving/v2/campuses",
    "donors": "https://api.planningcenteronline.com/people/v2/people"
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
        response = requests.get(next_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data["data"])
        next_url = data["links"].get("next")

    return all_data

def load_to_bigquery(dataset, table, data):
    """Load data into BigQuery, skipping empty data sets."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"
    rows = [{"id": item["id"], **item["attributes"]} for item in data]

    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        print(f"Failed to insert rows into {table_id}: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into {table_id}.")
