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
    "pco-designations": "https://api.planningcenteronline.com/giving/v2/donations/1/designations",
    "pco-funds": "https://api.planningcenteronline.com/giving/v2/funds",
    "pco-campuses": "https://api.planningcenteronline.com/giving/v2/campuses",
    "pco-donors": "https://api.planningcenteronline.com/giving/v2/people&per_page=100"
}

# Helper Functions
def format_datetime(value):
    """Format datetime strings for BigQuery compatibility."""
    if not value:
        return None
    try:
        return value.split(".")[0]  # Remove fractional seconds
    except Exception as e:
        print(f"Error formatting datetime: {value}, {e}")
        return None

def flatten_address_object(address):
    """Flatten the address object into individual fields."""
    if not address:
        return {
            "address_line1": None,
            "address_line2": None,
            "address_city": None,
            "address_state": None,
            "address_postal_code": None,
            "address_country": None,
        }
    return {
        "address_line1": address.get("line1"),
        "address_line2": address.get("line2"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_postal_code": address.get("postal_code"),
        "address_country": address.get("country"),
    }

def extract_first_email(emails):
    """Extract the first email address from the array."""
    if not emails or len(emails) == 0:
        return None
    return emails[0].get("address")

def extract_first_phone(phone_numbers):
    """Extract the first phone number from the array."""
    if not phone_numbers or len(phone_numbers) == 0:
        return None
    return phone_numbers[0].get("number")

def fetch_data(api_credentials, endpoint, filters=None):
    """Fetch data from Planning Center API with optional filters."""
    base_url = ENDPOINTS[endpoint]
    token = base64.b64encode(f"{api_credentials['client_id']}:{api_credentials['client_secret']}".encode()).decode()
    headers = {"Authorization": f"Basic {token}"}
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

            if endpoint == "pco-donations":
                fetched_data = [item for item in fetched_data if item["attributes"].get("payment_status") == "succeeded"]

            print(f"Fetched {len(fetched_data)} records from page.")
            all_data.extend(fetched_data)
            next_url = data["links"].get("next")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for endpoint {endpoint}: {e}")
            break

    print(f"Total records fetched from {endpoint}: {len(all_data)}")
    return all_data

def get_existing_record_ids(dataset, table):
    """Query BigQuery to retrieve IDs of existing records."""
    table_id = f"{dataset}.{table}"
    query = f"SELECT id FROM `{table_id}`"
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        return {row.id for row in results}
    except Exception as e:
        print(f"Error querying existing records from {table_id}: {e}")
        return set()

def update_records_in_bigquery(table_id, updates):
    """Update existing records in BigQuery."""
    temp_table_id = f"{table_id}_temp"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_json(updates, temp_table_id, job_config=job_config)
    job.result()

    query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.id = S.id
    WHEN MATCHED THEN
      UPDATE SET
        {', '.join([f"T.{col} = S.{col}" for col in updates[0].keys() if col != 'id'])}
    """
    query_job = bq_client.query(query)
    query_job.result()

    bq_client.delete_table(temp_table_id, not_found_ok=True)

def load_to_bigquery(dataset, table, data):
    """Load data into BigQuery, updating existing records and inserting new ones."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"
    inserts = []
    updates = []

    existing_ids = get_existing_record_ids(dataset, table)

    for item in data:
        attributes = item["attributes"]
        record_id = item["id"]

        row = {
            "id": record_id,
            **attributes,
            "created_at": format_datetime(attributes.get("created_at")),
            "updated_at": format_datetime(attributes.get("updated_at")),
        }

        if record_id in existing_ids:
            updates.append(row)
        else:
            inserts.append(row)

    if inserts:
        try:
            errors = bq_client.insert_rows_json(table_id, inserts)
            if errors:
                print(f"Failed to insert rows into {table_id}: {errors}")
            else:
                print(f"Inserted {len(inserts)} new rows into {table_id}.")
        except Exception as e:
            print(f"Error inserting data into BigQuery for table {table}: {e}")

    if updates:
        try:
            update_records_in_bigquery(table_id, updates)
            print(f"Updated {len(updates)} existing rows in {table_id}.")
        except Exception as e:
            print(f"Error updating data in BigQuery for table {table}: {e}")

def process_client(client):
    """Process all endpoints for a single client."""
    api_credentials = client["api"]
    dataset = client["bigquery"]["dataset"]

    for endpoint in ENDPOINTS:
        filters = None
        if endpoint == "pco-donations":
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
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
        print("config.json file not found.")
    except json.JSONDecodeError as e:
        print(f"Error parsing config.json: {e}")
    except Exception as e:
        print(f"Unexpected error in process_all_clients: {e}")
