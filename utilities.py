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

# Core Functions
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

            # Filter donations for `payment_status = 'succeeded'`
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

def load_to_bigquery(dataset, table, data):
    """Load data into BigQuery, skipping empty data sets."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"
    rows = []

    for item in data:
        attributes = item["attributes"]
        if table == "pco-campuses":
            address = flatten_address_object(attributes.get("address", {}))
            row = {
                "id": item["id"],
                "name": attributes.get("name"),
                "created_at": format_datetime(attributes.get("created_at")),
                "updated_at": format_datetime(attributes.get("updated_at")),
                **address,
            }
        elif table == "pco-donors":
            emails = attributes.get("emails", [])
            phone_numbers = attributes.get("phone_numbers", [])
            row = {
                "id": item["id"],
                "first_name": attributes.get("first_name"),
                "last_name": attributes.get("last_name"),
                "email_address": extract_first_email(emails),
                "phone_number": extract_first_phone(phone_numbers),
                "created_at": format_datetime(attributes.get("created_at")),
                "updated_at": format_datetime(attributes.get("updated_at")),
            }
        else:
            row = {
                "id": item["id"],
                **attributes,
                "created_at": format_datetime(attributes.get("created_at")),
                "updated_at": format_datetime(attributes.get("updated_at")),
            }
        rows.append(row)

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

    for endpoint in ENDPOINTS:
        filters = None
        if endpoint == "pco-donations":
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
        print("config.json file not found.")
    except json.JSONDecodeError as e:
        print(f"Error parsing config.json: {e}")
    except Exception as e:
        print(f"Unexpected error in process_all_clients: {e}")
