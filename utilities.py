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
    "pco-donors": "https://api.planningcenteronline.com/giving/v2/people?per_page=100"
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

def extract_primary_from_array(array, key):
    """Extract the primary value for a given key from an array of objects."""
    if not array:
        return None
    primary_item = next((item for item in array if item.get("primary", False)), None)
    return primary_item.get(key) if primary_item else None

def flatten_address_object(address):
    """Flatten the address object into individual fields."""
    if not address:
        return {
            "address_line1": None,
            "address_line2": None,
            "address_city": None,
            "address_state": None,
            "address_postal_code": None,
        }
    return {
        "address_line1": address.get("street_line_1"),
        "address_line2": address.get("street_line_2"),
        "address_city": address.get("city"),
        "address_state": address.get("state"),
        "address_postal_code": address.get("zip"),
    }

def flatten_donor_data(attributes):
    """Flatten donor attributes, including address, email, and phone number."""
    address = attributes.get("address", [])
    email_addresses = attributes.get("email_addresses", [])
    phone_numbers = attributes.get("phone_numbers", [])

    # Extract address fields
    flattened_address = flatten_address_object(extract_primary_from_array(address, None)) or {}

    # Extract primary email and phone number
    primary_email = extract_primary_from_array(email_addresses, "address")
    primary_phone = extract_primary_from_array(phone_numbers, "number")

    # Combine the flattened data
    return {
        **flattened_address,
        "email_address": primary_email,
        "phone_number": primary_phone,
    }

def get_existing_record_ids(dataset, table):
    """Query BigQuery to retrieve IDs of existing records."""
    table_id = f"{dataset}.{table}"
    query = f"SELECT id FROM `{table_id}`"
    try:
        query_job = bq_client.query(query)
        results = query_job.result()
        return {str(row.id) for row in results}  # Ensure all IDs are treated as strings
    except Exception as e:
        print(f"Error querying existing records from {table_id}: {e}")
        return set()

def cast_to_schema(row, schema):
    """Cast fields in the row to match the BigQuery schema."""
    casted_row = {}
    for field in schema:
        field_name = field["name"]
        field_type = field["type"]
        value = row.get(field_name)

        if value is None:
            casted_row[field_name] = None
        elif field_type == "STRING":
            casted_row[field_name] = str(value)
        elif field_type == "INTEGER":
            casted_row[field_name] = int(value)
        elif field_type == "FLOAT":
            casted_row[field_name] = float(value)
        elif field_type == "TIMESTAMP":
            casted_row[field_name] = format_datetime(value)
        else:
            casted_row[field_name] = value

    return casted_row

def load_to_bigquery(dataset, table, data, batch_size=500):
    """Load data into BigQuery in smaller batches to avoid payload size issues."""
    if not data:
        print(f"No data to insert for table: {table}")
        return

    table_id = f"{dataset}.{table}"

    # Fetch BigQuery schema for the table
    table_schema = bq_client.get_table(table_id).schema
    schema = [{"name": field.name, "type": field.field_type} for field in table_schema]

    total_rows = len(data)
    print(f"Preparing to insert {total_rows} rows into {table_id}.")

    inserts = []

    for item in data:
        attributes = item["attributes"]
        record_id = str(item["id"])  # Ensure IDs are treated as strings

        # Flatten donor-specific fields if processing donors
        if table == "pco-donors":
            flattened_donor = flatten_donor_data(attributes)
            attributes.update(flattened_donor)

        row = {
            "id": record_id,
            **attributes,
            "created_at": format_datetime(attributes.get("created_at")),
            "updated_at": format_datetime(attributes.get("updated_at")),
        }

        # Cast row to match schema
        row = cast_to_schema(row, schema)
        inserts.append(row)

    # Split the data into smaller batches
    for i in range(0, total_rows, batch_size):
        batch = inserts[i:i+batch_size]
        print(f"Inserting batch {i // batch_size + 1} with {len(batch)} rows.")
        try:
            errors = bq_client.insert_rows_json(table_id, batch)
            if errors:
                print(f"Failed to insert rows into {table_id}: {errors}")
            else:
                print(f"Successfully inserted {len(batch)} rows into {table_id}.")
        except Exception as e:
            print(f"Error inserting batch {i // batch_size + 1} into {table_id}: {e}")

def fetch_data(api_credentials, endpoint, filters=None):
    """Fetch data from Planning Center API with optional filters."""
    base_url = ENDPOINTS[endpoint]
    token = base64.b64encode(f"{api_credentials['client_id']}:{api_credentials['client_secret']}".encode()).decode()
    headers = {"Authorization": f"Basic {token}"}

    # Start with the base URL and include per_page=100 for pagination
    next_url = f"{base_url}?per_page=100"

    # Add filters if provided
    if filters:
        filter_string = "&".join([f"{key}={value}" for key, value in filters.items()])
        next_url += f"&{filter_string}"

    all_data = []

    while next_url:
        try:
            print(f"Making request to: {next_url}")
            response = requests.get(next_url, headers=headers)
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
