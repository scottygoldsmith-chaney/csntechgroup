import os
import time
from utilities import fetch_data_from_api, load_to_bigquery, get_existing_ids
from google.cloud import bigquery
from datetime import datetime, timedelta

# Initialize BigQuery client outside the function
client = bigquery.Client()

def process_endpoint(endpoint_name, base_url, id_field, headers, dataset_id, table_id, schema):
    print(f"Processing {endpoint_name}...")

    data_items = fetch_data_from_api(base_url, headers)

    if not data_items:  # Handle empty or null response
        print(f"No data retrieved for {endpoint_name}. Skipping.")
        return

    existing_ids = get_existing_ids(dataset_id, table_id, id_field)

    new_records = []
    for item in data_items:
        if not item:  # Skip empty items
            continue

        record = {id_field: str(item["id"])}  # Ensure ID is a string

        for field in schema:
            if field.name == id_field:
                continue

            value = item.get("attributes", {}).get(field.name)

            if value is None and "relationships" in item and field.name in item["relationships"]:
                rel_data = item["relationships"][field.name].get("data")
                if rel_data:
                    value = rel_data.get("id")  # or rel_data[0].get('id') if its a list

            # Explicitly handle and convert data types
            if value is not None:  # Convert only non-null values
                if field.field_type == "FLOAT":
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        value = 0.0  # Or handle the error as needed
                elif field.field_type == "INTEGER":
                    try:
                        value = int(value)
                    except (ValueError, TypeError):
                        value = 0
                elif field.field_type == "TIMESTAMP":
                    try:
                        # Replace Z with +00:00 and parse
                        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        value = None  # Or a default timestamp
                elif field.field_type == "STRING":
                    value = str(value)


            record[field.name] = value


        if str(record[id_field]) not in existing_ids:
            new_records.append(record)

    if new_records:
        load_to_bigquery(dataset_id, table_id, new_records, schema)


def main(request):

    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")

    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("API credentials not found in environment variables.")

    token = f"{CLIENT_ID}:{CLIENT_SECRET}"
    headers = {"Authorization": f"Basic {token}"}

    dataset_id = "your_dataset"  # Replace with your dataset

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


    endpoints = [
        # ... endpoint definitions (unchanged) ...
    ]


    try:
        for endpoint in endpoints:
            process_endpoint(
                endpoint_name=endpoint["name"],
                # ... other parameters (unchanged)
            )

        return "All endpoints processed successfully."
    except Exception as e:
        print(f"Error occurred: {e}")
        return f"Error: {e}", 500  # Return an appropriate error code


