import os
from utilities import fetch_data_from_api, load_to_bigquery, get_existing_ids
from google.cloud import bigquery
from datetime import datetime, timedelta

def process_endpoint(endpoint_name, base_url, id_field, headers, dataset_id, table_id, schema):
    """Generic function to process an API endpoint."""
    print(f"Processing {endpoint_name}...")

    # Fetch data from API
    data_items = fetch_data_from_api(base_url, headers)

    if data_items is None:  # Handle potential null return from API
        print(f"No data retrieved for {endpoint_name}. Skipping.")
        return

    # Check for existing records
    existing_ids = get_existing_ids(dataset_id, table_id, id_field)

    new_records = []
    for item in data_items:
        if not item: # Handle cases where item might be null or empty.
            continue

        record = {id_field: item["id"]} # Directly assign ID


        for field in schema:
            if field.name == id_field:
                continue  # Already handled

            value = item.get("attributes", {}).get(field.name)  # Safer attribute access

            if value is None and item.get('relationships') and field.name in item.get('relationships', {}):
                rel_data = item['relationships'][field.name].get('data')
                if rel_data:
                    value = rel_data.get('id')

            # Handle null values (adjust default/handling as needed)
            if value is None:
                if field.field_type == 'STRING':
                    value = ''  # Or None, depending on your needs.
                elif field.field_type == 'FLOAT' or field.field_type == 'INTEGER':
                    value = 0  # Or a suitable default.  Could also use a specific marker value for "missing."
                # Handle other data types appropriately

            record[field.name] = value

        if record[id_field] not in existing_ids:
            new_records.append(record)

    # Insert new records
    if new_records:
        load_to_bigquery(dataset_id, table_id, new_records, schema)
        print(f"Inserted {len(new_records)} new records into {table_id}.")
    else:
        print(f"No new records to insert into {table_id}.")

def main(request):
    # Retrieve API credentials from environment variables
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("API credentials not found in environment variables.")
    token = f"{CLIENT_ID}:{CLIENT_SECRET}"
    headers = {"Authorization": f"Basic {token}"}

    dataset_id = "your_dataset"  # Replace with your BigQuery dataset ID

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    endpoints = [
        {
            "name": "donations",
            "base_url": f"https://api.planningcenteronline.com/giving/v2/donations?where[completed_at][eq]={yesterday}&per_page=100",
            "id_field": "donation_id",
            "table_id": "pco-donations",
            "schema": [
                bigquery.SchemaField("donation_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("completed_date", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            ],
        },
        {
            "name": "designations",
            "base_url": "https://api.planningcenteronline.com/giving/v2/designations?per_page=100",
            "id_field": "designation_id",
            "table_id": "pco-designations",
            "schema": [
                bigquery.SchemaField("designation_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("fund_id", "STRING", mode="NULLABLE"),
            ],
        },
        {
            "name": "funds",
            "base_url": "https://api.planningcenteronline.com/giving/v2/funds?per_page=100",
            "id_field": "fund_id",
            "table_id": "pco-funds",
            "schema": [
                bigquery.SchemaField("fund_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            ],
        },
        {
            "name": "campuses",
            "base_url": "https://api.planningcenteronline.com/giving/v2/campuses?per_page=100",
            "id_field": "campus_id",
            "table_id": "pco-campuses",
            "schema": [
                bigquery.SchemaField("campus_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            ],
        },
        {
            "name": "donors",
            "base_url": "https://api.planningcenteronline.com/people/v2/people?per_page=100",
            "id_field": "donor_id",
            "table_id": "pco-donors",
            "schema": [
                bigquery.SchemaField("donor_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
            ],
        },
    ]

    try:
        for endpoint in endpoints:
            process_endpoint(
                endpoint_name=endpoint["name"],
                base_url=endpoint["base_url"],
                id_field=endpoint["id_field"],
                headers=headers,
                dataset_id=dataset_id,
                table_id=endpoint["table_id"],
                schema=endpoint["schema"],
            )

        return "All endpoints processed successfully."
    except Exception as e:
        print(f"Error occurred: {e}")
        return f"Error: {e}"