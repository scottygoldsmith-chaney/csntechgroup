import json
from utilities import process_endpoint
from datetime import datetime, timedelta

def main():
    # Load configuration
    with open("config.json", "r") as f:
        config = json.load(f)
    
    for client in config["clients"]:
        print(f"Processing data for client: {client['name']}")
        process_client(client)

def process_client(client):
    # Retrieve client-specific details
    api_credentials = client["api"]
    dataset = client["bigquery"]["dataset"]
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Process endpoints
    endpoints = ["donations", "designations", "funds", "campuses", "donors"]
    for endpoint in endpoints:
        process_endpoint(api_credentials, dataset, endpoint, yesterday)

if __name__ == "__main__":
    main()
