import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery
import functions_framework
import logging
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bill.com API configuration
BILL_API_URL = "https://gateway.prod.bill.com/connect/v3/spend/transactions"
# Bill.com API Token - for production, store as environment variable
BILL_API_TOKEN = os.environ.get("BILL_API_TOKEN", "CK3UG7RFHwT4syMsBJqJ0CZaGRrSe+pDP9JBjwK1Mho")

# BigQuery configuration
PROJECT_ID = "csntechgroup"
CONFIG_DATASET = "collaborative_stewardship"
CONFIG_TABLE = "gsd_clientsyncmaster"
DEFAULT_DATASET_ID = "liv265_test"  # Default dataset if config lookup fails
TABLE_ID = "divvy_transactions"

# Timezone configuration for scheduling
EASTERN_TZ = pytz.timezone('US/Eastern')

@functions_framework.http
def process_transactions(request):
    """
    Cloud Run function to fetch transactions from Bill.com and store in BigQuery
    """
    try:
        # Get client configuration from the master table
        client_config = get_client_config()
        
        if not client_config:
            logger.warning("No active client configurations found")
            return {"status": "warning", "message": "No active client configurations found"}, 200
        
        results = []
        
        # Process each client configuration
        for config in client_config:
            client_schema = config.get('Client_Schema')
            logger.info(f"Processing client schema: {client_schema}")
            
            # Get transactions from Bill.com
            transactions = fetch_transactions()
            
            if transactions:
                # Load transactions to BigQuery
                records_loaded = load_to_bigquery(transactions, client_schema)
                
                # Update the last sync date in the config table
                update_sync_status(config.get('Client_Schema'), 'SUCCESS', len(transactions))
                
                results.append({
                    "client_schema": client_schema,
                    "status": "success",
                    "records_loaded": records_loaded
                })
            else:
                update_sync_status(config.get('Client_Schema'), 'NO_DATA')
                results.append({
                    "client_schema": client_schema,
                    "status": "warning",
                    "message": "No transactions found"
                })
        
        return {"status": "success", "results": results}, 200
            
    except Exception as e:
        logger.error(f"Error processing transactions: {str(e)}")
        return {"status": "error", "message": str(e)}, 500

def get_client_config():
    """
    Get client configuration from the master table
    """
    client = bigquery.Client()
    query = f"""
    SELECT 
        Client_Schema, Platform, Client_ID, Client_Secret, 
        Redirect_URI, Refresh_Token, Sync_Status, Last_Sync_Date
    FROM 
        `{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}`
    WHERE
        Platform = 'Bill.com' AND Sync_Status != 'DISABLED'
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    config_list = [dict(row) for row in results]
    if not config_list:
        # Return default configuration if no configurations found
        logger.info("No client configurations found. Using default configuration.")
        return [{
            'Client_Schema': DEFAULT_DATASET_ID,
            'Platform': 'Bill.com',
            'Sync_Status': 'ACTIVE'
        }]
    
    return config_list

def update_sync_status(client_schema, status, records_processed=0):
    """
    Update the sync status in the config table
    """
    client = bigquery.Client()
    now = datetime.utcnow()
    
    query = f"""
    UPDATE 
        `{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}`
    SET 
        Sync_Status = '{status}',
        Last_Sync_Date = TIMESTAMP('{now.isoformat()}'),
        ChangeDate = TIMESTAMP('{now.isoformat()}')
    WHERE
        Client_Schema = '{client_schema}' AND Platform = 'Bill.com'
    """
    
    query_job = client.query(query)
    query_job.result()
    logger.info(f"Updated sync status for {client_schema} to {status}. Records processed: {records_processed}")

def fetch_transactions(page_size=100):
    """
    Fetch transactions from Bill.com API, with pagination support
    """
    all_transactions = []
    next_page = None
    headers = {
        "Authorization": f"Bearer {BILL_API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    # Continue fetching pages until there are no more
    while True:
        params = {"pageSize": page_size}
        if next_page:
            params["nextPage"] = next_page
            
        response = requests.get(BILL_API_URL, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        transactions = data.get("results", [])  # Updated to match the actual response structure
        all_transactions.extend(transactions)
        
        # Check if there are more pages
        next_page = data.get("nextPage")
        if not next_page:
            break
    
    logger.info(f"Fetched {len(all_transactions)} transactions from Bill.com")
    return all_transactions

def load_to_bigquery(transactions, dataset_id):
    """
    Load transactions to BigQuery
    """
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{dataset_id}.{TABLE_ID}"
    
    # Check if table exists, if not create it
    try:
        client.get_table(table_id)
        logger.info(f"Table {table_id} already exists")
    except Exception:
        logger.info(f"Table {table_id} does not exist. Creating it.")
        create_table(client, dataset_id, TABLE_ID)
    
    # Prepare rows for insertion
    rows_to_insert = []
    for transaction in transactions:
        # Add a timestamp for when this data was loaded
        transaction['loaded_at'] = datetime.utcnow().isoformat()
        rows_to_insert.append(transaction)
    
    # Insert rows
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logger.error(f"Errors inserting rows: {errors}")
        raise Exception(f"Error inserting rows: {errors}")
    else:
        logger.info(f"Successfully inserted {len(rows_to_insert)} rows into {table_id}")
        return len(rows_to_insert)

def create_table(client, dataset_id, table_id):
    """
    Create BigQuery table if it doesn't exist
    """
    # Schema based on the provided sample response
    schema = [
        bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("childTransactionIds", "STRING", mode="REPEATED"),
        bigquery.SchemaField("childTransactionUuids", "STRING", mode="REPEATED"),
        bigquery.SchemaField("isLocked", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("isReconciled", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("transactionType", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("parentTransactionId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("userId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("userUuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rawMerchantName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merchantName", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("budgetId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("budgetUuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("originalAuthTransactionId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("originalAuthTransactionUuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("isCredit", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("currencyData", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("exchangeRate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("exponent", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("originalCurrencyAmount", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("originalCurrencyCode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("symbol", "STRING", mode="NULLABLE")
        ]),
        bigquery.SchemaField("receiptRequired", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("reviewRequired", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("occurredTime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("updatedTime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("authorizedTime", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("complete", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("pointsAwarded", "INTEGER", mode="NULLABLE"),
        # Custom fields as a repeated record
        bigquery.SchemaField("customFields", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("isRequired", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("selectedValues", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("uuid", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value", "STRING", mode="NULLABLE")
            ])
        ]),
        # Receipts as a repeated record
        bigquery.SchemaField("receipts", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("filename", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("uuid", "STRING", mode="NULLABLE")
        ]),
        bigquery.SchemaField("network", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("isParent", "BOOLEAN", mode="NULLABLE"),
        # Reviews as a repeated record
        bigquery.SchemaField("reviews", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("isApproved", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("note", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("createdTime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("deletedTime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("reviewerId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reviewerUuid", "STRING", mode="NULLABLE")
        ]),
        bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("transactedAmount", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("fees", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("foreignExchangeFee", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("receiptStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("matchedClearTransactionId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("matchedClearTransactionUuid", "STRING", mode="NULLABLE"),
        # Accounting integration transactions as a repeated record
        bigquery.SchemaField("accountingIntegrationTransactions", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("billable", "BOOLEAN", mode="NULLABLE"),
            bigquery.SchemaField("integrationTxId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("syncStatus", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("syncMessage", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("integrationType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("integrationId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("syncRequestId", "STRING", mode="NULLABLE")
        ]),
        # Reviewers as a repeated record
        bigquery.SchemaField("reviewers", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("approverType", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("reviewedTime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("userId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("userUuid", "STRING", mode="NULLABLE")
        ]),
        bigquery.SchemaField("cardId", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cardUuid", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("receiptSyncStatus", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("merchantCategoryCode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("declineReason", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cardPresent", "BOOLEAN", mode="NULLABLE"),
        # Merchant location as a record
        bigquery.SchemaField("merchantLocation", "RECORD", mode="NULLABLE", fields=[
            bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("postalCode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE")
        ]),
        # Additional fields for tracking
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="NULLABLE")
    ]
    
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    
    # Create the table with time partitioning on the occurredTime field
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="occurredTime"
    )
    
    # Add clustering by merchant and transaction type for better query performance
    table.clustering_fields = ["merchantName", "transactionType"]
    
    table = client.create_table(table)
    logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")

def create_scheduler():
    """
    Helper function to create a Cloud Scheduler job (not called directly)
    Instructions for creating the scheduler job:
    
    gcloud scheduler jobs create http bill_transactions_daily_sync \
      --schedule="0 8 * * *" \
      --uri="{your_cloud_run_endpoint}" \
      --http-method=POST \
      --time-zone="America/New_York" \
      --attempt-deadline=540s
    
    Note: 8:00 UTC is 4:00 AM Eastern Time
    """
    pass
    
if __name__ == "__main__":
    # For local testing only
    process_transactions(None)
