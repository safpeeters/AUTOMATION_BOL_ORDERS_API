import os
import requests
import json
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuration and Secrets ---
# Use environment variables for all secrets
CLIENT_ID = os.environ.get('BOL_CLIENT_ID')
CLIENT_SECRET = os.environ.get('BOL_CLIENT_SECRET')

# BigQuery configuration
BIGQUERY_PROJECT_ID = 'advertentiedata-bol-ds'
BIGQUERY_DATASET_ID = 'DATASET_BOL_ADVERTENTIES_DS'
BIGQUERY_TABLE_ID = 'ORDERDATA_BOL_DS'

# The date for which to fetch orders (e.g., yesterday's date)
PROCESSING_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

# --- Initialization ---
BOL_API_URL = 'https://api.bol.com/retailer/orders'
BOL_TOKEN_URL = 'https://login.bol.com/token'

def get_bigquery_client():
    """Initializes and returns a BigQuery client using a service account key from an environment variable."""
    try:
        # Get the service account key from the environment variable (GitHub Secret)
        service_account_info = json.loads(os.environ.get('GCP_SA_KEY'))
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        print("Success: BigQuery client initialized.")
        return bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)
    except Exception as e:
        print(f"[CRITICAL ERROR] Failed to initialize BigQuery client: {e}")
        return None

def get_bol_token():
    """Fetches a Bol.com OAuth2 token."""
    print("Starting: Fetching Bol.com authentication token...")
    if not CLIENT_ID or not CLIENT_SECRET:
        print("[ERROR] BOL_CLIENT_ID or BOL_CLIENT_SECRET not set in environment variables.")
        return None

    headers = {'Content-Type': 'application/json'}
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    try:
        response = requests.post(BOL_TOKEN_URL, headers=headers, json=data)
        response.raise_for_status()
        token_info = response.json()
        print("Success: Bol.com access token received.")
        return token_info.get('access_token')
    except requests.exceptions.RequestException as err:
        print(f"[ERROR] Error during Bol.com token request: {err}")
        return None

def fetch_orders(token, date):
    """Fetches all orders for a specific date with pagination."""
    print(f"--- Processing started for date: {date} ---")
    print(f"Starting: Fetching orders for date: {date}...")
    all_orders = []
    page = 1
    page_size = 50
    max_retries = 3

    while True:
        url = f"{BOL_API_URL}?latest-change-date={date}&fulfilment-method=ALL&status=ALL&page={page}&page-size={page_size}"
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.retailer.v9+json',
        }

        try:
            print(f"[DEBUG] API call URL: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code == 429:
                if max_retries > 0:
                    wait_time = 60
                    print(f"[WARNING] Rate limit exceeded (429). Waiting {wait_time} seconds. Retrying...")
                    time.sleep(wait_time)
                    max_retries -= 1
                    continue
                else:
                    print(f"[CRITICAL ERROR] Rate limit exceeded after multiple retries. Script will stop.")
                    return None
            
            response.raise_for_status()
            data = response.json()

            orders = data.get('orders', [])
            if not orders:
                print(f"No more orders found on page {page}. Pagination complete.")
                break

            all_orders.extend(orders)
            page += 1
            # Add a small delay between requests to be polite to the API
            time.sleep(1)
        
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Error fetching orders: {e}")
            return None

    print(f"A total of {len(all_orders)} order(s) fetched for {date}.")
    return all_orders

def format_data_for_bigquery(orders):
    """Translates Bol.com API data into a BigQuery-friendly format."""
    rows = []
    for order in orders:
        # Simplified and validated data extraction
        for item in order.get("orderItems", []):
            is_cancelled = item.get("cancellationRequest", {}).get("isRequested", False)
            
            # Combine order and item data into a single row
            row = {
                "orderId": order.get("orderId"),
                "orderPlacedDateTime": order.get("orderPlacedDateTime"),
                "orderItemId": item.get("orderItemId"),
                "ean": item.get("ean"),
                "fulfilment": item.get("fulfilment"),
                "quantity": item.get("quantity"),
                "unitPrice": item.get("unitPrice"),
                "isGeannuleerd": is_cancelled,
                # Flatten customer data if needed, otherwise it's better to store as a JSON string or a nested field
                "customerDetails_email": order.get("customerDetails", {}).get("shipmentDetails", {}).get("email")
            }
            rows.append(row)
    return rows

def check_and_update_schema(client, table_ref):
    """Checks and updates the BigQuery schema for the 'isGeannuleerd' column."""
    try:
        table = client.get_table(table_ref)
        current_schema = table.schema
        
        new_field = bigquery.SchemaField('isGeannuleerd', 'BOOL', mode='NULLABLE')
        field_exists = any(field.name == new_field.name for field in current_schema)

        if not field_exists:
            print(f"[INFO] Field '{new_field.name}' does not exist, adding to schema.")
            new_schema = list(current_schema)
            new_schema.append(new_field)
            table.schema = new_schema
            client.update_table(table, ['schema'])
            print(f"[INFO] Field '{new_field.name}' successfully added to the table.")
        else:
            print(f"[INFO] Field '{new_field.name}' already exists in the schema. No action needed.")
    except Exception as e:
        print(f"[CRITICAL ERROR] Error checking or updating schema: {e}")
        return False
    return True

def push_to_bigquery(rows, bigquery_client):
    """Pushes data to BigQuery using the 'append' option."""
    print("Starting: Pushing data to BigQuery...")
    table_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TABLE_ID)

    if not check_and_update_schema(bigquery_client, table_ref):
        return

    # Check if there are rows to insert
    if not rows:
        print("No rows to insert. Skipping BigQuery push.")
        return

    try:
        errors = bigquery_client.insert_rows_json(table_ref, rows)

        if not errors:
            print(f"Success: Successfully pushed {len(rows)} rows to BigQuery.")
        else:
            print(f"[CRITICAL ERROR] Error pushing data to BigQuery: {errors}")
    except Exception as e:
        print(f"[CRITICAL ERROR] An unexpected error occurred during BigQuery insertion: {e}")

def main():
    """Main function of the script."""
    start_time = time.time()
    
    bol_token = get_bol_token()
    if not bol_token:
        print("Script stopped due to a token retrieval error.")
        return

    orders = fetch_orders(bol_token, PROCESSING_DATE)
    if not orders:
        print("No orders retrieved. Script completed with no further action.")
        return

    # Initialize BigQuery client only after successful token retrieval
    bigquery_client = get_bigquery_client()
    if not bigquery_client:
        return

    rows_to_insert = format_data_for_bigquery(orders)
    push_to_bigquery(rows_to_insert, bigquery_client)

    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)

    print("\n" + "="*50)
    print(" " * 15 + "Script Complete")
    print(f"  Total Duration: {int(minutes)} minute(s) and {int(seconds)} second(s)")
    print("="*50)

if __name__ == "__main__":
    main()
