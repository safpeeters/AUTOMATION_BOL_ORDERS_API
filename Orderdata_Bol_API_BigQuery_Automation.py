import os
import requests
import json
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from google.oauth2 import service_account

# --- Configuratie ---
# Zorg ervoor dat deze omgevingsvariabelen zijn ingesteld of vul ze direct in
CLIENT_ID = os.getenv('BOL_CLIENT_ID')
CLIENT_SECRET = os.getenv('BOL_CLIENT_SECRET')

# De datum waarvoor je de orders wilt ophalen (voor deze run)
# De datum in je output was 2025-09-02, maar je zou hier de huidige datum kunnen gebruiken
PROCESSING_DATE = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
# PROCESSING_DATE = '2025-09-02' # Je kunt een specifieke datum instellen voor testen

# BigQuery configuratie
BIGQUERY_PROJECT_ID = 'advertentiedata-bol-ds'
BIGQUERY_DATASET_ID = 'DATASET_BOL_ADVERTENTIES_DS'
BIGQUERY_TABLE_ID = 'ORDERDATA_BOL_DS'
BIGQUERY_CREDENTIALS_PATH = 'path/naar/je/service_account_file.json' # Pas dit aan!

# --- Initialisatie ---
bol_api_url = 'https://api.bol.com/retailer/orders'
bol_token_url = 'https://login.bol.com/token'

# BigQuery client
try:
    credentials = service_account.Credentials.from_service_account_file(BIGQUERY_CREDENTIALS_PATH)
    bigquery_client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)
    table_ref = bigquery_client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TABLE_ID)
except Exception as e:
    print(f"[KRITIEKE FOUT] Kan BigQuery-client niet initialiseren: {e}")
    exit()

def get_bol_token():
    """Haalt een Bol.com OAuth2-token op."""
    print("Start: Bol.com authenticatie token ophalen...")
    headers = {'Content-Type': 'application/json'}
    data = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    try:
        response = requests.post(bol_token_url, headers=headers, json=data)
        response.raise_for_status()
        token_info = response.json()
        print("Succes: Bol.com toegangstoken ontvangen.")
        return token_info['access_token']
    except requests.exceptions.HTTPError as errh:
        print(f"[FOUT] HTTP Fout: {errh}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"[FOUT] Fout bij aanvraag: {err}")
        return None

def fetch_orders(token, date):
    """Haalt alle orders op voor een specifieke datum met paginatie."""
    print(f"--- Verwerking gestart voor datum: {date} ---")
    print(f"Start: Orders ophalen voor datum: {date}...")
    all_orders = []
    page = 1
    page_size = 50
    rate_limit_waits = 0
    max_retries = 3

    while True:
        url = f"{bol_api_url}?latest-change-date={date}&fulfilment-method=ALL&status=ALL&page={page}&page-size={page_size}"
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.retailer.v9+json',
        }

        try:
            print(f"[DEBUG] URL van aanroep: {url}")
            response = requests.get(url, headers=headers)

            if response.status_code == 429:
                if rate_limit_waits < max_retries:
                    wait_time = 60
                    print(f"[WAARSCHUWING] Rate limit overschreden (429). Wacht {wait_time} seconden. Poging {rate_limit_waits + 1} van {max_retries}.")
                    time.sleep(wait_time)
                    rate_limit_waits += 1
                    continue
                else:
                    print(f"[KRITIEKE FOUT] Rate limit overschreden na {max_retries} pogingen. Script stopt.")
                    return None
            
            response.raise_for_status()
            data = response.json()

            if not data.get('orders'):
                print(f"Geen orders meer gevonden op pagina {page}. Paginatie voltooid.")
                break

            all_orders.extend(data['orders'])
            page += 1
        
        except requests.exceptions.RequestException as e:
            print(f"[FOUT] Fout bij het ophalen van orders: {e}")
            return None

    print(f"Uiteindelijk zijn er {len(all_orders)} order(s) geselecteerd voor {date}.")
    return all_orders

def format_data_for_bigquery(orders):
    """Vertaalt de Bol.com API-data naar een BigQuery-vriendelijk formaat."""
    rows = []
    for order in orders:
        row = {
            "orderId": order.get("orderId"),
            "orderPlacedDateTime": order.get("orderPlacedDateTime"),
            "orderItems": [],
            "customerDetails": order.get("customerDetails"),
            # Nieuwe velden of velden die aangepast moeten worden
            "isGeannuleerd": order.get("orderItems")[0].get("cancellationRequest"),
        }
        
        for item in order.get("orderItems", []):
            item_row = {
                "orderItemId": item.get("orderItemId"),
                "ean": item.get("ean"),
                "fulfilment": item.get("fulfilment"),
                "offer": item.get("offer"),
                "quantity": item.get("quantity"),
                "unitPrice": item.get("unitPrice"),
                "cancellationRequest": item.get("cancellationRequest"),
            }
            row["orderItems"].append(item_row)
        rows.append(row)
    return rows

def check_and_update_schema(client, table_ref):
    """Controleert en past het BigQuery-schema aan voor de 'Is Geannuleerd' kolom."""
    try:
        table = client.get_table(table_ref)
        current_schema = table.schema

        new_field = bigquery.SchemaField('isGeannuleerd', 'BOOL', mode='NULLABLE')
        field_exists = any(field.name == new_field.name for field in current_schema)

        if not field_exists:
            print(f"[INFO] Veld '{new_field.name}' bestaat niet, toevoegen aan het schema.")
            new_schema = list(current_schema)
            new_schema.append(new_field)
            table.schema = new_schema
            client.update_table(table, ['schema'])
            print(f"[INFO] Veld '{new_field.name}' succesvol toegevoegd aan de tabel.")
        else:
            print(f"[INFO] Veld '{new_field.name}' bestaat al in het schema. Geen actie nodig.")
    except Exception as e:
        print(f"[KRITIEKE FOUT] Fout bij het controleren of aanpassen van het schema: {e}")
        return False
    return True

def push_to_bigquery(rows):
    """Pusht de gegevens naar BigQuery met de 'append' optie."""
    print("Start: Gegevens naar BigQuery pushen met 'append' optie...")
    table = bigquery_client.get_table(table_ref)
    
    # Wees zeker van het schema
    if not check_and_update_schema(bigquery_client, table_ref):
        return
        
    errors = bigquery_client.insert_rows_json(table, rows)

    if errors == []:
        print("Succes: Gegevens succesvol naar BigQuery gepusht.")
    else:
        print(f"[KRITIEKE FOUT] Fout bij het pushen van gegevens naar BigQuery: {errors}")

def main():
    """Hoofdfunctie van het script."""
    start_time = time.time()
    
    bol_token = get_bol_token()
    if not bol_token:
        print("Script stopt vanwege fout bij het ophalen van token.")
        return

    orders = fetch_orders(bol_token, PROCESSING_DATE)
    if not orders:
        print("Geen orders opgehaald. Script voltooid zonder verdere actie.")
        return

    rows_to_insert = format_data_for_bigquery(orders)
    push_to_bigquery(rows_to_insert)

    end_time = time.time()
    duration = end_time - start_time
    minutes, seconds = divmod(duration, 60)

    print("\n" + "="*50)
    print(" " * 15 + "Script voltooid")
    print(f"  Totaal duur: {int(minutes)} minuut(en) en {int(seconds)} seconde(n)")
    print("="*50)

if __name__ == "__main__":
    main()
