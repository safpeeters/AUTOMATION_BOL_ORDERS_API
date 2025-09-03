import requests
import base64
import time
import pandas as pd
import os
from datetime import datetime, timedelta
from google.oauth2 import service_account
import pandas_gbq
import json

# =================================================================
# Configuratie Bol.com Retailer API
# =================================================================
# Haal de client ID en client secret op uit de omgevingsvariabelen (GitHub Secrets)
CLIENT_ID = os.environ.get('BOL_CLIENT_ID')
CLIENT_SECRET = os.environ.get('BOL_CLIENT_SECRET')

API_BASE_URL = 'https://api.bol.com/retailer'
TOKEN_URL = 'https://login.bol.com/token'

# =================================================================
# Globale configuratie
# =================================================================
MAX_PAGINA = 1000
PAGE_SIZE = 50

# --- RATE LIMIT CONFIGURATIE ---
SLEEP_TIME_ORDERS = 60 / 20
SLEEP_TIME_ORDER_DETAILS = 1 / 24
MAX_RETRY_ATTEMPTS = 3

# =================================================================
# Configuratie Google BigQuery
# =================================================================
PROJECT_ID = 'advertentiedata-bol-ds'
DATASET_ID = 'DATASET_BOL_ADVERTENTIES_DS'
TABLE_ID = 'ORDERDATA_BOL_DS'

# Haal de service account info op uit een omgevingsvariabele (GitHub Secret)
SERVICE_ACCOUNT_INFO = json.loads(os.environ.get('GCP_SA_KEY'))

# Globale variabele voor het opslaan van het token en de vervaltijd
bol_toegangstoken = None
token_verloopt_om = datetime.now()

# =================================================================
# Bol.com API authenticatie
# =================================================================
def krijg_bol_toegangstoken():
    """Haalt het Bol.com authenticatie token op en slaat het op met vervaltijd."""
    global bol_toegangstoken, token_verloopt_om

    print("Start: Bol.com authenticatie token ophalen...")
    auth_string = f'{CLIENT_ID}:{CLIENT_SECRET}'
    encoded_auth_string = base64.b64encode(auth_string.encode()).decode()

    headers = {
        'Authorization': f'Basic {encoded_auth_string}',
        'Accept': 'application/vnd.retailer.v10+json'
    }
    data = {'grant_type': 'client_credentials'}

    try:
        response = requests.post(TOKEN_URL, headers=headers, data=data)
        response.raise_for_status()

        response_json = response.json()
        access_token = response_json.get('access_token')
        expires_in = response_json.get('expires_in', 0)

        if access_token:
            print("Succes: Bol.com toegangstoken ontvangen.")
            bol_toegangstoken = access_token
            # Sla de vervaltijd op, met een kleine marge van 60 seconden
            token_verloopt_om = datetime.now() + timedelta(seconds=expires_in - 60)
            return bol_toegangstoken
        else:
            print("[FOUT] Geen toegangstoken ontvangen.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[FOUT] Fout bij het ophalen van het Bol.com toegangstoken: {e}")
        return None


def check_en_vernieuwt_token():
    """Controleert of het token nog geldig is en vernieuwt het indien nodig."""
    global bol_toegangstoken, token_verloopt_om
    if bol_toegangstoken is None or datetime.now() >= token_verloopt_om:
        print("Token is verlopen of niet aanwezig. Vernieuwen...")
        return krijg_bol_toegangstoken()
    return bol_toegangstoken


# =================================================================
# Orders ophalen
# =================================================================
def maak_api_call_met_retry(method, url, headers, params=None, data=None):
    """Maakt een API-aanroep en probeert het opnieuw bij een 429-fout."""
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params)
            else:
                response = requests.post(url, headers=headers, data=data)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = (attempt + 1) * 60
                print(
                    f"[WAARSCHUWING] Rate limit overschreden (429). Wacht {wait_time} seconden. Poging {attempt + 1} van {MAX_RETRY_ATTEMPTS}.")
                time.sleep(wait_time)
            elif e.response.status_code == 401:
                # Als het token niet meer geldig is, proberen we het te vernieuwen en de aanroep opnieuw te doen
                print("[WAARSCHUWING] 401 Unauthorized. Token wordt vernieuwd en aanroep wordt opnieuw geprobeerd.")
                check_en_vernieuwt_token()
                headers['Authorization'] = f'Bearer {bol_toegangstoken}'
                continue
            else:
                raise e
    raise requests.exceptions.RequestException(f"API-aanroep mislukt na {MAX_RETRY_ATTEMPTS} pogingen.")


def krijg_alle_orders_van_dag(datum):
    """Haalt alle orders op die zijn gewijzigd op een specifieke datum."""
    print(f"Start: Orders ophalen voor datum: {datum}...")
    orders_url = f'{API_BASE_URL}/orders'
    alle_orders = []
    page = 1

    token = check_en_vernieuwt_token()
    if not token:
        print("[KRITIEKE FOUT] Geen geldig Bol.com toegangstoken beschikbaar.")
        return None

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.retailer.v10+json'
    }

    try:
        datetime.strptime(datum, "%Y-%m-%d")
    except ValueError:
        print(f"[FOUT] Ongeldig datumformaat: {datum}. Gebruik 'YYYY-MM-DD'.")
        return None

    while True:
        params = {
            'latest-change-date': datum,
            'fulfilment-method': 'ALL',
            'status': 'ALL',
            'page': page,
            'page-size': PAGE_SIZE
        }

        print(f"[DEBUG] URL van aanroep: {requests.Request('GET', orders_url, params=params).prepare().url}")

        try:
            response = maak_api_call_met_retry('GET', orders_url, headers, params=params)
            response_json = response.json()
            orders = response_json.get('orders', [])
            for order in orders:
                alle_orders.append(order.get('orderId'))

            if len(orders) < PAGE_SIZE or page >= MAX_PAGINA:
                break

            page += 1
            time.sleep
