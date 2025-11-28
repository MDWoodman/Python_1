import requests

# Replace these with your actual credentials
API_KEY = "drwal.michal@gmail.com"
ACCESS_TOKEN = "M.Drwal"
BASE_URL = "https://ciapi.cityindex.com/v2/"

def get_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }

def get_account_details():
    url = f"{BASE_URL}accounts"
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def get_market_prices(instrument):
    url = f"{BASE_URL}markets/{instrument}/price"
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def place_order(instrument, units, order_type="MARKET", side="BUY"):
    url = f"{BASE_URL}orders"
    payload = {
        "instrument": instrument,
        "units": units,
        "type": order_type,
        "side": side
    }
    try:
        response = requests.post(url, json=payload, headers=get_headers(), timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

if __name__ == "__main__":
    # Example Usage++
    print("Account Details:", get_account_details())
    print("Market Price EUR/USD:", get_market_prices("EUR/USD"))
    # print("Placing Order:", place_order("EUR/USD", 1000, "MARKET", "BUY"))
