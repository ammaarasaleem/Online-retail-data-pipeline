import base64
import json
import os
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Load credentials from .env file
load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

def get_oauth_token():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Missing EBAY_CLIENT_ID or EBAY_CLIENT_SECRET in .env file.")
        
    url = "https://api.ebay.com/identity/v1/oauth2/token"
    creds = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {creds}"
    }
    body = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope"
    }
    res = requests.post(url, headers=headers, data=body)
    res.raise_for_status()
    return res.json()["access_token"]

def fetch_incremental_listings(token, category_id="9355", limit=50):
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Filter listings created in the past 7 days
    start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params = {
        "category_ids": category_id,
        "sort": "newlyListed",
        "filter": f"itemCreationDate:[{start_date}..]",
        "limit": limit
    }
    res = requests.get(url, headers=headers, params=params)
    res.raise_for_status()
    return res.json()

def main():
    token = get_oauth_token()
    raw_response = fetch_incremental_listings(token)
    items = raw_response.get("itemSummaries", [])
    
    batch_time = datetime.now(timezone.utc)
    batch_id = f"batch_{batch_time.strftime('%Y%m%d_%H%M%S')}"

    # Tag records with Bronze auditing schema (Section 4.1)
    bronze_records = []
    for item in items:
        bronze_records.append({
            "load_type": "incremental",
            "batch_id": batch_id,
            "ingestion_timestamp": batch_time.isoformat(),
            "raw_payload": {
                "itemId": item.get("itemId"),
                "title": item.get("title"),
                "categories": item.get("categories"),
                "price": item.get("price"),
                "seller": item.get("seller", {}).get("username"),
                "condition": item.get("condition"),
                "itemCreationDate": item.get("itemCreationDate")
            }
        })

    target_dir = os.path.join("data", "raw", "incremental_load")
    os.makedirs(target_dir, exist_ok=True)
    
    output_file = os.path.join(target_dir, "ebay_incremental_sample.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(bronze_records, f, indent=2)

    print(f"Successfully generated {len(bronze_records)} live items at: {output_file}")

if __name__ == "__main__":
    main()