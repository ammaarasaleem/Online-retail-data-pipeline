import base64
import json
import os
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")

# Target exact subcategories matching your 14 data/raw/full_load/ folders
TARGET_SUBCATEGORIES = {
    "43304": "cell phones and accessories-cell phones & smartphone parts",
    "178893": "cell phones and accessories-smartwatches",
    "175672": "coin & paper money-bullion",
    "3412": "coin & paper money-us paper money",
    "26429": "eBay Motor-Other Vehicles & Trailers",
    "4196": "jewelery and watches-fine jewelery",
    "10968": "jewelry and watches-fashion jewelery",
    "155101": "jewelry and watches-kids jewelry",
    "15841": "real estate-commercial real estate",
    "15840": "real estate-land read estate",
    "12605": "real estate-residential real estate",
    "15273": "sporting goods- fitness, running and yoga",
    "16034": "sporting goods-camping and hiking",
    "1513": "sporting goods-golf"
}

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

def fetch_subcategory_incremental(token, category_id, limit=5):
    """Fetches real live listings created recently in the target subcategory."""
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    # Past 7 days window for newly listed active items
    start_date = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    params = {
        "category_ids": category_id,
        "sort": "newlyListed",
        "filter": f"itemCreationDate:[{start_date}..]",
        "limit": limit
    }
    res = requests.get(url, headers=headers, params=params)
    if res.status_code == 200:
        return res.json().get("itemSummaries", [])
    return []

def main():
    token = get_oauth_token()
    batch_time = datetime.now(timezone.utc)
    batch_id = f"batch_{batch_time.strftime('%Y%m%d_%H%M%S')}"

    bronze_records = []

    for cat_id, subcat_name in TARGET_SUBCATEGORIES.items():
        print(f"Fetching incremental items for: {subcat_name} (ID: {cat_id})...")
        items = fetch_subcategory_incremental(token, cat_id, limit=4)
        
        for item in items:
            bronze_records.append({
                "load_type": "incremental",
                "batch_id": batch_id,
                "ingestion_timestamp": batch_time.isoformat(),
                "subcategory_partition": subcat_name,
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

    print(f"\nSuccessfully collected {len(bronze_records)} items across all 14 subcategories at: {output_file}")

if __name__ == "__main__":
    main()