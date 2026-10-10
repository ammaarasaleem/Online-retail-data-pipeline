import base64
import json
import os
import requests
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

# Resolve repository root dynamically relative to this file
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Load environment variables (.env at project root)
load_dotenv(os.path.join(REPO_ROOT, ".env"))

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

INCREMENTAL_DIR = os.path.join(REPO_ROOT, "data", "raw", "incremental_load")
WATERMARK_FILE = os.path.join(INCREMENTAL_DIR, ".watermark.txt")


def get_oauth_token():
    """Fetches eBay application OAuth token via Client Credentials flow."""
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


def get_stored_watermark() -> str:
    """Reads persisted watermark; defaults to 7 days lookback if first execution."""
    if os.path.exists(WATERMARK_FILE):
        with open(WATERMARK_FILE, "r", encoding="utf-8") as f:
            stored = f.read().strip()
            if stored:
                return stored
    default_start = datetime.now(timezone.utc) - timedelta(days=7)
    return default_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def save_watermark(new_watermark: str):
    """Persists highest seen itemCreationDate for future incremental runs."""
    os.makedirs(os.path.dirname(WATERMARK_FILE), exist_ok=True)
    with open(WATERMARK_FILE, "w", encoding="utf-8") as f:
        f.write(new_watermark)


def fetch_subcategory_incremental(token: str, category_id: str, watermark: str, limit: int = 5):
    """Fetches listings created after watermark timestamp."""
    url = "https://api.ebay.com/buy/browse/v1/item_summary/search"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {
        "category_ids": category_id,
        "sort": "newlyListed",
        "filter": f"itemCreationDate:[{watermark}..]",
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
    
    current_watermark = get_stored_watermark()
    print(f"[INFO] Running incremental fetch with watermark: >= {current_watermark}")

    bronze_records = []
    latest_seen_creation = current_watermark

    for cat_id, subcat_name in TARGET_SUBCATEGORIES.items():
        print(f"Fetching: {subcat_name} (ID: {cat_id})...")
        items = fetch_subcategory_incremental(token, cat_id, current_watermark, limit=4)
        
        for item in items:
            item_creation = item.get("itemCreationDate")
            if item_creation and item_creation > latest_seen_creation:
                latest_seen_creation = item_creation

            # Retains the schema structure matching your historical samples
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

    if not bronze_records:
        print("[INFO] No new items discovered past the current watermark.")
        return

    os.makedirs(INCREMENTAL_DIR, exist_ok=True)
    batch_filename = f"ebay_incremental_{batch_time.strftime('%Y%m%d_%H%M%S')}.json"
    output_file = os.path.join(INCREMENTAL_DIR, batch_filename)
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(bronze_records, f, indent=2)

    # Advance the watermark
    save_watermark(latest_seen_creation)

    print(f"\n[SUCCESS] Wrote {len(bronze_records)} records to {output_file}")
    print(f"[SUCCESS] Advanced watermark to: {latest_seen_creation}")


if __name__ == "__main__":
    main()