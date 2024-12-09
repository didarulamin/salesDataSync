#!/usr/bin/env python
# coding: utf-8

# In[3]:


get_ipython().system('pip install -r requirements.txt')


# In[ ]:


import requests
from pymongo import MongoClient, UpdateOne
import time
from datetime import datetime, timezone
import schedule
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB Connection
MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = "Daftra"
COLLECTION_NAME = "products"

client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# API Configuration
API_URL_PRODUCTS = "https://amamamed.daftra.com/api2/products"
API_URL_TRANSACTIONS = "https://amamamed.daftra.com/api2/stock_transactions"
API_URL_ORDERS = "https://amamamed.daftra.com/v2/api/entity/invoice/list/-1"
API_URL_ORDERS_ZID = "https://api.zid.sa/v1/managers/store/orders/{order_id}/view"

# Environment Variables
APIKEY = os.getenv("APIKEY")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
MANAGER_TOKEN = os.getenv("MANAGER_TOKEN")

# Headers
daftra_headers = {
    "APIKEY": APIKEY,
    "Content-Type": "application/json"
}

zid_headers = {
    "Authorization": f"Bearer {AUTH_TOKEN}",
    "X-Manager-Token": MANAGER_TOKEN,
    "Accept": "application/json",
}

# Start dates
start_date_utc3 = datetime(2024, 11, 29, 20, 0, 0, tzinfo=timezone.utc)
start_date_utc3_for_others = datetime(2024, 12, 5, 11, 0, 0, tzinfo=timezone.utc)

# Target Product IDs
TARGET_PRODUCT_IDS = ["1056856", "1058711", "1058627", "1058530"]

# Retry settings
MAX_RETRIES = 3
BACKOFF_FACTOR = 2


# Aggregation pipeline to filter new transactions
def get_filtered_results():
    pipeline = [
        {
            "$match": {
                "id": {"$in": TARGET_PRODUCT_IDS}
            }
        },
        {
            "$project": {
                "_id": 1,
                "id": 1,
                "name": 1,
                "barcode": 1,
                "transactions": {
                    "$filter": {
                        "input": "$transactions",
                        "as": "transaction",
                        "cond": {
                            "$and": [
                                {
                                    "$gte": [
                                        {
                                            "$dateFromString": {
                                                "dateString": "$$transaction.created",
                                                "timezone": "Asia/Riyadh"
                                            }
                                        },
                                        {
                                            "$cond": {
                                                "if": {"$in": ["$id", ["1058627", "1058530"]]},
                                                "then": start_date_utc3_for_others,
                                                "else": start_date_utc3
                                            }
                                        }
                                    ]
                                },
                                {"$eq": ["$$transaction.transaction_type", "2"]},
                                {
                                    "$or": [
                                        {"$eq": ["$$transaction.currency_updated", False]},
                                        {"$not": {"$ifNull": ["$$transaction.currency_updated", False]}}
                                    ]
                                }
                            ]
                        }
                    }
                }
            }
        },
    ]
    return list(collection.aggregate(pipeline))


# Function to fetch currency code from the APIs

def fetch_currency_code(order_id, branch_id,transaction_id):
    # For branch IDs other than 5, set currency_code to 'SAR'
    if branch_id != "5":
        print(f"Branch ID {branch_id} {order_id} {transaction_id}: Skipping currency_code fetch and setting currency_code to 'SAR'.")
        return "SAR"  # Return 'SAR' for branch IDs other than 5

    # Retry logic for branch ID 5
    retries = MAX_RETRIES
    while retries > 0:
        try:
            # Daftra API request to get order details
            daftra_request_url = f"{API_URL_ORDERS}?filter[branch_id]={branch_id}&filter[id]={order_id}"
            daftra_response = requests.get(daftra_request_url, headers=daftra_headers)
            daftra_response.raise_for_status()

            data = daftra_response.json().get("data", [])
            if not data:
                print(f"No data found for Order ID {order_id} in Daftra response.")
                return None

            # Extract the 'no' field from the Daftra response
            no_field = data[0].get("no")
            if not no_field:
                print(f"No 'no' field found for Order ID {order_id}.")
                return None

            # Zid API request to fetch currency code
            zid_request_url = API_URL_ORDERS_ZID.format(order_id=no_field)
            zid_response = requests.get(zid_request_url, headers=zid_headers)
            zid_response.raise_for_status()

            currency_code = zid_response.json().get("order", {}).get("currency_code")
            return currency_code

        except requests.RequestException as e:
            retries -= 1
            print(f"Error fetching currency code for Order ID {order_id}: {e}")
            if retries > 0:
                wait_time = BACKOFF_FACTOR * (2 ** (MAX_RETRIES - retries))
                print(f"Retrying... {retries} attempts left. Waiting {wait_time} seconds.")
                time.sleep(wait_time)
            else:
                print(f"All retries exhausted for Order ID {order_id}.")
                return None



# Function to fetch with retries
def fetch_with_retries(url, headers, retries=MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                time.sleep(BACKOFF_FACTOR * (2 ** attempt))
            else:
                raise


# Function to update transactions
def update_transactions():
    filtered_results = get_filtered_results()

    for product in filtered_results:
        transactions = product.get("transactions", [])

        for transaction in transactions:
            transaction_id = str(transaction.get("id"))
            order_id = transaction.get("order_id")
            branch_id = transaction.get("branch_id")

            if transaction.get("currency_updated"):
                print(f"Transaction {transaction_id}: Already processed, skipping.")
                continue

            if not order_id or not branch_id:
                print(f"Skipping transaction {transaction_id}: Missing order_id or branch_id.")
                continue

            currency_code = fetch_currency_code(order_id, branch_id,transaction_id)

            transaction["currency_code"] = currency_code
            transaction["currency_updated"] = True

            result = collection.update_one(
                {"_id": product["_id"], "transactions.id": transaction_id},
                {
                    "$set": {
                        "transactions.$.currency_code": currency_code,
                        "transactions.$.currency_updated": True
                    }
                }
            )

            if result.modified_count:
                print(f"Transaction {transaction_id} updated successfully with currency_code '{currency_code}'.")
            else:
                print(f"No updates made for Transaction {transaction_id}.")


# Function to update sync timestamp
def update_sync_timestamp(sync_type):
    current_timestamp = datetime.utcnow()
    try:
        update_field = 'startAt' if sync_type == 'start' else 'finishAt'
        db['update'].update_one({}, {"$set": {update_field: current_timestamp}}, upsert=True)
        print(f"Sync {sync_type} timestamp {current_timestamp.isoformat()} stored/updated")
    except Exception as error:
        print(f"Error storing {sync_type} timestamp: {error}")


# Function to fetch and store product data
def fetch_and_store_data():
    for product_id in TARGET_PRODUCT_IDS:
        print(f"Fetching product data for ID {product_id}...")
        product_url = f"{API_URL_PRODUCTS}/{product_id}.json"
        product_data = fetch_with_retries(product_url, daftra_headers)

        if product_data and "data" in product_data:
            product = product_data["data"]["Product"]

            print(f"Fetching transactions for product ID {product_id}...")
            all_transactions = []
            page_number = 1

            while True:
                transaction_url = f"{API_URL_TRANSACTIONS}?product_id={product_id}&page={page_number}"
                transaction_data = fetch_with_retries(transaction_url, daftra_headers)

                if transaction_data and "data" in transaction_data:
                    transactions = [record["StockTransaction"] for record in transaction_data["data"]]
                    all_transactions.extend(transactions)

                    if transaction_data.get("pagination", {}).get("next"):
                        page_number += 1
                    else:
                        break
                else:
                    break

            # Fetch the existing product document from the database
            existing_product = collection.find_one({"id": product_id})
            existing_transactions = existing_product.get("transactions", []) if existing_product else []

            # Create a dictionary of existing transactions for quick lookup by transaction ID
            existing_transactions_dict = {str(t["id"]): t for t in existing_transactions}

            # Merge transactions: Retain old ones with currency_updated=True, add new ones
            merged_transactions = []

            for transaction in all_transactions:
                transaction_id = str(transaction["id"])
                if transaction_id in existing_transactions_dict:
                    # Keep the existing transaction if currency_updated=True
                    if existing_transactions_dict[transaction_id].get("currency_updated", False):
                        merged_transactions.append(existing_transactions_dict[transaction_id])
                    else:
                        merged_transactions.append(transaction)
                else:
                    # New transaction, add to the list
                    merged_transactions.append(transaction)

            # Update the product document with merged transactions
            product["transactions"] = merged_transactions

            collection.update_one({"id": product["id"]}, {"$set": product}, upsert=True)
            print(f"Product {product_id} data updated with merged transactions.")



# Scheduler function
def scheduled_job():
    print("\n--- Scheduled job started ---")
    update_sync_timestamp('start')
    fetch_and_store_data()
    update_transactions()
    update_sync_timestamp('finish')
    print("--- Scheduled job completed ---\n")


# Schedule the job every hour
schedule.every(1).hour.do(scheduled_job)

# Keep the script running
if __name__ == "__main__":
    print("Scheduler started. Press Ctrl+C to stop.")
    scheduled_job()  # Run immediately on start
    while True:
        schedule.run_pending()
        time.sleep(1)

