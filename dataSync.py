import time
import schedule
import logging
import requests
from datetime import datetime
from pymongo import MongoClient
import pytz  # For handling time zones

# MongoDB connection setup
MONGO_URI = "mongodb://amin:Lemure17@3.0.158.189:27017/"  # Replace with your MongoDB URI
DB_NAME = "Daftra"  # Replace with your database name
COLLECTION_NAME = "statistics"

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("activity.log"),
        logging.StreamHandler()
    ]
)

# Riyadh timezone
RIYADH_TZ = pytz.timezone('Asia/Riyadh')

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    logging.info("Connected to MongoDB")
except Exception as e:
    logging.error("Failed to connect to MongoDB", exc_info=True)
    raise

# Function to fetch data for a specific product
def fetch_all_data_for_product(product_id, from_date, to_date):
    api_url = "https://amamamed.daftra.com/v2/api/entity/invoice_item/list/1"
    api_key = "70d7582b80ee4c5855daaed6872460519c0a528c"
    all_data = []
    current_page = 1

    try:
        while True:
            params = {
                "page": current_page,
                "filter[product_id]": product_id,
                "filter[invoice.date][from]": from_date,
                "filter[invoice.date][to]": to_date,
            }
            headers = {"APIKEY": api_key}

            response = requests.get(api_url, params=params, headers=headers)
            response.raise_for_status()

            data = response.json()
            all_data.extend(data["data"])

            # Log progress
            logging.info(f"Fetched page {current_page} for product_id {product_id}")

            # Break if we have reached the last page
            if current_page >= data["last_page"]:
                break

            current_page += 1

        logging.info(f"Completed fetching data for product_id {product_id}")
        return all_data
    except requests.RequestException as e:
        logging.error(f"Error fetching data for product_id {product_id}: {e}", exc_info=True)
        raise

# Function to calculate monthly totals
def calculate_monthly_totals(all_data):
    monthly_data = {}

    for item in all_data:
        # Extract the month and year from the item's created date
        date = datetime.strptime(item["created"], "%Y-%m-%d %H:%M:%S")
        month_year = f"{date.year}-{str(date.month).zfill(2)}"

        # Initialize the month group if not present
        if month_year not in monthly_data:
            monthly_data[month_year] = {"totalQuantity": 0, "totalSubtotal": 0, "totalWithoutVat": 0}

        effective_unit_price = item["unit_price"] - (item.get("calculated_discount", 0))
        monthly_data[month_year]["totalQuantity"] += item["quantity"]
        monthly_data[month_year]["totalSubtotal"] += item["subtotal"]
        monthly_data[month_year]["totalWithoutVat"] += effective_unit_price * item["quantity"]

    # Convert to a list for easier handling
    return [{"month": month, **totals} for month, totals in monthly_data.items()]

# Scheduler function
def scheduled_job():
    logging.info("\n--- Scheduled job started ---")
    today = datetime.now().strftime("%Y-%m-%d")
    product_data = [
        {"productId": "1056856", "fromDate": "2024-11-29"},
        {"productId": "1056857", "fromDate": "2025-01-07"},
        {"productId": "1058162", "fromDate": "2024-12-25"},
        {"productId": "1058530", "fromDate": "2024-12-05"},
        {"productId": "1058627", "fromDate": "2024-12-05"},
        {"productId": "1058711", "fromDate": "2024-11-29"},
        {"productId": "1065759", "fromDate": "2024-12-25"},
    ]

    try:
        # Fetch data for each product
        for product in product_data:
            product_id = product["productId"]
            from_date = product["fromDate"]

            logging.info(f"Starting data fetch for product_id {product_id}")

            all_data = fetch_all_data_for_product(product_id, from_date, today)

            # Extract name and barcode from the first item
            product_name = all_data[0]["product"]["name"] if all_data else None
            product_barcode = all_data[0]["product"]["barcode"] if all_data else None

            # Calculate totals
            totals = {
                "totalQuantity": sum(item["quantity"] for item in all_data),
                "totalSubtotal": sum(item["subtotal"] for item in all_data),
                "totalWithoutVat": sum(
                    (item["unit_price"] - item.get("calculated_discount", 0)) * item["quantity"]
                    for item in all_data
                ),
            }

            # Calculate monthly totals
            monthly_totals = calculate_monthly_totals(all_data)

            # Add the last updated timestamp in Riyadh timezone
            updated_at = datetime.now(RIYADH_TZ).strftime('%Y-%m-%d %H:%M:%S')

            # Prepare the document
            document = {
                "product_id": product_id,
                "name": product_name,
                "barcode": product_barcode,
                "totals": totals,
                "monthlyData": monthly_totals,
                "updatedAt": updated_at
            }

            # Check if the document exists; update if it does, insert if it doesn't
            collection.update_one({"product_id": product_id}, {"$set": document}, upsert=True)

            logging.info(f"Data processing completed for product_id {product_id}")

        logging.info("All data saved/updated in MongoDB successfully!")
    except Exception as e:
        logging.error("An error occurred during execution", exc_info=True)

    logging.info("--- Scheduled job completed ---\n")

# Schedule the job every two hours
schedule.every(2).hours.do(scheduled_job)

# Keep the script running
if __name__ == "__main__":
    logging.info("Scheduler started. Press Ctrl+C to stop.")
    scheduled_job()  # Run immediately on start
    while True:
        schedule.run_pending()
        time.sleep(1)
