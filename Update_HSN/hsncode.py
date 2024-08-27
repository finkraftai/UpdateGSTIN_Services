import psycopg2
import requests
from pymongo import MongoClient
import time

import concurrent.futures
import logging
import psycopg2
import time

# Configure logging
logging.basicConfig(filename='failed_gstin.log', level=logging.ERROR, 
                    format='%(asctime)s %(levelname)s:%(message)s')


DB_CONFIG = {
    'host': 'ec2-65-0-225-147.ap-south-1.compute.amazonaws.com',
    'port': '5432',
    'dbname': 'gstservice_db',
    'user': 'airlinedb_user',
    'password': 'AVNS_SFBbzpFCBpvhgbI5M1T'
}
MONGO_URI ='mongodb://airlinedb_user:8649OV57IGR3Y1JS@ec2-43-204-135-11.ap-south-1.compute.amazonaws.com/admin?directConnection=true&serverSelectionTimeoutMS=10000&appName=mongosh+2.2.3'
# Function to read token from the database
def get_token(cursor):
    cursor.execute("SELECT token FROM tokens")
    token = cursor.fetchone()
    if token:
        return token[0]
    else:
        return None

# Function to fetch GSTIN from MongoDB collection 'gst_vendor_master'
def fetch_gstin_from_mongo():
    client = MongoClient(MONGO_URI)  # Update with your MongoDB connection URI
    db = client["gstservice"]  # Replace with your MongoDB database name
    collection = db["GST_VENDOR_ALL_DATA"]  # Replace with your MongoDB collection name
    # gstins = collection.find({"info": {"$exists": False}})  # Fetch only the GSTIN field
    gstins = collection.find()  # Fetch only the GSTIN field
    return [gstin["_id"] for gstin in gstins]


def update_gst_vendor_master_in_mongo(gstin, response, key):
    client = MongoClient(MONGO_URI)  # Update with your MongoDB connection URI
    db = client["gstservice"]  # Replace with your MongoDB database name
    collection = db["GST_VENDOR_ALL_DATA"]  # Replace with your MongoDB collection name
    
    # Create a new document with gstin as _id if it doesn't exist, or update the existing document
    collection.update_one(
        {"_id": gstin},
        {"$set": {key: response}},
        upsert=True  # This ensures that if the document does not exist, it will be created
    )
    print("GSTIN updated", gstin)


def get_hsn_info(authtoken, gstin):
    url = 'https://services.gst.gov.in/services/api/search/goodservice'
    params = {
        'gstin': gstin,
    }
    
    # Define headers with authtoken
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Authorization': f'Bearer {authtoken}',  # Use Authorization header with Bearer token
        'Referer': 'https://services.gst.gov.in/services/searchtp',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    
    try:
        response = requests.get(
            url,
            params=params,
            headers=headers
        )
        response.raise_for_status()  # Raise HTTPError for bad requests
        return response.json()  # Return the JSON response
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)
  



def process_gstin(gstin, token):
    try:
        # Fetch and update HSN data
        hsn_response = get_hsn_info(token, gstin)
        if hsn_response:
            update_gst_vendor_master_in_mongo(gstin, hsn_response, 'HSN')
    
    except Exception as e:
        logging.error(f"Failed to process GSTIN {gstin}: {e}")



def main():
    # Connect to PostgreSQL
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Read token from the database
    token = get_token(cursor)
    if not token:
        print("No token found in the database. Exiting...")
        return
    
    print(token)

    # Fetch GSTIN from MongoDB collection 'gst-vendor-master'
    gstins = fetch_gstin_from_mongo()

    # gstins = ['01AAACJ7154M1ZL']
    
    print("gstins",gstins)
    print(len(gstins))

    # Use ThreadPoolExecutor to process GSTINs concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_gstin, gstin, token) for gstin in gstins]

        # Wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # This should not happen since exceptions are handled in process_gstin
                logging.error(f"Exception occurred: {e}")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()