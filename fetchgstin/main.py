import json
import os
from pymongo import MongoClient
import time
import logging
import subprocess

# Configure logging
log_file = os.path.join('fetchgstin', 'fetch_ids.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file),   # Log to a file
                        logging.StreamHandler()          # Also log to the console
                    ])

# MongoDB connection string and database setup
client = MongoClient('mongodb://airlinedb_user:8649OV57IGR3Y1JS@ec2-43-204-135-11.ap-south-1.compute.amazonaws.com/admin?directConnection=true&serverSelectionTimeoutMS=20000&appName=mongosh+2.2.3')
db = client["gstservice"]

# Define collections
collection1 = db["GST_VENDOR_ALL_DATA"]
collection2 = db["gst-vendor-master"]
collection3 = db["missing_gstins"]

def fetch_ids():
    try:
        # Fetch IDs from collection1
        ids_collection1 = set(doc['_id'] for doc in collection1.find({}, {'_id': 1}))
        
        # Fetch IDs from collection2 and collection3
        ids_collection2 = set(doc['_id'] for doc in collection2.find({}, {'_id': 1}))
        ids_collection3 = set(doc['_id'] for doc in collection3.find({}, {'_id': 1}))
        
        # Find IDs present in collection2 or collection3 but not in collection1
        ids_in_collection2_not_in_collection1 = list(ids_collection2 - ids_collection1)
        ids_in_collection3_not_in_collection1 = list(ids_collection3 - ids_collection1)

        # Combine the lists
        combined_ids = ids_in_collection2_not_in_collection1 + ids_in_collection3_not_in_collection1
        
        if combined_ids:
            logging.info("Combined IDs present in collection2 or collection3 but not in collection1:")
            logging.info(combined_ids)

            logging.info("Total count of IDs in collection2 or collection3 not in collection1: %d", len(combined_ids))

            # Define paths for ids.json and scrapedata.py within the fetchgstin folder
            ids_file_path = os.path.join('fetchgstin', 'ids.json')
            scrapedata_script_path = os.path.join('fetchgstin', 'scrapedata.py')

            # Write combined IDs to the JSON file
            with open(ids_file_path, 'w') as f:
                json.dump(combined_ids, f)
            
            # Run subprocess with the filename as an argument
            subprocess.run(['python', scrapedata_script_path, ids_file_path], check=True)

            # Clear the JSON file after processing
            os.remove(ids_file_path)
        else:
            logging.info("No new IDs found in collection2 or collection3.")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def main():
    while True:
        fetch_ids()
        time.sleep(60)

if __name__ == "__main__":
    main()
