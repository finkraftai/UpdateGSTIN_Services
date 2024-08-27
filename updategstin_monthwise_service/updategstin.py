from datetime import datetime
import psycopg2
import requests
from pymongo import MongoClient
import time

import concurrent.futures
import logging
import psycopg2
import time

import schedule

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
    collection = db["gst-vendor-master"]  # Replace with your MongoDB collection name
    # gstins = collection.find({"info": {"$exists": False}})  # Fetch only the GSTIN field
    gstins = collection.find()  # Fetch only the GSTIN field
    return [gstin["_id"] for gstin in gstins]

# Function to fetch data after successful login
def getGstinfo(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/tp'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken='+authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
 
    payload = {
        'gstin': gstin
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise HTTPError for bad requests
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)


def getPlaceOfBussinessinfo(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/tp/busplaces'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken='+authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
 
    payload = {
        'gstin': gstin
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise HTTPError for bad requests
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)

def einvoiceEnablement(gstin):
    url = 'https://api.prod.portal.irisirp.com/portal/einvoice/status'
    params = {'gstin': gstin}
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'origin': 'https://einvoice6.gst.gov.in',
        'priority': 'u=1, i',
        'referer': 'https://einvoice6.gst.gov.in/',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad requests
        return response.json()['response']
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)

def getFilingHistory(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/taxpayerReturnDetails'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Content-Type': 'application/json',
        'Cookie': 'AuthToken='+authtoken,
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/services/auth/searchtpbypan',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'at': authtoken,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
 
    payload = {
        'gstin': gstin
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raise HTTPError for bad requests
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)


def getLiablityHistory(authtoken, gstin):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/get/getLiabRatio'
    params = {'gstin': gstin}
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'at': authtoken,
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)



def getFilingYears(authtoken, gstin):
    url = f'https://publicservices.gst.gov.in/publicservices/auth/api/dropdownfinyear?gstin={gstin}'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'at': authtoken,
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()["data"]
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)

def getFilingHistory(authtoken, gstin, year):
    url = 'https://publicservices.gst.gov.in/publicservices/auth/api/search/taxpayerReturnDetails'
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Origin': 'https://services.gst.gov.in',
        'Referer': 'https://services.gst.gov.in/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
        'at': authtoken,
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"'
    }
    payload = {
        'gstin': gstin,
        'fy': year
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error: %s", errh)
    except Exception as err:
        print("An error occurred: %s", err)



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



def process_gstin(gstin, token):
    try:
        gstinforesponse = getGstinfo(token, gstin)
        update_gst_vendor_master_in_mongo(gstin, gstinforesponse, 'info')
        pobinforesponse = getPlaceOfBussinessinfo(token, gstin)
        update_gst_vendor_master_in_mongo(gstin, pobinforesponse,'pob')
        enablementResponse = einvoiceEnablement(gstin)
        update_gst_vendor_master_in_mongo(gstin, enablementResponse,'enablement')
        filing_years = getFilingYears(token, gstin)
        
        for year_info in filing_years:
            year = year_info["value"]
            filing_history_response = getFilingHistory(token, gstin, year)
            if filing_history_response:
                update_gst_vendor_master_in_mongo(gstin, filing_history_response, f'filinghistory.{year}')
        
        liabilityHistoryResponse = getLiablityHistory(token, gstin)
        update_gst_vendor_master_in_mongo(gstin, liabilityHistoryResponse, f'liabilityhistory')
    
    except Exception as e:
        logging.error(f"Failed to process GSTIN {gstin}: {e}")


def job():

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
    
    print("gstins", gstins)
    print(len(gstins))

    today = datetime.datetime.today()
    file_path = 'lastrun.txt'

    with open(file_path, 'w') as file:
        file.write(f"Lastrundate: {today}\n")
                

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

def main():
    # Schedule the job to run every day at midnight
    schedule.every().day.at("13:17").do(check_and_run_job)

    while True:
        schedule.run_pending()
        time.sleep(86400)  

def check_and_run_job():
    now = datetime.now()
    if now.day == 1:
        job()

if __name__ == "__main__":
    main()