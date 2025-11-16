import os
import json
import requests
import time
from urllib.parse import quote
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_sheets_service():
    credentials_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    credentials_dict = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(credentials_dict, scopes=SCOPES)
    return build('sheets', 'v4', credentials=credentials)

def fetch_property_names(service, spreadsheet_id, input_range):
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=input_range).execute()
        values = result.get('values', [])
        return [row[0] for row in values if row]
    except Exception as e:
        print(f"Error fetching property names: {e}")
        return []

def search_building_id(property_name):
    try:
        search_url = f"https://www.e-mansion.co.jp/bbs/estate/ajaxSearch/?q={quote(property_name)}"
        time.sleep(1)
        response = requests.get(search_url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        if data.get('building') and len(data['building']) > 0:
            return data['building'][0]['buildingid']
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"  403 Forbidden")
        else:
            print(f"  HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"  Error: {e}")
        return None

def write_results_to_sheets(service, spreadsheet_id, output_range, results):
    try:
        data = [['Building ID']]
        for result in results:
            row = [result.get('building_id', '')]
            data.append(row)
        body = {'values': data}
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=output_range, valueInputOption='RAW', body=body).execute()
        print(f"\nSuccessfully wrote {len(results)} results to {output_range}")
    except Exception as e:
        print(f"\nError writing to spreadsheet: {e}")

def main():
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    input_range = os.environ.get('INPUT_RANGE', 'Sheet1!A2:A')
    output_range = os.environ.get('OUTPUT_RANGE', '新着物件!L1')
    
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID is not set")
    
    service = get_sheets_service()
    property_names = fetch_property_names(service, spreadsheet_id, input_range)
    print(f"Found {len(property_names)} properties to process\n")
    
    results = []
    for i, property_name in enumerate(property_names, 1):
        print(f"[{i}/{len(property_names)}] {property_name}", end=" -> ")
        building_id = search_building_id(property_name)
        
        if building_id:
            print(f"OK: {building_id}")
            result = {'property_name': property_name, 'building_id': building_id}
        else:
            print(f"Not found")
            result = {'property_name': property_name, 'building_id': ''}
        
        results.append(result)
    
    write_results_to_sheets(service, spreadsheet_id, output_range, results)
    print("Process completed!")

if __name__ == '__main__':
    main()
