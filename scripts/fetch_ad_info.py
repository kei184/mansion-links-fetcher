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
    except Exception as e:
        print(f"  Error: {e}")
        return None

def fetch_ad_info(building_id):
    """Ajax JSON から広告情報を取得"""
    try:
        json_url = f"https://www.e-mansion.co.jp/bbs/yre/building/{building_id}/ajaxJson/"
        time.sleep(1)
        response = requests.get(json_url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        ad_info = {
            'p_dtlurl': None,
            'p_sold_flag': None,
            'l_project_cd': None,
            'l_sold_flag': None,
            'y_dtlurl': None,
            'y_sold_flag': None
        }
        
        # 純広告（p）
        if 'p' in data and data['p']:
            ad_info['p_dtlurl'] = data['p'].get('dtlurl')
            ad_info['p_sold_flag'] = data['p'].get('sold_flag')
        
        # L広告（l）
        if 'l' in data and data['l']:
            ad_info['l_project_cd'] = data['l'].get('project_cd')
            ad_info['l_sold_flag'] = data['l'].get('sold_flag')
        
        # Y広告（y）
        if 'y' in data and data['y']:
            ad_info['y_dtlurl'] = data['y'].get('dtlurl')
            ad_info['y_sold_flag'] = data['y'].get('sold_flag')
        
        return ad_info
    except Exception as e:
        print(f"  Error: {e}")
        return None

def write_results_to_sheets(service, spreadsheet_id, output_range, results):
    try:
        data = [['Building ID', 'p_dtlurl', 'p_sold_flag', 'l_project_cd', 'l_sold_flag', 'y_dtlurl', 'y_sold_flag']]
        for result in results:
            row = [
                result.get('building_id', ''),
                result.get('p_dtlurl', ''),
                result.get('p_sold_flag', ''),
                result.get('l_project_cd', ''),
                result.get('l_sold_flag', ''),
                result.get('y_dtlurl', ''),
                result.get('y_sold_flag', '')
            ]
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
            print(f"ID: {building_id}", end=" -> ")
            ad_info = fetch_ad_info(building_id)
            
            if ad_info:
                print(f"OK")
                result = {
                    'building_id': building_id,
                    'p_dtlurl': ad_info.get('p_dtlurl', ''),
                    'p_sold_flag': ad_info.get('p_sold_flag', ''),
                    'l_project_cd': ad_info.get('l_project_cd', ''),
                    'l_sold_flag': ad_info.get('l_sold_flag', ''),
                    'y_dtlurl': ad_info.get('y_dtlurl', ''),
                    'y_sold_flag': ad_info.get('y_sold_flag', '')
                }
            else:
                print(f"Failed")
                result = {'building_id': building_id}
        else:
            print(f"Not found")
            result = {'building_id': ''}
        
        results.append(result)
    
    write_results_to_sheets(service, spreadsheet_id, output_range, results)
    print("Process completed!")

if __name__ == '__main__':
    main()
