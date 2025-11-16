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
    """Google Sheets サービスを初期化"""
    credentials_json = os.environ.get('GOOGLE_SHEETS_CREDENTIALS')
    credentials_dict = json.loads(credentials_json)
    
    credentials = Credentials.from_service_account_info(
        credentials_dict,
        scopes=SCOPES
    )
    return build('sheets', 'v4', credentials=credentials)

def fetch_property_names(service, spreadsheet_id, input_range):
    """スプレッドシートから物件名を取得"""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=input_range
        ).execute()
        values = result.get('values', [])
        return [row[0] for row in values if row]
    except Exception as e:
        print(f"Error fetching property names: {e}")
        return []

def search_building_id(property_name):
    """Ajax Search で buildingid を取得"""
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
            print(f"  ❌ 403 Forbidden")
        else:
            print(f"  ❌ HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None

def write_results_to_sh
