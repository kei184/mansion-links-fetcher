import os
import json
import requests
import time
from urllib.parse import quote
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# 共通ヘッダー
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
        
        # リクエスト前に待機（レート制限対策）
        time.sleep(1)
        
        response = requests.get(search_url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        if data.get('building') and len(data['building']) > 0:
            return data['building'][0]['buildingid']
        return None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Warning: 403 Forbidden for {property_name} - API may be blocking requests")
        else:
            print(f"Error searching for {property_name}: {e}")
        return None
    except Exception as e:
        print(f"Error searching for {property_name}: {e}")
        return None

def fetch_ad_url(building_id):
    """Ajax JSON から広告URL を取得（優先順位: 純広告 > L広告 > Y広告）"""
    try:
        json_url = f"https://www.e-mansion.co.jp/bbs/yre/building/{building_id}/ajaxJson/"
        
        # リクエスト前に待機
        time.sleep(1)
        
        response = requests.get(json_url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        # 1. 純広告（優先度最高）
        if 'p' in data:
            p = data['p']
            if p.get('sold_flag') == 0:
                return p.get('dtlurl'), 'pure'
        
        # 2. L広告
        if 'l' in data:
            l = data['l']
            if l.get('sold_flag') == 0:
                project_cd = l.get('project_cd')
                ad_url = f"https://www.homes.co.jp/mansion/b-{project_cd}/?cmp_id=001_08359_0008683659&utm_campaign=v6_sumulab&utm_content=001_08359_0008683659&utm_medium=cpa&utm_source=sumulab&utm_term="
                return ad_url, 'L'
        
        # 3. Y広告
        if 'y' in data:
            y = data['y']
            if y.get('sold_flag') == 0:
                y_url = y.get('dtlurl')
                ad_url = f"{y_url}?sc_out=mikle_mansion_official"
                return ad_url, 'Y'
        
        return None, None
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Warning: 403 Forbidden for building {building_id}")
        else:
            print(f"Error fetching ad URL for building {building_id}: {e}")
        return None, None
    except Exception as e:
        print(f"Error fetching ad URL for building {building_id}: {e}")
        return None, None

def write_results_to_sheets(service, spreadsheet_id, output_range, results):
    """結果をスプレッドシートに書き込み"""
    try:
        data = [['物件名', 'Building ID', 'URL', '広告タイプ', '取得日時']]
        
        from datetime import datetime
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for result in results:
            row = [
                result['property_name'],
                result.get('building_id', ''),
                result.get('ad_url', ''),
                result.get('ad_type', ''),
                now
            ]
            data.append(row)
        
        body = {'values': data}
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=output_range,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Successfully wrote {len(results)} results to spreadsheet")
    except Exception as e:
        print(f"Error writing to spreadsheet: {e}")

def main():
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    input_range = os.environ.get('INPUT_RANGE', 'Sheet1!A2:A')
    output_range = os.environ.get('OUTPUT_RANGE', 'Sheet2!A1')
    
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID is not set")
    
    service = get_sheets_service()
    property_names = fetch_property_names(service, spreadsheet_id, input_range)
    print(f"Found {len(property_names)} properties to process")
    
    results = []
    for i, property_name in enumerate(property_names, 1):
        print(f"[{i}/{len(property_names)}] Processing: {property_name}")
        
        building_id = search_building_id(property_name)
        
        if building_id:
            ad_url, ad_type = fetch_ad_url(building_id)
            result = {
                'property_name': property_name,
                'building_id': building_id,
                'ad_url': ad_url,
                'ad_type': ad_type
            }
        else:
            result = {
                'property_name': property_name,
                'building_id': None,
                'ad_url': None,
                'ad_type': None
            }
        
        results.append(result)
        print(f"  → URL: {result.get('ad_url', 'Not found')}")
    
    write_results_to_sheets(service, spreadsheet_id, output_range, results)
    print("Process completed!")

if __name__ == '__main__':
    main()
