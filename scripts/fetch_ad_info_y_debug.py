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
        return [row[0] if row else '' for row in values]
    except Exception as e:
        print(f"Error fetching property names: {e}")
        return []

def search_building_id(property_name):
    try:
        if not property_name:
            return None
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
        
        print(f"    DEBUG: Top-level keys: {list(data.keys())}")
        
        ad_info = {
            'p_dtlurl': '',
            'p_sold_flag': '',
            'l_url': '',
            'l_sold_flag': '',
            'y_dtlurl': '',
            'y_sold_flag': ''
        }
        
        # result キーの中を確認
        if 'result' in data:
            result_data = data['result']
            print(f"    DEBUG: result keys: {list(result_data.keys()) if isinstance(result_data, dict) else 'Not a dict'}")
            
            if isinstance(result_data, dict):
                # 純広告（p）
                if 'p' in result_data and result_data['p']:
                    p = result_data['p']
                    ad_info['p_dtlurl'] = str(p.get('dtlurl', ''))
                    ad_info['p_sold_flag'] = str(p.get('sold_flag', ''))
                    print(f"      P found: {p}")
                else:
                    print(f"      P not found or empty")
                
                # L広告（l）- URL を構築
                if 'l' in result_data and result_data['l']:
                    l = result_data['l']
                    project_cd = l.get('project_cd', '')
                    if project_cd:
                        ad_info['l_url'] = f"https://www.homes.co.jp/mansion/b-{project_cd}/?cmp_id=001_08359_0009551273&utm_campaign=alliance_sumulab&utm_content=001_08359_0009551273&utm_medium=cpa&utm_source=sumulab&utm_term="
                    ad_info['l_sold_flag'] = str(l.get('sold_flag', ''))
                    print(f"      L found: {l}")
                else:
                    print(f"      L not found or empty")
                
                # Y広告（y）- 直接 URL を取得
                if 'y' in result_data:
                    y = result_data['y']
                    print(f"      Y type: {type(y)}, value: {y}")
                    
                    if isinstance(y, dict) and y:
                        ad_info['y_dtlurl'] = str(y.get('dtlurl', ''))
                        ad_info['y_sold_flag'] = str(y.get('sold_flag', ''))
                        print(f"      Y dict found: {y}")
                    elif isinstance(y, str):
                        # Y がストリングの場合、それを URL として扱う
                        ad_info['y_dtlurl'] = y
                        print(f"      Y string found: {y}")
                    else:
                        print(f"      Y is empty or unexpected type")
                else:
                    print(f"      Y key not found in result")
        
        return ad_info
    except Exception as e:
        print(f"  Error fetching ad info: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    input_range = os.environ.get('INPUT_RANGE', '新着物件!B2:B')
    
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID is not set")
    
    service = get_sheets_service()
    property_names = fetch_property_names(service, spreadsheet_id, input_range)
    print(f"Found {len(property_names)} properties to process\n")
    
    
    # L列(Building ID)の既存データを取得
    l_column_range = '新着物件!L2:L'
    existing_ids = []
    try:
        result_l = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=l_column_range).execute()
        existing_l_values = result_l.get('values', [])
        existing_ids = [row[0] if row else '' for row in existing_l_values]
        print(f"Found {len(existing_ids)} existing IDs in L column")
    except Exception as e:
        print(f"Error fetching L column: {e}")
        pass

    # L列用データ（Building ID）
    l_data = [['Building ID']]
    
    # M～R列用データ（広告情報）
    m_data = [['p_dtlurl', 'p_sold_flag', 'l_url', 'l_sold_flag', 'y_dtlurl', 'y_sold_flag']]
    
    # 最初の3件だけ処理（デバッグ用）
    for i, property_name in enumerate(property_names[:3], 1):
        print(f"[{i}] {property_name}")
        
        building_id = None
        # 既存のIDを確認（インデックス調整）
        if i-1 < len(existing_ids):
            existing_id = existing_ids[i-1].strip()
            if existing_id:
                building_id = existing_id
                print(f"  Existing ID: {building_id}")
        
        # 既存IDがなければ検索
        if not building_id:
            building_id = search_building_id(property_name)
        
        if building_id:
            print(f"  ID: {building_id}")
            ad_info = fetch_ad_info(building_id)
            
            if ad_info:
                l_data.append([str(building_id)])
                m_row = [
                    ad_info.get('p_dtlurl', ''),
                    ad_info.get('p_sold_flag', ''),
                    ad_info.get('l_url', ''),
                    ad_info.get('l_sold_flag', ''),
                    ad_info.get('y_dtlurl', ''),
                    ad_info.get('y_sold_flag', '')
                ]
                m_data.append(m_row)
                print(f"  M row: {m_row}")
        print()
    
    print(f"Total L data rows: {len(l_data)}")
    print(f"Total M data rows: {len(m_data)}")

if __name__ == '__main__':
    main()
