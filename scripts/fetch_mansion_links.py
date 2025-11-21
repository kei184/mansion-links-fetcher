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

def find_yahoo_url_and_flag(data):
    """JSON構造のどこからでも Yahoo新築マンション dtlurl と sold_flag を探す"""
    def deep_search(obj):
        if isinstance(obj, dict):
            if obj.get('dtlurl', '').startswith("https://realestate.yahoo.co.jp/new/mansion/dtl/"):
                return obj['dtlurl'], str(obj.get('sold_flag', ''))
            for v in obj.values():
                url, flag = deep_search(v)
                if url:
                    return url, flag
        elif isinstance(obj, list):
            for item in obj:
                url, flag = deep_search(item)
                if url:
                    return url, flag
        elif isinstance(obj, str):
            if obj.startswith("https://realestate.yahoo.co.jp/new/mansion/dtl/"):
                return obj, ''
        return None, None
    return deep_search(data)

def fetch_ad_info(building_id):
    """Ajax JSON から広告情報を取得"""
    try:
        json_url = f"https://www.e-mansion.co.jp/bbs/yre/building/{building_id}/ajaxJson/"
        time.sleep(1)
        response = requests.get(json_url, timeout=10, headers=HEADERS)
        response.raise_for_status()
        data = response.json()
        
        ad_info = {
            'p_dtlurl': '',
            'p_sold_flag': '',
            'l_url': '',
            'l_sold_flag': '',
            'y_dtlurl': '',
            'y_sold_flag': ''
        }
        
        if 'result' in data:
            result_data = data['result']
            
            # 純広告（p）
            if 'p' in result_data and result_data['p']:
                p = result_data['p']
                ad_info['p_dtlurl'] = str(p.get('dtlurl', ''))
                ad_info['p_sold_flag'] = str(p.get('sold_flag', ''))
                print(f"    P広告取得: {ad_info['p_dtlurl'][:60] if ad_info['p_dtlurl'] else 'なし'}...")
            
            # L広告（l）- URL を構築
            if 'l' in result_data and result_data['l']:
                l = result_data['l']
                project_cd = l.get('project_cd', '')
                if project_cd:
                    ad_info['l_url'] = f"https://www.homes.co.jp/mansion/b-{project_cd}/?cmp_id=001_08359_0008683659&utm_campaign=v6_sumulab&utm_content=001_08359_0008683659&utm_medium=cpa&utm_source=sumulab&utm_term="
                    print(f"    L広告取得: project_cd={project_cd}")
                ad_info['l_sold_flag'] = str(l.get('sold_flag', ''))
        
        # Y広告 - パターン網羅的に探索
        y_url, y_flag = find_yahoo_url_and_flag(data)
        if y_url:
            ad_info['y_dtlurl'] = y_url
            ad_info['y_sold_flag'] = y_flag
            print(f"    Y広告取得: {y_url[:60]}... (sold_flag: {y_flag})")
        else:
            print(f"    Y広告なし")
        
        return ad_info
    except Exception as e:
        print(f"  Error fetching ad info: {e}")
        return None

def main():
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    input_range = os.environ.get('INPUT_RANGE', 'Sheet1!A2:A')
    
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID is not set")
    
    service = get_sheets_service()
    property_names = fetch_property_names(service, spreadsheet_id, input_range)
    print(f"Found {len(property_names)} properties to process\n")
    
    # L列用データ（Building ID）
    l_data = [['Building ID']]
    
    # M～R列用データ（広告情報）
    m_data = [['p_dtlurl', 'p_sold_flag', 'l_url', 'l_sold_flag', 'y_dtlurl', 'y_sold_flag']]
    
    for i, property_name in enumerate(property_names, 1):
        print(f"[{i}/{len(property_names)}] {property_name}", end=" -> ")
        building_id = search_building_id(property_name)
        
        if building_id:
            print(f"ID: {building_id}")
            ad_info = fetch_ad_info(building_id)
            
            if ad_info:
                # L列に追加
                l_data.append([str(building_id)])
                
                # M～R列に追加
                m_row = [
                    ad_info.get('p_dtlurl', ''),
                    ad_info.get('p_sold_flag', ''),
                    ad_info.get('l_url', ''),
                    ad_info.get('l_sold_flag', ''),
                    ad_info.get('y_dtlurl', ''),
                    ad_info.get('y_sold_flag', '')
                ]
                m_data.append(m_row)
                print(f"    データ追加: P={bool(m_row[0])}, L={bool(m_row[2])}, Y={bool(m_row[4])}")
            else:
                l_data.append([str(building_id)])
                m_data.append(['', '', '', '', '', ''])
        else:
            print(f"Not found")
            l_data.append([''])
            m_data.append(['', '', '', '', '', ''])
    
    print(f"\nTotal L data rows: {len(l_data)}")
    print(f"Total M data rows: {len(m_data)}")
    print(f"\nSample M data (first 3 rows):")
    for idx, row in enumerate(m_data[:3]):
        print(f"  Row {idx}: {row}")
    
    # L列に書き込み
    try:
        body = {'values': l_data}
        result_l = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range='新着物件!L1', valueInputOption='RAW', body=body).execute()
        print(f"\nSuccessfully wrote {result_l.get('updatedRows')} Building IDs to L1:L")
    except Exception as e:
        print(f"Error writing L column: {e}")
        return
    
    # M～R列に書き込み
    try:
        body = {'values': m_data}
        result_m = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range='新着物件!M1', valueInputOption='RAW', body=body).execute()
        print(f"Successfully wrote {result_m.get('updatedRows')} AD infos to M1:R ({result_m.get('updatedColumns')} columns)")
    except Exception as e:
        print(f"Error writing M:R columns: {e}")
        return
    
    print("Process completed!")

if __name__ == '__main__':
    main()
