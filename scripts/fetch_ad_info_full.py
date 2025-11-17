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
            'p_dtlurl': '',
            'p_sold_flag': '',
            'l_project_cd': '',
            'l_sold_flag': '',
            'y_dtlurl': '',
            'y_sold_flag': ''
        }
        
        print(f"    JSON Keys: {list(data.keys())}")
        
        if 'p' in data and data['p']:
            p = data['p']
            ad_info['p_dtlurl'] = str(p.get('dtlurl', ''))
            ad_info['p_sold_flag'] = str(p.get('sold_flag', ''))
            print(f"      Pure Ad: dtlurl={ad_info['p_dtlurl'][:50] if ad_info['p_dtlurl'] else 'None'}, sold_flag={ad_info['p_sold_flag']}")
        
        if 'l' in data and data['l']:
            l = data['l']
            ad_info['l_project_cd'] = str(l.get('project_cd', ''))
            ad_info['l_sold_flag'] = str(l.get('sold_flag', ''))
            print(f"      L Ad: project_cd={ad_info['l_project_cd']}, sold_flag={ad_info['l_sold_flag']}")
        
        if 'y' in data and data['y']:
            y = data['y']
            ad_info['y_dtlurl'] = str(y.get('dtlurl', ''))
            ad_info['y_sold_flag'] = str(y.get('sold_flag', ''))
            print(f"      Y Ad: dtlurl={ad_info['y_dtlurl'][:50] if ad_info['y_dtlurl'] else 'None'}, sold_flag={ad_info['y_sold_flag']}")
        
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
    m_data = [['p_dtlurl', 'p_sold_flag', 'l_project_cd', 'l_sold_flag', 'y_dtlurl', 'y_sold_flag']]
    
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
                    ad_info.get('l_project_cd', ''),
                    ad_info.get('l_sold_flag', ''),
                    ad_info.get('y_dtlurl', ''),
                    ad_info.get('y_sold_flag', '')
                ]
                m_data.append(m_row)
                print(f"      Added to m_data: {m_row}")
            else:
                print(f"      Failed to fetch ad info")
                l_data.append([str(building_id)])
                m_data.append(['', '', '', '', '', ''])
        else:
            print(f"Not found")
            l_data.append([''])
            m_data.append(['', '', '', '', '', ''])
    
    print(f"\nTotal L data rows: {len(l_data)}")
    print(f"Total M data rows: {len(m_data)}")
    print(f"Sample L data (first 3 rows): {l_data[:3]}")
    print(f"Sample M data (first 3 rows): {m_data[:3]}")
    
    # L列に書き込み
    try:
        body = {'values': l_data}
        result_l = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range='新着物件!L1', valueInputOption='RAW', body=body).execute()
        print(f"\nSuccessfully wrote {len(l_data)-1} Building IDs to L1:L")
        print(f"  Update result: {result_l.get('updatedRows')} rows, {result_l.get('updatedColumns')} columns")
    except Exception as e:
        print(f"Error writing L column: {e}")
        return
    
    # M～R列に書き込み
    try:
        body = {'values': m_data}
        result_m = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range='新着物件!M1', valueInputOption='RAW', body=body).execute()
        print(f"Successfully wrote {len(m_data)-1} AD infos to M1:R")
        print(f"  Update result: {result_m.get('updatedRows')} rows, {result_m.get('updatedColumns')} columns")
    except Exception as e:
        print(f"Error writing M:R columns: {e}")
        return
    
    print("Process completed!")

if __name__ == '__main__':
    main()
