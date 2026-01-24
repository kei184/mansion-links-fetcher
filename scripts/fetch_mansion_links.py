import os
import json
import requests
import time
from urllib.parse import quote
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from datetime import datetime

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
        
        ad_info = {
            'p_dtlurl': '',
            'p_sold_flag': '',
            'l_url': '',
            'l_sold_flag': '',
            'y_dtlurl': '',
            'y_sold_flag': ''
        }
        
        if 'result' in data and data['result'] is not None:
            result_data = data['result']
            
            # 純広告（P） - result.p キー
            if 'p' in result_data and isinstance(result_data['p'], dict) and result_data['p']:
                p = result_data['p']
                ad_info['p_dtlurl'] = str(p.get('dtlurl') or '')
                ad_info['p_sold_flag'] = str(p.get('sold_flag') or '')
            
            # L広告（L） - result.l キー
            if 'l' in result_data and isinstance(result_data['l'], dict) and result_data['l']:
                l = result_data['l']
                project_cd = l.get('project_cd', '')
                if project_cd:
                    ad_info['l_url'] = f"https://www.homes.co.jp/mansion/b-{project_cd}/?cmp_id=001_08359_0009551273&utm_campaign=alliance_sumulab&utm_content=001_08359_0009551273&utm_medium=cpa&utm_source=sumulab&utm_term="
                ad_info['l_sold_flag'] = str(l.get('sold_flag') or '')
            
            
            # Y広告の処理
            # まずsold_flagを取得
            y_sold_flag = ''
            # 1. ynew キーを確認
            if 'ynew' in result_data and isinstance(result_data['ynew'], dict):
                y_sold_flag = str(result_data['ynew'].get('sold_flag') or '')
            # 2. a キーを確認
            if not y_sold_flag and 'a' in result_data and isinstance(result_data['a'], dict):
                y_sold_flag = str(result_data['a'].get('sold_flag') or '')
            # 3. result 直下を確認
            if not y_sold_flag and 'sold_flag' in result_data:
                # Noneが'None'にならないように処理
                flag_value = result_data.get('sold_flag')
                if flag_value is not None:
                    y_sold_flag = str(flag_value)
            
            # dtlurlを取得（複数の場所を確認）
            dtlurl = ''
            # 1. ynew キー内の dtlurl を確認
            if 'ynew' in result_data and isinstance(result_data['ynew'], dict):
                dtlurl = result_data['ynew'].get('dtlurl', '')
            # 2. a キー内の dtlurl を確認
            if not dtlurl and 'a' in result_data and isinstance(result_data['a'], dict):
                dtlurl = result_data['a'].get('dtlurl', '')
            # 3. result 直下の dtlurl を確認
            if not dtlurl and 'dtlurl' in result_data:
                dtlurl = result_data.get('dtlurl', '')
            
            # dtlurlが取得できた場合、パラメータを追加
            if dtlurl and dtlurl.startswith('https://realestate.yahoo.co.jp/new/mansion/dtl/'):
                # パラメータ二重付加しないようにガード
                if 'sc_out=mikle_mansion_official' not in dtlurl:
                    if '?' in dtlurl:
                        dtlurl += '&sc_out=mikle_mansion_official'
                    else:
                        dtlurl += '?sc_out=mikle_mansion_official'
                ad_info['y_dtlurl'] = dtlurl
            
            # sold_flagが取得できた場合のみ設定
            if y_sold_flag:
                ad_info['y_sold_flag'] = y_sold_flag








        
        return ad_info
    except Exception as e:
        print(f"  Error fetching ad info: {e}")
        return None

def main():
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    input_range = os.environ.get('INPUT_RANGE', '新着物件!B2:B')
    
    if not spreadsheet_id:
        raise ValueError("SPREADSHEET_ID is not set")
    
    service = get_sheets_service()
    property_names = fetch_property_names(service, spreadsheet_id, input_range)
    print(f"Found {len(property_names)} properties to process\n")
    
    # L列とS列、M列、O列、Q列、B列の既存データを取得
    l_column_range = '新着物件!L2:L'
    s_column_range = '新着物件!S2:S'
    m_column_range = '新着物件!M2:M'  # p_dtlurl
    o_column_range = '新着物件!O2:O'  # l_url
    q_column_range = '新着物件!Q2:Q'  # y_dtlurl
    b_column_range = '新着物件!B2:B'  # 物件名
    
    date_map = {}  # {building_id: date}
    url_map = {}   # {building_id: {'p_dtlurl': '', 'l_url': '', 'y_dtlurl': ''}}
    property_building_map = {}  # {property_name: building_id} - 物件名とBuilding IDの対応
    
    try:
        result_l = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=l_column_range).execute()
        existing_l_values = result_l.get('values', [])
        
        result_s = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=s_column_range).execute()
        existing_s_values = result_s.get('values', [])
        
        result_m = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=m_column_range).execute()
        existing_m_values = result_m.get('values', [])
        
        result_o = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=o_column_range).execute()
        existing_o_values = result_o.get('values', [])
        
        result_q = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=q_column_range).execute()
        existing_q_values = result_q.get('values', [])
        
        result_b = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=b_column_range).execute()
        existing_b_values = result_b.get('values', [])
        
        # Building IDと日付、URL、物件名をマッピング
        max_rows = max(len(existing_l_values), len(existing_s_values), len(existing_m_values), len(existing_o_values), len(existing_q_values), len(existing_b_values))
        for i in range(max_rows):
            building_id = existing_l_values[i][0].strip() if i < len(existing_l_values) and existing_l_values[i] else ''
            date_value = existing_s_values[i][0].strip() if i < len(existing_s_values) and existing_s_values[i] else ''
            p_url = existing_m_values[i][0].strip() if i < len(existing_m_values) and existing_m_values[i] else ''
            l_url = existing_o_values[i][0].strip() if i < len(existing_o_values) and existing_o_values[i] else ''
            y_url = existing_q_values[i][0].strip() if i < len(existing_q_values) and existing_q_values[i] else ''
            property_name = existing_b_values[i][0].strip() if i < len(existing_b_values) and existing_b_values[i] else ''
            
            if building_id:
                if date_value:
                    date_map[building_id] = date_value
                url_map[building_id] = {
                    'p_dtlurl': p_url,
                    'l_url': l_url,
                    'y_dtlurl': y_url
                }
                # 物件名とBuilding IDの対応を記録
                if property_name:
                    property_building_map[property_name] = building_id
        
        print(f"Created date mapping for {len(date_map)} Building IDs")
        print(f"Created URL mapping for {len(url_map)} Building IDs")
        print(f"Created property-building mapping for {len(property_building_map)} properties")
    except Exception as e:
        print(f"Error fetching existing data: {e}")
        pass

    # L列用データ（Building ID）
    l_data = [['Building ID']]
    
    # M～S列用データ（広告情報 + 日付）
    m_data = [['p_dtlurl', 'p_sold_flag', 'l_url', 'l_sold_flag', 'y_dtlurl', 'y_sold_flag', 'first_sold_out_date']]
    
    today_str = datetime.now().strftime('%Y/%m/%d')

    for i, property_name in enumerate(property_names, 1):
        print(f"[{i}/{len(property_names)}] {property_name}", end=" -> ")
        
        # 既存のBuilding IDがあればそれを使用、なければ検索
        building_id = property_building_map.get(property_name)
        if building_id:
            print(f"ID: {building_id} (cached)")
        else:
            building_id = search_building_id(property_name)

        if building_id:
            ad_info = fetch_ad_info(building_id)
            
            # Building IDから既存の日付とURLを取得
            current_date = date_map.get(str(building_id), '')
            existing_urls = url_map.get(str(building_id), {'p_dtlurl': '', 'l_url': '', 'y_dtlurl': ''})
            
            if ad_info:
                # L列に追加
                l_data.append([str(building_id)])
                
                p_flag = ad_info.get('p_sold_flag', '')
                l_flag = ad_info.get('l_sold_flag', '')
                y_flag = ad_info.get('y_sold_flag', '')
                
                # URLの決定: 新しいURLがあればそれを使用、なければ既存のURLを保持
                p_url = ad_info.get('p_dtlurl', '') or existing_urls['p_dtlurl']
                l_url = ad_info.get('l_url', '') or existing_urls['l_url']
                y_url = ad_info.get('y_dtlurl', '') or existing_urls['y_dtlurl']
                
                # 売り切れ判定: URLがあり、sold_flagが '0' (掲載中) 以外の値の場合
                # sold_flag='0' → 掲載中、sold_flag='1'等 → 売り切れ
                is_sold_out = False
                if p_url and p_flag and p_flag != '0':
                    is_sold_out = True
                if l_url and l_flag and l_flag != '0':
                    is_sold_out = True
                if y_url and y_flag and y_flag != '0':
                    is_sold_out = True
                
                # 日付の決定: 既存の日付を保持、売り切れが初めて検出されたら今日の日付
                date_to_write = current_date
                if not date_to_write and is_sold_out:
                    date_to_write = today_str

                # M～S列に追加
                m_row = [
                    p_url,
                    p_flag,
                    l_url,
                    l_flag,
                    y_url,
                    y_flag,
                    date_to_write
                ]
                m_data.append(m_row)
            else:
                # 広告情報が取れなかった場合でも既存のURLを保持
                l_data.append([str(building_id)])
                m_data.append([
                    existing_urls['p_dtlurl'],
                    '',
                    existing_urls['l_url'],
                    '',
                    existing_urls['y_dtlurl'],
                    '',
                    current_date
                ])
        else:
            # Building IDが見つからなかった場合
            print(f"Not found")
            l_data.append([''])
            m_data.append(['', '', '', '', '', '', ''])  # 日付も空にする
    
    print(f"\nTotal L data rows: {len(l_data)}")
    print(f"Total M data rows: {len(m_data)}")
    
    # 各広告タイプのカウント
    # 各広告タイプのカウント（URLが存在するものをカウント）
    p_count = sum(1 for row in m_data[1:] if row[0])
    l_count = sum(1 for row in m_data[1:] if row[2])
    y_count = sum(1 for row in m_data[1:] if row[4])
    
    print(f"\n=== 広告データ統計 ===")
    print(f"純広告（P）: {p_count} 件")
    print(f"L広告（L）: {l_count} 件")
    print(f"Yahoo広告（Y）: {y_count} 件")
    
    # L列に書き込み
    try:
        body = {'values': l_data}
        result_l = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='新着物件!L1:L',
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"\n=== L列書き込み結果 ===")
        print(f"Updated rows: {result_l.get('updatedRows')}")
        print(f"Updated range: {result_l.get('updatedRange')}")
    except Exception as e:
        print(f"Error writing L column: {e}")
        return
    
    # M～S列に書き込み
    try:
        body = {'values': m_data}
        result_m = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range='新着物件!M1:S',
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"\n=== M～S列書き込み結果 ===")
        print(f"Updated rows: {result_m.get('updatedRows')}")
        print(f"Updated range: {result_m.get('updatedRange')}")
    except Exception as e:
        print(f"Error writing M:S columns: {e}")
        return
    
    print("\n=== Process completed! ===")

if __name__ == '__main__':
    main()
