import os
import csv
import sys
import tempfile
import gspread
from prefect import flow, task
from prefect.blocks.system import Secret
from googleads import ad_manager

# --- SYSTEM STABILIZATION ---
os.environ['PYTHONUTF8'] = '1'

# --- TASK 1: THE CORE GOOGLE ADS LOGIC ---
@task(retries=2, retry_delay_seconds=60)
def fetch_and_process(cfg):
    # 1. Pull the JSON key from Prefect Secret Blocks
    secret_name = "oldgamkey" if cfg['label'] == 'OLD GAM' else "newgamkey"
    json_key_data = Secret.load(secret_name).get()

    # 2. Create a temporary file because the googleads library requires a physical file path
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(json_key_data)
        tmp_key_path = tmp.name

    try:
        yaml_config = f"""
ad_manager:
  application_name: "GAM_Dynamic_Shift_Dump"
  network_code: "{cfg['network_code']}"
  path_to_private_key_file: "{tmp_key_path}"
"""
        client = ad_manager.AdManagerClient.LoadFromString(yaml_config)
        inventory_service = client.GetService('InventoryService', version='v202602')

        all_units_raw = []
        page_limit = 1000
        current_offset = 0

        print(f">>> Processing {cfg['label']}...")

        while True:
            query_parts = [f"status = '{cfg['status_filter']}'"] if cfg['status_filter'] else []
            where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""
            query_str = f"{where_clause} ORDER BY id ASC LIMIT {page_limit} OFFSET {current_offset}".strip()

            response = inventory_service.getAdUnitsByStatement({'query': query_str})

            if 'results' in response and len(response['results']) > 0:
                all_units_raw.extend(response['results'])
                current_offset += page_limit
            else:
                break

        unit_map = {u.id: {'name': u.name, 'parentId': getattr(u, 'parentId', None)} for u in all_units_raw}
        processed_data = []

        for unit in all_units_raw:
            path_names, path_ids, curr_id = [], [], unit.id
            while curr_id:
                curr_info = unit_map.get(curr_id)
                if not curr_info: break
                path_names.append(curr_info['name'])
                path_ids.append(str(curr_id))
                curr_id = curr_info['parentId']

            path_names.reverse()
            path_ids.reverse()

            if len(path_ids) != cfg['depth']: continue
            if cfg['target_ids'] and not any(str(pid) in path_ids for pid in cfg['target_ids']): continue

            shifted_names = path_names[cfg['skip_levels']:]
            shifted_ids = path_ids[cfg['skip_levels']:]

            processed_data.append({
                'Source': cfg['label'],
                'Ad unit 1': shifted_names[0] if len(shifted_names) > 0 else "",
                'Ad unit 2': shifted_names[1] if len(shifted_names) > 1 else "",
                'Ad unit 3': shifted_names[2] if len(shifted_names) > 2 else "",
                'Final Ad unit': shifted_names[-1] if len(shifted_names) > 0 else "",
                'Ad unit 1 ID': shifted_ids[0] if len(shifted_ids) > 0 else "",
                'Ad unit 2 ID': shifted_ids[1] if len(shifted_ids) > 1 else "",
                'Ad unit 3 ID': shifted_ids[2] if len(shifted_ids) > 2 else "",
                'Final Ad unit ID': shifted_ids[-1] if len(shifted_ids) > 0 else "",
                'Status': unit.status
            })
        return processed_data
    finally:
        os.remove(tmp_key_path) # Clean up the temp file

# --- TASK 2: UPLOAD TO GOOGLE SHEETS ---
@task
def upload_to_sheets(data, sheet_name):
    # Pull the same JSON key used for New GAM (assuming it has access to Sheets)
    auth_json = Secret.load("newgamkey").get()
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth_json)
        tmp_path = tmp.name
    
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open(sheet_name)
        worksheet = sh.get_worksheet(0)
        
        headers = ['Source', 'Ad unit 1', 'Ad unit 2', 'Ad unit 3', 'Final Ad unit',
                   'Ad unit 1 ID', 'Ad unit 2 ID', 'Ad unit 3 ID', 'Final Ad unit ID', 'Status']
        
        rows = [[row[h] for h in headers] for row in data]
        worksheet.clear()
        worksheet.update('A1', [headers] + rows)
        print(f"Successfully updated Google Sheet: {sheet_name}")
    finally:
        os.remove(tmp_path)

# --- THE MAIN FLOW ---
@flow(log_prints=True)
def run_ad_unit_dump():
    configs = [
        {
            'label': 'OLD GAM', 'network_code': '7176', 'status_filter': 'ACTIVE',
            'target_ids': [23325198618, 23326563038], 'depth': 5, 'skip_levels': 1
        },
        {
            'label': 'New GAM', 'network_code': '23037861279', 'status_filter': None,
            'target_ids': None, 'depth': 6, 'skip_levels': 2
        }
    ]

    final_output = []
    for cfg in configs:
        try:
            data = fetch_and_process(cfg)
            final_output.extend(data)
        except Exception as e:
            print(f"!! Failed on {cfg['label']}: {e}")

    if final_output:
        # CHANGE THIS to your actual Google Sheet name
        upload_to_sheets(final_output, "Pubops_Ad_Units")

if __name__ == "__main__":
    run_ad_unit_dump()