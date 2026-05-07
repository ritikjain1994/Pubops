import os
import sys
import tempfile
import subprocess
import json
import smtplib
from email.mime.text import MIMEText
from prefect import flow, task
from prefect.blocks.system import Secret

# --- BOOTSTRAP: INSTALL LIBRARIES ---
def install_dependencies():
    try:
        import gspread
        from googleads import ad_manager
    except ImportError:
        print("Installing libraries in cloud environment...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread", "googleads"])

# --- TASK 1: FETCH DATA FROM GAM ---
@task(retries=2, retry_delay_seconds=60)
def fetch_gam_data(cfg):
    from googleads import ad_manager 
    secret_name = "oldgamkey" if cfg['label'] == 'OLD GAM' else "newgamkey"
    json_key_data = Secret.load(secret_name).get() 

    if isinstance(json_key_data, dict):
        json_key_data = json.dumps(json_key_data)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json', encoding='utf-8') as tmp:
        tmp.write(json_key_data)
        tmp_key_path = tmp.name

    try:
        yaml_config = f"ad_manager:\n  application_name: 'GAM_Dump'\n  network_code: '{cfg['network_code']}'\n  path_to_private_key_file: '{tmp_key_path}'"
        client = ad_manager.AdManagerClient.LoadFromString(yaml_config)
        inventory_service = client.GetService('InventoryService', version='v202602')

        all_units_raw, current_offset = [], 0
        while True:
            query = f"WHERE status = 'ACTIVE' ORDER BY id ASC LIMIT 1000 OFFSET {current_offset}" if cfg['status_filter'] else f"ORDER BY id ASC LIMIT 1000 OFFSET {current_offset}"
            response = inventory_service.getAdUnitsByStatement({'query': query})
            if 'results' in response and len(response['results']) > 0:
                all_units_raw.extend(response['results'])
                current_offset += 1000
            else: break

        unit_map = {u.id: {'name': u.name, 'parentId': getattr(u, 'parentId', None)} for u in all_units_raw}
        processed = []
        for unit in all_units_raw:
            path_names, path_ids, curr_id = [], [], unit.id
            while curr_id:
                curr_info = unit_map.get(curr_id)
                if not curr_info: break
                path_names.append(curr_info['name']); path_ids.append(str(curr_id))
                curr_id = curr_info['parentId']
            path_names.reverse(); path_ids.reverse()

            if len(path_ids) != cfg['depth']: continue
            if cfg['target_ids'] and not any(str(pid) in path_ids for pid in cfg['target_ids']): continue

            shifted_n = path_names[cfg['skip_levels']:]; shifted_i = path_ids[cfg['skip_levels']:]
            processed.append({
                'Source': cfg['label'], 'Ad unit 1': shifted_n[0] if shifted_n else "", 
                'Final Ad unit': shifted_n[-1] if shifted_n else "",
                'Final Ad unit ID': shifted_i[-1] if shifted_i else "", 'Status': unit.status
            })
        return processed
    finally:
        if os.path.exists(tmp_key_path): os.remove(tmp_key_path)

# --- TASK 2: GET EXISTING IDS & APPEND NEW ONES ---
@task
def sync_to_sheets(new_data, sheet_name):
    import gspread 
    auth_json = Secret.load("newgamkey").get()
    if isinstance(auth_json, dict): auth_json = json.dumps(auth_json)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth_json); tmp_path = tmp.name
    
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open(sheet_name)
        worksheet = sh.get_worksheet(0)
        
        # 1. Get existing IDs to avoid duplicates
        existing_rows = worksheet.get_all_records()
        existing_ids = {str(row['Final Ad unit ID']) for row in existing_rows}
        
        # 2. Filter for truly NEW units
        to_append = [u for u in new_data if str(u['Final Ad unit ID']) not in existing_ids]
        
        if to_append:
            headers = ['Source', 'Ad unit 1', 'Final Ad unit', 'Final Ad unit ID', 'Status']
            rows_to_add = [[u[h] for h in headers] for u in to_append]
            worksheet.append_rows(rows_to_add)
            print(f"Added {len(to_append)} new units.")
            return to_append
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

# --- TASK 3: SEND EMAIL NOTIFICATION ---
@task
def send_email(new_units):
    if not new_units: return
    
    sender_email = "ritik.jain@timesinternet.in"  # Replace with your Gmail
    app_password = Secret.load("gmail-app-password").get()
    recipient = "colombia.pubops@timesinternet.in"
    
    unit_list = "\n".join([f"- {u['Final Ad unit']} ({u['Source']})" for u in new_units])
    body = f"Hello,\n\nNew Ad Units have been detected and added to the sheet:\n\n{unit_list}\n\nAutomated by Prefect."
    
    msg = MIMEText(body)
    msg['Subject'] = f"Automated : ALERT: {len(new_units)} New Ad Units Created"
    msg['From'] = sender_email
    msg['To'] = recipient

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(sender_email, app_password)
        server.sendmail(sender_email, recipient, msg.as_string())
    print("Email sent successfully.")

# --- MAIN FLOW ---
@flow(log_prints=True)
def run_ad_unit_dump():
    install_dependencies()
    configs = [
        {'label': 'OLD GAM', 'network_code': '7176', 'status_filter': 'ACTIVE', 'target_ids': [23325198618, 23326563038], 'depth': 5, 'skip_levels': 1},
        {'label': 'New GAM', 'network_code': '23037861279', 'status_filter': None, 'target_ids': None, 'depth': 6, 'skip_levels': 2}
    ]
    
    all_current_data = []
    for cfg in configs:
        data = fetch_gam_data(cfg)
        all_current_data.extend(data)

    added_units = sync_to_sheets(all_current_data, "Pubops_Ad_Units")
    if added_units:
        send_email(added_units)

if __name__ == "__main__":
    run_ad_unit_dump()