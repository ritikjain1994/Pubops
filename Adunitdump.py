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
# --- UPDATED TASK 2: SPEEDY SYNC ---
@task
def sync_to_sheets(new_data, sheet_name):
    import gspread 
    print(f"Connecting to Google Sheets: {sheet_name}...")
    auth_json = Secret.load("newgamkey").get()
    if isinstance(auth_json, dict): auth_json = json.dumps(auth_json)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth_json); tmp_path = tmp.name
    
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open(sheet_name)
        worksheet = sh.get_worksheet(0)
        
        print("Reading existing IDs from sheet (Column D)...")
        # Optimization: Only get the ID column (Assuming ID is Column D/4)
        existing_ids = set(worksheet.col_values(4)) 
        
        print(f"Found {len(existing_ids)} existing units. Filtering new data...")
        to_append = [u for u in new_data if str(u['Final Ad unit ID']) not in existing_ids]
        
        if to_append:
            print(f"Appending {len(to_append)} new rows...")
            headers = ['Source', 'Ad unit 1', 'Final Ad unit', 'Final Ad unit ID', 'Status']
            rows_to_add = [[u[h] for h in headers] for u in to_append]
            worksheet.append_rows(rows_to_add)
            return to_append
        
        print("No new units found.")
        return []
    except Exception as e:
        print(f"Error in Sync: {e}")
        raise e
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

# --- UPDATED TASK 3: SMTP WITH TIMEOUT ---
@task
def send_email(new_units):
    if not new_units: return
    
    print("Preparing to send email alert...")
    sender_email = "your-gmail@gmail.com" 
    app_password = Secret.load("gmail-app-password").get()
    recipient = "colombia.pubops@timesinternet.in"
    
    unit_list = "\n".join([f"- {u['Final Ad unit']} ({u['Source']})" for u in new_units])
    body = f"New Ad Units Detected:\n\n{unit_list}"
    
    msg = MIMEText(body)
    msg['Subject'] = f"GAM ALERT: {len(new_units)} New Units"
    msg['From'] = sender_email
    msg['To'] = recipient

    try:
        print("Connecting to Gmail SMTP (Port 465)...")
        # Added a 30-second timeout so it doesn't hang forever
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            print("Logging in...")
            server.login(sender_email, app_password)
            print("Sending...")
            server.sendmail(sender_email, recipient, msg.as_string())
        print("Email sent!")
    except Exception as e:
        print(f"Email failed: {e}")
        # We don't want the whole flow to fail just because the email failed

        
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