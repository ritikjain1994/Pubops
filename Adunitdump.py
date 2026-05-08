import os, sys, tempfile, subprocess, json, smtplib, io, csv
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from prefect import flow, task
from prefect.blocks.system import Secret

# --- BOOTSTRAP: INSTALL LIBRARIES ---
def install_dependencies():
    try:
        import gspread
        from googleads import ad_manager
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread", "googleads"])

# --- TASK 1: GET GAM AD UNITS ---
@task(retries=2, retry_delay_seconds=60)
def fetch_gam_data(cfg):
    from googleads import ad_manager 
    json_key = Secret.load("oldgamkey" if cfg['label'] == 'OLD GAM' else "newgamkey").get()
    if isinstance(json_key, dict): json_key = json.dumps(json_key)

    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(json_key); tmp_path = tmp.name
    try:
        yaml = f"ad_manager:\n  application_name: 'GAM_Dump'\n  network_code: '{cfg['network_code']}'\n  path_to_private_key_file: '{tmp_path}'"
        client = ad_manager.AdManagerClient.LoadFromString(yaml)
        service = client.GetService('InventoryService', version='v202602')
        all_units, offset = [], 0
        while True:
            query = f"WHERE status = 'ACTIVE' ORDER BY id ASC LIMIT 1000 OFFSET {offset}" if cfg['status_filter'] else f"ORDER BY id ASC LIMIT 1000 OFFSET {offset}"
            res = service.getAdUnitsByStatement({'query': query})
            if 'results' in res:
                all_units.extend(res['results']); offset += 1000
                if len(res['results']) < 1000: break
            else: break
        
        unit_map = {u.id: {'name': u.name, 'parentId': getattr(u, 'parentId', None)} for u in all_units}
        processed = []
        for u in all_units:
            p_names, p_ids, curr = [], [], u.id
            while curr:
                info = unit_map.get(curr)
                if not info: break
                p_names.append(info['name']); p_ids.append(str(curr))
                curr = info['parentId']
            p_names.reverse(); p_ids.reverse()
            if len(p_ids) != cfg['depth']: continue
            if cfg['target_ids'] and not any(str(pid) in p_ids for pid in cfg['target_ids']): continue
            sn, si = p_names[cfg['skip_levels']:], p_ids[cfg['skip_levels']:]
            processed.append({
                'Source': cfg['label'], 'Ad unit 1': sn[0] if sn else "", 
                'Ad unit 2': sn[1] if len(sn) > 1 else "", 'Ad unit 3': sn[2] if len(sn) > 2 else "",
                'Final Ad unit': sn[-1] if sn else "", 'Ad unit 1 ID': si[0] if si else "", 
                'Ad unit 2 ID': si[1] if len(si) > 1 else "", 'Ad unit 3 ID': si[2] if len(si) > 2 else "", 
                'Final Ad unit ID': si[-1] if si else "", 'Status': u.status
            })
        return processed
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

# --- TASKS 2 & 3: SYNC TO MASTER VIA ID ---
@task
def sync_to_master_by_id(new_data, sheet_id):
    import gspread 
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open_by_key(sheet_id); ws = sh.get_worksheet(0)
        raw_ids = ws.col_values(9) # ID is in Column I
        existing_ids = {str(val).strip() for val in raw_ids if val}
        to_append = [u for u in new_data if str(u['Final Ad unit ID']).strip() not in existing_ids]
        if to_append:
            rows = [[u['Source'], u['Ad unit 1'], u['Ad unit 2'], u['Ad unit 3'], u['Final Ad unit'],
                     u['Ad unit 1 ID'], u['Ad unit 2 ID'], u['Ad unit 3 ID'], u['Final Ad unit ID'], u['Status']] for u in to_append]
            ws.append_rows(rows, value_input_option='USER_ENTERED')
            return to_append
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

# --- TASK 5: CHECK GAPS IN DIRECT ORDER VIA ID (READ-ONLY) ---
@task
def find_direct_order_gaps_by_id(all_gam_data, sheet_id):
    import gspread
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open_by_key(sheet_id); ws = sh.get_worksheet(0)
        
        # Read Column H (Index 8)
        direct_order_ids = {str(val).strip() for val in ws.col_values(8) if val}
        
        gaps = [u for u in all_gam_data if str(u['Final Ad unit ID']).strip() not in direct_order_ids]
        return gaps
    except Exception as e:
        print(f"Warning: Could not read Direct Order sheet: {e}")
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

# --- TASK 4: SEND MAILER (ALWAYS SENDS) ---
@task
def send_combined_email(added_to_master, direct_order_gaps):
    sender = "YOUR_ACTUAL_GMAIL@gmail.com" 
    pwd = Secret.load("gmail-app-password").get()
    to_recip = "colombia.pubops@timesinternet.in"
    cc_recip = "YOUR_ACTUAL_GMAIL@gmail.com" 

    now = datetime.now()
    d = now.day
    suffix = 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')
    date_str = now.strftime(f"{d}{suffix} %B %Y")

    msg = MIMEMultipart()
    msg['Subject'] = f"GAM Sync Report - {date_str}"
    msg['From'] = sender; msg['To'] = to_recip; msg['Cc'] = cc_recip
    
    # Dynamic Email Body
    body_text = f"Hello,\n\nAutomated GAM sync report for {date_str}:\n\n"
    
    if added_to_master:
        body_text += f"✅ Added to Master: {len(added_to_master)} new ad units (CSV attached).\n"
    else:
        body_text += "ℹ️ Master Sheet: No new ad units were found or added.\n"
        
    if direct_order_gaps:
        body_text += f"⚠️ Direct Order Sheet: {len(direct_order_gaps)} entries are missing (CSV attached).\n"
    else:
        body_text += "✅ Direct Order Sheet: No entries are missing; sheet is fully maintained.\n"

    body_text += "\nAutomated by Prefect."
    msg.attach(MIMEText(body_text, 'plain'))

    # Attachment Logic
    def attach_csv(data, fname):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Source", "Final Ad unit", "Final Ad unit ID"])
        writer.writeheader()
        for u in data:
            writer.writerow({"Source": u["Source"], "Final Ad unit": u["Final Ad unit"], "Final Ad unit ID": u["Final Ad unit ID"]})
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(output.getvalue().encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{fname}"')
        msg.attach(part)
        output.close()

    if added_to_master:
        attach_csv(added_to_master, f"New_Ad_Units_{date_str}.csv")
    if direct_order_gaps:
        attach_csv(direct_order_gaps, f"Direct_Order_Gaps_{date_str}.csv")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            server.login(sender, pwd)
            server.sendmail(sender, [to_recip, cc_recip], msg.as_string())
        print("Email Sent.")
    except Exception as e: print(f"Email Error: {e}")

# --- MASTER FLOW ---
@flow(log_prints=True)
def run_ad_unit_dump():
    install_dependencies()
    
    # --- ENTER YOUR SHEET IDs HERE ---
    MASTER_SHEET_ID = "1c6T7qbisk93oyABaoQIPc5Mny2P6h3ter47toAKff_w"
    DIRECT_ORDER_ID = "1r6qaWp3JB5f4Zxd3wMYcEuq4gMmCntaKRKklEgnRulc"
    
    configs = [
        {'label': 'OLD GAM', 'network_code': '7176', 'status_filter': 'ACTIVE', 'target_ids': [23325198618, 23326563038], 'depth': 5, 'skip_levels': 1},
        {'label': 'New GAM', 'network_code': '23037861279', 'status_filter': None, 'target_ids': None, 'depth': 6, 'skip_levels': 2}
    ]
    
    all_gam_data = []
    for cfg in configs:
        all_gam_data.extend(fetch_gam_data(cfg))
    
    added_to_master = sync_to_master_by_id(all_gam_data, MASTER_SHEET_ID)
    direct_order_gaps = find_direct_order_gaps_by_id(all_gam_data, DIRECT_ORDER_ID)
    
    send_combined_email(added_to_master, direct_order_gaps)

if __name__ == "__main__":
    run_ad_unit_dump()