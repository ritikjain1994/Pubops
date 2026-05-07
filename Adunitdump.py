import os, sys, tempfile, subprocess, json, smtplib
from email.mime.text import MIMEText
from prefect import flow, task
from prefect.blocks.system import Secret

def install_dependencies():
    try:
        import gspread
        from googleads import ad_manager
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread", "googleads"])
@task(retries=2)
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
            
            # Returning all 10 columns to match your original sheet structure
            processed.append({
                'Source': cfg['label'],
                'Ad unit 1': sn[0] if len(sn) > 0 else "",
                'Ad unit 2': sn[1] if len(sn) > 1 else "",
                'Ad unit 3': sn[2] if len(sn) > 2 else "",
                'Final Ad unit': sn[-1] if len(sn) > 0 else "",
                'Ad unit 1 ID': si[0] if len(si) > 0 else "",
                'Ad unit 2 ID': si[1] if len(si) > 1 else "",
                'Ad unit 3 ID': si[2] if len(si) > 2 else "",
                'Final Ad unit ID': si[-1] if len(si) > 0 else "",
                'Status': u.status
            })
        return processed
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

@task
def sync_to_sheets(new_data, sheet_name):
    import gspread 
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open(sheet_name); ws = sh.get_worksheet(0)
        
        print("Fetching existing IDs from Column I (index 9)...")
        # Column I is index 9
        raw_ids = ws.col_values(9) 
        existing_ids = {str(val).strip() for val in raw_ids if val}

        to_append = [u for u in new_data if str(u['Final Ad unit ID']).strip() not in existing_ids]
        
        if to_append:
            print(f"Found {len(to_append)} new units. Appending...")
            # Headers sequence: Source(A), AU1(B), AU2(C), AU3(D), FinalAU(E), AU1ID(F), AU2ID(G), AU3ID(H), FinalAUID(I), Status(J)
            rows = [[
                u['Source'], u['Ad unit 1'], u['Ad unit 2'], u['Ad unit 3'], u['Final Ad unit'],
                u['Ad unit 1 ID'], u['Ad unit 2 ID'], u['Ad unit 3 ID'], u['Final Ad unit ID'], u['Status']
            ] for u in to_append]
            
            ws.append_rows(rows, value_input_option='USER_ENTERED')
            return to_append
        
        print("No new units found. Sync complete.")
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)
@task
def send_email(new_units):
    if not new_units: return
    sender = "ritik.jain@timesinternet.in" # MUST MATCH APP PASSWORD ACCOUNT
    pwd = Secret.load("gmaillogin").get()
    recipient = "colombia.pubops@timesinternet.in"
    
    body = "New Ad Units:\n\n" + "\n".join([f"- {u['Final Ad unit']} ({u['Source']})" for u in new_units])
    msg = MIMEText(body)
    msg['Subject'] = f"ALERT: {len(new_units)} New Ad Units"
    msg['From'] = sender; msg['To'] = recipient

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            server.login(sender, pwd) # Error 535 happens here
            server.sendmail(sender, recipient, msg.as_string())
        print("Email Sent!")
    except Exception as e: print(f"Email Failed: {e}")

@flow(log_prints=True)
def run_ad_unit_dump():
    install_dependencies()
    configs = [
        {'label': 'OLD GAM', 'network_code': '7176', 'status_filter': 'ACTIVE', 'target_ids': [23325198618, 23326563038], 'depth': 5, 'skip_levels': 1},
        {'label': 'New GAM', 'network_code': '23037861279', 'status_filter': None, 'target_ids': None, 'depth': 6, 'skip_levels': 2}
    ]
    all_data = []
    for cfg in configs:
        data = fetch_gam_data(cfg)
        all_data.extend(data)
    
    added = sync_to_sheets(all_data, "Pubops_Ad_Units")
    if added: send_email(added)

if __name__ == "__main__":
    run_ad_unit_dump()