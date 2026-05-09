import os, sys, tempfile, subprocess, json, smtplib, io, csv
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret

import enricher  # <-- Pulls in all your logic from the file above

def install_dependencies():
    try:
        import gspread
        from googleads import ad_manager
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "gspread", "googleads"])

@task(retries=2, retry_delay_seconds=60)
def fetch_gam_data(cfg):
    from googleads import ad_manager
    logger = get_run_logger()
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
        logger.info(f"[{cfg['label']}] Fetched {len(processed)} ad units")
        return processed
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

@task
def sync_to_master_by_id(new_data, sheet_id):
    import gspread 
    logger = get_run_logger()
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        ws = gc.open_by_key(sheet_id).get_worksheet(0)
        
        raw_ids = ws.col_values(9) # ID is in Column I
        existing_ids = {str(val).strip().replace("'", "") for val in raw_ids if val}
        
        to_append = []
        for u in new_data:
            gam_id = str(u['Final Ad unit ID']).strip().replace("'", "")
            if gam_id not in existing_ids:
                # 1. Ask enricher.py for the logic
                custom_fields = enricher.apply_comprehensive_logic(u)
                u.update(custom_fields)
                to_append.append(u)

        if to_append:
            rows = [[
                u.get('Source', ''), u.get('Ad unit 1', ''), u.get('Ad unit 2', ''), u.get('Ad unit 3', ''), u.get('Final Ad unit', ''),
                u.get('Ad unit 1 ID', ''), u.get('Ad unit 2 ID', ''), u.get('Ad unit 3 ID', ''), u.get('Final Ad unit ID', ''), u.get('Status', ''),
                # EXACT 9 COLUMNS returned by enricher
                u.get('Expresso Website Name', ''), u.get('Business', ''), u.get('Site', ''), u.get('Platform', ''),
                u.get('Ad_Type', ''), u.get('Ad_Position', ''), u.get('Ad_Position_Granular', ''), u.get('Innovation', ''), u.get('Section Names', '')
            ] for u in to_append]
            ws.append_rows(rows, value_input_option='USER_ENTERED')
            logger.info(f"Appended {len(to_append)} rows to Master Sheet.")
            return to_append
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

@task
def find_direct_order_gaps_by_id(all_gam_data, sheet_id):
    import gspread
    logger = get_run_logger()
    auth = Secret.load("newgamkey").get()
    if isinstance(auth, dict): auth = json.dumps(auth)
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        tmp.write(auth); tmp_path = tmp.name
    try:
        gc = gspread.service_account(filename=tmp_path)
        sh = gc.open_by_key(sheet_id)
        try:
            ws = sh.worksheet("Ad_Unit_Mapping")
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.get_worksheet(0)
        
        raw_values = ws.col_values(8) # Column H
        direct_order_ids = {str(val).strip().replace("'", "") for val in raw_values if val}
        
        gaps = []
        for u in all_gam_data:
            gam_id = str(u['Final Ad unit ID']).strip().replace("'", "")
            if gam_id not in direct_order_ids:
                # Ask enricher.py for the logic to include in the CSV
                custom_fields = enricher.apply_comprehensive_logic(u)
                u.update(custom_fields)
                gaps.append(u)
        logger.info(f"Found {len(gaps)} missing units in Direct Order sheet.")
        return gaps
    except Exception as e:
        logger.error(f"Error checking gaps: {e}")
        return []
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

@task
def send_combined_email(added_to_master, direct_order_gaps):
    logger = get_run_logger()
    sender = "ritik.jain@timesinternet.in" 
    pwd = Secret.load("gmaillogin").get()
    to_recip = "colombia.pubops@timesinternet.in"
    cc_recip = "ritik.jain@timesinternet.in" 

    now = datetime.now()
    d = now.day
    suffix = 'th' if 11<=d<=13 else {1:'st',2:'nd',3:'rd'}.get(d%10, 'th')
    date_str = now.strftime(f"{d}{suffix} %B %Y")

    msg = MIMEMultipart()
    msg['Subject'] = f"GAM Sync Report - {date_str}"
    msg['From'] = sender; msg['To'] = to_recip; msg['Cc'] = cc_recip
    
    body_text = f"Hello,\n\nAutomated GAM sync report for {date_str}:\n\n"
    if added_to_master: body_text += f"✅ Added to Master: {len(added_to_master)} new units.\n"
    else: body_text += "ℹ️ Master Sheet: No new ad units found.\n"
        
    if direct_order_gaps: body_text += f"⚠️ Direct Order Sheet: {len(direct_order_gaps)} entries missing.\n"
    else: body_text += "✅ Direct Order Sheet: Fully maintained.\n"

    msg.attach(MIMEText(body_text, 'plain'))

    def attach_master_csv(data, fname):
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["Source", "Final Ad unit", "Final Ad unit ID"], extrasaction='ignore')
        writer.writeheader()
        for u in data: writer.writerow(u)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(output.getvalue().encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{fname}"')
        msg.attach(part)
        output.close()

    def attach_direct_csv(data, fname):
        output = io.StringIO()
        fieldnames = ["Final Ad unit", "Final Ad unit ID", "Expresso Website Name", "Business", "Site", "Platform", "Ad_Type", "Ad_Position", "Ad_Position_Granular", "Innovation", "Section Names"]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for u in data: writer.writerow(u)
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(output.getvalue().encode('utf-8'))
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{fname}"')
        msg.attach(part)
        output.close()

    if added_to_master: attach_master_csv(added_to_master, f"New_Ad_Units_Master_{date_str}.csv")
    if direct_order_gaps: attach_direct_csv(direct_order_gaps, f"Direct_Order_Gaps_Enriched_{date_str}.csv")

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, timeout=30) as server:
            server.login(sender, pwd)
            server.sendmail(sender, [to_recip, cc_recip], msg.as_string())
        logger.info("Email Sent.")
    except Exception as e: logger.error(f"Email Error: {e}")

@flow(log_prints=True)
def run_ad_unit_dump():
    install_dependencies()
    
    # EXACT SHEET IDs
    MASTER_SHEET_ID = "1c6T7qbisk93oyABaoQIPc5Mny2P6h3ter47toAKff_w"
    DIRECT_ORDER_ID = "1r6qaWp3JB5f4Zxd3wMYcEuq4gMmCntaKRKklEgnRulc"

    
    configs = [
        {'label': 'OLD GAM', 'network_code': '7176', 'status_filter': 'ACTIVE', 'target_ids': [23325198618, 23326563038], 'depth': 5, 'skip_levels': 1},
        {'label': 'New GAM', 'network_code': '23037861279', 'status_filter': None, 'target_ids': None, 'depth': 6, 'skip_levels': 2}
    ]
    
    all_gam_data = []
    for cfg in configs: all_gam_data.extend(fetch_gam_data(cfg))
    
    added_to_master = sync_to_master_by_id(all_gam_data, MASTER_SHEET_ID)
    direct_order_gaps = find_direct_order_gaps_by_id(all_gam_data, DIRECT_ORDER_ID)
    
    send_combined_email(added_to_master, direct_order_gaps)

if __name__ == "__main__":
    run_ad_unit_dump()