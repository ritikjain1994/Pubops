import os
import csv
import sys
from googleads import ad_manager

# --- SYSTEM STABILIZATION ---
os.environ['PYTHONUTF8'] = '1'
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''

if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass


def fetch_and_process(network_label, network_code, key_file, status_filter, target_parent_ids, required_depth,
                      skip_levels):
    yaml_config = f"""
ad_manager:
  application_name: "GAM_Dynamic_Shift_Dump"
  network_code: "{network_code}"
  path_to_private_key_file: "{key_file}"
"""
    client = ad_manager.AdManagerClient.LoadFromString(yaml_config)
    current_version = 'v202602'
    inventory_service = client.GetService('InventoryService', version=current_version)

    all_units_raw = []
    page_limit = 1000
    current_offset = 0

    print(f"\n>>> Processing {network_label}...")

    while True:
        query_parts = []
        if status_filter:
            query_parts.append(f"status = '{status_filter}'")

        where_clause = "WHERE " + " AND ".join(query_parts) if query_parts else ""
        query_str = f"{where_clause} ORDER BY id ASC LIMIT {page_limit} OFFSET {current_offset}".strip()

        statement = {'query': query_str}
        response = inventory_service.getAdUnitsByStatement(statement)

        if 'results' in response and len(response['results']) > 0:
            all_units_raw.extend(response['results'])
            print(f"   Fetched {len(all_units_raw)} units...")
            current_offset += page_limit
            if len(response['results']) < page_limit: break
        else:
            break

    unit_map = {u.id: {'name': u.name, 'parentId': getattr(u, 'parentId', None)} for u in all_units_raw}
    processed_data = []

    for unit in all_units_raw:
        path_names = []
        path_ids = []
        curr_id = unit.id

        while curr_id:
            curr_info = unit_map.get(curr_id)
            if not curr_info: break
            path_names.append(curr_info['name'])
            path_ids.append(str(curr_id))
            curr_id = curr_info['parentId']

        path_names.reverse()
        path_ids.reverse()

        # --- FILTERS ---
        # 1. Check Absolute Depth
        if len(path_ids) != required_depth:
            continue

        # 2. Ancestry Check (Mainly for Old GAM)
        if target_parent_ids:
            if not any(str(pid) in path_ids for pid in target_parent_ids):
                continue

        # --- RESTRUCTURING LOGIC (The "Shift") ---
        # Instead of searching for names, we skip a fixed number of rows
        shifted_names = path_names[skip_levels:]
        shifted_ids = path_ids[skip_levels:]

        # Map to the specific columns requested
        # Ad unit 1, Ad unit 2, Ad unit 3, Final Ad unit
        row = {
            'Source': network_label,
            'Ad unit 1': shifted_names[0] if len(shifted_names) > 0 else "",
            'Ad unit 2': shifted_names[1] if len(shifted_names) > 1 else "",
            'Ad unit 3': shifted_names[2] if len(shifted_names) > 2 else "",
            'Final Ad unit': shifted_names[-1] if len(shifted_names) > 0 else "",
            'Ad unit 1 ID': shifted_ids[0] if len(shifted_ids) > 0 else "",
            'Ad unit 2 ID': shifted_ids[1] if len(shifted_ids) > 1 else "",
            'Ad unit 3 ID': shifted_ids[2] if len(shifted_ids) > 2 else "",
            'Final Ad unit ID': shifted_ids[-1] if len(shifted_ids) > 0 else "",
            'Status': unit.status
        }

        processed_data.append(row)

    print(f"   Done. {len(processed_data)} units mapped.")
    return processed_data


if __name__ == '__main__':
    # --- CONFIGURATION WITH SKIP RULES ---
    configs = [
        {
            'label': 'OLD GAM',
            'network_code': '7176',  # Update
            'key_file': 'Key_oldGAM_ritik.json',  # Update
            'status_filter': 'ACTIVE',
            'target_ids': [23325198618, 23326563038],
            'depth': 5,
            'skip_levels': 1  # Skips Level 1 (e.g., 7176)
        },
        {
            'label': 'New GAM',
            'network_code': '23037861279',  # Update
            'key_file': 'KeyNG.json',  # Update
            'status_filter': None,
            'target_ids': None,
            'depth': 6,
            'skip_levels': 2  # Skips Level 1 & 2 (e.g., Network info & ca-pub ID)
        }
    ]

    final_output = []
    for cfg in configs:
        try:
            data = fetch_and_process(
                cfg['label'], cfg['network_code'], cfg['key_file'],
                cfg['status_filter'], cfg['target_ids'], cfg['depth'], cfg['skip_levels']
            )
            final_output.extend(data)
        except Exception as e:
            print(f"!! Failed on {cfg['label']}: {e}")

    # --- CSV EXPORT WITH PRECISE HEADERS ---
    filename = 'gam_final_restructured_dump.csv'
    headers = [
        'Source', 'Ad unit 1', 'Ad unit 2', 'Ad unit 3', 'Final Ad unit',
        'Ad unit 1 ID', 'Ad unit 2 ID', 'Ad unit 3 ID', 'Final Ad unit ID', 'Status'
    ]

    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(final_output)

    print(f"\n--- SUCCESS ---")
    print(f"File Saved: {os.path.abspath(filename)}")
    print(f"Total Rows: {len(final_output)}")