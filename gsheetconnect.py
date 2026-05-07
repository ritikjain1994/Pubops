import gspread

# Replace with your actual JSON filename
# and your actual Google Sheet name
json_file = 'KeyNG.json' 
sheet_name = 'Pubops_Ad_Units'

try:
    gc = gspread.service_account(filename=json_file)
    sh = gc.open(sheet_name)
    print("SUCCESS: Key has access to the sheet!")
    print(f"Sheet URL: {sh.url}")
except Exception as e:
    print(f"FAILED: {e}")