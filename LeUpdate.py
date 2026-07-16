import os,json,time,requests,gspread
from google.oauth2.service_account import Credentials

MASTER_SPREADSHEET_ID="1vPl31_edSUGUwByK9Jn-w2htBGO8njQs2qxNhsYEUsY"
MASTER_SHEET_NAME="Master List"
TARGET_SHEET_NAME="Sheet1"

BATCH_SIZE=100
API_DELAY=0.40
REQUEST_TIMEOUT=30

API_URL="https://platform.tracxn.com/data/entities/3.0/w/legal-entity"

GOOGLE_SERVICE_ACCOUNT_INFO=json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"])
ACCESS_TOKEN=os.environ["ACCESS_TOKEN"]

creds=Credentials.from_service_account_info(
    GOOGLE_SERVICE_ACCOUNT_INFO,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc=gspread.authorize(creds)
master=gc.open_by_key(MASTER_SPREADSHEET_ID).worksheet(MASTER_SHEET_NAME)

session=requests.Session()
HEADERS={
    "Content-Type":"application/json",
    "accessToken":ACCESS_TOKEN,
    "X-Request-Source":"Python Bulk Update",
    "cache-control":"no-cache"
}

rows=master.get_all_values()
for master_row,row in enumerate(rows[1:],start=2):
    sheet_id=(row[1] if len(row)>1 else "").strip()
    if not sheet_id:
        continue
    master.update(f"E{master_row}","Processing")
    processed=0
    try:
        ws=gc.open_by_key(sheet_id).worksheet(TARGET_SHEET_NAME)
        values=ws.get_all_values()
        start_row=2
        if len(row)>3 and row[3].strip().isdigit():
            start_row=max(2,int(row[3]))
        updates=[]
        for r in range(start_row,len(values)+1):
            rec=values[r-1]+[""]*10
            le_id=rec[4].strip(); pa=rec[6].strip(); feed=rec[7].strip()
            status="Success"; reason=""
            if not(le_id and pa and feed):
                status="Failed"; reason="Missing LE ID/PA/Feed"
            else:
                payload={"object":{"id":le_id,"primaryTaxonomyManual":{"0":{"practiceArea":pa,"feed":feed}}},"opType":"Update"}
                try:
                    resp=session.put(API_URL,headers=HEADERS,json=payload,timeout=REQUEST_TIMEOUT)
                    if not resp.ok:
                        status="Failed"; reason=f"{resp.status_code} - {resp.text[:500]}"
                except Exception as ex:
                    status="Failed"; reason=str(ex)
                time.sleep(API_DELAY)
            updates.append({"range":f"I{r}:J{r}","values":[[status,reason]]})
            processed+=1
            if len(updates)>=BATCH_SIZE:
                ws.batch_update(updates,value_input_option="RAW")
                updates=[]
                master.update(f"C{master_row}",processed)
        if updates:
            ws.batch_update(updates,value_input_option="RAW")
        master.update(f"C{master_row}",processed)
        master.update(f"E{master_row}","Completed")
    except Exception as ex:
        master.update(f"E{master_row}","Failed")
        print(sheet_id,ex)
print("Done")
