import os, json, time, requests, gspread
from google.oauth2.service_account import Credentials

MASTER_SPREADSHEET_ID = "1vPl31_edSUGUwByK9Jn-w2htBGO8njQs2qxNhsYEUsY"
MASTER_SHEET_NAME = os.environ["MASTER_SHEET"]
API_URL = "https://platform.tracxn.com/data/entities/3.0/w/legal-entity"
BATCH_SIZE = 100
API_DELAY = 0.4
REQUEST_TIMEOUT = 30

COL_SHEET_ID = 2
COL_SHEET_NAME = 3
COL_ROWS_PROCESSED = 4
COL_START_ROW = 5
COL_STATUS = 6


def gspread_auth():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ])
    return gspread.authorize(creds)


def create_session():
    s = requests.Session()
    h = {
        "Content-Type": "application/json",
        "accessToken": os.environ["ACCESS_TOKEN"],
        "X-Request-Source": "Python Bulk Update",
        "cache-control": "no-cache"
    }
    return s, h


def build_payload(le_id, pa_id, feed_ids):
    pt = {}
    for i, f in enumerate([x.strip() for x in feed_ids.split(",") if x.strip()]):
        pt[str(i)] = {"practiceArea": pa_id, "feed": f}
    return {"object": {"id": le_id, "primaryTaxonomyManual": pt}, "opType": "Update"}


def update_platform(session, headers, payload):
    try:
        r = session.put(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.ok:
            return "Success", ""
        return "Failed", f"{r.status_code} - {r.text[:500]}"
    except Exception as e:
        return "Failed", str(e)


def flush(ws, updates):
    if updates:
        ws.batch_update(updates, value_input_option="RAW")
        updates.clear()


def process_target(gc, master_ws, master_row, sheet_id, sheet_name, session, headers):
    ws = gc.open_by_key(sheet_id).worksheet(sheet_name)
    values = ws.get_all_values()

    # Read fresh values from master to calculate exact resume point
    mv = master_ws.row_values(master_row)

    start_row = 2
    if len(mv) >= COL_START_ROW:
        v = mv[COL_START_ROW - 1].strip()
        if v.isdigit():
            start_row = max(2, int(v))

    processed = 0
    if len(mv) >= COL_ROWS_PROCESSED:
        p = mv[COL_ROWS_PROCESSED - 1].strip()
        if p.isdigit():
            processed = int(p)

    # Calculate the exact row to resume from
    resume_row = start_row + processed

    updates = []
    for r in range(resume_row, len(values) + 1):
        rec = values[r - 1] + [""] * 20
        le_id = rec[3].strip()
        pa_id = rec[7].strip()
        feed_ids = rec[8].strip()

        if not (le_id and pa_id and feed_ids):
            status, reason = "Failed", "Missing LE ID / PA ID / Feed ID"
        else:
            status, reason = update_platform(session, headers, build_payload(le_id, pa_id, feed_ids))
            time.sleep(API_DELAY)

        updates.append({"range": f"J{r}:K{r}", "values": [[status, reason]]})
        processed += 1

        if len(updates) >= BATCH_SIZE:
            flush(ws, updates)
            master_ws.update_cell(master_row, COL_ROWS_PROCESSED, processed)

    flush(ws, updates)
    master_ws.update_cell(master_row, COL_ROWS_PROCESSED, processed)


def process_master(gc, session, headers):
    m = gc.open_by_key(MASTER_SPREADSHEET_ID).worksheet(MASTER_SHEET_NAME)
    rows = m.get_all_values()

    for mr, row in enumerate(rows[1:], start=2):
        sid = row[COL_SHEET_ID - 1].strip() if len(row) >= COL_SHEET_ID else ""
        sheet_name = row[COL_SHEET_NAME - 1].strip() if len(row) >= COL_SHEET_NAME else ""
        status = row[COL_STATUS - 1].strip() if len(row) >= COL_STATUS else ""

        # Skip if missing required data
        if not sid or not sheet_name:
            continue

        # Skip if already completed
        if status.lower() == "completed":
            continue

        m.update_cell(mr, COL_STATUS, "Processing")
        try:
            process_target(gc, m, mr, sid, sheet_name, session, headers)
            m.update_cell(mr, COL_STATUS, "Completed")
        except Exception as e:
            m.update_cell(mr, COL_STATUS, "Failed")
            print(f"Error on Sheet ID {sid}: {e}")


def main():
    gc = gspread_auth()
    session, headers = create_session()
    process_master(gc, session, headers)
    print("Done")


if __name__ == "__main__":
    main()