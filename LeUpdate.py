import os
import json
import time
import requests
import gspread
from google.oauth2.service_account import Credentials

MASTER_SPREADSHEET_ID = "1vPl31_edSUGUwByK9Jn-w2htBGO8njQs2qxNhsYEUsY"
MASTER_SHEET_NAME = "Master List"
TARGET_SHEET_NAME = "Sheet1"

API_URL = "https://platform.tracxn.com/data/entities/3.0/w/legal-entity"
BATCH_SIZE = 100
API_DELAY = 0.4
REQUEST_TIMEOUT = 30


def gspread_auth():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def create_session():
    session = requests.Session()
    headers = {
        "Content-Type": "application/json",
        "accessToken": os.environ["ACCESS_TOKEN"],
        "X-Request-Source": "Python Bulk Update",
        "cache-control": "no-cache",
    }
    return session, headers


def build_payload(le_id, pa_id, feed_ids):
    taxonomy = {}
    for i, feed in enumerate([x.strip() for x in feed_ids.split(",") if x.strip()]):
        taxonomy[str(i)] = {"practiceArea": pa_id, "feed": feed}
    return {
        "object": {"id": le_id, "primaryTaxonomyManual": taxonomy},
        "opType": "Update",
    }


def update_platform(session, headers, payload):
    try:
        r = session.put(API_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.ok:
            return "Success", ""
        return "Failed", f"{r.status_code} - {r.text[:500]}"
    except Exception as e:
        return "Failed", str(e)


def flush_updates(ws, updates):
    if updates:
        ws.batch_update(updates, value_input_option="RAW")
        updates.clear()


def process_target_sheet(gc, master_ws, master_row, sheet_id, session, headers):
    ws = gc.open_by_key(sheet_id).worksheet(TARGET_SHEET_NAME)
    values = ws.get_all_values()

    start_row = 2
    master_vals = master_ws.row_values(master_row)
    if len(master_vals) >= 4 and master_vals[3].isdigit():
        start_row = max(2, int(master_vals[3]))

    processed = 0
    updates = []

    for r in range(start_row, len(values) + 1):
        rec = values[r - 1] + [""] * 20

        le_id = rec[3].strip()
        pa_id = rec[7].strip()
        feed_ids = rec[8].strip()

        if not (le_id and pa_id and feed_ids):
            status, reason = "Failed", "Missing LE ID / PA ID / Feed ID"
        else:
            payload = build_payload(le_id, pa_id, feed_ids)
            status, reason = update_platform(session, headers, payload)
            time.sleep(API_DELAY)

        updates.append({
            "range": f"J{r}:K{r}",
            "values": [[status, reason]]
        })

        processed += 1

        if len(updates) >= BATCH_SIZE:
            flush_updates(ws, updates)
            master_ws.update(f"C{master_row}", processed)

    flush_updates(ws, updates)
    master_ws.update(f"C{master_row}", processed)


def process_master_sheet(gc, session, headers):
    master_ws = gc.open_by_key(MASTER_SPREADSHEET_ID).worksheet(MASTER_SHEET_NAME)
    rows = master_ws.get_all_values()

    for master_row, row in enumerate(rows[1:], start=2):
        sheet_id = row[1].strip() if len(row) > 1 else ""
        if not sheet_id:
            continue

        print(f"Processing {sheet_id}")
        master_ws.update(f"E{master_row}", "Processing")

        try:
            process_target_sheet(gc, master_ws, master_row, sheet_id, session, headers)
            master_ws.update(f"E{master_row}", "Completed")
        except Exception as e:
            master_ws.update(f"E{master_row}", "Failed")
            print(f"{sheet_id}: {e}")


def main():
    gc = gspread_auth()
    session, headers = create_session()
    process_master_sheet(gc, session, headers)
    print("Done")


if __name__ == "__main__":
    main()
