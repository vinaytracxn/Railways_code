import os
import json
import time
import re
import gc
import threading
from datetime import datetime
from urllib.parse import unquote, urlsplit
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# =========================================================
# CONFIG
# =========================================================
CONFIG = {
    "SERVICE_ACCOUNT_FILE": os.environ.get("SERVICE_ACCOUNT_FILE"),
    "DRIVE_FOLDER_ID": os.environ.get("DRIVE_FOLDER_ID"),

    # ---- Master sheet (list of sheets to process) ----
    "MASTER_SHEET_ID": os.environ.get("MASTER_SHEET_ID"),
    "MASTER_SHEET_NAME": os.environ.get("MASTER_SHEET_NAME", "Sheet1"),
    "MASTER_START_ROW": 2,  # 1-indexed, matches the sheet

    # Master sheet columns (1-indexed)
    "MASTER_COL_SNO": 1,               # A - Sno
    "MASTER_COL_DOC_TYPE": 2,          # B - Doc Type
    "MASTER_COL_SHEET_LINK": 3,        # C - Sheet Link
    "MASTER_COL_USER": 4,              # D - User
    "MASTER_COL_DRIVE_STATUS": 5,      # E - Drive Link Status
    "MASTER_COL_EXTRACTION_USER": 6,   # F - extraction User
    "MASTER_COL_EXTRACTION_STATUS": 7, # G - Extarction status

    "DRIVE_STATUS_DONE": "Done",
    "DRIVE_STATUS_PROCESSING": "Processing",

    # Username written into the master sheet's User column (D) when a row
    # is picked up for processing. Set this via env var so different
    # deployments/runners can identify themselves.
    "PROCESS_USER": os.environ.get("PROCESS_USER"),

    # ---- Target sheet layout (same for every sheet linked in master) ----
    "TARGET_SHEET_NAME": "Sheet1",
    "TARGET_START_ROW": 2,        # 1-indexed
    "DOC_URL_COLUMN": 5,          # E (1-indexed) - source document URL
    "DRIVE_LINK_COLUMN": 6,       # F (1-indexed) - resulting Drive link written back
    "SKIP_ROWS_WITH_EXISTING_LINK": True,

    "CONFIG_SHEET": "Config",     # sheet holding tokens in column B, lives in each target sheet

    "ROW_BATCH_SIZE": 100,
    # Lowered from 40 -> much smaller so we only ever hold a handful of PDFs
    # in memory at once. Each concurrent worker streams (not fully buffers)
    # a PDF, but Railway's free/small plans have very little RAM headroom,
    # so keep this conservative. Override via env vars if you have more RAM.
    "REQUEST_BATCH_SIZE": int(os.environ.get("REQUEST_BATCH_SIZE", 6)),
    "MAX_WORKERS": int(os.environ.get("MAX_WORKERS", 6)),

    # Bytes per chunk when streaming a PDF from source -> Google Drive.
    # Smaller = less peak RAM per concurrent upload, slightly more HTTP overhead.
    "UPLOAD_CHUNK_SIZE": int(os.environ.get("UPLOAD_CHUNK_SIZE", 4 * 1024 * 1024)),  # 4MB

    "SLEEP_BETWEEN_BATCHES": 0.25,  # seconds
}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Thread-local storage so each worker thread reuses its own HTTP connection
# pool instead of opening brand-new sockets/TLS handshakes for every request.
_thread_local = threading.local()


def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=CONFIG["MAX_WORKERS"],
            pool_maxsize=CONFIG["MAX_WORKERS"],
        )
        _thread_local.session.mount("http://", adapter)
        _thread_local.session.mount("https://", adapter)
    return _thread_local.session


# =========================================================
# AUTH / CLIENTS
# =========================================================
def get_credentials():
    raw = CONFIG["SERVICE_ACCOUNT_FILE"].strip()

    if raw.startswith("{"):
        try:
            info = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "SERVICE_ACCOUNT_FILE looks like JSON but failed to parse. "
                "Make sure the full key file contents were pasted correctly."
            ) from e
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    return service_account.Credentials.from_service_account_file(raw, scopes=SCOPES)


def get_sheets_service(creds):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_tokens(sheets_service, sheet_id):
    """Reads tokens from the Config sheet, column B, starting row 2, of a given spreadsheet."""
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=sheet_id,
            range=f"{CONFIG['CONFIG_SHEET']}!B2:B",
        )
        .execute()
    )
    rows = result.get("values", [])
    tokens = [r[0] for r in rows if r and r[0]]

    if not tokens:
        raise ValueError(f"No tokens found in the Config sheet (column B) of spreadsheet {sheet_id}.")

    return tokens


def random_token(tokens):
    import random
    return random.choice(tokens)


# =========================================================
# MASTER SHEET HELPERS
# =========================================================
def extract_sheet_id_from_url(url):
    """Pulls the spreadsheet ID out of a Google Sheets URL like
    https://docs.google.com/spreadsheets/d/<ID>/edit#gid=0
    Falls back to treating the whole string as an ID if no match is found.
    """
    if not url:
        return None
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    url = url.strip()
    return url if url else None


def col_num_to_letter(n):
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def read_master_rows(sheets_service):
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=CONFIG["MASTER_SHEET_ID"],
            range=CONFIG["MASTER_SHEET_NAME"],
        )
        .execute()
    )
    values = result.get("values", [])

    rows = []
    start_row = CONFIG["MASTER_START_ROW"]
    link_col = CONFIG["MASTER_COL_SHEET_LINK"]
    status_col = CONFIG["MASTER_COL_DRIVE_STATUS"]
    user_col = CONFIG["MASTER_COL_USER"]

    for row_index, row in enumerate(values):
        sheet_row = row_index + 1
        if sheet_row < start_row:
            continue

        link = row[link_col - 1] if link_col - 1 < len(row) else None
        if not link:
            continue

        status = row[status_col - 1] if status_col - 1 < len(row) else ""
        if status.strip().lower() == CONFIG["DRIVE_STATUS_DONE"].lower():
            continue  # already processed

        user_val = row[user_col - 1] if user_col - 1 < len(row) else ""
        if user_val.strip():
            continue  # skip rows where User (col D) is already filled in; only pick empty ones

        sheet_id = extract_sheet_id_from_url(link)
        if not sheet_id:
            continue

        rows.append({"master_row": sheet_row, "sheet_id": sheet_id, "link": link})

    return rows


def write_master_status(sheets_service, master_row, status_text):
    status_col_letter = col_num_to_letter(CONFIG["MASTER_COL_DRIVE_STATUS"])
    body = {"values": [[status_text]]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=CONFIG["MASTER_SHEET_ID"],
        range=f"{CONFIG['MASTER_SHEET_NAME']}!{status_col_letter}{master_row}",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()


def write_master_user(sheets_service, master_row, user_name):
    user_col_letter = col_num_to_letter(CONFIG["MASTER_COL_USER"])
    body = {"values": [[user_name]]}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=CONFIG["MASTER_SHEET_ID"],
        range=f"{CONFIG['MASTER_SHEET_NAME']}!{user_col_letter}{master_row}",
        valueInputOption="USER_ENTERED",
        body=body,
    ).execute()


# =========================================================
# TARGET SHEET READING
# =========================================================
def read_all_values(sheets_service, sheet_id):
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=sheet_id, range=CONFIG["TARGET_SHEET_NAME"])
        .execute()
    )
    return result.get("values", [])


def build_request_list(values):
    requests_list = []
    start_row = CONFIG["TARGET_START_ROW"]
    doc_col = CONFIG["DOC_URL_COLUMN"]
    link_col = CONFIG["DRIVE_LINK_COLUMN"]

    for row_index, row in enumerate(values):
        sheet_row = row_index + 1
        if sheet_row < start_row:
            continue

        url = row[doc_col - 1] if doc_col - 1 < len(row) else None
        if not url:
            continue

        if CONFIG["SKIP_ROWS_WITH_EXISTING_LINK"]:
            existing_link = row[link_col - 1] if link_col - 1 < len(row) else None
            if existing_link:
                continue

        requests_list.append({"row": sheet_row, "col": doc_col, "url": url})

    return requests_list


# =========================================================
# TARGET SHEET WRITING (Drive link back to column F)
# =========================================================
def write_links_batch(sheets_service, sheet_id, link_updates):
    if not link_updates:
        return

    link_col_letter = col_num_to_letter(CONFIG["DRIVE_LINK_COLUMN"])
    data = [
        {
            "range": f"{CONFIG['TARGET_SHEET_NAME']}!{link_col_letter}{row}",
            "values": [[link]],
        }
        for row, link in link_updates
    ]

    body = {"valueInputOption": "USER_ENTERED", "data": data}
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=sheet_id, body=body
    ).execute()


# =========================================================
# DRIVE HELPERS
# =========================================================
def verify_folder_access(drive_service, folder_id):
    try:
        folder = (
            drive_service.files()
            .get(fileId=folder_id, fields="id, name, driveId", supportsAllDrives=True)
            .execute()
        )
        print(f"Drive folder OK: '{folder.get('name')}' (id: {folder.get('id')})")
    except Exception as e:
        sa_email = "?"
        try:
            raw = CONFIG["SERVICE_ACCOUNT_FILE"].strip()
            if raw.startswith("{"):
                sa_email = json.loads(raw).get("client_email", "?")
            else:
                with open(raw, "r") as f:
                    sa_email = json.load(f).get("client_email", "?")
        except Exception:
            pass
        raise RuntimeError(
            f"Cannot access Drive folder '{folder_id}': {e}\n\n"
            f"Fix: open the folder in Drive, click Share, and add this "
            f"service account as an Editor:\n    {sa_email}\n"
            f"If the folder lives inside a Shared Drive, add the service "
            f"account as a member of that Shared Drive instead."
        )


def get_existing_files(drive_service, folder_id):
    files = {}
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"

    while True:
        resp = (
            drive_service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, webViewLink)",
                pageSize=1000,
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="allDrives",
            )
            .execute()
        )
        for f in resp.get("files", []):
            link = f.get("webViewLink") or f"https://drive.google.com/file/d/{f['id']}/view"
            files[f["name"]] = link
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


def upload_stream_to_drive(drive_service, folder_id, filename, file_like_obj):
    """Uploads directly from a file-like stream (e.g. resp.raw) instead of
    a fully-buffered bytes object. Combined with a small chunksize this caps
    peak RAM per upload to roughly UPLOAD_CHUNK_SIZE, regardless of PDF size."""
    media = MediaIoBaseUpload(
        file_like_obj,
        mimetype="application/pdf",
        chunksize=CONFIG["UPLOAD_CHUNK_SIZE"],
        resumable=True,
    )
    file_metadata = {"name": filename, "parents": [folder_id]}
    request = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id, webViewLink",
        supportsAllDrives=True,
    )

    response = None
    while response is None:
        _, response = request.next_chunk()

    link = response.get("webViewLink")
    if not link:
        link = f"https://drive.google.com/file/d/{response['id']}/view"
    return link


# =========================================================
# FILENAME RESOLUTION
# =========================================================
def filename_from_url(signed_url):
    try:
        path = urlsplit(signed_url).path
        filename = unquote(path.rsplit("/", 1)[-1])
        if filename:
            return filename
    except Exception:
        pass
    return None


def filename_from_headers(headers):
    disposition = headers.get("content-disposition", "") or headers.get(
        "Content-Disposition", ""
    )
    match = re.search(r'filename="?([^"]+)"?', disposition, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def fallback_filename():
    return f"Document_{int(datetime.now().timestamp() * 1000)}.pdf"


# =========================================================
# PER-ITEM PROCESSING
# =========================================================
def resolve_signed_url(item, token):
    try:
        resp = get_session().get(
            item["url"],
            headers={"accessToken": token},
            allow_redirects=False,
            timeout=30,
        )
        signed_url = resp.headers.get("Location") or resp.headers.get("location")
        if not signed_url:
            print(f"No signed URL for row {item['row']} col {item['col']}: {item['url']}")
            return None
        return signed_url
    except Exception as e:
        print(f"Error resolving signed URL for row {item['row']}: {e}")
        return None


def open_pdf_stream(signed_url):
    """Opens a streaming GET (headers only, body not read yet). Caller is
    responsible for either reading resp.raw (on success) or calling
    resp.close() (if abandoning, e.g. file turned out to already exist)."""
    try:
        resp = get_session().get(signed_url, stream=True, timeout=60)
        if resp.status_code != 200:
            print(f"Download failed ({resp.status_code}) for {signed_url}")
            resp.close()
            return None
        resp.raw.decode_content = True  # handle gzip/deflate transparently
        return resp
    except Exception as e:
        print(f"Error opening stream for {signed_url}: {e}")
        return None


def process_item(item, tokens, existing_files, existing_lock):
    """Resolves the signed URL and decides what to do next, but does NOT
    read the PDF body here -- that happens later, streamed straight into
    the Drive upload, so this function never holds a full PDF in memory."""
    token = random_token(tokens)
    signed_url = resolve_signed_url(item, token)
    if not signed_url:
        return None

    guessed_name = filename_from_url(signed_url)
    if guessed_name:
        with existing_lock:
            existing_link = existing_files.get(guessed_name)
        if existing_link:
            return {
                "row": item["row"],
                "status": "existing",
                "filename": guessed_name,
                "link": existing_link,
            }

    resp = open_pdf_stream(signed_url)
    if resp is None:
        return None

    filename = guessed_name or filename_from_headers(resp.headers or {}) or fallback_filename()

    with existing_lock:
        existing_link = existing_files.get(filename)
    if existing_link:
        resp.close()  # already have it -- don't bother reading the body
        return {
            "row": item["row"],
            "status": "existing",
            "filename": filename,
            "link": existing_link,
        }

    return {
        "row": item["row"],
        "status": "to_upload",
        "filename": filename,
        "response": resp,  # kept open; body streamed later by upload_stream_to_drive
    }


# =========================================================
# BATCH PROCESSING (within one target sheet)
# =========================================================
def process_batch(batch, tokens, drive_service, folder_id, existing_files, existing_lock, sheets_service, sheet_id):
    link_updates = []

    with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as pool:
        futures = {
            pool.submit(process_item, item, tokens, existing_files, existing_lock): item
            for item in batch
        }

        for future in as_completed(futures):
            item = futures[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"Unhandled error for row {item['row']}: {e}")
                continue

            if not result:
                continue

            if result["status"] == "existing":
                link_updates.append((result["row"], result["link"]))
                print(f"Already in Drive, backfilling link: {result['filename']} -> row {result['row']}")
                continue

            # status == "to_upload" -- stream straight from source response to Drive
            filename = result["filename"]
            resp = result["response"]
            try:
                link = upload_stream_to_drive(drive_service, folder_id, filename, resp.raw)
                with existing_lock:
                    existing_files[filename] = link
                link_updates.append((result["row"], link))
                print(f"Streamed: {filename} -> row {result['row']}")
            except Exception as e:
                print(f"Upload failed for {filename}: {e}")
            finally:
                resp.close()

    try:
        write_links_batch(sheets_service, sheet_id, link_updates)
    except Exception as e:
        print(f"Failed to write links back to sheet {sheet_id}: {e}")


# =========================================================
# SINGLE TARGET SHEET PIPELINE
# =========================================================
def process_target_sheet(sheet_id, sheets_service, drive_service):
    """Runs the full extract+upload pipeline against one target spreadsheet."""
    print(f"\n=== Processing target sheet: {sheet_id} ===")

    tokens = get_tokens(sheets_service, sheet_id)
    print(f"Loaded {len(tokens)} token(s) for {sheet_id}.")

    values = read_all_values(sheets_service, sheet_id)
    request_list = build_request_list(values)
    print(f"Pending document URLs in {sheet_id}: {len(request_list)}")

    existing_files = get_existing_files(drive_service, CONFIG["DRIVE_FOLDER_ID"])
    existing_lock = threading.Lock()
    print(f"Existing files already in Drive folder: {len(existing_files)}")

    total = len(request_list)
    processed = 0
    idx = 0
    while idx < total:
        chunk = request_list[idx: idx + CONFIG["REQUEST_BATCH_SIZE"]]
        process_batch(
            chunk, tokens, drive_service,
            CONFIG["DRIVE_FOLDER_ID"], existing_files, existing_lock,
            sheets_service, sheet_id,
        )

        idx += len(chunk)
        processed += len(chunk)
        print(f"[{sheet_id}] Progress: {processed}/{total}")

        time.sleep(CONFIG["SLEEP_BETWEEN_BATCHES"])

    # existing_files can hold thousands of entries for large Drive folders;
    # drop the reference and force a collection before moving to the next sheet.
    del existing_files
    gc.collect()

    print(f"=== Completed target sheet: {sheet_id} ===")


# =========================================================
# MAIN
# =========================================================
def main():
    print("Authenticating...")
    creds = get_credentials()
    sheets_service = get_sheets_service(creds)
    drive_service = get_drive_service(creds)

    print("Checking Drive folder access...")
    verify_folder_access(drive_service, CONFIG["DRIVE_FOLDER_ID"])

    print("Reading master sheet...")
    master_rows = read_master_rows(sheets_service)
    print(f"Sheets pending processing (Drive Link Status != Done): {len(master_rows)}")

    for entry in master_rows:
        master_row = entry["master_row"]
        sheet_id = entry["sheet_id"]

        try:
            # Claim this row before doing any work: stamp the User column
            # and mark status "Processing" so other/concurrent runs skip it.
            write_master_user(sheets_service, master_row, CONFIG["PROCESS_USER"])
            write_master_status(sheets_service, master_row, CONFIG["DRIVE_STATUS_PROCESSING"])

            process_target_sheet(sheet_id, sheets_service, drive_service)
            write_master_status(sheets_service, master_row, CONFIG["DRIVE_STATUS_DONE"])
            print(f"Marked master row {master_row} as '{CONFIG['DRIVE_STATUS_DONE']}'.")
        except Exception as e:
            error_text = f"Error: {e}"[:200]  # keep cell value reasonable
            print(f"Failed processing sheet {sheet_id} (master row {master_row}): {e}")
            try:
                write_master_status(sheets_service, master_row, error_text)
            except Exception as write_err:
                print(f"Also failed to write error status to master row {master_row}: {write_err}")
            continue  # move on to the next sheet regardless

    print("\nAll master sheet rows processed.")


if __name__ == "__main__":
    main()