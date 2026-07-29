import json
import os
import time
import io
import re
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
    "SHEET_ID": os.environ.get("SHEET_ID", "1DTkmPtSgP8AJ4eaG6n7cSO2OLrOVVJAST69MDk36nt4"),
    "SHEET_NAME": os.environ.get("SHEET_NAME", "Sheet1"),
    "CONFIG_SHEET": os.environ.get("CONFIG_SHEET", "Config"),      # sheet holding tokens in column B

    "START_ROW": 2,                # 1-indexed, matches the sheet

    "DOC_URL_COLUMN": 5,           # E (1-indexed) - source document URL
    "DRIVE_LINK_COLUMN": 6,        # F (1-indexed) - resulting Drive link written back

    "ROW_BATCH_SIZE": 100,
    "REQUEST_BATCH_SIZE": 40,      # parallel requests per sub-batch
    "MAX_WORKERS": 40,             # thread pool size for parallel fetches

    "DRIVE_FOLDER_ID": os.environ.get("DRIVE_FOLDER_ID", "15KsJ1a51I4n6132IIK-qCGJSVoaBCU0g"),

    "SERVICE_ACCOUNT_FILE": "service_account.json",

    "SLEEP_BETWEEN_BATCHES": 0.25,  # seconds

    "SKIP_ROWS_WITH_EXISTING_LINK": True,  # skip rows where col F is already filled
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
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw_json:
        info = json.loads(raw_json)
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    return service_account.Credentials.from_service_account_file(
        CONFIG["SERVICE_ACCOUNT_FILE"], scopes=SCOPES
    )


def get_sheets_service(creds):
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def get_drive_service(creds):
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def get_tokens(sheets_service):
    """Reads tokens from the Config sheet, column B, starting row 2."""
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=CONFIG["SHEET_ID"],
            range=f"{CONFIG['CONFIG_SHEET']}!B2:B",
        )
        .execute()
    )
    rows = result.get("values", [])
    tokens = [r[0] for r in rows if r and r[0]]

    if not tokens:
        raise ValueError("No tokens found in the Config sheet (column B).")

    return tokens


def random_token(tokens):
    import random
    return random.choice(tokens)


# =========================================================
# SHEET READING
# =========================================================
def read_all_values(sheets_service):
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=CONFIG["SHEET_ID"], range=CONFIG["SHEET_NAME"])
        .execute()
    )
    return result.get("values", [])


def build_request_list(values):
    requests_list = []
    start_row = CONFIG["START_ROW"]
    doc_col = CONFIG["DOC_URL_COLUMN"]
    link_col = CONFIG["DRIVE_LINK_COLUMN"]

    for row_index, row in enumerate(values):
        sheet_row = row_index + 1  # 1-indexed sheet row
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
# SHEET WRITING (Drive link back to column F)
# =========================================================
def col_num_to_letter(n):
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def write_links_batch(sheets_service, link_updates):
    if not link_updates:
        return

    link_col_letter = col_num_to_letter(CONFIG["DRIVE_LINK_COLUMN"])
    data = [
        {
            "range": f"{CONFIG['SHEET_NAME']}!{link_col_letter}{row}",
            "values": [[link]],
        }
        for row, link in link_updates
    ]

    body = {"valueInputOption": "USER_ENTERED", "data": data}
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=CONFIG["SHEET_ID"], body=body
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
        raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
        try:
            if raw_json:
                sa_email = json.loads(raw_json).get("client_email", "?")
            else:
                with open(CONFIG["SERVICE_ACCOUNT_FILE"], "r") as f:
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


def upload_to_drive(drive_service, folder_id, filename, content_bytes):
    media = MediaIoBaseUpload(
        io.BytesIO(content_bytes), mimetype="application/pdf", resumable=False
    )
    file_metadata = {"name": filename, "parents": [folder_id]}
    created = (
        drive_service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    link = created.get("webViewLink")
    if not link:
        link = f"https://drive.google.com/file/d/{created['id']}/view"
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


def download_pdf(signed_url):
    try:
        resp = get_session().get(signed_url, timeout=60)
        if resp.status_code != 200:
            print(f"Download failed ({resp.status_code}) for {signed_url}")
            return None, None
        return resp.content, resp.headers
    except Exception as e:
        print(f"Error downloading {signed_url}: {e}")
        return None, None


def process_item(item, tokens, existing_files, existing_lock):
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

    content, headers = download_pdf(signed_url)
    if content is None:
        return None

    filename = guessed_name or filename_from_headers(headers or {}) or fallback_filename()

    with existing_lock:
        existing_link = existing_files.get(filename)
    if existing_link:
        return {
            "row": item["row"],
            "status": "existing",
            "filename": filename,
            "link": existing_link,
        }

    return {
        "row": item["row"],
        "status": "uploaded",
        "filename": filename,
        "content": content,
    }


# =========================================================
# BATCH PROCESSING
# =========================================================
def process_batch(batch, tokens, drive_service, folder_id, existing_files, existing_lock, sheets_service):
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

            filename = result["filename"]
            content = result["content"]
            try:
                link = upload_to_drive(drive_service, folder_id, filename, content)
                with existing_lock:
                    existing_files[filename] = link
                link_updates.append((result["row"], link))
                print(f"Downloaded: {filename} -> row {result['row']}")
            except Exception as e:
                print(f"Upload failed for {filename}: {e}")

    try:
        write_links_batch(sheets_service, link_updates)
    except Exception as e:
        print(f"Failed to write links back to sheet: {e}")


# =========================================================
# MAIN
# =========================================================
def main():
    print("Authenticating...")
    creds = get_credentials()
    sheets_service = get_sheets_service(creds)
    drive_service = get_drive_service(creds)

    print("Reading tokens from Config sheet...")
    tokens = get_tokens(sheets_service)
    print(f"Loaded {len(tokens)} token(s).")

    print("Checking Drive folder access...")
    verify_folder_access(drive_service, CONFIG["DRIVE_FOLDER_ID"])

    print("Reading sheet values...")
    values = read_all_values(sheets_service)
    request_list = build_request_list(values)
    print(f"Total document URLs found (column E, pending): {len(request_list)}")

    print("Preloading existing Drive files...")
    existing_files = get_existing_files(drive_service, CONFIG["DRIVE_FOLDER_ID"])
    existing_lock = threading.Lock()
    print(f"Existing files already in folder: {len(existing_files)}")

    total = len(request_list)
    processed = 0
    idx = 0
    while idx < total:
        chunk = request_list[idx: idx + CONFIG["REQUEST_BATCH_SIZE"]]
        process_batch(
            chunk, tokens, drive_service,
            CONFIG["DRIVE_FOLDER_ID"], existing_files, existing_lock, sheets_service,
        )

        idx += len(chunk)
        processed += len(chunk)
        print(f"Progress: {processed}/{total}")

        time.sleep(CONFIG["SLEEP_BETWEEN_BATCHES"])

    print("Completed. All rows processed.")


if __name__ == "__main__":
    main()