import os
import json
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
    # These four now come from environment variables (Railway -> Variables),
    # falling back to the original hardcoded values if not set.
    "SHEET_ID": os.environ.get("SHEET_ID"),
    "SHEET_NAME": os.environ.get("SHEET_NAME"),
    "SERVICE_ACCOUNT_FILE": os.environ.get("SERVICE_ACCOUNT_FILE"),
    "DRIVE_FOLDER_ID": os.environ.get("DRIVE_FOLDER_ID"),

    "CONFIG_SHEET": "Config",      # sheet holding tokens in column B

    "START_ROW": 2,                # 1-indexed, matches the sheet

    "DOC_URL_COLUMN": 5,           # E (1-indexed) - source document URL
    "DRIVE_LINK_COLUMN": 6,        # F (1-indexed) - resulting Drive link written back

    "ROW_BATCH_SIZE": 40,
    "REQUEST_BATCH_SIZE": 40,      # parallel requests per sub-batch
    "MAX_WORKERS": 20,             # thread pool size for parallel fetches

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
    raw = CONFIG["SERVICE_ACCOUNT_FILE"].strip()

    # If SERVICE_ACCOUNT_FILE holds the JSON key contents directly (starts
    # with '{'), parse it as JSON instead of trying to open() it as a path.
    if raw.startswith("{"):
        try:
            info = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "SERVICE_ACCOUNT_FILE looks like JSON but failed to parse. "
                "Make sure the full key file contents were pasted correctly."
            ) from e
        return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    # Otherwise treat it as a real path on disk.
    return service_account.Credentials.from_service_account_file(raw, scopes=SCOPES)


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
    """
    values is 0-indexed (row 0 = sheet row 1).
    Only column E (DOC_URL_COLUMN) is read per row. Rows that already have
    a Drive link in column F are skipped (if SKIP_ROWS_WITH_EXISTING_LINK).
    """
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
    """
    link_updates: list of (row, link) tuples.
    Writes each link to column F of its respective row in one batchUpdate call.
    """
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
    """
    Fails fast with a clear message instead of letting every upload 404.
    A 404 here almost always means either:
      1) the folder ID is wrong, or
      2) the folder hasn't been shared with the service account's email, or
      3) the folder lives in a Shared Drive (needs supportsAllDrives).
    """
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
    """
    Preload existing files in the target folder as a {filename: driveLink}
    map. This lets us both (a) avoid re-downloading files that already
    exist, and (b) backfill column F for rows whose file already exists
    but whose sheet row was never marked with a link.
    """
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
    """Uploads the PDF and returns its Drive webViewLink (shareable link)."""
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
        # Fallback: construct the standard viewer link from the file id.
        link = f"https://drive.google.com/file/d/{created['id']}/view"
    return link


# =========================================================
# FILENAME RESOLUTION
# =========================================================
def filename_from_url(signed_url):
    """Cheap, network-free filename guess derived purely from the URL path."""
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
    """First request: get the redirect (Location header) signed URL."""
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
    """Second request: download the actual PDF bytes (only when needed)."""
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
    """
    Runs the resolve + (maybe) download steps for a single row.

    Returns a dict:
      {"row": row, "status": "existing", "filename": name, "link": link}
        -> file already in Drive; caller just needs to write the link back.
      {"row": row, "status": "uploaded", "filename": name, "content": bytes}
        -> new file, caller must upload it.
      None -> nothing usable (resolve/download failure).
    """
    token = random_token(tokens)
    signed_url = resolve_signed_url(item, token)
    if not signed_url:
        return None

    # Cheap, network-free filename guess first -- lets us skip the (large)
    # PDF download entirely if the file is already sitting in Drive.
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

    # Re-check in case another thread just uploaded the same filename.
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
    link_updates = []  # (row, link) to write back to column F

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
                # File already lives in Drive -- backfill column F, no
                # re-upload needed. This is exactly the "unprocessed row"
                # case: existing file, empty column F.
                link_updates.append((result["row"], result["link"]))
                print(f"Already in Drive, backfilling link: {result['filename']} -> row {result['row']}")
                continue

            # status == "uploaded"
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

    # Write all Drive links for this batch back to column F in one call.
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