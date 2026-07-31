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
    # ---- Master queue sheet ----
    "MASTER_SHEET_ID": "1oNr3g2Pjpyu9u09w0lCFVT9vJwbBGn8O2rbx4kvjd88",
    "MASTER_SHEET_NAME": "Sheet1",
    # Master sheet columns (1-indexed)
    "MASTER_SNO_COL": 1,
    "MASTER_DOC_TYPE_COL": 2,
    "MASTER_DOC_LINK_COL": 3,
    "MASTER_USER_COL": 4,
    "MASTER_STATUS_COL": 5,
    "MASTER_EXTRACTION_USER_COL": 6,
    "MASTER_EXTRACTION_STATUS_COL": 7,

    # Name written into the master "User" column when this worker claims a row.
    "WORKER_USER": os.environ.get("WORKER_USER"),

    # ---- Child sheet (per Doc Link) ----
    "CHILD_SHEET_NAME": "Sheet1",
    "CHILD_CONFIG_SHEET": "Config",   # sheet holding tokens in column B
    "CHILD_START_ROW": 2,             # 1-indexed, matches the sheet
    "CHILD_DOC_URL_COLUMN": 5,        # E - source document URL
    "CHILD_DRIVE_LINK_COLUMN": 6,     # F - resulting Drive link written back
    "SKIP_ROWS_WITH_EXISTING_LINK": True,

    # ---- Shared ----
    "SERVICE_ACCOUNT_FILE": os.environ.get("SERVICE_ACCOUNT_FILE"),
    "DRIVE_FOLDER_ID": os.environ.get("DRIVE_FOLDER_ID"),

    "ROW_BATCH_SIZE": 100,
    "REQUEST_BATCH_SIZE": 40,      # parallel requests per sub-batch
    "MAX_WORKERS": 40,             # thread pool size for parallel fetches
    "SLEEP_BETWEEN_BATCHES": 0.25,  # seconds
}

# Every uploaded (or backfilled) Drive file is force-shared with these
# service-account emails, regardless of whether they already have access.
PROJECT_SHARE_EMAILS = [
    "python-ss@my-project-1718797367466.iam.gserviceaccount.com",
    "ss-scraper-service@gen-lang-client-0608981289.iam.gserviceaccount.com",
    "moa-process@gen-lang-client-0608981289.iam.gserviceaccount.com",
    "amoa-process@gen-lang-client-0608981289.iam.gserviceaccount.com",
    "emoa-process@gen-lang-client-0608981289.iam.gserviceaccount.com",
    "pdf-extractor@pdf-extractor-501710.iam.gserviceaccount.com",
]
PROJECT_SHARE_ROLE = "writer"  # Editor access

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
    raw = (CONFIG["SERVICE_ACCOUNT_FILE"] or "").strip()
    if not raw:
        raise ValueError("SERVICE_ACCOUNT_FILE env var is not set.")

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


def get_tokens(sheets_service, child_sheet_id):
    """Reads tokens from the child sheet's Config tab, column B, starting row 2."""
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=child_sheet_id,
            range=f"{CONFIG['CHILD_CONFIG_SHEET']}!B2:B",
        )
        .execute()
    )
    rows = result.get("values", [])
    tokens = [r[0] for r in rows if r and r[0]]

    if not tokens:
        raise ValueError(
            f"No tokens found in the Config sheet (column B) of child sheet {child_sheet_id}."
        )

    return tokens


def random_token(tokens):
    import random
    return random.choice(tokens)


# =========================================================
# GENERIC SHEET HELPERS
# =========================================================
def col_num_to_letter(n):
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = chr(65 + remainder) + letters
    return letters


def read_all_values(sheets_service, spreadsheet_id, sheet_name):
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=sheet_name)
        .execute()
    )
    return result.get("values", [])


def write_row_cells(sheets_service, spreadsheet_id, sheet_name, row, col_to_value):
    """
    col_to_value: dict of {col_index (1-indexed): value}. Writes each cell
    in a single batchUpdate call.
    """
    if not col_to_value:
        return
    data = [
        {
            "range": f"{sheet_name}!{col_num_to_letter(col)}{row}",
            "values": [[value]],
        }
        for col, value in col_to_value.items()
    ]
    body = {"valueInputOption": "USER_ENTERED", "data": data}
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


# =========================================================
# MASTER SHEET QUEUE LOGIC
# =========================================================
def extract_sheet_id_from_url(url):
    """Pulls the {id} out of .../spreadsheets/d/{id}/edit ..."""
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url or "")
    return match.group(1) if match else None


def find_next_unclaimed_master_row(master_values):
    """
    Returns (sheet_row, doc_link) for the first row where Doc Link is
    non-empty and User is empty. Returns (None, None) if none remain.
    """
    doc_link_col = CONFIG["MASTER_DOC_LINK_COL"]
    user_col = CONFIG["MASTER_USER_COL"]

    for row_index, row in enumerate(master_values):
        sheet_row = row_index + 1  # 1-indexed
        if sheet_row == 1:
            continue  # header row

        doc_link = row[doc_link_col - 1] if doc_link_col - 1 < len(row) else None
        if not doc_link:
            continue  # nothing to process for this row

        user = row[user_col - 1] if user_col - 1 < len(row) else None
        if user:
            continue  # already claimed

        return sheet_row, doc_link

    return None, None


def claim_master_row(sheets_service, master_sheet_id, master_sheet_name, row):
    write_row_cells(
        sheets_service,
        master_sheet_id,
        master_sheet_name,
        row,
        {
            CONFIG["MASTER_USER_COL"]: CONFIG["WORKER_USER"],
            CONFIG["MASTER_STATUS_COL"]: "Processing",
        },
    )


def mark_master_row_done(sheets_service, master_sheet_id, master_sheet_name, row):
    write_row_cells(
        sheets_service,
        master_sheet_id,
        master_sheet_name,
        row,
        {CONFIG["MASTER_STATUS_COL"]: "Done"},
    )


# =========================================================
# CHILD SHEET: BUILD REQUEST LIST
# =========================================================
def build_request_list(values):
    """
    values is 0-indexed (row 0 = sheet row 1).
    Only column E (CHILD_DOC_URL_COLUMN) is read per row. Rows that already
    have a Drive link in column F are skipped (if SKIP_ROWS_WITH_EXISTING_LINK).
    """
    requests_list = []
    start_row = CONFIG["CHILD_START_ROW"]
    doc_col = CONFIG["CHILD_DOC_URL_COLUMN"]
    link_col = CONFIG["CHILD_DRIVE_LINK_COLUMN"]

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


def write_links_batch(sheets_service, spreadsheet_id, sheet_name, link_updates):
    """
    link_updates: list of (row, link) tuples.
    Writes each link to the Drive-link column of its respective row in one
    batchUpdate call.
    """
    if not link_updates:
        return

    link_col_letter = col_num_to_letter(CONFIG["CHILD_DRIVE_LINK_COLUMN"])
    data = [
        {
            "range": f"{sheet_name}!{link_col_letter}{row}",
            "values": [[link]],
        }
        for row, link in link_updates
    ]

    body = {"valueInputOption": "USER_ENTERED", "data": data}
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
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
            raw = (CONFIG["SERVICE_ACCOUNT_FILE"] or "").strip()
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
    Preload existing files in the target folder as a
    {filename: {"link": driveLink, "id": fileId}} map. This lets us both
    (a) avoid re-downloading files that already exist, and (b) backfill the
    Drive-link column (and re-share) for rows whose file already exists but
    whose sheet row was never marked with a link.
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
            files[f["name"]] = {"link": link, "id": f["id"]}
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


def get_shared_emails(drive_service, file_id):
    """
    Returns the set of PROJECT_SHARE_EMAILS that already have a permission
    grant on this file. Used to avoid re-granting on files that were
    already shared in a previous run.
    """
    shared = set()
    try:
        resp = (
            drive_service.permissions()
            .list(
                fileId=file_id,
                fields="permissions(emailAddress,role)",
                supportsAllDrives=True,
            )
            .execute()
        )
        for p in resp.get("permissions", []):
            email = p.get("emailAddress")
            if email in PROJECT_SHARE_EMAILS:
                shared.add(email)
    except Exception as e:
        print(f"  Could not list existing permissions for {file_id}: {e}")
    return shared


def share_file_with_project_emails(drive_service, file_id, skip_already_shared=False):
    """
    Force-shares a Drive file with every address in PROJECT_SHARE_EMAILS,
    using a single batched HTTP request instead of one call per email.

    skip_already_shared=True first checks which emails already have a grant
    (one extra API call) and only batches the missing ones -- worthwhile for
    files that may have been shared in a previous run (the "existing"
    backfill case). New uploads never need this check, since a freshly
    created file can't have any grants yet.
    """
    emails_to_share = PROJECT_SHARE_EMAILS
    if skip_already_shared:
        already = get_shared_emails(drive_service, file_id)
        emails_to_share = [e for e in PROJECT_SHARE_EMAILS if e not in already]
        if not emails_to_share:
            return  # already fully shared, nothing to do

    def _callback(request_id, response, exception):
        if exception:
            print(f"  Failed to share file {file_id} with {request_id}: {exception}")

    batch = drive_service.new_batch_http_request(callback=_callback)
    for email in emails_to_share:
        batch.add(
            drive_service.permissions().create(
                fileId=file_id,
                body={
                    "type": "user",
                    "role": PROJECT_SHARE_ROLE,
                    "emailAddress": email,
                },
                fields="id",
                supportsAllDrives=True,
                sendNotificationEmail=False,
            ),
            request_id=email,
        )
    batch.execute()


def upload_to_drive(drive_service, folder_id, filename, content_bytes):
    """Uploads the PDF and returns (file_id, webViewLink)."""
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

    file_id = created["id"]
    link = created.get("webViewLink")
    if not link:
        # Fallback: construct the standard viewer link from the file id.
        link = f"https://drive.google.com/file/d/{file_id}/view"
    return file_id, link


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
            existing_entry = existing_files.get(guessed_name)
        if existing_entry:
            return {
                "row": item["row"],
                "status": "existing",
                "filename": guessed_name,
                "link": existing_entry["link"],
                "file_id": existing_entry["id"],
            }

    content, headers = download_pdf(signed_url)
    if content is None:
        return None

    filename = guessed_name or filename_from_headers(headers or {}) or fallback_filename()

    # Re-check in case another thread just uploaded the same filename.
    with existing_lock:
        existing_entry = existing_files.get(filename)
    if existing_entry:
        return {
            "row": item["row"],
            "status": "existing",
            "filename": filename,
            "link": existing_entry["link"],
            "file_id": existing_entry["id"],
        }

    return {
        "row": item["row"],
        "status": "uploaded",
        "filename": filename,
        "content": content,
    }


# =========================================================
# BATCH PROCESSING (within a single child sheet)
# =========================================================
def process_batch(
    batch, tokens, drive_service, folder_id, existing_files, existing_lock,
    sheets_service, child_sheet_id, child_sheet_name,
):
    link_updates = []  # (row, link) to write back to the Drive-link column

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
                # File already lives in Drive -- backfill the link column,
                # no re-upload needed. This is exactly the "unprocessed
                # row" case: existing file, empty link column.
                link_updates.append((result["row"], result["link"]))
                share_file_with_project_emails(drive_service, result["file_id"], skip_already_shared=True)
                print(f"Already in Drive, backfilling link: {result['filename']} -> row {result['row']}")
                continue

            # status == "uploaded"
            filename = result["filename"]
            content = result["content"]
            try:
                file_id, link = upload_to_drive(drive_service, folder_id, filename, content)
                with existing_lock:
                    existing_files[filename] = {"link": link, "id": file_id}
                link_updates.append((result["row"], link))
                share_file_with_project_emails(drive_service, file_id)
                print(f"Downloaded: {filename} -> row {result['row']}")
            except Exception as e:
                print(f"Upload failed for {filename}: {e}")

    # Write all Drive links for this batch back to the child sheet in one call.
    try:
        write_links_batch(sheets_service, child_sheet_id, child_sheet_name, link_updates)
    except Exception as e:
        print(f"Failed to write links back to child sheet {child_sheet_id}: {e}")


def process_child_sheet(
    sheets_service, drive_service, folder_id, existing_files, existing_lock, child_sheet_id,
):
    """
    Runs the full download/upload pipeline against a single child
    spreadsheet until every pending row has been handled.
    """
    child_sheet_name = CONFIG["CHILD_SHEET_NAME"]

    print(f"  Reading tokens from child sheet {child_sheet_id}...")
    tokens = get_tokens(sheets_service, child_sheet_id)
    print(f"  Loaded {len(tokens)} token(s).")

    print(f"  Reading values from child sheet {child_sheet_id}...")
    values = read_all_values(sheets_service, child_sheet_id, child_sheet_name)
    request_list = build_request_list(values)
    print(f"  Pending document URLs in child sheet: {len(request_list)}")

    total = len(request_list)
    processed = 0
    idx = 0
    while idx < total:
        chunk = request_list[idx: idx + CONFIG["REQUEST_BATCH_SIZE"]]
        process_batch(
            chunk, tokens, drive_service, folder_id, existing_files, existing_lock,
            sheets_service, child_sheet_id, child_sheet_name,
        )

        idx += len(chunk)
        processed += len(chunk)
        print(f"  Progress: {processed}/{total}")

        time.sleep(CONFIG["SLEEP_BETWEEN_BATCHES"])

    print(f"  Child sheet {child_sheet_id} completed. All rows processed.")


# =========================================================
# MAIN
# =========================================================
def main():
    if not CONFIG["WORKER_USER"]:
        raise ValueError("WORKER_USER env var must be set (name used to claim master rows).")

    print("Authenticating...")
    creds = get_credentials()
    sheets_service = get_sheets_service(creds)
    drive_service = get_drive_service(creds)

    print("Checking Drive folder access...")
    verify_folder_access(drive_service, CONFIG["DRIVE_FOLDER_ID"])

    print("Preloading existing Drive files...")
    existing_files = get_existing_files(drive_service, CONFIG["DRIVE_FOLDER_ID"])
    existing_lock = threading.Lock()
    print(f"Existing files already in folder: {len(existing_files)}")

    master_sheet_id = CONFIG["MASTER_SHEET_ID"]
    master_sheet_name = CONFIG["MASTER_SHEET_NAME"]

    while True:
        print("\nChecking master sheet for the next unclaimed row...")
        master_values = read_all_values(sheets_service, master_sheet_id, master_sheet_name)
        row, doc_link = find_next_unclaimed_master_row(master_values)

        if row is None:
            print("No unclaimed rows remain in the master sheet. Done.")
            break

        child_sheet_id = extract_sheet_id_from_url(doc_link)
        if not child_sheet_id:
            print(f"Row {row}: could not parse a spreadsheet id from Doc Link '{doc_link}'. Skipping claim.")
            # Mark it so we don't loop on it forever, but don't call it "Done".
            write_row_cells(
                sheets_service, master_sheet_id, master_sheet_name, row,
                {CONFIG["MASTER_STATUS_COL"]: "Invalid link"},
            )
            continue

        print(f"Claiming master row {row} (child sheet {child_sheet_id}) as '{CONFIG['WORKER_USER']}'...")
        claim_master_row(sheets_service, master_sheet_id, master_sheet_name, row)

        try:
            process_child_sheet(
                sheets_service, drive_service, CONFIG["DRIVE_FOLDER_ID"],
                existing_files, existing_lock, child_sheet_id,
            )
            mark_master_row_done(sheets_service, master_sheet_id, master_sheet_name, row)
            print(f"Master row {row} marked Done.")
        except Exception as e:
            print(f"Error processing child sheet for master row {row}: {e}")
            write_row_cells(
                sheets_service, master_sheet_id, master_sheet_name, row,
                {CONFIG["MASTER_STATUS_COL"]: "Error"},
            )

    print("\nAll available master rows have been processed.")


if __name__ == "__main__":
    main()