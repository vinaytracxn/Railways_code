import os
import json
import io
import re
import gc
import time
import string
import requests
from requests.adapters import HTTPAdapter
import gspread

try:
    import pymupdf as fitz
except ImportError:
    import fitz
import pytesseract
from PIL import Image

# Keep decompression-bomb protection ON (removed unlimited override) to avoid
# unbounded memory allocation on malformed/huge PDF pages.
Image.MAX_IMAGE_PIXELS = 200_000_000  # ~200MP safety ceiling instead of unlimited

from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials
import google.auth.transport.requests as google_requests

# ---------------- CONFIG (edit these directly for local runs) ----------------
# ---------------- Railway Configuration ----------------

def load_service_account_infos():
    """Loads one or more service-account JSON blobs from env vars.

    Primary account: GOOGLE_SERVICE_ACCOUNT_JSON
    Additional accounts (optional, any number): GOOGLE_SERVICE_ACCOUNT_JSON_2,
    GOOGLE_SERVICE_ACCOUNT_JSON_3, ...

    Order matters -- accounts are tried in this order for Drive downloads,
    falling back to the next account when one lacks access to a file.
    """
    infos = []

    primary = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if primary:
        infos.append(json.loads(primary))

    idx = 2
    while True:
        val = os.environ.get(f"GOOGLE_SERVICE_ACCOUNT_JSON_{idx}")
        if not val:
            break
        infos.append(json.loads(val))
        idx += 1

    if not infos:
        raise ValueError(
            "No service account JSON found. Set GOOGLE_SERVICE_ACCOUNT_JSON "
            "(and optionally GOOGLE_SERVICE_ACCOUNT_JSON_2, _3, ...)."
        )
    return infos


GOOGLE_SERVICE_ACCOUNT_INFOS = load_service_account_infos()
# Primary account is used for Sheets I/O (writing results, master sheet).
GOOGLE_SERVICE_ACCOUNT_INFO = GOOGLE_SERVICE_ACCOUNT_INFOS[0]

# The sheet to process is no longer fixed by env vars -- it's picked up
# dynamically from the master "Sheet List" spreadsheet (see MASTER_* below
# and get_next_extraction_sheet()). Only USER_NAME needs to be set.
USER_NAME = os.environ.get("USER_NAME")

START_ROW = int(os.getenv("START_ROW", "2"))

# ---------------- Master sheet ("Sheet List") config ----------------
# Header row: Doc Type | Sheet Link | User | Drive Link Status | extraction User | Extarction status
MASTER_SHEET_ID = "1oNr3g2Pjpyu9u09w0lCFVT9vJwbBGn8O2rbx4kvjd88"
MASTER_SHEET_TAB = "Sheet List"

# This script tracks its own progress via the "extraction User" /
# "Extarction status" columns (E/F), separately from the Drive-link phase
# which uses "User" / "Drive Link Status" (C/D).
MASTER_EXTRACTION_USER_COL_LETTER = "E"
MASTER_EXTRACTION_USER_COL_IDX = 4
MASTER_EXTRACTION_STATUS_COL_LETTER = "F"
MASTER_EXTRACTION_STATUS_COL_IDX = 5

START_MARKERS = [
    "MEMORANDUM OF ASSOCIATION OF A COMPANY LIMITED BY SHARES"
]

END_MARKERS = [
    "Matters which are necessary for furtherance of the objects specified in clause",
    "The furtherence of the object specified in clause",
    "The Objects incidental or ancillary to the attainment of the above main objects",
    "The Objects incidental or ancillary to the attainment of the main objects",
    "Objects incidental or ancillary to the attainment of the main objects",
    "Objects incidental and ancillary to the attainment of the main objects",
    "Objects incidental to the attainment of the main objects",
    "The other objects not included in objects",
    "Objects and ancillary or",
    "Objects, ancillary or",
]

# ---- Sheet header names (must match your header row exactly) ----
CIN_HEADER = "CIN"
LE_NAME_HEADER = "LE Name"
DISPLAY_NAME_HEADER = "Display Name"
DATE_HEADER = "Date"
LINK_HEADER = "Link"                 # old Tracxn link column -- kept, not used for fetching
DRIVE_LINK_HEADER = "Drive link"     # this is what we now fetch PDFs from
EXTRACTION_HEADER = "extraction"
STATUS_HEADER = "extraction_status"

STATUS_CLAUSE_MATCHED = "clause 3a"
STATUS_FULL_EXTRACT = "Full Extract"
STATUS_NONE = ""
STATUS_SKIPPED = "Skipped - Same CIN"
STATUS_NO_LINK = "No Drive link"
STATUS_BAD_LINK = "Could not parse Drive link"

# ---- MEMORY / CONCURRENCY TUNING (env-overridable) ----
# Drive downloads and Sheets calls are I/O-bound (waiting on network), so
# these can run comfortably higher than CPU core count. PDF text extraction
# via PyMuPDF also releases the GIL during the C-level parse, so threads
# still help there too.
PREFETCH_WORKERS = int(os.getenv("PREFETCH_WORKERS", "10"))
EXTRACTION_WORKERS = int(os.getenv("EXTRACTION_WORKERS", "10"))
INPUT_BATCH_SIZE = int(os.getenv("INPUT_BATCH_SIZE", "60"))
# Overlap the next batch's Sheets-read + Drive-download with the current
# batch's extraction + Sheets-write. Set to "0" to disable and go back to
# strictly sequential batches.
PIPELINE_LOOKAHEAD = os.getenv("PIPELINE_LOOKAHEAD", "1") != "0"

SHEETS_MAX_RETRIES = 5
SHEETS_RETRY_BASE_DELAY = 5

DRIVE_MAX_RETRIES = 4
DRIVE_RETRY_BASE_DELAY = 3

# Status codes that mean "this account doesn't have access" -- move on to
# the next service account instead of burning retries on the same one.
DRIVE_PERMISSION_STATUS_CODES = {403, 404}
# -----------------------------------------

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
]
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Shared connection-pooled session for Drive downloads -- reusing TCP/TLS
# connections across requests is a meaningful speedup vs. requests.get()
# opening a fresh connection every time. Pool sized to the concurrency level.
_DRIVE_SESSION = requests.Session()
_drive_adapter = HTTPAdapter(
    pool_connections=PREFETCH_WORKERS, pool_maxsize=PREFETCH_WORKERS, max_retries=0
)
_DRIVE_SESSION.mount("https://", _drive_adapter)
_DRIVE_SESSION.mount("http://", _drive_adapter)


def call_with_retry(func, *args, **kwargs):
    last_exc = None
    for attempt in range(1, SHEETS_MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except (
                requests.exceptions.RequestException,
                OSError,
                gspread.exceptions.APIError,
        ) as e:
            last_exc = e
            if attempt == SHEETS_MAX_RETRIES:
                break
            delay = SHEETS_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            print(f"  [retry] Sheets API call failed (attempt {attempt}/"
                  f"{SHEETS_MAX_RETRIES}): {e}")
            print(f"  [retry] Retrying in {delay}s...")
            time.sleep(delay)
    raise last_exc


# ---------------- AUTH ----------------

def gspread_auth():
    creds = Credentials.from_service_account_info(GOOGLE_SERVICE_ACCOUNT_INFO, scopes=SHEETS_SCOPES)
    return gspread.authorize(creds)


def drive_credentials_list():
    """One refreshed Credentials object (readonly Drive scope) per configured
    service account, reused across threads for downloading file bytes."""
    creds_list = []
    for info in GOOGLE_SERVICE_ACCOUNT_INFOS:
        creds = Credentials.from_service_account_info(info, scopes=DRIVE_SCOPES)
        creds.refresh(google_requests.Request())
        creds_list.append(creds)
    return creds_list


def drive_auth_header(creds: Credentials) -> dict:
    # Refresh if close to/at expiry so long-running batches don't 401 midway.
    if not creds.valid:
        creds.refresh(google_requests.Request())
    return {"Authorization": f"Bearer {creds.token}"}


# ---------------- MASTER SHEET RESOLUTION ----------------

_MASTER_LINK_ID_PATTERN = r"/spreadsheets/d/([a-zA-Z0-9-_]+)"
_MASTER_LINK_GID_PATTERN = r"[#&]gid=(\d+)"


def extract_sheet_id_and_gid(url: str):
    """Extracts (spreadsheet_id, gid) from a Google Sheets URL."""
    id_match = re.search(_MASTER_LINK_ID_PATTERN, url or "")
    sheet_id = id_match.group(1) if id_match else None

    gid_match = re.search(_MASTER_LINK_GID_PATTERN, url or "")
    gid = int(gid_match.group(1)) if gid_match else 0

    return sheet_id, gid


def get_sheet_title_from_gid(client, spreadsheet_id, gid) -> str:
    """Resolves a tab's title from its gid by listing worksheets."""
    spreadsheet = call_with_retry(client.open_by_key, spreadsheet_id)
    worksheets = call_with_retry(spreadsheet.worksheets)
    for ws in worksheets:
        if ws.id == gid:
            return ws.title

    # Fallback: gid wasn't found (e.g. link had no #gid=), use the first tab.
    if worksheets:
        return worksheets[0].title

    raise ValueError(f"No tabs found in spreadsheet {spreadsheet_id}")


def get_master_worksheet(client):
    spreadsheet = call_with_retry(client.open_by_key, MASTER_SHEET_ID)
    return call_with_retry(spreadsheet.worksheet, MASTER_SHEET_TAB)


def set_master_row(client, row_number, user_col=None, status_col=None, user_name=None, status=None):
    """Writes to a master 'Sheet List' row: the given user/status column
    letters. Pass only the fields you want to update."""
    data = []
    if user_name is not None and user_col is not None:
        data.append({"range": f"{user_col}{row_number}", "values": [[user_name]]})
    if status is not None and status_col is not None:
        data.append({"range": f"{status_col}{row_number}", "values": [[status]]})
    if not data:
        return

    ws = get_master_worksheet(client)
    call_with_retry(ws.batch_update, data)


def mark_master_row_done(client, row_number, status_col):
    """Marks a master-sheet row's status column as 'Done' once processing finishes."""
    set_master_row(client, row_number, status_col=status_col, status="Done")
    print(f"  [master] Marked master row {row_number} (col {status_col}) as Done.")


def get_target_sheet_from_master(client, user_name, user_col_idx, status_col_idx,
                                  user_col_letter, status_col_letter):
    """
    Reads the master 'Sheet List' tab (Doc Type | Sheet Link | User |
    Drive Link Status | extraction User | Extarction status) and picks a
    sheet to work on, tracking progress via the given user/status columns.

    1) If a row already has this user in the user column with the status
       column == 'processing', resume that sheet.
    2) Otherwise, claim the first row with an empty user column: write
       user_name into it and 'Processing' into the status column, then use
       that row's sheet.
    3) If neither exists, return None -- no more sheets left to pick up.

    Returns (spreadsheet_id, sheet_title, master_row_number), or None.
    """
    if not user_name:
        raise ValueError("USER_NAME environment variable is not set.")

    ws = get_master_worksheet(client)
    rows = call_with_retry(ws.get, "A2:F")

    unclaimed_row_number = None

    for row_index, row in enumerate(rows):
        master_row_number = row_index + 2  # A2 is the first data row
        sheet_link = row[1] if len(row) > 1 else ""
        row_user = row[user_col_idx] if len(row) > user_col_idx else ""
        row_status = row[status_col_idx] if len(row) > status_col_idx else ""

        if (
            row_user.strip().lower() == user_name.strip().lower()
            and row_status.strip().lower() == "processing"
        ):
            spreadsheet_id, gid = extract_sheet_id_and_gid(sheet_link)
            if not spreadsheet_id:
                raise ValueError(f"Could not parse a spreadsheet ID from Sheet Link: {sheet_link}")
            sheet_title = get_sheet_title_from_gid(client, spreadsheet_id, gid)
            print(
                f"  [master] Found in-progress sheet for user '{user_name}': "
                f"{sheet_link} -> tab '{sheet_title}' (master row {master_row_number})"
            )
            return spreadsheet_id, sheet_title, master_row_number

        if unclaimed_row_number is None and not row_user.strip() and sheet_link.strip():
            unclaimed_row_number = master_row_number

    if unclaimed_row_number is not None:
        row = rows[unclaimed_row_number - 2]
        sheet_link = row[1] if len(row) > 1 else ""
        spreadsheet_id, gid = extract_sheet_id_and_gid(sheet_link)
        if not spreadsheet_id:
            raise ValueError(f"Could not parse a spreadsheet ID from Sheet Link: {sheet_link}")
        sheet_title = get_sheet_title_from_gid(client, spreadsheet_id, gid)

        set_master_row(
            client, unclaimed_row_number,
            user_col=user_col_letter, status_col=status_col_letter,
            user_name=user_name, status="Processing",
        )
        print(
            f"  [master] Assigned unclaimed master row {unclaimed_row_number} to user "
            f"'{user_name}': {sheet_link} -> tab '{sheet_title}'"
        )
        return spreadsheet_id, sheet_title, unclaimed_row_number

    return None


def get_next_extraction_sheet(client, user_name):
    """Picks/resumes a sheet tracked via 'extraction User' / 'Extarction status' (E/F)."""
    return get_target_sheet_from_master(
        client, user_name,
        user_col_idx=MASTER_EXTRACTION_USER_COL_IDX,
        status_col_idx=MASTER_EXTRACTION_STATUS_COL_IDX,
        user_col_letter=MASTER_EXTRACTION_USER_COL_LETTER,
        status_col_letter=MASTER_EXTRACTION_STATUS_COL_LETTER,
    )


# ---------------- DRIVE FILE FETCHING ----------------

_DRIVE_ID_PATTERNS = [
    r"/file/d/([a-zA-Z0-9_-]{10,})",   # https://drive.google.com/file/d/<ID>/view
    r"[?&]id=([a-zA-Z0-9_-]{10,})",    # https://drive.google.com/open?id=<ID> or uc?id=<ID>
    r"/document/d/([a-zA-Z0-9_-]{10,})",
    r"/uc\?export=download&id=([a-zA-Z0-9_-]{10,})",
]


def extract_drive_file_id(link: str) -> str:
    """Pulls the Drive file ID out of any common Google Drive share-link
    format. Falls back to treating the whole string as an ID if it already
    looks like a bare ID (no slashes/scheme)."""
    if not link:
        return ""
    for pattern in _DRIVE_ID_PATTERNS:
        match = re.search(pattern, link)
        if match:
            return match.group(1)
    if "/" not in link and "://" not in link:
        return link.strip()
    return ""


def download_drive_file_bytes(creds_list: list, file_id: str) -> bytes:
    """Tries each service account in order. If an account lacks access
    (403/404) it moves straight to the next account without wasting retries
    on the same one; transient errors still get exponential-backoff retries
    on that same account before moving on. Uses a shared pooled session for
    faster repeat connections."""
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}"
    params = {"alt": "media", "supportsAllDrives": "true"}

    last_exc = None
    for account_index, creds in enumerate(creds_list, start=1):
        for attempt in range(1, DRIVE_MAX_RETRIES + 1):
            try:
                response = _DRIVE_SESSION.get(
                    url, headers=drive_auth_header(creds), params=params, timeout=60
                )
                if response.status_code in DRIVE_PERMISSION_STATUS_CODES:
                    print(
                        f"  [drive] Account #{account_index} lacks access to "
                        f"{file_id} ({response.status_code}); trying next account."
                    )
                    last_exc = requests.exceptions.HTTPError(
                        f"{response.status_code} for file {file_id} (account #{account_index})"
                    )
                    break  # no point retrying a permission error on this account
                response.raise_for_status()
                return response.content
            except requests.exceptions.RequestException as e:
                last_exc = e
                if attempt == DRIVE_MAX_RETRIES:
                    break
                time.sleep(DRIVE_RETRY_BASE_DELAY * (2 ** (attempt - 1)))
    raise last_exc


def prefetch_drive_files(creds_list: list, links: list) -> dict:
    """Downloads bytes for each distinct Drive link in parallel, falling
    back across service accounts per-file when one lacks access.
    Returns {link: bytes} -- links that fail to parse/download are omitted."""
    results = {}
    if not links:
        return results

    def _fetch_one(link):
        file_id = extract_drive_file_id(link)
        if not file_id:
            print(f"  [drive] Could not parse a file ID from link: {link}")
            return link, None
        try:
            return link, download_drive_file_bytes(creds_list, file_id)
        except Exception as e:
            print(f"  [drive] Download failed for {link} (file_id={file_id}): {e}")
            return link, None

    with ThreadPoolExecutor(max_workers=PREFETCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_one, link) for link in links]
        for future in as_completed(futures):
            link, pdf_bytes = future.result()
            if pdf_bytes is not None:
                results[link] = pdf_bytes

    return results


# ---------------- PDF TEXT EXTRACTION ----------------

PLACEHOLDER_SIGNATURE = "If this message is not eventually replaced by the proper contents"
MIN_TEXT_CHARS_FOR_TEXT_PDF = 30
MAX_CELL_CHARS = 49500
TRUNCATION_SUFFIX = " ...[TRUNCATED -- exceeded Google Sheets 50,000 char cell limit]"


def truncate_for_sheet_cell(text: str) -> str:
    if len(text) <= MAX_CELL_CHARS:
        return text
    cutoff = MAX_CELL_CHARS - len(TRUNCATION_SUFFIX)
    return text[:cutoff] + TRUNCATION_SUFFIX


def is_unsupported_xfa_placeholder(text: str) -> bool:
    return PLACEHOLDER_SIGNATURE.lower() in text.lower()


def is_scanned_photo_pdf(text: str) -> bool:
    non_whitespace_chars = len(re.sub(r"\s+", "", text))
    return non_whitespace_chars < MIN_TEXT_CHARS_FOR_TEXT_PDF


# ---------------- OCR FALLBACK ----------------

OCR_DPI = 200
OCR_LANGUAGE = "eng"


def ocr_pdf_bytes(pdf_bytes: bytes) -> str:
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        zoom = OCR_DPI / 72
        matrix = fitz.Matrix(zoom, zoom)
        for page in doc:
            pix = page.get_pixmap(matrix=matrix, colorspace=fitz.csGRAY)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            try:
                page_text = pytesseract.image_to_string(img, lang=OCR_LANGUAGE)
            finally:
                img.close()
            text_parts.append(page_text)
            pix = None
    finally:
        doc.close()
    return "\n".join(text_parts)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """PyMuPDF text extraction -- notably faster than PyPDF2 and doesn't
    need a Python-level BytesIO wrapper."""
    text_parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        for page in doc:
            try:
                page_text = page.get_text()
                if page_text:
                    text_parts.append(page_text)
            except Exception:
                pass
    finally:
        doc.close()
    return "\n".join(text_parts)


def extract_main_objects(text: str) -> str:
    def to_flexible_pattern(marker: str) -> str:
        return r"\s+".join(re.escape(word) for word in marker.split())

    start_alternation = "|".join(to_flexible_pattern(m) for m in START_MARKERS)
    end_alternation = "|".join(to_flexible_pattern(m) for m in END_MARKERS)

    pattern = re.compile(
        rf"(?:{start_alternation})\s*(.*?)\s*(?:{end_alternation})",
        re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def build_extraction_result(raw_text: str) -> tuple:
    objects_text = extract_main_objects(raw_text)
    if objects_text:
        return objects_text, STATUS_CLAUSE_MATCHED

    full_text = re.sub(r"\s+", " ", raw_text).strip()
    return full_text, STATUS_FULL_EXTRACT


def extract_doc_text(pdf_bytes: bytes) -> tuple:
    pdf_text = extract_pdf_text(pdf_bytes)

    if is_unsupported_xfa_placeholder(pdf_text):
        return "Content not available", STATUS_NONE

    if is_scanned_photo_pdf(pdf_text):
        try:
            ocr_text = ocr_pdf_bytes(pdf_bytes)
        except Exception:
            return "Photo pdf", STATUS_NONE

        if is_scanned_photo_pdf(ocr_text):
            return "Photo pdf", STATUS_NONE
        return build_extraction_result(ocr_text)

    return build_extraction_result(pdf_text)


# ---------------- UTILS ----------------

def parse_header(header_row):
    indices = {}
    for i, col in enumerate(header_row):
        name = col.strip()
        if name == CIN_HEADER and CIN_HEADER not in indices:
            indices[CIN_HEADER] = i
        elif name == LE_NAME_HEADER and LE_NAME_HEADER not in indices:
            indices[LE_NAME_HEADER] = i
        elif name == DISPLAY_NAME_HEADER and DISPLAY_NAME_HEADER not in indices:
            indices[DISPLAY_NAME_HEADER] = i
        elif name == DATE_HEADER and DATE_HEADER not in indices:
            indices[DATE_HEADER] = i
        elif name == LINK_HEADER and LINK_HEADER not in indices:
            indices[LINK_HEADER] = i
        elif name == DRIVE_LINK_HEADER and DRIVE_LINK_HEADER not in indices:
            indices[DRIVE_LINK_HEADER] = i
        elif name == EXTRACTION_HEADER and EXTRACTION_HEADER not in indices:
            indices[EXTRACTION_HEADER] = i
        elif name == STATUS_HEADER and STATUS_HEADER not in indices:
            indices[STATUS_HEADER] = i

    required = [CIN_HEADER, DRIVE_LINK_HEADER, EXTRACTION_HEADER, STATUS_HEADER]
    missing = [h for h in required if h not in indices]
    if missing:
        raise ValueError(f"Could not find column(s) {missing} in header row.")
    return indices


def safe_get(row, idx):
    return row[idx].strip() if idx is not None and idx < len(row) and row[idx] else ""


def col_num_to_letter(n: int) -> str:
    letters = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        letters = string.ascii_uppercase[remainder] + letters
    return letters


def write_column_values(input_sheet, batch_start, batch_end, col_idx,
                        chunk_rows, new_values_by_row, label):
    col_letter = col_num_to_letter(col_idx + 1)
    values = []
    for offset, sheet_row in enumerate(range(batch_start, batch_end + 1)):
        if sheet_row in new_values_by_row:
            values.append([new_values_by_row[sheet_row]])
        else:
            existing = safe_get(chunk_rows[offset], col_idx) if offset < len(chunk_rows) else ""
            values.append([existing])

    range_name = f"{col_letter}{batch_start}:{col_letter}{batch_end}"
    call_with_retry(
        input_sheet.update,
        range_name=range_name, values=values, value_input_option="RAW",
    )
    print(f"  --> Wrote {len(new_values_by_row)} {label} result(s) to column {col_letter} ({batch_start}-{batch_end})")


# ---------------- MAIN ----------------

def _prepare_batch(input_sheet, last_col_letter, batch_start, batch_end,
                    cin_idx, drive_link_idx, extraction_idx, status_idx,
                    successful_cins, drive_creds_list):
    """Sheets-read + row triage + Drive prefetch for one batch. Safe to run
    on a background thread while a previous batch's extraction/write is
    still in progress -- it only reads `successful_cins`, never mutates it."""
    range_name = f"A{batch_start}:{last_col_letter}{batch_end}"
    chunk_rows = call_with_retry(input_sheet.get, range_name)

    if not chunk_rows:
        return {"chunk_rows": [], "any_data_in_chunk": False}

    row_infos = []
    rows_to_skip = []
    unique_links = set()
    any_data_in_chunk = False
    skipped_already_processed = 0
    skipped_no_link = 0
    newly_confirmed_cins = set()

    for offset, row in enumerate(chunk_rows):
        sheet_row_number = batch_start + offset
        cin = safe_get(row, cin_idx)
        if not cin:
            continue
        any_data_in_chunk = True

        status_val = safe_get(row, status_idx)
        extracted_val = safe_get(row, extraction_idx)

        if status_val == STATUS_CLAUSE_MATCHED:
            newly_confirmed_cins.add(cin)
            skipped_already_processed += 1
            continue

        if cin in successful_cins or cin in newly_confirmed_cins:
            if extracted_val != "Skipped":
                rows_to_skip.append((sheet_row_number, cin))
            else:
                skipped_already_processed += 1
            continue

        if bool(extracted_val):
            skipped_already_processed += 1
            continue

        drive_link = safe_get(row, drive_link_idx)
        if not drive_link:
            skipped_no_link += 1
            continue

        row_infos.append((sheet_row_number, cin, drive_link))
        unique_links.add(drive_link)

    prefetch_cache = prefetch_drive_files(drive_creds_list, unique_links) if unique_links else {}

    return {
        "chunk_rows": chunk_rows,
        "any_data_in_chunk": any_data_in_chunk,
        "row_infos": row_infos,
        "rows_to_skip": rows_to_skip,
        "unique_links": unique_links,
        "newly_confirmed_cins": newly_confirmed_cins,
        "skipped_already_processed": skipped_already_processed,
        "skipped_no_link": skipped_no_link,
        "prefetch_cache": prefetch_cache,
    }


def process_sheet(client, drive_creds_list, spreadsheet_id, sheet_name, master_row):
    """Runs the full extraction pass for one sheet, then marks its master
    row Done via the extraction status column."""
    spreadsheet = call_with_retry(client.open_by_key, spreadsheet_id)
    input_sheet = call_with_retry(spreadsheet.worksheet, sheet_name)

    header_row = call_with_retry(input_sheet.row_values, 1)
    if not header_row:
        print("Input sheet has no header row.")
        return

    indices = parse_header(header_row)
    cin_idx = indices[CIN_HEADER]
    drive_link_idx = indices[DRIVE_LINK_HEADER]
    extraction_idx = indices[EXTRACTION_HEADER]
    status_idx = indices[STATUS_HEADER]

    last_col_letter = col_num_to_letter(max(indices.values()) + 1)
    total_rows_in_sheet = input_sheet.row_count

    # Pre-scan the sheet to build a set of CINs that ALREADY have clause 3a
    successful_cins = set()
    print("Pre-scanning sheet to map already successful CINs...")
    try:
        all_cins = call_with_retry(input_sheet.col_values, cin_idx + 1)
        all_statuses = call_with_retry(input_sheet.col_values, status_idx + 1)
        for c, s in zip(all_cins[1:], all_statuses[1:]):  # skip header
            if s.strip() == STATUS_CLAUSE_MATCHED:
                successful_cins.add(c.strip())
        print(f"Found {len(successful_cins)} CINs that already have '{STATUS_CLAUSE_MATCHED}'.")
        del all_cins, all_statuses
    except Exception as e:
        print(f"Warning: Initial status pre-scan failed ({e}). Proceeding without seeding memory.")

    print(f"\n--- Starting processing from row {START_ROW} ---")

    lookahead_pool = ThreadPoolExecutor(max_workers=1) if PIPELINE_LOOKAHEAD else None

    def kickoff(batch_start, batch_end):
        args = (input_sheet, last_col_letter, batch_start, batch_end,
                cin_idx, drive_link_idx, extraction_idx, status_idx,
                successful_cins, drive_creds_list)
        if lookahead_pool is not None:
            return lookahead_pool.submit(_prepare_batch, *args)
        return _prepare_batch(*args)

    def resolve(fut_or_result):
        return fut_or_result.result() if lookahead_pool is not None else fut_or_result

    try:
        current_row = START_ROW
        pending = kickoff(current_row, min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet))

        while current_row <= total_rows_in_sheet:
            batch_start = current_row
            batch_end = min(current_row + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)

            batch = resolve(pending)

            # Kick off the next batch's read+prefetch immediately so it runs
            # concurrently with this batch's extraction/write below.
            next_start = batch_end + 1
            if next_start <= total_rows_in_sheet:
                next_end = min(next_start + INPUT_BATCH_SIZE - 1, total_rows_in_sheet)
                pending = kickoff(next_start, next_end)
            else:
                pending = None

            if not batch["any_data_in_chunk"]:
                print("  No CIN values found in this batch -- stopping.")
                break

            chunk_rows = batch["chunk_rows"]
            row_infos = batch["row_infos"]
            rows_to_skip = batch["rows_to_skip"]
            prefetch_cache = batch["prefetch_cache"]
            successful_cins |= batch["newly_confirmed_cins"]

            print(f"\n=== Batch: input rows {batch_start}-{batch_end} ({len(chunk_rows)} fetched) ===")
            if batch["skipped_already_processed"]:
                print(f"  Skipping {batch['skipped_already_processed']} row(s) already processed.")
            if rows_to_skip:
                print(f"  Skipping {len(rows_to_skip)} row(s) because their CIN already has clause 3a.")
            if batch["skipped_no_link"]:
                print(f"  Skipping {batch['skipped_no_link']} row(s) with no Drive link value.")

            if not row_infos and not rows_to_skip:
                print("  Nothing new to process or mark in this batch.")
                current_row = batch_end + 1
                del chunk_rows, row_infos, rows_to_skip, prefetch_cache, batch
                gc.collect()
                continue

            extracted_result_cache = {}
            unique_links = batch["unique_links"]
            unresolved_links = unique_links - set(prefetch_cache.keys())
            for link in unresolved_links:
                extracted_result_cache[link] = (
                    STATUS_BAD_LINK if not extract_drive_file_id(link) else "Content not available",
                    STATUS_NONE,
                )

            docs_to_extract = sorted(link for link in prefetch_cache.keys())
            if docs_to_extract:
                with ThreadPoolExecutor(max_workers=EXTRACTION_WORKERS) as executor:
                    future_to_link = {
                        executor.submit(extract_doc_text, prefetch_cache[link]): link
                        for link in docs_to_extract
                    }
                    for future in as_completed(future_to_link):
                        link = future_to_link[future]
                        try:
                            extract_text, status_value = future.result()
                        except Exception as e:
                            print(f"  [extract] Extraction failed for {link}: {e}")
                            extract_text, status_value = "Content not available", STATUS_NONE
                        extracted_result_cache[link] = (extract_text, status_value)

            prefetch_cache.clear()

            new_extraction_values = {}
            new_status_values = {}

            for sheet_row_number, cin in rows_to_skip:
                new_extraction_values[sheet_row_number] = "Skipped"
                new_status_values[sheet_row_number] = STATUS_SKIPPED

            for sheet_row_number, cin, drive_link in row_infos:
                if cin in successful_cins:
                    new_extraction_values[sheet_row_number] = "Skipped"
                    new_status_values[sheet_row_number] = STATUS_SKIPPED
                    continue

                extract_text, status_value = extracted_result_cache.get(
                    drive_link, ("Content not available", STATUS_NONE)
                )
                new_extraction_values[sheet_row_number] = truncate_for_sheet_cell(extract_text)
                new_status_values[sheet_row_number] = status_value

                if status_value == STATUS_CLAUSE_MATCHED:
                    successful_cins.add(cin)

            write_column_values(
                input_sheet, batch_start, batch_end, extraction_idx,
                chunk_rows, new_extraction_values, EXTRACTION_HEADER,
            )
            write_column_values(
                input_sheet, batch_start, batch_end, status_idx,
                chunk_rows, new_status_values, STATUS_HEADER,
            )

            del (
                chunk_rows, row_infos, rows_to_skip, unique_links,
                extracted_result_cache, new_extraction_values, new_status_values,
                prefetch_cache, batch,
            )
            gc.collect()

            current_row = batch_end + 1
    finally:
        if lookahead_pool is not None:
            lookahead_pool.shutdown(wait=False, cancel_futures=True)

    print(f"\nSheet '{sheet_name}' done.")
    mark_master_row_done(client, master_row, MASTER_EXTRACTION_STATUS_COL_LETTER)


def main():
    client = gspread_auth()
    drive_creds_list = drive_credentials_list()
    print(f"Loaded {len(drive_creds_list)} Drive service account(s) for fallback.")

    while True:
        print(f"\nLooking up sheet for user '{USER_NAME}'...")
        target = get_next_extraction_sheet(client, USER_NAME)
        if not target:
            print(
                f"No in-progress row for extraction User='{USER_NAME}' and no unclaimed "
                f"row available in '{MASTER_SHEET_TAB}'. All done."
            )
            break

        spreadsheet_id, sheet_name, master_row = target
        print(f"Target sheet: {spreadsheet_id} (tab: {sheet_name})")

        process_sheet(client, drive_creds_list, spreadsheet_id, sheet_name, master_row)
        # Loop back around to pick up the next sheet for this user.


if __name__ == "__main__":
    main()